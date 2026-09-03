// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! v2 `SubscriptionService`: filtered, real-time streams of checkpoints,
//! transactions, and events.
//!
//! Each Subscribe API pairs with the LedgerService List API of the same name:
//! requests take the same DNF filter message and responses use identical cursor
//! semantics, so clients can replay subscription gaps with the paired List API.
//!
//! A subscription behaves like an unbounded ascending scan. Every transaction
//! and event frame carries a `watermark`; its payload is optional. Every
//! checkpoint frame carries a scalar `cursor`; its checkpoint payload is
//! optional, and progress-only checkpoint frames occur only on filtered
//! streams. At non-genesis entry, transaction and event streams and filtered
//! checkpoint streams begin with a progress-only frame establishing the safe
//! position immediately before entry. Unfiltered checkpoint streams begin with
//! a checkpoint payload. Further progress-only frames are emitted when a stream
//! advances a configured number of checkpoints without a matching item (see
//! `RpcConfig::subscription_watermark_interval`). Streams have no successful
//! end: when the subscription actor drops a subscriber (lag or backpressure),
//! the stream simply closes and the client reconnects, backfilling via List.

use std::time::{Duration, Instant};

use mysten_common::ZipDebugEqIteratorExt;
use sui_inverted_index::BitmapQuery;
use sui_rpc::field::FieldMaskTree;
use sui_rpc::merge::Merge;
use sui_rpc::proto::sui::rpc::v2::Checkpoint;
use sui_rpc::proto::sui::rpc::v2::Event as ProtoEvent;
use sui_rpc::proto::sui::rpc::v2::EventFilter;
use sui_rpc::proto::sui::rpc::v2::ExecutedTransaction;
use sui_rpc::proto::sui::rpc::v2::ObjectSet as ProtoObjectSet;
use sui_rpc::proto::sui::rpc::v2::SubscribeCheckpointsRequest;
use sui_rpc::proto::sui::rpc::v2::SubscribeCheckpointsResponse;
use sui_rpc::proto::sui::rpc::v2::SubscribeEventsRequest;
use sui_rpc::proto::sui::rpc::v2::SubscribeEventsResponse;
use sui_rpc::proto::sui::rpc::v2::SubscribeTransactionsRequest;
use sui_rpc::proto::sui::rpc::v2::SubscribeTransactionsResponse;
use sui_rpc::proto::sui::rpc::v2::TransactionFilter;
use sui_rpc::proto::sui::rpc::v2::subscription_service_server::SubscriptionService;
use sui_rpc_cursor::Position;
use sui_types::balance_change::derive_balance_changes_2;
use sui_types::effects::TransactionEffectsAPI;
use tokio::sync::mpsc;
use tonic::codegen::BoxStream;

use crate::RpcError;
use crate::RpcService;
use crate::ledger_history::filter::event_filter_to_query;
use crate::ledger_history::filter::transaction_filter_to_query;
use crate::ledger_history::query_options::QueryOptions;
use crate::ledger_history::watermark::advance_covered_bound_before_checkpoint;
use crate::ledger_history::watermark::boundary_watermark;
use crate::ledger_history::watermark::item_watermark;
use crate::ledger_history::watermark::merge_covered_checkpoint_bound;
use crate::metrics::SubscriptionFrameKind;
use crate::metrics::SubscriptionStreamMetrics;
use crate::read_mask_defaults;
use crate::subscription::SubscriptionKind;
use crate::subscription::SubscriptionSpec;
use crate::subscription::SubscriptionUpdate;

const CAUSAL_STREAM_HEADER: &str = "x-sui-causal-stream";
const CAUSAL_STREAM_HEARTBEAT: Duration = Duration::from_secs(10);

fn requests_causal_stream(metadata: &tonic::metadata::MetadataMap) -> Result<bool, tonic::Status> {
    match metadata.get(CAUSAL_STREAM_HEADER) {
        None => Ok(false),
        Some(value) if value == "true" => Ok(true),
        Some(_) => Err(tonic::Status::invalid_argument(
            "invalid causal stream metadata",
        )),
    }
}

fn validate_causal_stream_request(
    request: &SubscribeTransactionsRequest,
) -> Result<(), tonic::Status> {
    if request.filter.is_some() {
        return Err(tonic::Status::invalid_argument(
            "causal finality streams do not support transaction filters",
        ));
    }
    let Some(mask) = request.read_mask.as_ref() else {
        return Err(tonic::Status::invalid_argument(
            "causal finality streams require an explicit read mask",
        ));
    };
    for path in &mask.paths {
        let root = path.split('.').next().unwrap_or_default();
        if !matches!(root, "digest" | "effects" | "events" | "objects") {
            return Err(tonic::Status::invalid_argument(format!(
                "causal finality stream cannot provide '{path}'"
            )));
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl SubscriptionService for RpcService {
    async fn subscribe_checkpoints(
        &self,
        request: tonic::Request<SubscribeCheckpointsRequest>,
    ) -> Result<tonic::Response<BoxStream<SubscribeCheckpointsResponse>>, tonic::Status> {
        let request = request.into_inner();
        let read_mask = read_mask_defaults::validate_read_mask::<Checkpoint>(
            request.read_mask,
            read_mask_defaults::CHECKPOINT,
        )?;
        let query = compile_transaction_filter(self, request.filter.as_ref())?;
        let (mut receiver, stream_metrics) = register(
            self,
            SubscriptionSpec {
                kind: SubscriptionKind::Checkpoints,
                query,
            },
        )
        .await?;

        let response = Box::pin(async_stream::stream! {
            while let Some(update) = receiver.recv().await {
                let mut response = SubscribeCheckpointsResponse::default();
                let frame_kind = match update {
                    SubscriptionUpdate::Matched(matched) => {
                        let cp = matched.checkpoint.summary.sequence_number;
                        response.cursor = Some(cp);
                        response.checkpoint =
                            Some(render_checkpoint_message(&matched.checkpoint, &read_mask));
                        SubscriptionFrameKind::Payload
                    }
                    SubscriptionUpdate::WatermarkTick { checkpoint: cp, .. } => {
                        response.cursor = Some(cp);
                        SubscriptionFrameKind::Watermark
                    }
                };
                stream_metrics.observe_frame(&response, frame_kind);
                let yielded_at = Instant::now();
                yield Ok(response);
                stream_metrics.observe_yield_wait(yielded_at.elapsed());
            }
        });

        Ok(tonic::Response::new(response))
    }

    async fn subscribe_transactions(
        &self,
        request: tonic::Request<SubscribeTransactionsRequest>,
    ) -> Result<tonic::Response<BoxStream<SubscribeTransactionsResponse>>, tonic::Status> {
        let causal_stream = requests_causal_stream(request.metadata())?;
        if causal_stream {
            validate_causal_stream_request(request.get_ref())?;
        }
        let request = request.into_inner();
        let read_mask = read_mask_defaults::validate_read_mask::<ExecutedTransaction>(
            request.read_mask,
            read_mask_defaults::TRANSACTION,
        )?;
        if causal_stream {
            let executor = self
                .executor
                .clone()
                .ok_or_else(|| tonic::Status::unimplemented("no transaction executor"))?;
            let service = self.clone();
            let mut receiver = self.subscribe_causal_finality();
            let response = Box::pin(async_stream::stream! {
                let mut heartbeat = tokio::time::interval(CAUSAL_STREAM_HEARTBEAT);
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            // An empty frame distinguishes a healthy idle stream from a
                            // transport that has stopped delivering response bodies.
                            yield Ok(SubscribeTransactionsResponse::default());
                        }
                        received = receiver.recv() => {
                            let digest = match received {
                                Ok(digest) => digest,
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                    tracing::warn!(skipped, "Causal finality subscriber lagged; checkpoint replay remains authoritative");
                                    continue;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            };
                            let Some(receipt) = executor.retained_causal_transaction(&digest) else {
                                continue;
                            };
                            let mut response = SubscribeTransactionsResponse::default();
                            response.transaction = Some(render_causal_transaction(
                                &service,
                                receipt,
                                &read_mask,
                            ));
                            yield Ok(response);
                        }
                    }
                }
            });
            let mut response = tonic::Response::new(response as BoxStream<_>);
            response.metadata_mut().insert(
                CAUSAL_STREAM_HEADER,
                tonic::metadata::MetadataValue::from_static("true"),
            );
            return Ok(response);
        }
        let query = compile_transaction_filter(self, request.filter.as_ref())?;
        let (mut receiver, stream_metrics) = register(
            self,
            SubscriptionSpec {
                kind: SubscriptionKind::Transactions,
                query,
            },
        )
        .await?;

        let service = self.clone();
        let options = QueryOptions::subscription();
        let response = Box::pin(async_stream::stream! {
            let mut boundary: Option<u64> = None;
            let mut entry_checkpoint = None;
            while let Some(update) = receiver.recv().await {
                match update {
                    SubscriptionUpdate::Matched(matched) => {
                        let checkpoint = &matched.checkpoint;
                        let Some(indices) = matched
                            .matches
                            .transaction_indices(checkpoint.transactions.len() as u32)
                        else {
                            continue;
                        };
                        let cp = checkpoint.summary.sequence_number;
                        let entry_checkpoint = *entry_checkpoint.get_or_insert(cp);
                        let tx_hi = checkpoint.summary.data().network_total_transactions;
                        let tx_lo = tx_hi - checkpoint.transactions.len() as u64;
                        for i in indices {
                            let tx_seq = tx_lo + i as u64;
                            // An item never proves its own checkpoint
                            // complete (list_transactions semantics).
                            boundary = advance_covered_bound_before_checkpoint(
                                boundary,
                                cp,
                                entry_checkpoint,
                                &options,
                            );
                            let mut response = SubscribeTransactionsResponse::default();
                            response.transaction = Some(render_transaction_message(
                                &service,
                                checkpoint,
                                i as usize,
                                &read_mask,
                            ));
                            response.watermark = Some(item_watermark(
                                Position::Transactions {
                                    checkpoint: cp,
                                    tx_seq,
                                },
                                boundary,
                            ));
                            stream_metrics.observe_frame(
                                &response,
                                SubscriptionFrameKind::Payload,
                            );
                            let yielded_at = Instant::now();
                            yield Ok(response);
                            stream_metrics.observe_yield_wait(yielded_at.elapsed());
                        }
                    }
                    SubscriptionUpdate::WatermarkTick { checkpoint: cp, tx_hi } => {
                        record_watermark_tick_coverage(
                            &mut boundary,
                            &mut entry_checkpoint,
                            cp,
                            &options,
                        );
                        let mut response = SubscribeTransactionsResponse::default();
                        response.watermark = Some(boundary_watermark(
                            Position::Transactions {
                                checkpoint: cp + 1,
                                tx_seq: tx_hi,
                            },
                            boundary,
                        ));
                        stream_metrics.observe_frame(
                            &response,
                            SubscriptionFrameKind::Watermark,
                        );
                        let yielded_at = Instant::now();
                        yield Ok(response);
                        stream_metrics.observe_yield_wait(yielded_at.elapsed());
                    }
                }
            }
        });

        Ok(tonic::Response::new(response))
    }

    async fn subscribe_events(
        &self,
        request: tonic::Request<SubscribeEventsRequest>,
    ) -> Result<tonic::Response<BoxStream<SubscribeEventsResponse>>, tonic::Status> {
        let request = request.into_inner();
        let read_mask = read_mask_defaults::validate_read_mask::<ProtoEvent>(
            request.read_mask,
            read_mask_defaults::EVENT,
        )?;
        let query = compile_event_filter(self, request.filter.as_ref())?;
        let (mut receiver, stream_metrics) = register(
            self,
            SubscriptionSpec {
                kind: SubscriptionKind::Events,
                query,
            },
        )
        .await?;

        let service = self.clone();
        let options = QueryOptions::subscription();
        let response = Box::pin(async_stream::stream! {
            let mut boundary: Option<u64> = None;
            let mut entry_checkpoint = None;
            while let Some(update) = receiver.recv().await {
                match update {
                    SubscriptionUpdate::Matched(matched) => {
                        let checkpoint = &matched.checkpoint;
                        let Some(pairs) = matched.matches.event_indices(checkpoint) else {
                            continue;
                        };
                        let cp = checkpoint.summary.sequence_number;
                        let entry_checkpoint = *entry_checkpoint.get_or_insert(cp);
                        let tx_hi = checkpoint.summary.data().network_total_transactions;
                        let tx_lo = tx_hi - checkpoint.transactions.len() as u64;
                        for (tx_idx, ev) in pairs {
                            let tx = &checkpoint.transactions[tx_idx as usize];
                            let tx_seq = tx_lo + tx_idx as u64;
                            boundary = advance_covered_bound_before_checkpoint(
                                boundary,
                                cp,
                                entry_checkpoint,
                                &options,
                            );
                            let event = &tx
                                .events
                                .as_ref()
                                .expect("matched event implies events")
                                .data[ev as usize];
                            let mut proto_event = service.render_event_to_proto(
                                event,
                                &read_mask,
                                &checkpoint.object_set,
                            );
                            if read_mask.contains(ProtoEvent::CHECKPOINT_FIELD.name) {
                                proto_event.checkpoint = Some(cp);
                            }
                            if read_mask.contains(ProtoEvent::TRANSACTION_DIGEST_FIELD.name) {
                                proto_event.transaction_digest =
                                    Some(tx.effects.transaction_digest().base58_encode());
                            }
                            if read_mask.contains(ProtoEvent::TRANSACTION_INDEX_FIELD.name) {
                                proto_event.transaction_index = Some(tx_idx as u64);
                            }
                            if read_mask.contains(ProtoEvent::EVENT_INDEX_FIELD.name) {
                                proto_event.event_index = Some(ev);
                            }
                            let mut response = SubscribeEventsResponse::default();
                            response.event = Some(proto_event);
                            response.watermark = Some(item_watermark(
                                Position::Events {
                                    checkpoint: cp,
                                    tx_seq,
                                    event_index: ev,
                                },
                                boundary,
                            ));
                            stream_metrics.observe_frame(
                                &response,
                                SubscriptionFrameKind::Payload,
                            );
                            let yielded_at = Instant::now();
                            yield Ok(response);
                            stream_metrics.observe_yield_wait(yielded_at.elapsed());
                        }
                    }
                    SubscriptionUpdate::WatermarkTick { checkpoint: cp, tx_hi } => {
                        record_watermark_tick_coverage(
                            &mut boundary,
                            &mut entry_checkpoint,
                            cp,
                            &options,
                        );
                        let mut response = SubscribeEventsResponse::default();
                        response.watermark = Some(boundary_watermark(
                            Position::Events {
                                checkpoint: cp + 1,
                                tx_seq: tx_hi,
                                event_index: 0,
                            },
                            boundary,
                        ));
                        stream_metrics.observe_frame(
                            &response,
                            SubscriptionFrameKind::Watermark,
                        );
                        let yielded_at = Instant::now();
                        yield Ok(response);
                        stream_metrics.observe_yield_wait(yielded_at.elapsed());
                    }
                }
            }
        });

        Ok(tonic::Response::new(response))
    }
}

/// Render one retained causal receipt without claiming checkpoint inclusion.
fn render_causal_transaction(
    service: &RpcService,
    receipt: sui_types::transaction_driver_types::ExecuteTransactionResponseV3,
    read_mask: &FieldMaskTree,
) -> ExecutedTransaction {
    let effects = receipt.effects.data();
    let mut objects = sui_types::full_checkpoint_content::ObjectSet::default();
    for object in receipt.output_objects.into_iter().flatten() {
        objects.insert(object);
    }

    let events = read_mask
        .subtree(ExecutedTransaction::EVENTS_FIELD)
        .and_then(|mask| {
            receipt
                .events
                .map(|events| service.render_events_to_proto(&events, &mask, &objects))
        });
    let rendered_effects = read_mask
        .subtree(ExecutedTransaction::EFFECTS_FIELD)
        .map(|mask| service.render_effects_to_proto(effects, &[], &objects, &mask));

    let mut transaction = ExecutedTransaction::default();
    transaction.digest = read_mask
        .contains(ExecutedTransaction::DIGEST_FIELD)
        .then(|| effects.transaction_digest().to_string());
    transaction.effects = rendered_effects;
    transaction.events = events;
    transaction.objects = read_mask
        .subtree(
            ExecutedTransaction::path_builder()
                .objects()
                .objects()
                .finish(),
        )
        .map(|mask| {
            ProtoObjectSet::default().with_objects(
                objects
                    .iter()
                    .map(|object| service.render_object_to_proto(object, &mask, &objects))
                    .collect(),
            )
        });
    transaction
}

/// Render a full `Checkpoint` message from live executed-checkpoint data,
/// including the `transactions.balance_changes` special case that
/// `merge_from` cannot fill (it needs the checkpoint's `ObjectSet`).
fn render_checkpoint_message(
    checkpoint: &sui_types::full_checkpoint_content::Checkpoint,
    read_mask: &FieldMaskTree,
) -> Checkpoint {
    let mut checkpoint_message = Checkpoint::merge_from(checkpoint, read_mask);

    if read_mask.contains("transactions.balance_changes") {
        for (txn, effects) in checkpoint_message
            .transactions_mut()
            .iter_mut()
            .zip_debug_eq(checkpoint.transactions.iter().map(|t| &t.effects))
        {
            *txn.balance_changes_mut() = derive_balance_changes_2(effects, &checkpoint.object_set)
                .into_iter()
                .map(Into::into)
                .collect();
        }
    }

    checkpoint_message
}

fn transaction_objects<'a>(
    transaction: &'a sui_types::full_checkpoint_content::ExecutedTransaction,
    checkpoint_objects: &'a sui_types::full_checkpoint_content::ObjectSet,
) -> impl Iterator<Item = &'a sui_types::object::Object> + 'a {
    sui_types::storage::get_transaction_object_set(
        &transaction.transaction,
        &transaction.effects,
        &transaction.unchanged_loaded_runtime_objects,
    )
    .into_iter()
    .filter_map(move |key| checkpoint_objects.get(&key))
}

/// Render one `ExecutedTransaction` message from live executed-checkpoint
/// data. The nested-transaction `merge_from` does not set `checkpoint` /
/// `timestamp` (the checkpoint-level merge does), so set them here, along
/// with `balance_changes` which needs the checkpoint's `ObjectSet`.
fn render_transaction_message(
    service: &RpcService,
    checkpoint: &sui_types::full_checkpoint_content::Checkpoint,
    index: usize,
    read_mask: &FieldMaskTree,
) -> ExecutedTransaction {
    let tx = &checkpoint.transactions[index];
    let mut message = ExecutedTransaction::merge_from(tx, read_mask);

    if let Some(object_mask) = read_mask
        .subtree(ExecutedTransaction::OBJECTS_FIELD)
        .and_then(|submask| submask.subtree(ProtoObjectSet::OBJECTS_FIELD))
    {
        message.objects = Some(
            ProtoObjectSet::default().with_objects(
                transaction_objects(tx, &checkpoint.object_set)
                    .map(|object| {
                        service.render_object_to_proto(object, &object_mask, &checkpoint.object_set)
                    })
                    .collect(),
            ),
        );
    }

    if read_mask.contains(ExecutedTransaction::CHECKPOINT_FIELD) {
        message.checkpoint = Some(checkpoint.summary.sequence_number);
    }
    if read_mask.contains(ExecutedTransaction::TIMESTAMP_FIELD) {
        message.timestamp = Some(sui_rpc::proto::timestamp_ms_to_proto(
            checkpoint.summary.timestamp_ms,
        ));
    }
    if read_mask.contains(ExecutedTransaction::BALANCE_CHANGES_FIELD) {
        message.balance_changes = derive_balance_changes_2(&tx.effects, &checkpoint.object_set)
            .into_iter()
            .map(Into::into)
            .collect();
    }
    if read_mask.contains(ExecutedTransaction::TRANSACTION_INDEX_FIELD) {
        message.transaction_index = Some(index as u64);
    }

    message
}

/// Fold a subscription progress tick into the stream's checkpoint coverage.
/// A subscription's first tick names the checkpoint immediately before its
/// entry checkpoint; later ticks name checkpoints the actor fully processed.
/// Both establish a safe covered boundary.
fn record_watermark_tick_coverage(
    covered_checkpoint_bound: &mut Option<u64>,
    entry_checkpoint: &mut Option<u64>,
    checkpoint: u64,
    options: &QueryOptions,
) {
    if entry_checkpoint.is_none() {
        *entry_checkpoint = Some(checkpoint.saturating_add(1));
    }
    *covered_checkpoint_bound =
        merge_covered_checkpoint_bound(*covered_checkpoint_bound, checkpoint, options);
}

fn compile_transaction_filter(
    service: &RpcService,
    filter: Option<&TransactionFilter>,
) -> Result<Option<BitmapQuery>, RpcError> {
    let max_literals = service.config.ledger_history().max_bitmap_filter_literals();
    filter
        .map(|filter| transaction_filter_to_query(filter, max_literals))
        .transpose()
}

fn compile_event_filter(
    service: &RpcService,
    filter: Option<&EventFilter>,
) -> Result<Option<BitmapQuery>, RpcError> {
    let max_literals = service.config.ledger_history().max_bitmap_filter_literals();
    filter
        .map(|filter| event_filter_to_query(filter, max_literals))
        .transpose()
}

async fn register(
    service: &RpcService,
    spec: SubscriptionSpec,
) -> Result<
    (
        mpsc::Receiver<SubscriptionUpdate>,
        SubscriptionStreamMetrics,
    ),
    tonic::Status,
> {
    let handle = service
        .subscription_service_handle
        .as_ref()
        .ok_or_else(|| tonic::Status::unimplemented("subscription service not enabled"))?;
    let kind = spec.kind;
    let receiver = handle
        .register_subscription(spec)
        .await
        .ok_or_else(|| tonic::Status::unavailable("subscription service is unavailable"))?;
    let stream_metrics = handle.stream_metrics(kind);
    Ok((receiver, stream_metrics))
}

#[cfg(test)]
mod tests {
    use sui_types::test_checkpoint_data_builder::TestCheckpointBuilder;

    use super::*;

    #[test]
    fn causal_stream_metadata_is_explicit_and_strict() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        assert!(!requests_causal_stream(&metadata).unwrap());

        metadata.insert(
            CAUSAL_STREAM_HEADER,
            tonic::metadata::MetadataValue::from_static("true"),
        );
        assert!(requests_causal_stream(&metadata).unwrap());

        metadata.insert(
            CAUSAL_STREAM_HEADER,
            tonic::metadata::MetadataValue::from_static("false"),
        );
        assert_eq!(
            requests_causal_stream(&metadata).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn causal_stream_accepts_only_receipt_fields() {
        let mut request = SubscribeTransactionsRequest::default();
        assert_eq!(
            validate_causal_stream_request(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        request.read_mask = Some(prost_types::FieldMask {
            paths: vec![
                "digest".to_owned(),
                "effects.bcs".to_owned(),
                "events.events".to_owned(),
                "objects.objects.contents".to_owned(),
            ],
        });
        validate_causal_stream_request(&request).unwrap();

        request.read_mask.as_mut().unwrap().paths = vec!["checkpoint".to_owned()];
        assert_eq!(
            validate_causal_stream_request(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        request.read_mask.as_mut().unwrap().paths = vec!["digest".to_owned()];
        request.filter = Some(TransactionFilter::default());
        assert_eq!(
            validate_causal_stream_request(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn initial_watermark_tick_covers_checkpoint_before_entry() {
        let options = QueryOptions::subscription();
        let mut covered_checkpoint_bound = None;
        let mut entry_checkpoint = None;

        record_watermark_tick_coverage(
            &mut covered_checkpoint_bound,
            &mut entry_checkpoint,
            41,
            &options,
        );
        let watermark = boundary_watermark(
            Position::Transactions {
                checkpoint: 42,
                tx_seq: 100,
            },
            covered_checkpoint_bound,
        );

        assert_eq!(entry_checkpoint, Some(42));
        assert_eq!(watermark.checkpoint, Some(41));
    }

    #[test]
    fn transaction_objects_exclude_checkpoint_siblings() {
        let checkpoint = TestCheckpointBuilder::new(1)
            .start_transaction(0)
            .create_owned_object(10)
            .finish_transaction()
            .start_transaction(1)
            .create_owned_object(20)
            .finish_transaction()
            .build_checkpoint();
        let first_object = TestCheckpointBuilder::derive_object_id(10);
        let second_object = TestCheckpointBuilder::derive_object_id(20);

        let first_ids = transaction_objects(&checkpoint.transactions[0], &checkpoint.object_set)
            .map(|object| object.id())
            .collect::<Vec<_>>();
        let second_ids = transaction_objects(&checkpoint.transactions[1], &checkpoint.object_set)
            .map(|object| object.id())
            .collect::<Vec<_>>();

        assert!(first_ids.contains(&first_object));
        assert!(!first_ids.contains(&second_object));
        assert!(second_ids.contains(&second_object));
        assert!(!second_ids.contains(&first_object));
    }
}
