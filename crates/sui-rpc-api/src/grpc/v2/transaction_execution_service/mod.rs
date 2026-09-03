// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::ErrorReason;
use crate::RpcError;
use crate::RpcService;
use prost_types::FieldMask;
use std::str::FromStr;
use sui_rpc::field::FieldMaskTree;
use sui_rpc::field::FieldMaskUtil;
use sui_rpc::merge::Merge;
use sui_rpc::proto::google::rpc::bad_request::FieldViolation;
use sui_rpc::proto::sui::rpc::v2::ExecuteTransactionRequest;
use sui_rpc::proto::sui::rpc::v2::ExecuteTransactionResponse;
use sui_rpc::proto::sui::rpc::v2::ExecutedTransaction;
use sui_rpc::proto::sui::rpc::v2::ObjectSet;
use sui_rpc::proto::sui::rpc::v2::SimulateTransactionRequest;
use sui_rpc::proto::sui::rpc::v2::SimulateTransactionResponse;
use sui_rpc::proto::sui::rpc::v2::Transaction;
use sui_rpc::proto::sui::rpc::v2::UserSignature;
use sui_rpc::proto::sui::rpc::v2::transaction_execution_service_server::TransactionExecutionService;
use sui_types::balance_change::derive_balance_changes_2;
use sui_types::base_types::TransactionDigest;
use sui_types::effects::TransactionEffectsAPI;
use sui_types::transaction_executor::TransactionExecutor;
use tap::Pipe;
use tracing::warn;

use super::checkpoint_wait;

mod simulate;

const CAUSAL_PARENT_HEADER: &str = "x-sui-causal-parent";
const CAUSAL_RECORD_HEADER: &str = "x-sui-causal-record";
const CAUSAL_APPLIED_HEADER: &str = "x-sui-causal-applied";
const CAUSAL_RETAINED_HEADER: &str = "x-sui-causal-retained";

#[derive(Clone, Copy, Debug)]
struct CausalExecution {
    parent: Option<TransactionDigest>,
}

struct CausalExecutionOutcome {
    applied: bool,
    retained: bool,
}

fn parse_causal_parent(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Option<TransactionDigest>, tonic::Status> {
    metadata
        .get(CAUSAL_PARENT_HEADER)
        .map(|value| {
            let value = value
                .to_str()
                .map_err(|_| tonic::Status::invalid_argument("invalid causal parent metadata"))?;
            TransactionDigest::from_str(value)
                .map_err(|_| tonic::Status::invalid_argument("invalid causal parent digest"))
        })
        .transpose()
}

fn requests_causal_record(metadata: &tonic::metadata::MetadataMap) -> Result<bool, tonic::Status> {
    match metadata.get(CAUSAL_RECORD_HEADER) {
        None => Ok(false),
        Some(value) if value == "true" => Ok(true),
        Some(_) => Err(tonic::Status::invalid_argument(
            "invalid causal record metadata",
        )),
    }
}

fn usable_causal_parent(
    executor: &dyn TransactionExecutor,
    parent: Option<TransactionDigest>,
) -> Result<(Option<TransactionDigest>, bool), tonic::Status> {
    let Some(parent) = parent else {
        return Ok((None, true));
    };
    match executor.causal_parent_is_retained(parent) {
        Ok(true) => Ok((Some(parent), true)),
        Ok(false) => Ok((None, false)),
        Err(error) => Err(tonic::Status::failed_precondition(error.to_string())),
    }
}

#[tonic::async_trait]
impl TransactionExecutionService for RpcService {
    async fn execute_transaction(
        &self,
        request: tonic::Request<ExecuteTransactionRequest>,
    ) -> Result<tonic::Response<ExecuteTransactionResponse>, tonic::Status> {
        let executor = self
            .executor
            .as_ref()
            .ok_or_else(|| tonic::Status::unimplemented("no transaction executor"))?;

        let wait_for_checkpoint = checkpoint_wait::is_requested(request.metadata())?;
        if wait_for_checkpoint && !executor.supports_checkpoint_wait() {
            return Err(tonic::Status::unimplemented(
                "transaction checkpoint waiting is not supported by this node",
            ));
        }
        let requested_parent = parse_causal_parent(request.metadata())?;
        let record = requests_causal_record(request.metadata())?;
        let causal = (requested_parent.is_some() || record).then_some(CausalExecution {
            parent: requested_parent,
        });
        let (response, causal_outcome) = execute_transaction(
            self,
            executor,
            request.into_inner(),
            causal,
            wait_for_checkpoint,
        )
        .await
        .map_err(tonic::Status::from)?;
        let mut response = tonic::Response::new(response);
        if executor.supports_checkpoint_wait() {
            checkpoint_wait::mark_supported(&mut response);
        }
        if let Some(causal_outcome) = causal_outcome {
            let applied = if causal_outcome.applied {
                tonic::metadata::MetadataValue::from_static("true")
            } else {
                tonic::metadata::MetadataValue::from_static("false")
            };
            response
                .metadata_mut()
                .insert(CAUSAL_APPLIED_HEADER, applied);
            let retained = if causal_outcome.retained {
                tonic::metadata::MetadataValue::from_static("true")
            } else {
                tonic::metadata::MetadataValue::from_static("false")
            };
            response
                .metadata_mut()
                .insert(CAUSAL_RETAINED_HEADER, retained);
        }
        Ok(response)
    }

    async fn simulate_transaction(
        &self,
        request: tonic::Request<SimulateTransactionRequest>,
    ) -> Result<tonic::Response<SimulateTransactionResponse>, tonic::Status> {
        let service = self.clone();
        let requested_parent = parse_causal_parent(request.metadata())?;
        let (causal_parent, applied) = match requested_parent {
            Some(_) => {
                let executor = self
                    .executor
                    .as_ref()
                    .ok_or_else(|| tonic::Status::unimplemented("no transaction executor"))?;
                usable_causal_parent(executor.as_ref(), requested_parent)?
            }
            None => (None, true),
        };
        let request = request.into_inner();
        let response = tokio::task::spawn_blocking(move || {
            simulate::simulate_transaction(&service, request, causal_parent)
        })
        .await
        .map_err(|e| tonic::Status::internal(format!("simulate_transaction task failed: {e}")))?
        .map_err(tonic::Status::from)?;
        let mut response = tonic::Response::new(response);
        if requested_parent.is_some() {
            let applied = if applied {
                tonic::metadata::MetadataValue::from_static("true")
            } else {
                tonic::metadata::MetadataValue::from_static("false")
            };
            response
                .metadata_mut()
                .insert(CAUSAL_APPLIED_HEADER, applied);
        }
        Ok(response)
    }
}

pub const EXECUTE_TRANSACTION_READ_MASK_DEFAULT: &str =
    crate::read_mask_defaults::EXECUTE_TRANSACTION;
// Current maximum number of supported UserSignature's,
// one for the sender and one for an optional sponsor
const MAX_NUMBER_OF_SIGNATURES: usize = 2;

#[tracing::instrument(skip(service, executor))]
async fn execute_transaction(
    service: &RpcService,
    executor: &std::sync::Arc<dyn TransactionExecutor>,
    request: ExecuteTransactionRequest,
    causal: Option<CausalExecution>,
    wait_for_checkpoint: bool,
) -> Result<(ExecuteTransactionResponse, Option<CausalExecutionOutcome>), RpcError> {
    let retain_causal_receipt = causal.is_some();
    let transaction = request
        .transaction
        .as_ref()
        .ok_or_else(|| FieldViolation::new("transaction").with_reason(ErrorReason::FieldMissing))?
        .pipe(sui_sdk_types::Transaction::try_from)
        .map_err(|e| {
            FieldViolation::new("transaction")
                .with_description(format!("invalid transaction: {e}"))
                .with_reason(ErrorReason::FieldInvalid)
        })?;

    if request.signatures.len() > MAX_NUMBER_OF_SIGNATURES {
        return Err(FieldViolation::new("signatures")
            .with_description(format!(
                "{} provided signatures exceeds the maximum allowed of {}",
                request.signatures.len(),
                MAX_NUMBER_OF_SIGNATURES
            ))
            .with_reason(ErrorReason::FieldInvalid)
            .into());
    }

    let signatures = request
        .signatures
        .iter()
        .enumerate()
        .map(|(i, signature)| {
            sui_sdk_types::UserSignature::try_from(signature).map_err(|e| {
                FieldViolation::new_at("signatures", i)
                    .with_description(format!("invalid signature: {e}"))
                    .with_reason(ErrorReason::FieldInvalid)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let signed_transaction = sui_sdk_types::SignedTransaction {
        transaction: transaction.clone(),
        signatures: signatures.clone(),
    };

    let read_mask = {
        let read_mask = request
            .read_mask
            .unwrap_or_else(|| FieldMask::from_str(EXECUTE_TRANSACTION_READ_MASK_DEFAULT));
        read_mask
            .validate::<ExecutedTransaction>()
            .map_err(|path| {
                FieldViolation::new("read_mask")
                    .with_description(format!("invalid read_mask path: {path}"))
                    .with_reason(ErrorReason::FieldInvalid)
            })?;
        FieldMaskTree::from(read_mask)
    };

    let request = sui_types::transaction_driver_types::ExecuteTransactionRequestV3 {
        transaction: signed_transaction.try_into()?,
        include_events: retain_causal_receipt
            || read_mask.contains(ExecutedTransaction::EVENTS_FIELD.name),
        include_input_objects: read_mask.contains(ExecutedTransaction::BALANCE_CHANGES_FIELD.name)
            || read_mask.contains(ExecutedTransaction::OBJECTS_FIELD.name)
            || read_mask.contains(ExecutedTransaction::EFFECTS_FIELD.name),
        include_output_objects: read_mask.contains(ExecutedTransaction::BALANCE_CHANGES_FIELD.name)
            || read_mask.contains(ExecutedTransaction::OBJECTS_FIELD.name)
            || read_mask.contains(ExecutedTransaction::EFFECTS_FIELD.name),
        include_auxiliary_data: false,
    };
    let is_consensus_transaction = request.transaction.is_consensus_tx();

    let (driver_response, causal_outcome) = match causal {
        Some(causal) => {
            let result = executor
                .execute_transaction_with_causal_parent(request, None, causal.parent)
                .await?;
            let outcome = CausalExecutionOutcome {
                applied: result.applied,
                retained: result.retained,
            };
            (result.response, Some(outcome))
        }
        None => (executor.execute_transaction(request, None).await?, None),
    };
    let sui_types::transaction_driver_types::ExecuteTransactionResponseV3 {
        effects:
            sui_types::transaction_driver_types::FinalizedEffects {
                effects,
                finality_info: _,
            },
        events,
        input_objects,
        output_objects,
        auxiliary_data: _,
    } = driver_response;

    let checkpoint = if wait_for_checkpoint {
        let transaction_digest = *effects.transaction_digest();
        Some(
            executor
                .wait_for_transaction_checkpoint(transaction_digest, is_consensus_transaction)
                .await
                .map_err(|error| {
                    warn!(
                        ?transaction_digest,
                        ?error,
                        "Transaction checkpoint wait failed"
                    );
                    RpcError::new(
                        tonic::Code::Unavailable,
                        format!("transaction checkpoint wait failed: {error}"),
                    )
                })?,
        )
    } else {
        None
    };

    let executed_transaction = {
        // Build the objects set first so we can use it for event JSON rendering.
        // This allows resolving types from packages that were just published in this transaction.
        let objects = {
            let mut objects = sui_types::full_checkpoint_content::ObjectSet::default();
            for o in input_objects
                .into_iter()
                .chain(output_objects.into_iter())
                .flatten()
            {
                objects.insert(o);
            }
            objects
        };

        let events = read_mask
            .subtree(ExecutedTransaction::EVENTS_FIELD)
            .and_then(|mask| {
                events.map(|events| service.render_events_to_proto(&events, &mask, &objects))
            });

        let balance_changes = if read_mask.contains(ExecutedTransaction::BALANCE_CHANGES_FIELD) {
            derive_balance_changes_2(&effects, &objects)
                .into_iter()
                .map(Into::into)
                .collect()
        } else {
            vec![]
        };

        let effects = read_mask
            .subtree(ExecutedTransaction::EFFECTS_FIELD)
            .map(|mask| service.render_effects_to_proto(&effects, &[], &objects, &mask));

        let mut message = ExecutedTransaction::default();
        message.digest = read_mask
            .contains(ExecutedTransaction::DIGEST_FIELD)
            .then(|| transaction.digest().to_string());
        message.checkpoint =
            if wait_for_checkpoint || read_mask.contains(ExecutedTransaction::CHECKPOINT_FIELD) {
                checkpoint
            } else {
                None
            };
        message.transaction = read_mask
            .subtree(ExecutedTransaction::TRANSACTION_FIELD)
            .map(|mask| Transaction::merge_from(transaction, &mask));
        message.signatures = read_mask
            .subtree(ExecutedTransaction::SIGNATURES_FIELD)
            .map(|mask| {
                signatures
                    .into_iter()
                    .map(|s| UserSignature::merge_from(s, &mask))
                    .collect()
            })
            .unwrap_or_default();
        message.effects = effects;
        message.events = events;
        message.balance_changes = balance_changes;
        message.objects = read_mask
            .subtree(
                ExecutedTransaction::path_builder()
                    .objects()
                    .objects()
                    .finish(),
            )
            .map(|mask| {
                ObjectSet::default().with_objects(
                    objects
                        .iter()
                        .map(|o| service.render_object_to_proto(o, &mask, &objects))
                        .collect(),
                )
            });
        message
    };

    if causal_outcome
        .as_ref()
        .is_some_and(|outcome| outcome.retained)
    {
        service.publish_causal_finality(*effects.transaction_digest());
    }

    Ok((
        ExecuteTransactionResponse::default().with_transaction(executed_transaction),
        causal_outcome,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ParentStatus(bool);

    #[async_trait::async_trait]
    impl TransactionExecutor for ParentStatus {
        async fn execute_transaction(
            &self,
            _request: sui_types::transaction_driver_types::ExecuteTransactionRequestV3,
            _client_addr: Option<std::net::SocketAddr>,
        ) -> Result<
            sui_types::transaction_driver_types::ExecuteTransactionResponseV3,
            sui_types::transaction_driver_types::TransactionSubmissionError,
        > {
            unreachable!()
        }

        fn causal_parent_is_retained(
            &self,
            _causal_parent: TransactionDigest,
        ) -> Result<bool, sui_types::error::SuiError> {
            Ok(self.0)
        }

        fn simulate_transaction(
            &self,
            _transaction: sui_types::transaction::TransactionData,
            _checks: sui_types::transaction_executor::TransactionChecks,
            _allow_mock_gas_coin: bool,
        ) -> Result<
            sui_types::transaction_executor::SimulateTransactionResult,
            sui_types::error::SuiError,
        > {
            unreachable!()
        }
    }

    #[test]
    fn causal_parent_metadata_requires_a_transaction_digest() {
        let digest = TransactionDigest::random();
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(CAUSAL_PARENT_HEADER, digest.to_string().parse().unwrap());
        assert_eq!(parse_causal_parent(&metadata).unwrap(), Some(digest));

        metadata.insert(CAUSAL_PARENT_HEADER, "not-a-digest".parse().unwrap());
        assert_eq!(
            parse_causal_parent(&metadata).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn causal_record_metadata_accepts_only_true() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        assert!(!requests_causal_record(&metadata).unwrap());

        metadata.insert(CAUSAL_RECORD_HEADER, "true".parse().unwrap());
        assert!(requests_causal_record(&metadata).unwrap());

        metadata.insert(CAUSAL_RECORD_HEADER, "false".parse().unwrap());
        assert_eq!(
            requests_causal_record(&metadata).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn checkpointed_parent_uses_canonical_state() {
        let parent = TransactionDigest::random();
        assert_eq!(
            usable_causal_parent(&ParentStatus(true), Some(parent)).unwrap(),
            (Some(parent), true)
        );
        assert_eq!(
            usable_causal_parent(&ParentStatus(false), Some(parent)).unwrap(),
            (None, false)
        );
    }
}
