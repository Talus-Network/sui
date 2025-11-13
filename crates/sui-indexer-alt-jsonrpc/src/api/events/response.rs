// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use anyhow::Context as _;
use move_core_types::annotated_value::{MoveDatatypeLayout, MoveTypeLayout};
use sui_json_rpc_types::{EventPage, SuiEvent};
use sui_types::{
    digests::TransactionDigest,
    event::Event,
};

use crate::{
    context::Context,
    data::{kv_loader::TransactionContents, tx_digests::TxDigestKey},
    error::{rpc_bail, RpcError},
    paginate::{JsonCursor, Page},
};

use super::error::Error;

type Cursor = JsonCursor<(u64, u64)>; // (tx_sequence_number, event_seq)

/// Build a page of events from transaction sequence numbers
pub(super) async fn build_event_page(
    ctx: &Context,
    page: &Page<Cursor>,
    mut results: Vec<(i64, Vec<u8>)>,
) -> Result<EventPage, RpcError<Error>> {
    let has_next_page = results.len() > page.limit as usize;
    if has_next_page {
        results.truncate(page.limit as usize);
    }

    // Group by transaction sequence number to batch load transactions
    let mut tx_map: HashMap<u64, Vec<usize>> = HashMap::new();
    for (idx, (tx_seq, _)) in results.iter().enumerate() {
        tx_map
            .entry(*tx_seq as u64)
            .or_insert_with(Vec::new)
            .push(idx);
    }

    // Load transaction digests
    let tx_sequences: Vec<u64> = tx_map.keys().copied().collect();
    let digest_map = ctx
        .pg_loader()
        .load_many(tx_sequences.iter().map(|&seq| TxDigestKey(seq)))
        .await
        .context("Failed to load transaction digests")?;

    // Load full transaction contents one by one (there's no batch load method)
    let mut tx_contents: HashMap<TransactionDigest, TransactionContents> = HashMap::new();
    for stored in digest_map.values() {
        let digest = TransactionDigest::try_from(stored.tx_digest.as_slice())
            .context("Failed to deserialize transaction digest")?;

        if let Some(tx) = ctx
            .kv_loader()
            .load_one_transaction(digest)
            .await
            .context("Failed to load transaction contents")?
        {
            tx_contents.insert(digest, tx);
        }
    }

    // Extract events and build SuiEvent responses
    let mut events: Vec<SuiEvent> = Vec::with_capacity(results.len());

    for (tx_seq, _sender) in results {
        let digest_entry = digest_map
            .get(&TxDigestKey(tx_seq as u64))
            .with_context(|| format!("Missing transaction digest for tx {}", tx_seq))?;

        let digest = TransactionDigest::try_from(digest_entry.tx_digest.as_slice())
            .context("Failed to deserialize transaction digest")?;

        let tx = tx_contents
            .get(&digest)
            .with_context(|| format!("Missing transaction contents for {}", digest))?;

        // Get timestamp from transaction (stored in kv_transactions.timestamp_ms)
        let timestamp_ms = tx.timestamp_ms();

        // Extract all events from this transaction
        let tx_events: Vec<Event> = tx.events()?;

        // Convert each event to SuiEvent
        for (event_seq, event) in tx_events.into_iter().enumerate() {
            let layout = match ctx
                .package_resolver()
                .type_layout(event.type_.clone().into())
                .await
                .with_context(|| {
                    format!(
                        "Failed to resolve layout for {}",
                        event.type_.to_canonical_display(/* with_prefix */ true)
                    )
                })? {
                MoveTypeLayout::Struct(s) => MoveDatatypeLayout::Struct(s),
                MoveTypeLayout::Enum(e) => MoveDatatypeLayout::Enum(e),
                _ => rpc_bail!(
                    "Event {event_seq} is not a struct or enum: {}",
                    event.type_.to_canonical_display(/* with_prefix */ true)
                ),
            };

            let sui_event = SuiEvent::try_from(
                event,
                digest,
                event_seq as u64,
                Some(timestamp_ms),
                layout,
            )
            .context("Failed to convert event to SuiEvent")?;

            events.push(sui_event);
        }
    }

    // Determine next cursor from the last event
    let next_cursor = if has_next_page && !events.is_empty() {
        let last = events.last().unwrap();
        Some(last.id)
    } else {
        None
    };

    Ok(EventPage {
        data: events,
        next_cursor,
        has_next_page,
    })
}
