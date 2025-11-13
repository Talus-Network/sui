// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::store::{Connection, Store};

use super::Handler;

/// Starts a task for a tasked pipeline to track the main reader lo. This task is responsible for
/// managing the watch channel so consumers know when the sender has been dropped and break
/// accordingly.
pub(super) fn track_main_reader_lo<H: Handler + 'static>(
    reader_lo_tx: watch::Sender<Option<u64>>,
    reader_interval_ms: Option<u64>,
    cancel: CancellationToken,
    store: H::Store,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Set the interval to half the provided interval to ensure we refresh the watermark read
        // frequently enough.
        let mut reader_interval = interval(
            reader_interval_ms
                .map(|ms| Duration::from_millis(ms / 2))
                // Dummy value that won't be used
                .unwrap_or(Duration::from_secs(5)),
        );

        // If we miss ticks, skip them to ensure we have the latest watermark.
        reader_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        if reader_interval_ms.is_none() {
            reader_lo_tx.send(Some(0)).ok();
        }

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!(pipeline = H::NAME, "Shutdown received");
                    break;
                }


                _ = reader_lo_tx.closed() => {
                    info!(pipeline = H::NAME, "All main reader lo receivers dropped, shutting down task");
                    break;
                }

                // Periodic refresh of the main reader watermark.
                _ = reader_interval.tick(), if reader_interval_ms.is_some() => {
                    match store.connect().await {
                        Ok(mut conn) => {
                            match conn.reader_watermark(H::NAME).await {
                                Ok(watermark_opt) => {
                                    // If the reader watermark is not found, we assume that pruning
                                    // is not enabled, and checkpoints >= 0 are valid.
                                    if reader_lo_tx.send(Some(watermark_opt.map_or(0, |wm| wm.reader_lo))).is_err() {
                                        info!(pipeline = H::NAME, "Main reader lo receiver dropped, shutting down task");
                                        break;
                                    }
                                }
                                Err(e) => {
                                    warn!(pipeline = H::NAME, "Failed to get reader watermark: {e}");
                                }
                            }
                        },
                        Err(e) => {
                            warn!(pipeline = H::NAME, "Failed to connect to store: {e}");
                        }
                    }
                }
            }
        }
    })
}
