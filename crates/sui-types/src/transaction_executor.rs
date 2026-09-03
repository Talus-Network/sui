// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::base_types::{ObjectID, TransactionDigest};
use crate::effects::TransactionEffects;
use crate::effects::TransactionEvents;
use crate::error::ExecutionError;
use crate::error::SuiError;
use crate::execution::ExecutionResult;
use crate::full_checkpoint_content::ObjectSet;
use crate::messages_checkpoint::CheckpointSequenceNumber;
use crate::object::Object;
use crate::storage::ObjectKey;
use crate::transaction::AllowedProposers;
use crate::transaction::TransactionData;
use crate::transaction_driver_types::ExecuteTransactionRequestV3;
use crate::transaction_driver_types::ExecuteTransactionResponseV3;
use crate::transaction_driver_types::TransactionSubmissionError;

/// Result of reading an object from a retained causal view.
///
/// `Unchanged` delegates to canonical storage because the causal chain did not
/// write the object. `Removed` prevents an older canonical value from being
/// used after the causal chain deleted or wrapped it.
pub enum CausalObjectRead {
    Unchanged,
    Live(Object),
    Removed,
}

/// Trait to define the interface for how the gRPC service interacts with a  QuorumDriver or a
/// simulated transaction executor.
#[async_trait::async_trait]
pub trait TransactionExecutor: Send + Sync {
    async fn execute_transaction(
        &self,
        request: ExecuteTransactionRequestV3,
        client_addr: Option<std::net::SocketAddr>,
    ) -> Result<ExecuteTransactionResponseV3, TransactionSubmissionError>;

    /// Execute a transaction and retain a temporary view linked to its parent.
    async fn execute_transaction_with_causal_parent(
        &self,
        _request: ExecuteTransactionRequestV3,
        _client_addr: Option<std::net::SocketAddr>,
        _causal_parent: Option<TransactionDigest>,
    ) -> Result<ExecuteTransactionResponseV3, TransactionSubmissionError> {
        Err(TransactionSubmissionError::CausalViewUnavailable(
            "causal transaction execution is not supported by this node".to_string(),
        ))
    }

    /// Return whether execution can wait for local checkpoint visibility.
    fn supports_checkpoint_wait(&self) -> bool {
        false
    }

    /// Wait until the transaction checkpoint is visible through local state.
    ///
    /// Implementations must register the keyed notification before reading
    /// storage so a checkpoint committed during registration cannot be
    /// missed.
    async fn wait_for_transaction_checkpoint(
        &self,
        _transaction: TransactionDigest,
        _is_consensus_transaction: bool,
    ) -> Result<CheckpointSequenceNumber, SuiError> {
        Err(crate::error::SuiErrorKind::UnsupportedFeatureError {
            error: "transaction checkpoint waiting is not supported by this node".to_string(),
        }
        .into())
    }

    /// Return a retained quorum finalized receipt for an opt in causal client.
    ///
    /// Implementations which do not retain causal views return `None`. The
    /// receipt is read only and never changes canonical node state.
    fn retained_causal_transaction(
        &self,
        _transaction: &TransactionDigest,
    ) -> Option<ExecuteTransactionResponseV3> {
        None
    }

    /// Read one object as of a retained causal parent.
    ///
    /// Implementations must distinguish an object untouched by the causal
    /// chain from one removed by it. This lets transaction resolution use the
    /// same state view as the simulation which follows it.
    fn read_object_at_causal_parent(
        &self,
        _causal_parent: TransactionDigest,
        _object_id: ObjectID,
    ) -> Result<CausalObjectRead, SuiError> {
        Err(crate::error::SuiErrorKind::UnsupportedFeatureError {
            error: "causal object resolution is not supported by this node".to_string(),
        }
        .into())
    }

    fn simulate_transaction(
        &self,
        transaction: TransactionData,
        checks: TransactionChecks,
        allow_mock_gas_coin: bool,
    ) -> Result<SimulateTransactionResult, SuiError>;

    /// Simulate against a view which includes the finalized parent.
    fn simulate_transaction_with_causal_parent(
        &self,
        _transaction: TransactionData,
        _checks: TransactionChecks,
        _allow_mock_gas_coin: bool,
        _causal_parent: TransactionDigest,
    ) -> Result<SimulateTransactionResult, SuiError> {
        Err(crate::error::SuiErrorKind::UnsupportedFeatureError {
            error: "causal transaction simulation is not supported by this node".to_string(),
        }
        .into())
    }
}

/// Trait to let the gRPC service name the validators a transaction should be submitted to,
/// without depending on the transaction driver that tracks them.
pub trait ProposerSelector: Send + Sync {
    /// Up to `max` validators this node would prefer to submit to, as committee indices for the
    /// current epoch. `None` when no preference can be formed, in which case the transaction is
    /// left unrestricted rather than pinned to an arbitrary set.
    ///
    /// The returned indices are strictly increasing, as `TransactionExpiration::Validity`
    /// requires.
    fn preferred_proposers(&self, max: usize) -> Option<AllowedProposers>;
}

pub struct SimulateTransactionResult {
    pub effects: TransactionEffects,
    pub events: Option<TransactionEvents>,
    pub objects: ObjectSet,
    pub execution_result: Result<Vec<ExecutionResult>, ExecutionError>,
    pub mock_gas_id: Option<ObjectID>,
    pub unchanged_loaded_runtime_objects: Vec<ObjectKey>,
    pub suggested_gas_price: Option<u64>,
}

#[derive(Default, Debug, Copy, Clone)]
pub enum TransactionChecks {
    #[default]
    Enabled,
    Disabled,
}

impl TransactionChecks {
    pub fn disabled(self) -> bool {
        matches!(self, Self::Disabled)
    }

    pub fn enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}
