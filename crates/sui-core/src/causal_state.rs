// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Temporary object views rooted in quorum finalized transaction effects.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use parking_lot::{Mutex, MutexGuard};
use sui_types::{
    accumulator_root::AccumulatorObjId,
    base_types::{EpochId, ObjectID, ObjectRef, SequenceNumber, TransactionDigest},
    effects::{AccumulatorOperation, AccumulatorValue, AccumulatorWriteV1, TransactionEffectsAPI},
    error::{SuiError, SuiErrorKind, SuiResult},
    object::{Object, Owner},
    storage::{
        BackingPackageStore, BackingStore, ObjectStore, PackageObject, ParentSync,
        RuntimeObjectResolver, load_package_object_from_object_store,
    },
    transaction_driver_types::ExecuteTransactionResponseV3,
};

const MAX_CAUSAL_STATE_BYTES: u64 = 256 * 1024 * 1024;
const MIN_CAUSAL_STATE_WEIGHT: u64 = 4 * 1024;
const MAX_CAUSAL_DEPTH: u16 = 64;

#[derive(Clone, Debug)]
pub(crate) enum CausalObject {
    Live(Object),
    Removed(ObjectRef),
}

/// One immutable node verified view of finalized object changes.
#[derive(Debug)]
pub(crate) struct CausalState {
    epoch: EpochId,
    transaction: TransactionDigest,
    causal_parent: Option<TransactionDigest>,
    parent: Option<Arc<CausalState>>,
    depth: u16,
    effects: sui_types::transaction_driver_types::FinalizedEffects,
    events: Option<sui_types::effects::TransactionEvents>,
    /// Object changes made by this transaction only.
    objects: BTreeMap<ObjectID, CausalObject>,
    /// Address balance changes made by this transaction only.
    accumulator_updates: BTreeMap<AccumulatorObjId, AccumulatorWriteV1>,
    object_count: usize,
    weight: u64,
}

impl CausalState {
    pub(crate) fn object(&self, id: &ObjectID) -> Option<&CausalObject> {
        let mut state = Some(self);
        while let Some(current) = state {
            if let Some(object) = current.objects.get(id) {
                return Some(object);
            }
            state = current.parent.as_deref();
        }
        None
    }

    pub(crate) fn epoch(&self) -> EpochId {
        self.epoch
    }

    pub(crate) fn transaction(&self) -> TransactionDigest {
        self.transaction
    }

    pub(crate) fn object_count(&self) -> usize {
        self.object_count
    }

    /// Reconstruct the verified receipt and complete retained object view.
    ///
    /// The object set is the flattened causal view rather than only this
    /// transaction's writes. A subscriber can therefore recover an exact
    /// application snapshot without observing every ancestor receipt.
    pub(crate) fn receipt(&self) -> ExecuteTransactionResponseV3 {
        let mut objects = BTreeMap::new();
        let mut state = Some(self);
        while let Some(current) = state {
            for (id, object) in &current.objects {
                objects.entry(*id).or_insert_with(|| object.clone());
            }
            state = current.parent.as_deref();
        }
        let output_objects = objects
            .into_values()
            .filter_map(|object| match object {
                CausalObject::Live(object) => Some(object),
                CausalObject::Removed(_) => None,
            })
            .collect();
        ExecuteTransactionResponseV3 {
            effects: self.effects.clone(),
            events: self.events.clone(),
            input_objects: None,
            output_objects: Some(output_objects),
            auxiliary_data: None,
        }
    }

    /// Return a balance that never exceeds the amount available after this view.
    ///
    /// Checkpoint state may already contain some retained withdrawals, so this
    /// calculation can subtract them twice. That is intentionally conservative.
    /// Deposits are ignored because relying on an unsettled deposit could allow a
    /// simulation that validators reject.
    pub(crate) fn conservative_account_amount(
        &self,
        account: &AccumulatorObjId,
        visible_amount: u128,
    ) -> SuiResult<u128> {
        let mut withdrawn = 0u128;
        let mut state = Some(self);
        while let Some(current) = state {
            if let Some(update) = current.accumulator_updates.get(account) {
                match (&update.operation, &update.value) {
                    (AccumulatorOperation::Split, AccumulatorValue::Integer(amount)) => {
                        withdrawn = withdrawn.checked_add(*amount as u128).ok_or_else(|| {
                            SuiError::from("causal address balance withdrawal overflow")
                        })?;
                    }
                    (AccumulatorOperation::Merge, AccumulatorValue::Integer(_)) => {}
                    _ => {
                        return Err(SuiErrorKind::UnsupportedFeatureError {
                            error: format!(
                                "causal simulation cannot represent accumulator update for {account}"
                            ),
                        }
                        .into());
                    }
                }
            }
            state = current.parent.as_deref();
        }
        Ok(visible_amount.saturating_sub(withdrawn))
    }

    pub(crate) fn has_account_update(&self, account: &AccumulatorObjId) -> bool {
        let mut state = Some(self);
        while let Some(current) = state {
            if current.accumulator_updates.contains_key(account) {
                return true;
            }
            state = current.parent.as_deref();
        }
        false
    }
}

struct CausalStateEntry {
    state: Arc<CausalState>,
    children: u32,
    pins: usize,
    last_access: u64,
}

struct CausalStateCacheInner {
    states: HashMap<TransactionDigest, CausalStateEntry>,
    leaves: BTreeSet<(u64, TransactionDigest)>,
    access_sequence: u64,
    weight: u64,
    evictions: u64,
}

impl CausalStateCacheInner {
    fn next_access(&mut self) -> u64 {
        self.access_sequence = self.access_sequence.saturating_add(1);
        self.access_sequence
    }

    fn get(&mut self, transaction: &TransactionDigest) -> Option<Arc<CausalState>> {
        let access = self.next_access();
        let entry = self.states.get_mut(transaction)?;
        let previous_access = entry.last_access;
        entry.last_access = access;
        let is_leaf = entry.children == 0 && entry.pins == 0;
        let state = Arc::clone(&entry.state);
        if is_leaf {
            self.leaves.remove(&(previous_access, *transaction));
            self.leaves.insert((access, *transaction));
        }
        Some(state)
    }

    fn next_leaf(&mut self) -> Option<TransactionDigest> {
        while let Some((access, transaction)) = self.leaves.pop_first() {
            if self.states.get(&transaction).is_some_and(|entry| {
                entry.children == 0 && entry.pins == 0 && entry.last_access == access
            }) {
                return Some(transaction);
            }
        }
        None
    }

    fn remove_leaf(&mut self, transaction: TransactionDigest) {
        let Some(entry) = self.states.remove(&transaction) else {
            return;
        };
        debug_assert_eq!(entry.children, 0);
        debug_assert_eq!(entry.pins, 0);
        self.weight = self.weight.saturating_sub(entry.state.weight);
        self.evictions = self.evictions.saturating_add(1);
        if let Some(parent_digest) = entry.state.causal_parent {
            let parent_leaf = self.states.get_mut(&parent_digest).and_then(|parent| {
                parent.children = parent.children.saturating_sub(1);
                (parent.children == 0 && parent.pins == 0).then_some(parent.last_access)
            });
            if let Some(access) = parent_leaf {
                self.leaves.insert((access, parent_digest));
            }
        }
    }

    /// Remove complete least recently used branches until the byte bound holds.
    fn evict_to_capacity(&mut self, protected: TransactionDigest, capacity: u64) -> bool {
        let mut protected_leaf = None;
        while self.weight > capacity {
            let Some(transaction) = self.next_leaf() else {
                break;
            };
            if transaction == protected {
                protected_leaf = Some(transaction);
                continue;
            }
            self.remove_leaf(transaction);
        }

        let retained = self.weight <= capacity;
        if retained {
            if let Some(transaction) = protected_leaf
                && let Some(entry) = self.states.get(&transaction)
            {
                self.leaves.insert((entry.last_access, transaction));
            }
        } else if self
            .states
            .get(&protected)
            .is_some_and(|entry| entry.children == 0 && entry.pins == 0)
        {
            self.remove_leaf(protected);
        }
        retained
    }
}

/// Exact cache resource use after one operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CausalStateCacheStats {
    pub(crate) entries: usize,
    pub(crate) weight_bytes: u64,
    pub(crate) evictions: u64,
}

/// Bounded node local retention for causal simulation parents.
///
/// Each entry owns only one transaction delta. Parents remain addressable while
/// descendants exist, and capacity eviction removes whole unused branches from
/// their leaves. The byte bound therefore describes retained data rather than
/// multiplied copies of every ancestor snapshot.
pub(crate) struct CausalStateCache {
    inner: Mutex<CausalStateCacheInner>,
    capacity: u64,
}

impl CausalStateCache {
    pub(crate) fn new() -> Self {
        Self::with_capacity(MAX_CAUSAL_STATE_BYTES)
    }

    fn with_capacity(capacity: u64) -> Self {
        Self {
            inner: Mutex::new(CausalStateCacheInner {
                states: HashMap::new(),
                leaves: BTreeSet::new(),
                access_sequence: 0,
                weight: 0,
                evictions: 0,
            }),
            capacity,
        }
    }

    pub(crate) fn get(&self, transaction: &TransactionDigest) -> Option<Arc<CausalState>> {
        self.lock().get(transaction)
    }

    /// Keep one parent addressable while its child reaches finality.
    pub(crate) fn pin(&self, transaction: TransactionDigest) -> Option<CausalStatePin<'_>> {
        let mut cache = self.lock();
        let access = cache.next_access();
        let (was_leaf, previous_access) = {
            let entry = cache.states.get_mut(&transaction)?;
            let pins = entry.pins.checked_add(1)?;
            let was_leaf = entry.children == 0 && entry.pins == 0;
            let previous_access = entry.last_access;
            entry.pins = pins;
            entry.last_access = access;
            (was_leaf, previous_access)
        };
        if was_leaf {
            cache.leaves.remove(&(previous_access, transaction));
        }
        Some(CausalStatePin {
            cache: self,
            transaction,
        })
    }

    pub(crate) fn stats(&self) -> CausalStateCacheStats {
        let inner = self.lock();
        CausalStateCacheStats {
            entries: inner.states.len(),
            weight_bytes: inner.weight,
            evictions: inner.evictions,
        }
    }

    fn lock(&self) -> MutexGuard<'_, CausalStateCacheInner> {
        self.inner.lock()
    }

    fn release_pin(&self, transaction: TransactionDigest) {
        let mut cache = self.lock();
        let leaf_access = {
            let entry = cache
                .states
                .get_mut(&transaction)
                .expect("a pinned causal state cannot be evicted");
            debug_assert!(entry.pins > 0);
            entry.pins = entry.pins.saturating_sub(1);
            (entry.children == 0 && entry.pins == 0).then_some(entry.last_access)
        };
        if let Some(access) = leaf_access {
            cache.leaves.insert((access, transaction));
        }
    }

    /// Retain verified output objects without publishing them as canonical state.
    ///
    /// A missing causal parent is always rejected. An executed effects record
    /// does not prove that canonical shared object reads have reached the same
    /// transaction, so treating local execution as visibility could simulate a
    /// partial state.
    pub(crate) fn record(
        &self,
        causal_parent: Option<TransactionDigest>,
        response: &ExecuteTransactionResponseV3,
    ) -> SuiResult<bool> {
        let effects = response.effects.data();
        let epoch = response.effects.epoch();

        let expected = effects
            .all_changed_objects()
            .into_iter()
            .map(|(object_ref, _, _)| (object_ref.0, object_ref))
            .collect::<BTreeMap<_, _>>();
        let output_objects = response.output_objects.as_deref().unwrap_or_default();
        if output_objects.len() != expected.len() {
            return Err(SuiError::from(format!(
                "causal state for transaction {} has {} output objects, expected {}",
                effects.transaction_digest(),
                output_objects.len(),
                expected.len(),
            )));
        }

        let mut objects = BTreeMap::new();
        let mut output_ids = std::collections::BTreeSet::new();
        for object in output_objects {
            let object_ref = object.compute_object_reference();
            if expected.get(&object_ref.0) != Some(&object_ref) {
                return Err(SuiError::from(format!(
                    "causal output object {} does not match finalized effects for transaction {}",
                    object_ref.0,
                    effects.transaction_digest(),
                )));
            }
            output_ids.insert(object_ref.0);
            objects.insert(object_ref.0, CausalObject::Live(object.clone()));
        }
        if output_ids.len() != expected.len() {
            return Err(SuiError::from(format!(
                "causal state for transaction {} contains duplicate output objects",
                effects.transaction_digest(),
            )));
        }
        for object_ref in effects
            .deleted()
            .into_iter()
            .chain(effects.wrapped())
            .chain(effects.unwrapped_then_deleted())
        {
            objects.insert(object_ref.0, CausalObject::Removed(object_ref));
        }

        let transaction = *effects.transaction_digest();
        let accumulator_updates = effects
            .accumulator_updates()
            .into_iter()
            .map(|(id, update)| (AccumulatorObjId::new_unchecked(id), update))
            .collect::<BTreeMap<_, _>>();

        let weight = objects
            .values()
            .fold(0u64, |size, object| {
                size.saturating_add(match object {
                    CausalObject::Live(object) => object.object_size_for_gas_metering() as u64,
                    CausalObject::Removed(_) => std::mem::size_of::<CausalObject>() as u64,
                } as u64)
            })
            .saturating_add(
                objects
                    .len()
                    .saturating_mul(std::mem::size_of::<(ObjectID, CausalObject)>())
                    as u64,
            )
            .saturating_add(
                bcs::serialized_size(&response.effects)
                    .unwrap_or(std::mem::size_of_val(&response.effects)) as u64,
            )
            .saturating_add(
                response
                    .events
                    .as_ref()
                    .map(|events| {
                        bcs::serialized_size(events).unwrap_or(std::mem::size_of_val(events)) as u64
                    })
                    .unwrap_or_default(),
            )
            .saturating_add(
                accumulator_updates
                    .len()
                    .saturating_mul(std::mem::size_of::<(AccumulatorObjId, AccumulatorWriteV1)>())
                    as u64,
            )
            .max(MIN_CAUSAL_STATE_WEIGHT);

        let mut cache = self.lock();
        if let Some(existing) = cache.states.get(&transaction) {
            return Ok(
                existing.state.epoch == epoch && existing.state.causal_parent == causal_parent
            );
        }
        let parent = match causal_parent {
            Some(digest) => match cache.states.get(&digest) {
                Some(parent)
                    if parent.state.epoch == epoch && parent.state.depth < MAX_CAUSAL_DEPTH =>
                {
                    Some(Arc::clone(&parent.state))
                }
                Some(_) | None => return Ok(false),
            },
            None => None,
        };
        let object_count = parent.as_ref().map_or(objects.len(), |parent| {
            parent.object_count
                + objects
                    .keys()
                    .filter(|id| parent.object(id).is_none())
                    .count()
        });
        let state = Arc::new(CausalState {
            epoch,
            transaction,
            causal_parent,
            parent,
            depth: causal_parent
                .and_then(|digest| cache.states.get(&digest))
                .map_or(1, |parent| parent.state.depth + 1),
            effects: response.effects.clone(),
            events: response.events.clone(),
            objects,
            accumulator_updates,
            object_count,
            weight,
        });
        let access = cache.next_access();
        if let Some(parent_digest) = causal_parent {
            let parent = cache
                .states
                .get_mut(&parent_digest)
                .expect("validated causal parent remains locked");
            let parent_access = parent.last_access;
            parent.children = parent.children.saturating_add(1);
            parent.last_access = access;
            cache.leaves.remove(&(parent_access, parent_digest));
        }
        tracing::debug!(
            %transaction,
            causal_parent = ?causal_parent,
            parent_objects = state.parent.as_ref().map_or(0, |parent| parent.object_count),
            output_objects = output_objects.len(),
            retained_objects = state.object_count,
            delta_weight_bytes = state.weight,
            "Retaining causal transaction state"
        );
        cache.weight = cache.weight.saturating_add(state.weight);
        cache.states.insert(
            transaction,
            CausalStateEntry {
                state,
                children: 0,
                pins: 0,
                last_access: access,
            },
        );
        cache.leaves.insert((access, transaction));
        Ok(cache.evict_to_capacity(transaction, self.capacity))
    }
}

/// Temporary cache reservation for one causal parent.
pub(crate) struct CausalStatePin<'a> {
    cache: &'a CausalStateCache,
    transaction: TransactionDigest,
}

impl Drop for CausalStatePin<'_> {
    fn drop(&mut self) {
        self.cache.release_pin(self.transaction);
    }
}

/// Read only overlay used by simulation and never by canonical RPC reads.
pub(crate) struct CausalBackingStore<'a> {
    state: &'a CausalState,
    backing: &'a (dyn BackingStore + Send + Sync),
}

impl<'a> CausalBackingStore<'a> {
    pub(crate) fn new(
        state: &'a CausalState,
        backing: &'a (dyn BackingStore + Send + Sync),
    ) -> Self {
        Self { state, backing }
    }
}

impl ObjectStore for CausalBackingStore<'_> {
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        match self.state.object(object_id) {
            Some(CausalObject::Live(object)) => Some(object.clone()),
            Some(CausalObject::Removed(_)) => None,
            None => self.backing.get_object(object_id),
        }
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: SequenceNumber) -> Option<Object> {
        match self.state.object(object_id) {
            Some(CausalObject::Live(object)) if object.version() == version => Some(object.clone()),
            Some(CausalObject::Live(_)) | Some(CausalObject::Removed(_)) => None,
            None => self.backing.get_object_by_key(object_id, version),
        }
    }
}

impl BackingPackageStore for CausalBackingStore<'_> {
    fn get_package_object(&self, package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        match self.state.object(package_id) {
            Some(CausalObject::Live(object)) => {
                load_package_object_from_object_store(&std::slice::from_ref(object), package_id)
            }
            Some(CausalObject::Removed(_)) => Ok(None),
            None => self.backing.get_package_object(package_id),
        }
    }
}

impl RuntimeObjectResolver for CausalBackingStore<'_> {
    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        match self.state.object(child) {
            Some(CausalObject::Live(object)) if object.version() <= child_version_upper_bound => {
                if object.owner != Owner::ObjectOwner((*parent).into()) {
                    return Err(SuiErrorKind::InvalidChildObjectAccess {
                        object: *child,
                        given_parent: *parent,
                        actual_owner: object.owner.clone(),
                    }
                    .into());
                }
                Ok(Some(object.clone()))
            }
            Some(CausalObject::Removed(_)) => Ok(None),
            Some(CausalObject::Live(_)) => Ok(None),
            None => self
                .backing
                .read_child_object(parent, child, child_version_upper_bound),
        }
    }

    fn get_object_received_at_version(
        &self,
        owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: SequenceNumber,
        epoch_id: EpochId,
    ) -> SuiResult<Option<Object>> {
        match self.state.object(receiving_object_id) {
            Some(CausalObject::Live(object))
                if object.version() == receive_object_at_version
                    && object.owner == Owner::AddressOwner((*owner).into()) =>
            {
                Ok(Some(object.clone()))
            }
            Some(_) => Ok(None),
            None => self.backing.get_object_received_at_version(
                owner,
                receiving_object_id,
                receive_object_at_version,
                epoch_id,
            ),
        }
    }
}

impl ParentSync for CausalBackingStore<'_> {
    fn get_latest_parent_entry_ref_deprecated(&self, object_id: ObjectID) -> Option<ObjectRef> {
        match self.state.object(&object_id) {
            Some(CausalObject::Live(object)) => Some(object.compute_object_reference()),
            Some(CausalObject::Removed(object_ref)) => Some(*object_ref),
            None => self
                .backing
                .get_latest_parent_entry_ref_deprecated(object_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui_types::{
        base_types::SuiAddress,
        effects::{AccumulatorAddress, EffectsObjectChange, TransactionEffects},
        execution_status::ExecutionStatus,
        gas::GasCostSummary,
        in_memory_storage::InMemoryStorage,
        transaction_driver_types::{EffectsFinalityInfo, FinalizedEffects},
    };

    fn address(byte: u8) -> SuiAddress {
        SuiAddress::from_bytes([byte; 32]).unwrap()
    }

    fn object(id: ObjectID, version: u64, owner: SuiAddress) -> Object {
        Object::with_id_owner_version_for_testing(
            id,
            SequenceNumber::from_u64(version),
            Owner::AddressOwner(owner),
        )
    }

    fn response(
        epoch: EpochId,
        outputs: Vec<Object>,
        removed: Vec<ObjectRef>,
        accumulator_updates: Vec<(ObjectID, AccumulatorWriteV1)>,
    ) -> ExecuteTransactionResponseV3 {
        let transaction = TransactionDigest::random();
        let lamport_version = outputs
            .iter()
            .map(|object| object.version())
            .max()
            .unwrap_or_else(|| SequenceNumber::from_u64(1));
        let mut changes = BTreeMap::new();
        for output in &outputs {
            changes.insert(
                output.id(),
                EffectsObjectChange::new(None, Some(output), true, false),
            );
        }
        for object_ref in removed {
            changes.insert(
                object_ref.0,
                EffectsObjectChange::new(
                    Some((
                        (object_ref.1, object_ref.2),
                        Owner::AddressOwner(SuiAddress::ZERO),
                    )),
                    None,
                    false,
                    true,
                ),
            );
        }
        for (id, update) in accumulator_updates {
            changes.insert(id, EffectsObjectChange::new_from_accumulator_write(update));
        }
        let effects = TransactionEffects::new_from_execution_v2(
            ExecutionStatus::Success,
            epoch,
            GasCostSummary::default(),
            Vec::new(),
            transaction,
            lamport_version,
            changes,
            None,
            None,
            Vec::new(),
        );
        ExecuteTransactionResponseV3 {
            effects: FinalizedEffects {
                effects,
                finality_info: EffectsFinalityInfo::QuorumExecuted(epoch),
            },
            events: None,
            input_objects: None,
            output_objects: Some(outputs),
            auxiliary_data: None,
        }
    }

    #[test]
    fn cache_is_empty_before_verified_receipts_are_recorded() {
        let cache = CausalStateCache::new();
        assert!(cache.get(&TransactionDigest::ZERO).is_none());
    }

    #[test]
    fn record_requires_every_output_named_by_finalized_effects() {
        let cache = CausalStateCache::new();
        let output = object(ObjectID::from_single_byte(1), 2, address(1));
        let mut missing = response(7, vec![output.clone()], Vec::new(), Vec::new());
        missing.output_objects = None;
        assert!(cache.record(None, &missing).is_err());

        let mut mismatched = response(7, vec![output], Vec::new(), Vec::new());
        mismatched.output_objects =
            Some(vec![object(ObjectID::from_single_byte(1), 2, address(2))]);
        assert!(cache.record(None, &mismatched).is_err());
    }

    #[test]
    fn child_view_inherits_parent_and_masks_removed_objects() {
        let cache = CausalStateCache::new();
        let first = object(ObjectID::from_single_byte(1), 2, address(1));
        let first_response = response(7, vec![first.clone()], Vec::new(), Vec::new());
        let first_digest = *first_response.effects.data().transaction_digest();
        assert!(cache.record(None, &first_response).unwrap());

        let second = object(ObjectID::from_single_byte(2), 3, address(1));
        let removed = object(ObjectID::from_single_byte(3), 1, address(1));
        let second_response = response(
            7,
            vec![second.clone()],
            vec![removed.compute_object_reference()],
            Vec::new(),
        );
        let second_digest = *second_response.effects.data().transaction_digest();
        assert!(cache.record(Some(first_digest), &second_response).unwrap());

        let state = cache.get(&second_digest).unwrap();
        assert!(!state.objects.contains_key(&first.id()));
        assert!(
            matches!(state.object(&first.id()), Some(CausalObject::Live(value)) if value.compute_object_reference() == first.compute_object_reference())
        );
        assert!(
            matches!(state.object(&second.id()), Some(CausalObject::Live(value)) if value.compute_object_reference() == second.compute_object_reference())
        );
        assert!(matches!(
            state.object(&removed.id()),
            Some(CausalObject::Removed(_))
        ));

        let backing = InMemoryStorage::new(vec![removed.clone()]);
        let store = CausalBackingStore::new(state.as_ref(), &backing);
        assert!(store.get_object(&removed.id()).is_none());
        assert!(
            store
                .get_object_by_key(&removed.id(), removed.version())
                .is_none()
        );
    }

    #[test]
    fn retained_receipt_contains_the_flattened_live_view() {
        let cache = CausalStateCache::new();
        let first = object(ObjectID::from_single_byte(1), 2, address(1));
        let first_response = response(7, vec![first.clone()], Vec::new(), Vec::new());
        let first_digest = *first_response.effects.data().transaction_digest();
        assert!(cache.record(None, &first_response).unwrap());

        let second = object(ObjectID::from_single_byte(2), 3, address(1));
        let second_response = response(7, vec![second.clone()], Vec::new(), Vec::new());
        let second_digest = *second_response.effects.data().transaction_digest();
        assert!(cache.record(Some(first_digest), &second_response).unwrap());

        let receipt = cache.get(&second_digest).unwrap().receipt();
        assert_eq!(receipt.effects.data().transaction_digest(), &second_digest);
        let output_objects = receipt.output_objects.unwrap();
        assert!(
            output_objects
                .iter()
                .any(|object| object.id() == first.id())
        );
        assert!(
            output_objects
                .iter()
                .any(|object| object.id() == second.id())
        );
    }

    #[test]
    fn each_state_accounts_for_only_its_own_delta() {
        let cache = CausalStateCache::new();
        let mut parent = None;
        let mut last = None;
        for byte in 1..=8 {
            let output = object(
                ObjectID::from_single_byte(byte),
                byte as u64 + 1,
                address(1),
            );
            let receipt = response(7, vec![output], Vec::new(), Vec::new());
            let digest = *receipt.effects.data().transaction_digest();
            assert!(cache.record(parent, &receipt).unwrap());
            parent = Some(digest);
            last = cache.get(&digest);
        }

        let last = last.unwrap();
        assert_eq!(last.objects.len(), 1);
        assert_eq!(last.object_count(), 8);
        assert_eq!(cache.stats().weight_bytes, 8 * MIN_CAUSAL_STATE_WEIGHT);
    }

    #[test]
    fn capacity_evicts_the_oldest_leaf_and_remains_exactly_bounded() {
        let cache = CausalStateCache::with_capacity(3 * MIN_CAUSAL_STATE_WEIGHT);
        let mut digests = Vec::new();
        for byte in 1..=10 {
            let receipt = response(
                7,
                vec![object(
                    ObjectID::from_single_byte(byte),
                    byte as u64 + 1,
                    address(1),
                )],
                Vec::new(),
                Vec::new(),
            );
            let digest = *receipt.effects.data().transaction_digest();
            assert!(cache.record(None, &receipt).unwrap());
            digests.push(digest);
        }

        let stats = cache.stats();
        assert_eq!(stats.entries, 3);
        assert_eq!(stats.weight_bytes, 3 * MIN_CAUSAL_STATE_WEIGHT);
        assert_eq!(stats.evictions, 7);
        assert!(cache.get(&digests[0]).is_none());
        assert!(cache.get(digests.last().unwrap()).is_some());
    }

    #[test]
    fn repeated_reads_keep_one_leaf_index_entry() {
        let cache = CausalStateCache::new();
        let receipt = response(7, Vec::new(), Vec::new(), Vec::new());
        let digest = *receipt.effects.data().transaction_digest();
        assert!(cache.record(None, &receipt).unwrap());
        for _ in 0..1_000 {
            assert!(cache.get(&digest).is_some());
        }

        let inner = cache.lock();
        assert_eq!(inner.leaves.len(), 1);
        assert_eq!(inner.states.len(), 1);
    }

    #[test]
    fn pinned_parent_survives_capacity_pressure() {
        let cache = CausalStateCache::with_capacity(2 * MIN_CAUSAL_STATE_WEIGHT);
        let parent = response(7, Vec::new(), Vec::new(), Vec::new());
        let parent_digest = *parent.effects.data().transaction_digest();
        assert!(cache.record(None, &parent).unwrap());

        let pin = cache.pin(parent_digest).unwrap();
        for _ in 0..2 {
            let receipt = response(7, Vec::new(), Vec::new(), Vec::new());
            assert!(cache.record(None, &receipt).unwrap());
        }
        assert!(cache.lock().states.contains_key(&parent_digest));
        assert_eq!(cache.stats().weight_bytes, 2 * MIN_CAUSAL_STATE_WEIGHT);

        drop(pin);
        let receipt = response(7, Vec::new(), Vec::new(), Vec::new());
        assert!(cache.record(None, &receipt).unwrap());
        assert!(cache.get(&parent_digest).is_none());
        assert_eq!(cache.stats().weight_bytes, 2 * MIN_CAUSAL_STATE_WEIGHT);
    }

    #[test]
    fn missing_parent_is_never_assumed_canonical() {
        let cache = CausalStateCache::new();
        let parent = TransactionDigest::random();
        let receipt = response(7, Vec::new(), Vec::new(), Vec::new());

        assert!(!cache.record(Some(parent), &receipt).unwrap());
    }

    #[test]
    fn finalized_transaction_cannot_be_rebound_to_another_parent() {
        let cache = CausalStateCache::new();
        let first_parent = response(7, Vec::new(), Vec::new(), Vec::new());
        let first_parent_digest = *first_parent.effects.data().transaction_digest();
        assert!(cache.record(None, &first_parent).unwrap());

        let second_parent = response(7, Vec::new(), Vec::new(), Vec::new());
        let second_parent_digest = *second_parent.effects.data().transaction_digest();
        assert!(cache.record(None, &second_parent).unwrap());

        let child = response(7, Vec::new(), Vec::new(), Vec::new());
        let child_digest = *child.effects.data().transaction_digest();
        assert!(cache.record(Some(first_parent_digest), &child).unwrap());
        assert!(!cache.record(Some(second_parent_digest), &child).unwrap());
        assert_eq!(
            cache.get(&child_digest).unwrap().causal_parent,
            Some(first_parent_digest)
        );
    }

    #[test]
    fn causal_balance_is_a_conservative_bound() {
        let cache = CausalStateCache::new();
        let account = AccumulatorObjId::new_unchecked(ObjectID::from_single_byte(9));
        let address = AccumulatorAddress::new(SuiAddress::ZERO, sui_types::TypeTag::U64);
        let withdrawal = AccumulatorWriteV1 {
            address: address.clone(),
            operation: AccumulatorOperation::Split,
            value: AccumulatorValue::Integer(30),
        };
        let first_response = response(
            7,
            Vec::new(),
            Vec::new(),
            vec![(*account.inner(), withdrawal)],
        );
        let first_digest = *first_response.effects.data().transaction_digest();
        assert!(cache.record(None, &first_response).unwrap());

        let deposit = AccumulatorWriteV1 {
            address,
            operation: AccumulatorOperation::Merge,
            value: AccumulatorValue::Integer(100),
        };
        let second_response =
            response(7, Vec::new(), Vec::new(), vec![(*account.inner(), deposit)]);
        let second_digest = *second_response.effects.data().transaction_digest();
        assert!(cache.record(Some(first_digest), &second_response).unwrap());

        let state = cache.get(&second_digest).unwrap();
        assert!(state.has_account_update(&account));
        assert_eq!(
            state.conservative_account_amount(&account, 100).unwrap(),
            70
        );
        assert_eq!(state.conservative_account_amount(&account, 70).unwrap(), 40);
    }
}
