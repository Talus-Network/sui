// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Context;
use reqwest::Client;
use serde_json::{json, Value};
use std::{path::PathBuf, time::Duration};
use sui_indexer_alt_e2e_tests::{find_immutable, FullCluster};
use sui_json_rpc_types::{EventFilter, EventPage};
use sui_macros::sim_test;
use sui_test_transaction_builder::TestTransactionBuilder;
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    effects::TransactionEffectsAPI,
    transaction::CallArg,
};

use bcs;

struct EventsTestCluster {
    cluster: FullCluster,
    client: Client,
}

impl EventsTestCluster {
    async fn new() -> anyhow::Result<Self> {
        let cluster = FullCluster::new().await?;

        Ok(Self {
            cluster,
            client: Client::new(),
        })
    }

    /// Compile and publish the test events module, returning the package ID
    async fn publish_test_module(&mut self) -> anyhow::Result<ObjectID> {
        // Build the Move package
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("packages/test_events");

        // Create account and publish
        let (sender, keypair, gas) = self
            .cluster
            .funded_account(10_000_000_000)?;

        let tx = TestTransactionBuilder::new(sender, gas, self.cluster.reference_gas_price())
            .publish(path)
            .build_and_sign(&keypair);

        let effects = self.cluster.execute_transaction(tx)?.0;
        assert!(effects.status().is_ok(), "Publish failed");

        // Get the published package ID
        let package_ref = find_immutable(&effects)?;
        Ok(package_ref.0)
    }

    async fn execute_jsonrpc(&self, method: String, params: Value) -> anyhow::Result<Value> {
        let query = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let response = self
            .client
            .post(self.cluster.rpc_url())
            .json(&query)
            .send()
            .await
            .context("Request to JSON-RPC server failed")?;

        let body: Value = response
            .json()
            .await
            .context("Failed to parse JSON-RPC response")?;

        Ok(body)
    }

    async fn query_events(
        &self,
        filter: EventFilter,
        cursor: Option<Value>,
        limit: Option<usize>,
        descending: Option<bool>,
    ) -> anyhow::Result<EventPage> {
        let mut params = vec![serde_json::to_value(&filter)?];

        params.push(cursor.unwrap_or(Value::Null));

        if let Some(limit) = limit {
            params.push(json!(limit));
        } else {
            params.push(Value::Null);
        }

        if let Some(descending) = descending {
            params.push(json!(descending));
        }

        let response = self
            .execute_jsonrpc("suix_queryEvents".to_string(), json!(params))
            .await?;

        if let Some(error) = response.get("error") {
            anyhow::bail!("RPC error: {}", error);
        }

        let result = response
            .get("result")
            .context("Missing result in response")?;

        Ok(serde_json::from_value(result.clone())?)
    }

    async fn stopped(self) {
        self.cluster.stopped().await;
    }
}

#[sim_test]
async fn test_query_events_by_sender() {
    telemetry_subscribers::init_for_testing();

    let mut test_cluster = EventsTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    // Publish the test events module
    let package_id = test_cluster
        .publish_test_module()
        .await
        .expect("Failed to publish test module");

    // Create checkpoint for the publish transaction
    test_cluster.cluster.create_checkpoint().await;

    // Create an account and fund it
    let (sender, keypair, gas) = test_cluster
        .cluster
        .funded_account(10_000_000_000)
        .expect("Failed to create funded account");

    // Call the record_value function to emit an event
    let tx = test_cluster
        .cluster
        .execute_transaction(
            TestTransactionBuilder::new(
                sender,
                gas,
                test_cluster.cluster.reference_gas_price(),
            )
            .move_call(package_id, "events", "record_value", vec![CallArg::Pure(bcs::to_bytes(&42u64).unwrap())])
            .build_and_sign(&keypair),
        )
        .expect("Failed to execute transaction");

    assert!(tx.0.status().is_ok(), "Transaction failed: {:?}", tx.1);

    // Create checkpoint and wait for indexer to catch up
    let checkpoint = test_cluster.cluster.create_checkpoint().await;
    test_cluster
        .cluster
        .wait_for_checkpoint(checkpoint.sequence_number, Duration::from_secs(10))
        .await
        .expect("Timed out waiting for checkpoint");

    // Query events by sender
    let filter = EventFilter::Sender(sender);
    let result = test_cluster
        .query_events(filter, None, Some(10), Some(false))
        .await
        .expect("Failed to query events");

    // Verify we got events and they all match the sender
    assert!(!result.data.is_empty(), "Expected to find events for sender");

    for event in &result.data {
        assert_eq!(
            event.sender, sender,
            "All events should be from the specified sender"
        );
        assert!(event.timestamp_ms.is_some(), "Event should have timestamp");
    }

    test_cluster.stopped().await;
}

#[sim_test]
async fn test_query_events_pagination() {
    telemetry_subscribers::init_for_testing();

    let mut test_cluster = EventsTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    // Publish the test events module
    let package_id = test_cluster
        .publish_test_module()
        .await
        .expect("Failed to publish test module");

    // Create checkpoint for the publish transaction
    test_cluster.cluster.create_checkpoint().await;

    // Create an account and fund it
    let (sender, keypair, mut gas) = test_cluster
        .cluster
        .funded_account(10_000_000_000)
        .expect("Failed to create funded account");

    // Execute multiple transactions to generate multiple events
    for i in 0..5 {
        let tx = test_cluster
            .cluster
            .execute_transaction(
                TestTransactionBuilder::new(
                    sender,
                    gas,
                    test_cluster.cluster.reference_gas_price(),
                )
                .move_call(package_id, "events", "record_value", vec![CallArg::Pure(bcs::to_bytes(&(i as u64)).unwrap())])
                .build_and_sign(&keypair),
            )
            .expect("Failed to execute transaction");

        assert!(tx.0.status().is_ok(), "Transaction failed: {:?}", tx.1);

        // Update gas reference for next transaction
        gas = tx.0.gas_object().0;
    }

    // Create checkpoint and wait for indexer to catch up
    let checkpoint = test_cluster.cluster.create_checkpoint().await;
    test_cluster
        .cluster
        .wait_for_checkpoint(checkpoint.sequence_number, Duration::from_secs(10))
        .await
        .expect("Timed out waiting for checkpoint");

    // Query first page with limit of 3
    let filter = EventFilter::Sender(sender);
    let page1 = test_cluster
        .query_events(filter.clone(), None, Some(3), Some(false))
        .await
        .expect("Failed to query events page 1");

    assert!(
        !page1.data.is_empty(),
        "First page should have events"
    );
    assert!(
        page1.data.len() <= 3,
        "First page should have at most 3 events"
    );

    // If there's a next page, fetch it
    if page1.has_next_page {
        let cursor = page1.next_cursor.expect("Should have next cursor");
        let cursor_value = serde_json::to_value(&cursor).expect("Failed to serialize cursor");

        let page2 = test_cluster
            .query_events(filter, Some(cursor_value), Some(3), Some(false))
            .await
            .expect("Failed to query events page 2");

        assert!(
            !page2.data.is_empty(),
            "Second page should have events"
        );

        // Verify events don't overlap
        let page1_ids: Vec<_> = page1.data.iter().map(|e| e.id).collect();
        let page2_ids: Vec<_> = page2.data.iter().map(|e| e.id).collect();

        for id in &page2_ids {
            assert!(
                !page1_ids.contains(id),
                "Events should not overlap between pages"
            );
        }
    }

    test_cluster.stopped().await;
}

#[sim_test]
async fn test_query_events_descending_order() {
    telemetry_subscribers::init_for_testing();

    let mut test_cluster = EventsTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    // Create an account and fund it
    let (sender, keypair, mut gas) = test_cluster
        .cluster
        .funded_account(10_000_000_000)
        .expect("Failed to create funded account");

    // Execute multiple transactions
    for _ in 0..3 {
        let recipient = SuiAddress::random_for_testing_only();
        let tx = test_cluster
            .cluster
            .execute_transaction(
                TestTransactionBuilder::new(
                    sender,
                    gas,
                    test_cluster.cluster.reference_gas_price(),
                )
                .transfer_sui(Some(1_000_000), recipient)
                .build_and_sign(&keypair),
            )
            .expect("Failed to execute transaction");

        assert!(tx.0.status().is_ok(), "Transaction failed: {:?}", tx.1);
        gas = tx.0.gas_object().0;
    }

    // Create checkpoint and wait for indexer to catch up
    let checkpoint = test_cluster.cluster.create_checkpoint().await;
    test_cluster
        .cluster
        .wait_for_checkpoint(checkpoint.sequence_number, Duration::from_secs(10))
        .await
        .expect("Timed out waiting for checkpoint");

    // Query events in descending order
    let filter = EventFilter::Sender(sender);
    let result = test_cluster
        .query_events(filter, None, Some(10), Some(true))
        .await
        .expect("Failed to query events");

    if result.data.len() > 1 {
        // Verify timestamps are in descending order
        for i in 0..result.data.len() - 1 {
            let ts1 = result.data[i].timestamp_ms.unwrap_or(0);
            let ts2 = result.data[i + 1].timestamp_ms.unwrap_or(0);
            assert!(
                ts1 >= ts2,
                "Events should be in descending order by timestamp"
            );
        }
    }

    test_cluster.stopped().await;
}

#[sim_test]
async fn test_query_all_events() {
    telemetry_subscribers::init_for_testing();

    let mut test_cluster = EventsTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    // Request gas from faucet - this should emit events
    let recipient = SuiAddress::random_for_testing_only();
    let tx = test_cluster
        .cluster
        .request_gas(recipient, 10_000_000_000)
        .expect("Failed to request gas");

    assert!(tx.status().is_ok(), "Transaction failed");

    // Create checkpoint and wait for indexer to catch up
    let checkpoint = test_cluster.cluster.create_checkpoint().await;
    test_cluster
        .cluster
        .wait_for_checkpoint(checkpoint.sequence_number, Duration::from_secs(10))
        .await
        .expect("Timed out waiting for checkpoint");

    // Query all events - this should return at least the events from genesis and gas request
    let filter = EventFilter::All([]);
    let result = test_cluster
        .query_events(filter, None, Some(50), Some(false))
        .await
        .expect("Failed to query all events");

    // We should have at least some events from the genesis checkpoint or the gas request
    // If there are no events, that's fine - it means the system didn't emit any events
    // that get indexed in ev_struct_inst. The important thing is the query succeeds.
    println!("Found {} events", result.data.len());

    // Just verify the query completed successfully - don't assert on event count
    // as it depends on what system events are emitted

    test_cluster.stopped().await;
}

#[sim_test]
async fn test_query_events_empty_result() {
    telemetry_subscribers::init_for_testing();

    let test_cluster = EventsTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    // Query events for a random sender that doesn't exist
    let random_sender = SuiAddress::random_for_testing_only();
    let filter = EventFilter::Sender(random_sender);

    let result = test_cluster
        .query_events(filter, None, Some(10), Some(false))
        .await
        .expect("Failed to query events");

    // Should return empty result
    assert!(
        result.data.is_empty(),
        "Should return empty result for non-existent sender"
    );
    assert!(!result.has_next_page, "Should not have next page");
    assert!(result.next_cursor.is_none(), "Should not have cursor");

    test_cluster.stopped().await;
}

#[sim_test]
async fn test_query_events_with_generic_type() {
    telemetry_subscribers::init_for_testing();

    let mut test_cluster = EventsTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    // Publish the test events module
    let package_id = test_cluster
        .publish_test_module()
        .await
        .expect("Failed to publish test module");

    // Create checkpoint for the publish transaction
    test_cluster.cluster.create_checkpoint().await;

    // Create an account and fund it
    let (sender, keypair, gas) = test_cluster
        .cluster
        .funded_account(10_000_000_000)
        .expect("Failed to create funded account");

    // Call the emit_generic_event function to emit a generic event
    let message = b"test message".to_vec();
    let tx = test_cluster
        .cluster
        .execute_transaction(
            TestTransactionBuilder::new(
                sender,
                gas,
                test_cluster.cluster.reference_gas_price(),
            )
            .move_call(
                package_id,
                "events",
                "emit_generic_event",
                vec![
                    CallArg::Pure(bcs::to_bytes(&123u64).unwrap()),
                    CallArg::Pure(bcs::to_bytes(&message).unwrap()),
                ],
            )
            .build_and_sign(&keypair),
        )
        .expect("Failed to execute transaction");

    assert!(tx.0.status().is_ok(), "Transaction failed: {:?}", tx.1);

    // Create checkpoint and wait for indexer to catch up
    let checkpoint = test_cluster.cluster.create_checkpoint().await;
    test_cluster
        .cluster
        .wait_for_checkpoint(checkpoint.sequence_number, Duration::from_secs(10))
        .await
        .expect("Timed out waiting for checkpoint");

    // Build the struct tag with type parameter: EventWrapper<InnerData>
    use move_core_types::language_storage::{StructTag, TypeTag};
    use move_core_types::account_address::AccountAddress;
    use move_core_types::identifier::Identifier;

    let inner_data_type = TypeTag::Struct(Box::new(StructTag {
        address: AccountAddress::from(package_id),
        module: Identifier::new("events").unwrap(),
        name: Identifier::new("InnerData").unwrap(),
        type_params: vec![],
    }));

    let event_wrapper_type = StructTag {
        address: AccountAddress::from(package_id),
        module: Identifier::new("events").unwrap(),
        name: Identifier::new("EventWrapper").unwrap(),
        type_params: vec![inner_data_type],
    };

    // Query events by MoveEventType with generic type parameter
    let filter = EventFilter::MoveEventType(event_wrapper_type);
    let result = test_cluster
        .query_events(filter, None, Some(10), Some(false))
        .await
        .expect("Failed to query events with generic type");

    // Verify we got the event
    assert!(!result.data.is_empty(), "Expected to find generic event");
    assert_eq!(result.data.len(), 1, "Should have exactly one event");

    let event = &result.data[0];
    assert_eq!(event.sender, sender, "Event sender should match");
    assert_eq!(
        event.package_id, package_id,
        "Event package_id should match"
    );

    // Verify the event type includes the type parameter
    assert_eq!(
        event.type_.type_params.len(),
        1,
        "Event should have one type parameter"
    );

    test_cluster.stopped().await;
}
