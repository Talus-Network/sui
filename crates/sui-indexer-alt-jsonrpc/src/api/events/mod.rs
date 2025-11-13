// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use sui_json_rpc_types::{EventFilter, EventPage};
use sui_open_rpc::Module;
use sui_open_rpc_macros::open_rpc;
use sui_types::event::EventID;

use crate::context::Context;

use super::rpc_module::RpcModule;

mod error;
mod filter;
mod response;

#[open_rpc(namespace = "suix", tag = "Extended API")]
#[rpc(server, namespace = "suix")]
trait EventsApi {
    /// Return list of events for a specified query criteria.
    #[method(name = "queryEvents")]
    async fn query_events(
        &self,
        /// The event query criteria. See [Event filter](https://docs.sui.io/build/event_api#event-filters) documentation for examples.
        query: EventFilter,
        /// Optional paging cursor
        cursor: Option<EventID>,
        /// Maximum number of items per page, default to QUERY_MAX_RESULT_LIMIT if not specified.
        limit: Option<usize>,
        /// Query result ordering, default to false (ascending order), oldest record first.
        descending_order: Option<bool>,
    ) -> RpcResult<EventPage>;
}

pub(crate) struct Events(pub Context);

#[async_trait::async_trait]
impl EventsApiServer for Events {
    async fn query_events(
        &self,
        query: EventFilter,
        cursor: Option<EventID>,
        limit: Option<usize>,
        descending_order: Option<bool>,
    ) -> RpcResult<EventPage> {
        let Self(ctx) = self;
        Ok(filter::query_events(ctx, &query, cursor, limit, descending_order).await?)
    }
}

impl RpcModule for Events {
    fn schema(&self) -> Module {
        EventsApiOpenRpc::module_doc()
    }

    fn into_impl(self) -> jsonrpsee::RpcModule<Self> {
        self.into_rpc()
    }
}

