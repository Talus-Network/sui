// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Context as _;
use diesel::{ExpressionMethods, JoinOnDsl, QueryDsl};
use sui_indexer_alt_schema::schema::{ev_struct_inst, tx_digests};
use sui_json_rpc_types::{EventFilter, EventPage};
use sui_types::event::EventID;

use crate::{
    context::Context,
    error::{invalid_params, RpcError},
    paginate::{Cursor as _, JsonCursor, Page},
};

use super::{error::Error, response};

type Cursor = JsonCursor<(u64, u64)>; // (tx_sequence_number, event_seq)

/// Query events based on the provided filter and pagination parameters
pub(super) async fn query_events(
    ctx: &Context,
    filter: &EventFilter,
    cursor: Option<EventID>,
    limit: Option<usize>,
    descending_order: Option<bool>,
) -> Result<EventPage, RpcError<Error>> {
    let config = &ctx.config().transactions;

    // Convert EventID cursor to our internal cursor format
    let json_cursor = if let Some(event_id) = cursor {
        // We need to look up the tx_sequence_number from the tx_digest
        let tx_seq = lookup_tx_sequence_number(ctx, &event_id).await?;
        Some(JsonCursor((tx_seq, event_id.event_seq)))
    } else {
        None
    };

    let page: Page<Cursor> = Page::from_params(
        config.default_page_size,
        config.max_page_size,
        json_cursor
            .map(|c| c.encode())
            .transpose()
            .map_err(Error::from)?,
        limit,
        descending_order,
    )?;

    match filter {
        EventFilter::MoveEventType(struct_tag) => {
            by_move_event_type(ctx, &page, struct_tag).await
        }
        EventFilter::Sender(sender) => by_sender(ctx, &page, *sender).await,
        EventFilter::MoveEventModule { package, module } => {
            by_move_event_module(ctx, &page, *package, module).await
        }
        EventFilter::All(_) => all_events(ctx, &page).await,
        _ => Err(invalid_params(Error::UnsupportedFilter(format!(
            "{:?}",
            filter
        )))),
    }
}

/// Look up the transaction sequence number from a transaction digest
async fn lookup_tx_sequence_number(
    ctx: &Context,
    event_id: &EventID,
) -> Result<u64, RpcError<Error>> {
    use tx_digests::dsl as d;

    let tx_digest = event_id.tx_digest.inner().to_vec();

    let results: Vec<i64> = ctx
        .pg_reader()
        .connect()
        .await
        .context("Failed to connect to the database")?
        .results(
            d::tx_digests
                .select(d::tx_sequence_number)
                .filter(d::tx_digest.eq(tx_digest))
                .into_boxed(),
        )
        .await
        .context("Failed to lookup transaction sequence number")?;

    results
        .first()
        .map(|seq| *seq as u64)
        .ok_or_else(|| invalid_params(Error::InvalidCursor(*event_id)))
}

/// Fetch events by MoveEventType (struct tag)
async fn by_move_event_type(
    ctx: &Context,
    page: &Page<Cursor>,
    struct_tag: &move_core_types::language_storage::StructTag,
) -> Result<EventPage, RpcError<Error>> {
    use ev_struct_inst::dsl as e;
    use sui_indexer_alt_schema::schema::{kv_transactions, tx_digests};

    let package = struct_tag.address.to_vec();
    let module = struct_tag.module.to_string();
    let name = struct_tag.name.to_string();
    let instantiation = bcs::to_bytes(&struct_tag.type_params)
        .context("Failed to serialize type parameters")?;

    // Join with tx_digests and kv_transactions to ensure we only return events
    // for transactions that have their full data available
    let mut query = e::ev_struct_inst
        .inner_join(tx_digests::table.on(e::tx_sequence_number.eq(tx_digests::tx_sequence_number)))
        .inner_join(kv_transactions::table.on(tx_digests::tx_digest.eq(kv_transactions::tx_digest)))
        .select((e::tx_sequence_number, e::sender))
        .filter(e::package.eq(package))
        .filter(e::module.eq(module))
        .filter(e::name.eq(name))
        .filter(e::instantiation.eq(instantiation))
        .into_boxed();

    // Apply cursor-based pagination
    if let Some(JsonCursor((tx_seq, _event_seq))) = page.cursor {
        if page.descending {
            query = query.filter(e::tx_sequence_number.lt(tx_seq as i64));
        } else {
            query = query.filter(e::tx_sequence_number.gt(tx_seq as i64));
        }
    }

    // Order and limit
    if page.descending {
        query = query.order(e::tx_sequence_number.desc());
    } else {
        query = query.order(e::tx_sequence_number.asc());
    }

    query = query.limit(page.limit + 1);

    let results: Vec<(i64, Vec<u8>)> = ctx
        .pg_reader()
        .connect()
        .await
        .context("Failed to connect to the database")?
        .results(query)
        .await
        .context("Failed to fetch events")?;

    response::build_event_page(ctx, page, results).await
}

/// Fetch events by sender address
async fn by_sender(
    ctx: &Context,
    page: &Page<Cursor>,
    sender: sui_types::base_types::SuiAddress,
) -> Result<EventPage, RpcError<Error>> {
    use ev_struct_inst::dsl as e;
    use sui_indexer_alt_schema::schema::{kv_transactions, tx_digests};

    let sender_bytes = sender.to_vec();

    // Join with tx_digests and kv_transactions to ensure we only return events
    // for transactions that have their full data available
    let mut query = e::ev_struct_inst
        .inner_join(tx_digests::table.on(e::tx_sequence_number.eq(tx_digests::tx_sequence_number)))
        .inner_join(kv_transactions::table.on(tx_digests::tx_digest.eq(kv_transactions::tx_digest)))
        .select((e::tx_sequence_number, e::sender))
        .filter(e::sender.eq(sender_bytes))
        .into_boxed();

    // Apply cursor-based pagination
    if let Some(JsonCursor((tx_seq, _event_seq))) = page.cursor {
        if page.descending {
            query = query.filter(e::tx_sequence_number.lt(tx_seq as i64));
        } else {
            query = query.filter(e::tx_sequence_number.gt(tx_seq as i64));
        }
    }

    // Order and limit
    if page.descending {
        query = query.order(e::tx_sequence_number.desc());
    } else {
        query = query.order(e::tx_sequence_number.asc());
    }

    query = query.limit(page.limit + 1);

    let results: Vec<(i64, Vec<u8>)> = ctx
        .pg_reader()
        .connect()
        .await
        .context("Failed to connect to the database")?
        .results(query)
        .await
        .context("Failed to fetch events")?;

    response::build_event_page(ctx, page, results).await
}

/// Fetch events by MoveEventModule (package + module where event struct is defined)
async fn by_move_event_module(
    ctx: &Context,
    page: &Page<Cursor>,
    package: sui_types::base_types::ObjectID,
    module: &move_core_types::identifier::Identifier,
) -> Result<EventPage, RpcError<Error>> {
    use ev_struct_inst::dsl as e;
    use sui_indexer_alt_schema::schema::{kv_transactions, tx_digests};

    let package_bytes = package.to_vec();
    let module_str = module.to_string();

    // Join with tx_digests and kv_transactions to ensure we only return events
    // for transactions that have their full data available
    let mut query = e::ev_struct_inst
        .inner_join(tx_digests::table.on(e::tx_sequence_number.eq(tx_digests::tx_sequence_number)))
        .inner_join(kv_transactions::table.on(tx_digests::tx_digest.eq(kv_transactions::tx_digest)))
        .select((e::tx_sequence_number, e::sender))
        .filter(e::package.eq(package_bytes))
        .filter(e::module.eq(module_str))
        .into_boxed();

    // Apply cursor-based pagination
    if let Some(JsonCursor((tx_seq, _event_seq))) = page.cursor {
        if page.descending {
            query = query.filter(e::tx_sequence_number.lt(tx_seq as i64));
        } else {
            query = query.filter(e::tx_sequence_number.gt(tx_seq as i64));
        }
    }

    // Order and limit
    if page.descending {
        query = query.order(e::tx_sequence_number.desc());
    } else {
        query = query.order(e::tx_sequence_number.asc());
    }

    query = query.limit(page.limit + 1);

    let results: Vec<(i64, Vec<u8>)> = ctx
        .pg_reader()
        .connect()
        .await
        .context("Failed to connect to the database")?
        .results(query)
        .await
        .context("Failed to fetch events")?;

    response::build_event_page(ctx, page, results).await
}

/// Fetch all events without filtering
async fn all_events(
    ctx: &Context,
    page: &Page<Cursor>,
) -> Result<EventPage, RpcError<Error>> {
    use ev_struct_inst::dsl as e;
    use sui_indexer_alt_schema::schema::{kv_transactions, tx_digests};

    // Join with tx_digests and kv_transactions to ensure we only return events
    // for transactions that have their full data available
    let mut query = e::ev_struct_inst
        .inner_join(tx_digests::table.on(e::tx_sequence_number.eq(tx_digests::tx_sequence_number)))
        .inner_join(kv_transactions::table.on(tx_digests::tx_digest.eq(kv_transactions::tx_digest)))
        .select((e::tx_sequence_number, e::sender))
        .into_boxed();

    // Apply cursor-based pagination
    if let Some(JsonCursor((tx_seq, _event_seq))) = page.cursor {
        if page.descending {
            query = query.filter(e::tx_sequence_number.lt(tx_seq as i64));
        } else {
            query = query.filter(e::tx_sequence_number.gt(tx_seq as i64));
        }
    }

    // Order and limit
    if page.descending {
        query = query.order(e::tx_sequence_number.desc());
    } else {
        query = query.order(e::tx_sequence_number.asc());
    }

    query = query.limit(page.limit + 1);

    let results: Vec<(i64, Vec<u8>)> = ctx
        .pg_reader()
        .connect()
        .await
        .context("Failed to connect to the database")?
        .results(query)
        .await
        .context("Failed to fetch events")?;

    response::build_event_page(ctx, page, results).await
}
