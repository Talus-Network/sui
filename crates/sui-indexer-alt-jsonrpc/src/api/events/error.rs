// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use sui_types::event::EventID;

use crate::{error::RpcError, paginate};

#[derive(Debug)]
pub(super) enum Error {
    /// Unsupported event filter type
    UnsupportedFilter(String),
    /// Event not found at cursor
    InvalidCursor(EventID),
    /// Pagination error
    Pagination(paginate::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnsupportedFilter(filter_type) => {
                write!(f, "Event filter type '{}' is not yet supported", filter_type)
            }
            Error::InvalidCursor(event_id) => {
                write!(
                    f,
                    "Invalid cursor: event {}:{} not found",
                    event_id.tx_digest, event_id.event_seq
                )
            }
            Error::Pagination(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}

impl From<Error> for RpcError<Error> {
    fn from(e: Error) -> Self {
        RpcError::InvalidParams(e)
    }
}

impl From<paginate::Error> for Error {
    fn from(e: paginate::Error) -> Self {
        Error::Pagination(e)
    }
}
