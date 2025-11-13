// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// A simple test module that emits events for testing the events API
module test::events {
    use sui::event;

    /// Event emitted when a value is recorded
    public struct ValueRecorded has copy, drop {
        value: u64,
        sender: address,
    }

    /// Event emitted when two values are added
    public struct ValuesAdded has copy, drop {
        a: u64,
        b: u64,
        result: u64,
    }

    /// Generic event wrapper for testing type parameters
    public struct EventWrapper<T: copy + drop> has copy, drop {
        data: T,
    }

    /// Inner event data
    public struct InnerData has copy, drop {
        id: u64,
        message: vector<u8>,
    }

    /// Record a value and emit an event
    public entry fun record_value(value: u64, ctx: &mut TxContext) {
        event::emit(ValueRecorded {
            value,
            sender: ctx.sender(),
        });
    }

    /// Add two values and emit an event
    public entry fun add_values(a: u64, b: u64, _ctx: &mut TxContext) {
        event::emit(ValuesAdded {
            a,
            b,
            result: a + b,
        });
    }

    /// Emit a generic event with type parameter
    public entry fun emit_generic_event(id: u64, message: vector<u8>, _ctx: &mut TxContext) {
        event::emit(EventWrapper<InnerData> {
            data: InnerData { id, message },
        });
    }
}
