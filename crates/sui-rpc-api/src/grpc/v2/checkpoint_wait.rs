// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

const HEADER: &str = "x-sui-checkpoint-wait";

/// Return whether an execution request asks for local checkpoint visibility.
pub(super) fn is_requested(metadata: &tonic::metadata::MetadataMap) -> Result<bool, tonic::Status> {
    match metadata.get(HEADER) {
        None => Ok(false),
        Some(value) if value == "true" => Ok(true),
        Some(_) => Err(tonic::Status::invalid_argument(
            "invalid transaction checkpoint wait metadata",
        )),
    }
}

/// Mark a response from a service which supports checkpoint waiting.
pub(super) fn mark_supported<T>(response: &mut tonic::Response<T>) {
    response
        .metadata_mut()
        .insert(HEADER, tonic::metadata::MetadataValue::from_static("true"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_wait_accepts_only_true() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        assert!(!is_requested(&metadata).unwrap());

        metadata.insert(HEADER, "true".parse().unwrap());
        assert!(is_requested(&metadata).unwrap());

        metadata.insert(HEADER, "false".parse().unwrap());
        assert_eq!(
            is_requested(&metadata).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn checkpoint_wait_capability_is_explicit() {
        let mut response = tonic::Response::new(());
        mark_supported(&mut response);
        assert_eq!(response.metadata().get(HEADER).unwrap(), "true");
    }
}
