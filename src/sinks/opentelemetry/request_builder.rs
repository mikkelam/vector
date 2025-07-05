//! Request builder for the `opentelemetry` sink.

use std::io;

use bytes::Bytes;

use crate::sinks::{
    prelude::*,
    util::{http::HttpRequest, Compression},
};

use super::{encoder::OtlpEncoder, sink::PartitionKey};

/// Builds `HttpRequest`s for the OTLP sink.
#[derive(Debug, Clone)]
pub(super) struct OtlpRequestBuilder {
    encoder: OtlpEncoder,
    compression: Compression,
}

impl OtlpRequestBuilder {
    /// Creates a new `OtlpRequestBuilder`.
    pub(super) fn new(
        compression: Compression,
        config: super::config::OpenTelemetryConfig,
    ) -> Self {
        Self {
            encoder: OtlpEncoder::new(config),
            compression,
        }
    }
}

impl RequestBuilder<(PartitionKey, Vec<Event>)> for OtlpRequestBuilder {
    type Metadata = (PartitionKey, EventFinalizers);
    type Events = Vec<Event>;
    type Encoder = OtlpEncoder;
    type Payload = Bytes;
    type Request = HttpRequest<PartitionKey>;
    type Error = io::Error;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        input: (PartitionKey, Vec<Event>),
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let (key, mut events) = input;
        let finalizers = events.take_finalizers();
        let builder = RequestMetadataBuilder::from_events(&events);
        ((key, finalizers), builder, events)
    }

    fn build_request(
        &self,
        metadata: Self::Metadata,
        request_metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let (key, finalizers) = metadata;
        HttpRequest::new(payload.into_payload(), finalizers, request_metadata, key)
    }
}
