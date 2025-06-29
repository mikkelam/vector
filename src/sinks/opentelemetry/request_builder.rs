//! Request builder for the `opentelemetry` sink.

use std::io;

use bytes::Bytes;

use crate::sinks::{prelude::*, util::http::HttpRequest};

use super::{encoder::OtlpEncoder, sink::PartitionKey};

/// Builds `HttpRequest`s for the OTLP sink.
#[derive(Debug, Clone)]
pub(super) struct OtlpRequestBuilder {
    encoder: OtlpEncoder,
}

impl OtlpRequestBuilder {
    /// Creates a new `OtlpRequestBuilder`.
    pub(super) const fn new() -> Self {
        Self {
            encoder: OtlpEncoder::new(),
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
        // OTLP/HTTP has its own compression negotiation via the `Content-Encoding`
        // header, which is handled by the HTTP client. We don't need to compress
        // the payload at this stage.
        Compression::None
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
