//! Service implementation for the `opentelemetry` sink.

use bytes::Bytes;
use http::{header::CONTENT_TYPE, Request, Uri};

use crate::{
    http::Auth,
    sinks::{
        prelude::*,
        util::http::{HttpRequest, HttpServiceRequestBuilder},
    },
};

use super::sink::PartitionKey;

/// Builds the final `http::Request` for an OTLP batch.
#[derive(Debug, Clone)]
pub(super) struct OtlpServiceRequestBuilder {
    pub(super) auth: Option<Auth>,
}

impl HttpServiceRequestBuilder<PartitionKey> for OtlpServiceRequestBuilder {
    fn build(
        &self,
        mut request: HttpRequest<PartitionKey>,
    ) -> Result<Request<Bytes>, crate::Error> {
        let metadata = request.get_additional_metadata();
        let uri = metadata.endpoint.parse::<Uri>().map_err(|err| {
            emit!(SinkRequestBuildError {
                error: format!("Failed to parse endpoint URI: {}", err)
            });
            crate::Error::from(format!("Invalid URI: {}", err))
        })?;

        let builder = Request::post(uri).header(CONTENT_TYPE, "application/x-protobuf");

        let mut http_request = builder
            .body(request.take_payload())
            .map_err(|err| crate::Error::from(format!("Failed to build HTTP request: {}", err)))?;

        if let Some(auth) = &self.auth {
            auth.apply(&mut http_request);
        }

        Ok(http_request)
    }
}
