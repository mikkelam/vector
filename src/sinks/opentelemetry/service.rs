//! Service implementation for the `opentelemetry` sink.

use bytes::Bytes;
use http::{header::CONTENT_TYPE, HeaderName, HeaderValue, Method, Request, Uri};
use indexmap::IndexMap;

use crate::{
    http::Auth,
    sinks::{
        prelude::*,
        util::{
            http::{HttpRequest, HttpServiceRequestBuilder},
            Compression,
        },
    },
};

use super::{
    config::{ContentEncoding, HttpMethod},
    sink::PartitionKey,
};

/// Builds the final `http::Request` for an OTLP batch.
#[derive(Debug, Clone)]
pub(super) struct OtlpServiceRequestBuilder {
    pub(super) auth: Option<Auth>,
    pub(super) method: HttpMethod,
    pub(super) compression: Compression,
    pub(super) encoding: ContentEncoding,
    pub(super) headers: IndexMap<HeaderName, HeaderValue>,
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

        // Use the configured HTTP method
        let http_method: Method = self.method.into();
        let mut builder = Request::builder().method(http_method).uri(uri);

        // Set content type based on encoding
        let content_type = match self.encoding {
            ContentEncoding::Protobuf => "application/x-protobuf",
        };
        builder = builder.header(CONTENT_TYPE, content_type);

        // Set compression headers if compression is enabled
        if !matches!(self.compression, Compression::None) {
            let encoding = match self.compression {
                Compression::Gzip(_) => "gzip",
                Compression::Zlib(_) => "deflate",
                Compression::Zstd(_) => "zstd",
                Compression::Snappy => "snappy",
                Compression::None => unreachable!(),
            };
            builder = builder.header("Content-Encoding", encoding);
        }

        let mut http_request = builder
            .body(request.take_payload())
            .map_err(|err| crate::Error::from(format!("Failed to build HTTP request: {}", err)))?;

        // Apply custom headers from request config
        for (name, value) in &self.headers {
            http_request
                .headers_mut()
                .insert(name.clone(), value.clone());
        }

        if let Some(auth) = &self.auth {
            auth.apply(&mut http_request);
        }

        Ok(http_request)
    }
}
