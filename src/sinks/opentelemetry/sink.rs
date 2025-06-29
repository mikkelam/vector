//! The core implementation of the `opentelemetry` sink.

use futures_util::stream::BoxStream;

use crate::sinks::{prelude::*, util::http::HttpRequest};

use super::request_builder::OtlpRequestBuilder;

/// The key used to partition events by their signal type (log, metric, trace).
///
/// The partitioner uses this key to group events, ensuring that batches sent
/// to the encoder contain only one type of signal, which can then be routed
/// to the correct OTLP endpoint.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(super) struct PartitionKey {
    /// The full endpoint URI for this signal type.
    pub endpoint: String,
}

/// Partitions events based on their signal type.
pub(super) struct KeyPartitioner {
    log_endpoint: String,
    trace_endpoint: String,
    metric_endpoint: String,
}

impl KeyPartitioner {
    /// Creates a new `KeyPartitioner`.
    pub fn new(log_endpoint: String, trace_endpoint: String, metric_endpoint: String) -> Self {
        Self {
            log_endpoint,
            trace_endpoint,
            metric_endpoint,
        }
    }
}

impl Partitioner for KeyPartitioner {
    type Item = Event;
    type Key = PartitionKey;

    fn partition(&self, event: &Self::Item) -> Self::Key {
        match event {
            Event::Log(_) => PartitionKey {
                endpoint: self.log_endpoint.clone(),
            },
            Event::Metric(_) => PartitionKey {
                endpoint: self.metric_endpoint.clone(),
            },
            Event::Trace(_) => PartitionKey {
                endpoint: self.trace_endpoint.clone(),
            },
        }
    }
}

/// The core `opentelemetry` sink implementation.
pub(super) struct OpenTelemetrySink<S> {
    service: S,
    batch_settings: BatcherSettings,
    request_builder: OtlpRequestBuilder,
    key_partitioner: KeyPartitioner,
}

impl<S> OpenTelemetrySink<S>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    /// Creates a new `OpenTelemetrySink`.
    pub(super) fn new(
        service: S,
        batch_settings: BatcherSettings,
        request_builder: OtlpRequestBuilder,
        log_endpoint: String,
        trace_endpoint: String,
        metric_endpoint: String,
    ) -> Self {
        let key_partitioner = KeyPartitioner::new(log_endpoint, trace_endpoint, metric_endpoint);
        Self {
            service,
            batch_settings,
            request_builder,
            key_partitioner,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        input
            .batched_partitioned(self.key_partitioner, || {
                self.batch_settings.as_byte_size_config()
            })
            .request_builder(
                default_request_builder_concurrency_limit(),
                self.request_builder,
            )
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamSink<Event> for OpenTelemetrySink<S>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
