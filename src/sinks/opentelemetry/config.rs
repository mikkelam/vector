//! Configuration for the `opentelemetry` sink.

use std::str::FromStr;

use http::{Request, Uri};

use crate::{
    http::{Auth, HttpClient},
    sinks::{
        prelude::*,
        util::{
            http::{http_response_retry_logic, HttpService, RequestConfig},
            service::ServiceBuilderExt,
            BatchConfig, RealtimeSizeBasedDefaultBatchSettings, UriSerde,
        },
    },
};

use super::{
    request_builder::OtlpRequestBuilder, service::OtlpServiceRequestBuilder,
    sink::OpenTelemetrySink,
};

/// Configuration for the `opentelemetry` sink.
#[configurable_component(sink("opentelemetry", "Deliver OTLP data over HTTP and gRPC."))]
#[derive(Clone, Debug)]
pub struct OpenTelemetryConfig {
    /// The base endpoint for the OTLP collector.
    ///
    /// The sink will append the appropriate signal-specific path, e.g., `/v1/logs`.
    #[configurable(validation(format = "uri"))]
    #[configurable(metadata(docs::examples = "http://localhost:4318"))]
    endpoint: String,

    /// The protocol to use for sending data.
    #[configurable(derived)]
    #[serde(default)]
    protocol: OtlpProtocol,

    #[configurable(derived)]
    #[serde(default)]
    auth: Option<Auth>,

    #[configurable(derived)]
    #[serde(default)]
    request: RequestConfig,

    #[configurable(derived)]
    #[serde(default)]
    batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,

    #[configurable(derived)]
    tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(default)]
    acknowledgements: AcknowledgementsConfig,
}

/// The protocol used to send OTLP data.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub enum OtlpProtocol {
    /// Send data over HTTP with Protobuf encoding.
    #[default]
    Http,
    // Grpc, // To be implemented in the future
}

impl Default for OpenTelemetryConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4318".to_string(),
            protocol: OtlpProtocol::default(),
            auth: None,
            request: RequestConfig::default(),
            batch: BatchConfig::default(),
            tls: None,
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

impl_generate_config_from_default!(OpenTelemetryConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "opentelemetry")]
impl SinkConfig for OpenTelemetryConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let batch_settings = self.batch.validate()?.into_batcher_settings()?;
        let request_settings = self.request.tower.into_settings();

        let tls_settings = TlsSettings::from_options(self.tls.as_ref())?;
        let client = HttpClient::new(tls_settings, cx.proxy())?;

        let healthcheck =
            healthcheck(self.endpoint.clone(), self.auth.clone(), client.clone()).boxed();

        let sink = match self.protocol {
            OtlpProtocol::Http => {
                self.build_http_sink(cx, client, batch_settings, request_settings)?
            }
        };

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Log | DataType::Metric | DataType::Trace)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl OpenTelemetryConfig {
    fn build_http_sink(
        &self,
        _cx: SinkContext,
        client: HttpClient,
        batch_settings: BatcherSettings,
        request_settings: crate::sinks::util::TowerRequestSettings,
    ) -> crate::Result<VectorSink> {
        let endpoint = UriSerde::from_str(&self.endpoint)?;

        // The full paths for each signal type.
        let logs_endpoint = endpoint.append_path("v1/logs")?;
        let traces_endpoint = endpoint.append_path("v1/traces")?;
        let metrics_endpoint = endpoint.append_path("v1/metrics")?;

        let service_builder = OtlpServiceRequestBuilder {
            auth: self.auth.clone(),
        };

        let service = ServiceBuilder::new()
            .settings(request_settings, http_response_retry_logic())
            .service(HttpService::new(client, service_builder));

        let request_builder = OtlpRequestBuilder::new();

        let sink = OpenTelemetrySink::new(
            service,
            batch_settings,
            request_builder,
            logs_endpoint.to_string(),
            traces_endpoint.to_string(),
            metrics_endpoint.to_string(),
        );

        Ok(VectorSink::from_event_streamsink(sink))
    }
}

async fn healthcheck(
    endpoint: String,
    auth: Option<Auth>,
    client: HttpClient,
) -> crate::Result<()> {
    // The OTLP spec does not define a healthcheck endpoint.
    // A common approach is to send a GET request to the base path,
    // which might not be a valid OTLP endpoint but can indicate reachability.
    // We expect a client or success error, but not a server error.
    let uri = Uri::try_from(endpoint)?;

    let mut request = Request::get(uri).body(hyper::Body::empty())?;

    if let Some(auth) = &auth {
        auth.apply(&mut request);
    }

    let response = client.send(request).await?;

    if response.status().is_success() || response.status().is_client_error() {
        Ok(())
    } else {
        Err(HealthcheckError::UnexpectedStatus {
            status: response.status(),
        }
        .into())
    }
}
