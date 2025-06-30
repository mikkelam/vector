//! Configuration for the `opentelemetry` sink.

use std::str::FromStr;

use http::{Method, Request, Uri};
use indexmap::IndexMap;

use crate::{
    http::{Auth, HttpClient},
    sinks::{
        prelude::*,
        util::{
            http::{http_response_retry_logic, HttpService, RequestConfig},
            service::ServiceBuilderExt,
            BatchConfig, Compression, RealtimeSizeBasedDefaultBatchSettings, UriSerde,
        },
    },
};

use super::{
    request_builder::OtlpRequestBuilder, service::OtlpServiceRequestBuilder,
    sink::OpenTelemetrySink,
};

const DEFAULT_LOGS_PATH: &str = "/v1/logs";
const DEFAULT_METRICS_PATH: &str = "/v1/metrics";
const DEFAULT_TRACES_PATH: &str = "/v1/traces";

fn default_logs_path() -> String {
    DEFAULT_LOGS_PATH.to_string()
}

fn default_metrics_path() -> String {
    DEFAULT_METRICS_PATH.to_string()
}

fn default_traces_path() -> String {
    DEFAULT_TRACES_PATH.to_string()
}

fn default_max_export_batch_size() -> usize {
    512
}

fn default_export_timeout_secs() -> u64 {
    30
}

/// HTTP method to use for OTLP requests.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum HttpMethod {
    /// POST method (recommended for OTLP).
    #[default]
    Post,
    /// PUT method.
    Put,
}

impl From<HttpMethod> for Method {
    fn from(method: HttpMethod) -> Self {
        match method {
            HttpMethod::Post => Method::POST,
            HttpMethod::Put => Method::PUT,
        }
    }
}

/// Content encoding format for OTLP data.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum ContentEncoding {
    /// Protocol Buffers binary encoding (recommended).
    #[default]
    Protobuf,
    // JSON encoding support may be added in future versions
}

fn default_health_path() -> String {
    "/health".to_string()
}

/// Healthcheck configuration for the OTLP sink.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct HealthcheckConfig {
    /// Health endpoint path (relative to base endpoint).
    ///
    /// The healthcheck will first try this path, and if it returns 404,
    /// it will fall back to the root path. Common health paths include
    /// "/health", "/healthz", and "/status".
    #[configurable(metadata(docs::examples = "/health"))]
    #[configurable(metadata(docs::examples = "/healthz"))]
    #[configurable(metadata(docs::examples = "/status"))]
    #[serde(default = "default_health_path")]
    pub path: String,

    /// Skip healthcheck entirely.
    ///
    /// This can be useful in environments where healthcheck requests
    /// are not allowed or cause issues during startup.
    #[serde(default)]
    pub skip: bool,
}

impl Default for HealthcheckConfig {
    fn default() -> Self {
        Self {
            path: default_health_path(),
            skip: false,
        }
    }
}

/// HTTP transport configuration for OTLP.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct HttpConfig {
    /// The HTTP method to use when sending requests.
    ///
    /// POST is the standard method for OTLP and is recommended for most use cases.
    #[configurable(derived)]
    #[serde(default)]
    pub method: HttpMethod,

    /// Compression algorithm to use for request bodies.
    ///
    /// Compression can significantly reduce bandwidth usage, especially for large payloads.
    /// Most OTLP collectors support gzip compression.
    #[configurable(derived)]
    #[serde(default)]
    pub compression: Compression,

    /// Content encoding format for the request body.
    ///
    /// Protocol Buffers is the standard and recommended encoding for OTLP.
    #[configurable(derived)]
    #[serde(default)]
    pub encoding: ContentEncoding,
}

/// gRPC transport configuration for OTLP.
///
/// Currently not implemented but reserved for future gRPC support.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct GrpcConfig {
    /// Compression algorithm to use for gRPC requests.
    ///
    /// gRPC has built-in compression support that's more efficient than HTTP-level compression.
    #[configurable(derived)]
    #[serde(default)]
    pub compression: Compression,

    /// Content encoding format for the request body.
    ///
    /// Protocol Buffers is the standard and recommended encoding for OTLP over gRPC.
    #[configurable(derived)]
    #[serde(default)]
    pub encoding: ContentEncoding,
}

/// OTLP-specific configuration options.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct OtlpConfig {
    /// Default resource attributes to add to all telemetry data.
    ///
    /// These attributes describe the source of the telemetry data, such as the service name,
    /// version, environment, etc. They are applied to all logs, metrics, and traces sent
    /// through this sink.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "resource_attributes_examples()"))]
    pub resource_attributes: IndexMap<String, String>,

    /// Override the service name for all telemetry data.
    ///
    /// If not specified, the service name from individual events or a default will be used.
    #[configurable(metadata(docs::examples = "my-service"))]
    pub service_name: Option<String>,

    /// Override the service version for all telemetry data.
    ///
    /// If not specified, the service version from individual events or a default will be used.
    #[configurable(metadata(docs::examples = "1.0.0"))]
    pub service_version: Option<String>,

    /// Maximum number of items to include in a single export batch.
    ///
    /// Larger batches can improve throughput but may increase memory usage and latency.
    #[serde(default = "default_max_export_batch_size")]
    #[configurable(metadata(docs::type_unit = "events"))]
    pub max_export_batch_size: usize,

    /// Maximum time to wait for a batch to be exported.
    ///
    /// This timeout applies to the HTTP request itself, not the batching process.
    #[serde(default = "default_export_timeout_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub export_timeout_secs: u64,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            resource_attributes: IndexMap::new(),
            service_name: None,
            service_version: None,
            max_export_batch_size: default_max_export_batch_size(),
            export_timeout_secs: default_export_timeout_secs(),
        }
    }
}

fn resource_attributes_examples() -> IndexMap<String, String> {
    IndexMap::from([
        ("service.name".to_string(), "my-service".to_string()),
        ("service.version".to_string(), "1.0.0".to_string()),
        (
            "deployment.environment".to_string(),
            "production".to_string(),
        ),
    ])
}

/// Configuration for the `opentelemetry` sink.
///
/// This sink sends observability data to OpenTelemetry-compatible collectors
/// using the OTLP (OpenTelemetry Protocol) specification. Currently only HTTP
/// transport is supported, but gRPC support is planned for future releases.
#[configurable_component(sink("opentelemetry", "Deliver OTLP data over HTTP."))]
#[derive(Clone, Debug)]
pub struct OpenTelemetryConfig {
    /// The base endpoint for the OTLP collector.
    ///
    /// The path values are appended to this base URL.
    #[configurable(validation(format = "uri"))]
    #[configurable(metadata(docs::examples = "http://localhost:4318"))]
    pub endpoint: String,

    /// The path to use for logs.
    #[serde(default = "default_logs_path")]
    #[configurable(metadata(docs::examples = "/v1/logs"))]
    #[configurable(metadata(docs::examples = "/custom/logs"))]
    pub logs_path: String,

    /// The path to use for metrics.
    #[serde(default = "default_metrics_path")]
    #[configurable(metadata(docs::examples = "/v1/metrics"))]
    #[configurable(metadata(docs::examples = "/custom/metrics"))]
    pub metrics_path: String,

    /// The path to use for traces.
    #[serde(default = "default_traces_path")]
    #[configurable(metadata(docs::examples = "/v1/traces"))]
    #[configurable(metadata(docs::examples = "/custom/traces"))]
    pub traces_path: String,

    /// The protocol to use for sending data.
    ///
    /// Currently only HTTP is supported. gRPC support is planned for future releases.
    #[configurable(derived)]
    #[serde(default)]
    pub protocol: OtlpProtocol,

    /// HTTP transport configuration.
    ///
    /// Configuration options for HTTP transport including method, compression,
    /// and encoding settings. Only used when protocol is set to HTTP.
    #[configurable(derived)]
    #[serde(default)]
    pub http: HttpConfig,

    /// gRPC transport configuration.
    ///
    /// Configuration options for gRPC transport including compression and encoding.
    /// Currently reserved for future gRPC support.
    #[configurable(derived)]
    #[serde(default)]
    pub grpc: GrpcConfig,

    /// OTLP-specific configuration options.
    ///
    /// These options control OTLP-specific behavior such as resource attributes,
    /// service identification, and export settings.
    #[configurable(derived)]
    #[serde(default)]
    pub otlp: OtlpConfig,

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

    /// Healthcheck configuration.
    ///
    /// Configures how the sink performs healthchecks to verify connectivity
    /// to the OTLP endpoint before starting to send data.
    #[configurable(derived)]
    #[serde(default)]
    pub healthcheck: HealthcheckConfig,
}

/// The protocol used to send OTLP data.
///
/// Currently only HTTP is supported, but gRPC support is planned for future releases.
/// The OTLP specification supports both HTTP and gRPC transports, and this enum
/// will be extended to include gRPC once the implementation is complete.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub enum OtlpProtocol {
    /// Send data over HTTP with Protobuf encoding.
    ///
    /// Uses the OTLP/HTTP protocol as defined in the OpenTelemetry specification.
    /// Data is sent as binary-encoded Protocol Buffers over HTTP POST requests.
    #[default]
    Http,
    // Grpc, // TODO: gRPC support to be implemented in future releases
}

impl Default for OpenTelemetryConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4318".to_string(),
            logs_path: DEFAULT_LOGS_PATH.to_string(),
            metrics_path: DEFAULT_METRICS_PATH.to_string(),
            traces_path: DEFAULT_TRACES_PATH.to_string(),
            protocol: OtlpProtocol::default(),
            http: HttpConfig::default(),
            grpc: GrpcConfig::default(),
            otlp: OtlpConfig::default(),
            auth: None,
            request: RequestConfig::default(),
            batch: BatchConfig::default(),
            tls: None,
            acknowledgements: AcknowledgementsConfig::default(),
            healthcheck: HealthcheckConfig::default(),
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

        let healthcheck = healthcheck(
            self.endpoint.clone(),
            self.healthcheck.clone(),
            self.auth.clone(),
            client.clone(),
        )
        .boxed();

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
        let logs_endpoint = endpoint.append_path(&self.logs_path.trim_start_matches('/'))?;
        let traces_endpoint = endpoint.append_path(&self.traces_path.trim_start_matches('/'))?;
        let metrics_endpoint = endpoint.append_path(&self.metrics_path.trim_start_matches('/'))?;

        let service_builder = OtlpServiceRequestBuilder {
            auth: self.auth.clone(),
            method: self.http.method,
            compression: self.http.compression.clone(),
            encoding: self.http.encoding,
        };

        let service = ServiceBuilder::new()
            .settings(request_settings, http_response_retry_logic())
            .service(HttpService::new(client, service_builder));

        let request_builder =
            OtlpRequestBuilder::new(self.http.compression.clone(), self.otlp.clone());

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

async fn fetch_health_status(
    path: &str,
    endpoint: &str,
    auth: &Option<Auth>,
    client: &HttpClient,
) -> crate::Result<http::StatusCode> {
    let url = format!("{}{}", endpoint.trim_end_matches('/'), path);
    let uri = Uri::try_from(url)?;
    let mut request = Request::get(uri).body(hyper::Body::empty())?;

    if let Some(auth) = auth {
        auth.apply(&mut request);
    }

    Ok(client.send(request).await?.status())
}

async fn healthcheck(
    endpoint: String,
    healthcheck_config: HealthcheckConfig,
    auth: Option<Auth>,
    client: HttpClient,
) -> crate::Result<()> {
    if healthcheck_config.skip {
        return Ok(());
    }

    // Try the configured health endpoint first
    let health_status =
        fetch_health_status(&healthcheck_config.path, &endpoint, &auth, &client).await?;

    let status = match health_status {
        http::StatusCode::NOT_FOUND => {
            debug!(
                "Health endpoint '{}' not found. Trying root path.",
                healthcheck_config.path
            );
            fetch_health_status("/", &endpoint, &auth, &client).await?
        }
        status => status,
    };

    // Accept success responses and client errors (like 404, 405)
    // Client errors indicate the service is reachable but the endpoint doesn't exist
    // or doesn't support the method, which is fine for a connectivity check
    match status {
        s if s.is_success() || s.is_client_error() => Ok(()),
        other => Err(HealthcheckError::UnexpectedStatus { status: other }.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_healthcheck_config_defaults() {
        let config = HealthcheckConfig::default();
        assert_eq!(config.path, "/health");
        assert!(!config.skip);
    }

    #[test]
    fn test_opentelemetry_config_defaults() {
        let config = OpenTelemetryConfig::default();
        assert_eq!(config.healthcheck.path, "/health");
        assert!(!config.healthcheck.skip);
    }

    #[test]
    fn test_healthcheck_config_serialization() {
        let config_str = r#"
            skip = true
            path = "/custom-health"
        "#;

        let config: HealthcheckConfig = toml::from_str(config_str).unwrap();
        assert_eq!(config.path, "/custom-health");
        assert!(config.skip);
    }

    #[test]
    fn test_full_config_with_healthcheck() {
        let config_str = r#"
            endpoint = "http://localhost:4318"

            [healthcheck]
            path = "/status"
            skip = false
        "#;

        let config: OpenTelemetryConfig = toml::from_str(config_str).unwrap();
        assert_eq!(config.endpoint, "http://localhost:4318");
        assert_eq!(config.healthcheck.path, "/status");
        assert!(!config.healthcheck.skip);
    }
}
