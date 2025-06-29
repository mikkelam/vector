//! Unit and integration tests for the `opentelemetry` sink.

use futures::stream;
use http::{Request, Response};
use hyper::Body;
use prost::Message;
use rstest::rstest;
use tower::service_fn;
use vrl::event_path;

use crate::{
    config::{SinkConfig, SinkContext},
    event::{Event, LogEvent, Metric, MetricKind, MetricValue},
    sinks::{
        opentelemetry::{config::OpenTelemetryConfig, encoder::OtlpEncoder, sink::KeyPartitioner},
        prelude::*,
    },
    test_util::{
        components::{run_and_assert_sink_compliance, SINK_TAGS},
        trace_init,
    },
};
use vector_lib::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;

#[test]
fn generate_config() {
    crate::test_util::test_generate_config::<OpenTelemetryConfig>();
}

#[rstest]
#[case(
    Event::Log(LogEvent::from("hello world")),
    "http://localhost:4318/v1/logs"
)]
#[case(
    Event::Metric(Metric::new(
        "test_metric",
        MetricKind::Absolute,
        MetricValue::Counter { value: 1.0 }
    )),
    "http://localhost:4318/v1/metrics"
)]
// TODO: Add trace event test case once trace event generation is easier
fn test_partitioner(#[case] event: Event, #[case] expected_endpoint: &str) {
    let partitioner = KeyPartitioner::new(
        "http://localhost:4318/v1/logs".to_string(),
        "http://localhost:4318/v1/traces".to_string(),
        "http://localhost:4318/v1/metrics".to_string(),
    );
    let key = partitioner.partition(&event);
    assert_eq!(key.endpoint, expected_endpoint);
}

#[tokio::test]
async fn test_http_log_request() {
    trace_init();

    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

    // Mock HTTP server
    let server_handle = tokio::spawn(async move {
        let service = service_fn(move |req: Request<Body>| {
            let tx = sender.clone();
            async move {
                let (parts, body) = req.into_parts();
                let body_bytes = hyper::body::to_bytes(body).await.unwrap();
                tx.send((parts, body_bytes)).await.unwrap();
                Ok::<_, hyper::Error>(Response::new(Body::empty()))
            }
        });
        let addr = ([127, 0, 0, 1], 0).into();
        let server = hyper::Server::bind(&addr).serve(tower::make::Shared::new(service));
        let local_addr = server.local_addr();
        tokio::spawn(server);
        local_addr
    });

    let server_addr = server_handle.await.unwrap();

    let config_str = format!(
        r#"
endpoint = "http://{}"
"#,
        server_addr
    );

    let config: OpenTelemetryConfig = toml::from_str(&config_str).unwrap();
    let cx = SinkContext::default();
    let (sink, _healthcheck) = config.build(cx).await.unwrap();

    let mut log = LogEvent::from("hello otlp");
    log.insert("host", "example.com");
    log.insert(event_path!("resource", "service.name"), "vector-test-suite");

    let event = Event::Log(log);
    run_and_assert_sink_compliance(sink, stream::once(async { event }), &SINK_TAGS).await;

    // Assert on the received request
    let (parts, body_bytes) = receiver.recv().await.unwrap();

    assert_eq!(parts.method, http::Method::POST);
    assert_eq!(parts.uri.path(), "/v1/logs");
    assert_eq!(
        parts.headers.get("content-type").unwrap(),
        "application/x-protobuf"
    );

    let request = ExportLogsServiceRequest::decode(body_bytes).unwrap();

    assert_eq!(request.resource_logs.len(), 1);
    let resource_log = &request.resource_logs[0];

    // Check resource attributes
    let resource = resource_log.resource.as_ref().unwrap();
    assert_eq!(resource.attributes.len(), 1);
    let resource_attr = &resource.attributes[0];
    assert_eq!(resource_attr.key, "service.name");
    assert_eq!(
        resource_attr.value.as_ref().unwrap().value,
        Some(
            vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                "vector-test-suite".to_string()
            )
        )
    );

    // Check log records
    assert_eq!(resource_log.scope_logs.len(), 1);
    let scope_log = &resource_log.scope_logs[0];
    assert_eq!(scope_log.log_records.len(), 1);
    let log_record = &scope_log.log_records[0];

    // Check body
    let body_value = log_record.body.as_ref().unwrap();
    assert_eq!(
        body_value.value,
        Some(
            vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                "hello otlp".to_string()
            )
        )
    );

    // Check log attributes
    assert_eq!(log_record.attributes.len(), 1);
    let log_attr = &log_record.attributes[0];
    assert_eq!(log_attr.key, "host");
    assert_eq!(
        log_attr.value.as_ref().unwrap().value,
        Some(
            vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                "example.com".to_string()
            )
        )
    );
}

#[test]
fn test_encoder_resource_extraction() {
    let encoder = OtlpEncoder;

    let mut log = LogEvent::from("test message");
    log.insert("host", "example.com");
    log.insert(event_path!("resource", "service.name"), "vector-test-suite");

    let events = vec![Event::Log(log)];
    let result = encoder.encode_logs(events).unwrap();

    // Decode the protobuf to verify structure
    let request = ExportLogsServiceRequest::decode(result.as_ref()).unwrap();

    // Check that we have resource attributes
    assert_eq!(request.resource_logs.len(), 1);
    let resource_log = &request.resource_logs[0];
    let resource = resource_log.resource.as_ref().unwrap();

    // Should have the service.name resource attribute
    assert_eq!(resource.attributes.len(), 1);
    let resource_attr = &resource.attributes[0];
    assert_eq!(resource_attr.key, "service.name");
    assert_eq!(
        resource_attr.value.as_ref().unwrap().value,
        Some(
            vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                "vector-test-suite".to_string()
            )
        )
    );
}
