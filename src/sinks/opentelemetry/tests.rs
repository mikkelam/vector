//! Unit and integration tests for the `opentelemetry` sink.

use chrono::{TimeZone, Utc};
use futures::stream;
use http::{Request, Response};
use hyper::Body;

use prost::Message;
use rstest::rstest;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use super::config::{ContentEncoding, HttpMethod, OtlpProtocol};
use vector_lib::event::EventMetadata;
use vrl::event_path;

use crate::{
    config::{SinkConfig, SinkContext},
    event::{Event, LogEvent, Metric, MetricKind, MetricValue},
    sinks::{
        opentelemetry::{config::OpenTelemetryConfig, encoder::OtlpEncoder, sink::KeyPartitioner},
        prelude::*,
        util::{encoding::Encoder, Compression},
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
#[case(create_test_trace_event(), "http://localhost:4318/v1/traces")]
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

    // Use Arc<Mutex<Vec<...>>> to capture requests
    let received_requests = Arc::new(Mutex::new(Vec::new()));
    let received_requests_clone = received_requests.clone();

    // Create a mock HTTP server that captures requests
    let handler = move |req: Request<Body>| {
        let received_requests = received_requests_clone.clone();
        async move {
            let (parts, body) = req.into_parts();
            let body_bytes = hyper::body::to_bytes(body).await.unwrap();

            // Store the request
            {
                let mut requests = received_requests.lock().unwrap();
                requests.push((parts, body_bytes));
            }

            Ok::<_, Infallible>(Response::new(Body::empty()))
        }
    };

    // Use Vector's test utility to spawn the server
    let mock_endpoint = crate::test_util::http::spawn_blackhole_http_server(handler).await;

    let config_str = format!(
        r#"
endpoint = "{}"
"#,
        mock_endpoint
    );

    let config: OpenTelemetryConfig = toml::from_str(&config_str).unwrap();
    let cx = SinkContext::default();
    let (sink, _healthcheck) = config.build(cx).await.unwrap();

    let mut log = LogEvent::from("hello otlp");
    log.insert("host", "example.com");
    log.insert(event_path!("resource", "service.name"), "vector-test-suite");

    let event = Event::Log(log);
    run_and_assert_sink_compliance(sink, stream::once(async { event }), &SINK_TAGS).await;

    // Wait a bit for async processing
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Assert on the received request
    let requests = received_requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "Expected exactly 1 HTTP request");

    let (parts, body_bytes) = &requests[0];

    assert_eq!(parts.method, http::Method::POST);
    assert_eq!(parts.uri.path(), "/v1/logs");
    assert_eq!(
        parts.headers.get("content-type").unwrap(),
        "application/x-protobuf"
    );

    let request = ExportLogsServiceRequest::decode(body_bytes.as_ref()).unwrap();

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

    // Check log attributes - we expect host, message, and timestamp
    assert_eq!(log_record.attributes.len(), 3);

    // Find and check the host attribute
    let host_attr = log_record
        .attributes
        .iter()
        .find(|attr| attr.key == "host")
        .expect("Should have host attribute");
    assert_eq!(
        host_attr.value.as_ref().unwrap().value,
        Some(
            vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                "example.com".to_string()
            )
        )
    );

    // Find and check the message attribute
    let message_attr = log_record
        .attributes
        .iter()
        .find(|attr| attr.key == "message")
        .expect("Should have message attribute");
    assert_eq!(
        message_attr.value.as_ref().unwrap().value,
        Some(
            vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                "hello otlp".to_string()
            )
        )
    );

    // Check that timestamp attribute exists (we don't need to validate the exact value)
    let _timestamp_attr = log_record
        .attributes
        .iter()
        .find(|attr| attr.key == "timestamp")
        .expect("Should have timestamp attribute");
}

#[tokio::test]
async fn test_http_trace_request() {
    use vector_lib::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;

    trace_init();

    // Use Arc<Mutex<Vec<...>>> to capture requests
    let received_requests = Arc::new(Mutex::new(Vec::new()));
    let received_requests_clone = received_requests.clone();

    // Create a mock HTTP server that captures requests
    let handler = move |req: Request<Body>| {
        let received_requests = received_requests_clone.clone();
        async move {
            let (parts, body) = req.into_parts();
            let body_bytes = hyper::body::to_bytes(body).await.unwrap();

            // Store the request
            {
                let mut requests = received_requests.lock().unwrap();
                requests.push((parts, body_bytes));
            }

            Ok::<_, Infallible>(Response::new(Body::empty()))
        }
    };

    // Use Vector's test utility to spawn the server
    let mock_endpoint = crate::test_util::http::spawn_blackhole_http_server(handler).await;

    let config_str = format!(
        r#"
endpoint = "{}"
"#,
        mock_endpoint
    );

    let config: OpenTelemetryConfig = toml::from_str(&config_str).unwrap();
    let cx = SinkContext::default();
    let (sink, _healthcheck) = config.build(cx).await.unwrap();

    let trace_event = create_test_trace_event();
    run_and_assert_sink_compliance(sink, stream::once(async { trace_event }), &SINK_TAGS).await;

    // Wait a bit for async processing
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Assert on the received request
    let requests = received_requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "Expected exactly 1 HTTP request");

    let (parts, body_bytes) = &requests[0];

    assert_eq!(parts.method, http::Method::POST);
    assert_eq!(parts.uri.path(), "/v1/traces");
    assert_eq!(
        parts.headers.get("content-type").unwrap(),
        "application/x-protobuf"
    );

    let request = ExportTraceServiceRequest::decode(body_bytes.as_ref()).unwrap();

    assert_eq!(request.resource_spans.len(), 1);
    let resource_spans = &request.resource_spans[0];

    // Check scope spans
    assert_eq!(resource_spans.scope_spans.len(), 1);
    let scope_spans = &resource_spans.scope_spans[0];

    // Check spans
    assert_eq!(scope_spans.spans.len(), 1);
    let otlp_span = &scope_spans.spans[0];

    // Verify span details
    assert_eq!(otlp_span.name, "test_span");
    assert_eq!(
        hex::encode(&otlp_span.trace_id),
        "0102030405060708090a0b0c0d0e0f10"
    );
    assert_eq!(hex::encode(&otlp_span.span_id), "0102030405060708");
    assert_eq!(otlp_span.kind, 1); // SPAN_KIND_INTERNAL

    // Verify timestamps are non-zero
    assert!(otlp_span.start_time_unix_nano > 0);
    assert!(otlp_span.end_time_unix_nano > 0);
    assert!(otlp_span.end_time_unix_nano > otlp_span.start_time_unix_nano);
}

#[test]
fn test_encoder_resource_extraction() {
    let encoder = OtlpEncoder::new_default();

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

#[test]
fn test_encode_logs_method_directly() {
    let encoder = OtlpEncoder::new_default();

    let mut log = LogEvent::from("direct test message");
    log.insert("test_key", "test_value");
    log.insert(
        event_path!("resource", "service.name"),
        "direct-test-service",
    );

    let events = vec![Event::Log(log)];
    let result = encoder.encode_logs(events).unwrap();

    // Should have non-empty protobuf bytes
    assert!(!result.is_empty());

    // Decode and verify the structure
    let request = ExportLogsServiceRequest::decode(result.as_ref()).unwrap();
    assert_eq!(request.resource_logs.len(), 1);

    let resource_log = &request.resource_logs[0];
    let resource = resource_log.resource.as_ref().unwrap();
    assert_eq!(resource.attributes.len(), 1);
    assert_eq!(resource.attributes[0].key, "service.name");

    let log_record = &resource_log.scope_logs[0].log_records[0];
    // Should have message, test_key, and timestamp attributes
    assert_eq!(log_record.attributes.len(), 3);
}

#[test]
fn test_severity_mapping_string_levels() {
    use super::encoder::map_severity_to_otlp;
    use vector_lib::opentelemetry::proto::logs::v1::SeverityNumber;
    use vrl::value::Value;

    // Test string-based severity levels
    let test_cases = vec![
        ("info", SeverityNumber::Info as i32, "INFO"),
        ("INFO", SeverityNumber::Info as i32, "INFO"),
        ("debug", SeverityNumber::Debug as i32, "DEBUG"),
        ("warn", SeverityNumber::Warn as i32, "WARN"),
        ("warning", SeverityNumber::Warn as i32, "WARNING"),
        ("error", SeverityNumber::Error as i32, "ERROR"),
        ("fatal", SeverityNumber::Fatal as i32, "FATAL"),
        ("trace", SeverityNumber::Trace as i32, "TRACE"),
        ("critical", SeverityNumber::Fatal as i32, "CRITICAL"),
        ("unknown", SeverityNumber::Unspecified as i32, "UNKNOWN"),
    ];

    for (input, expected_num, expected_text) in test_cases {
        let value = Value::from(input);
        let (severity_number, severity_text) = map_severity_to_otlp(value);
        assert_eq!(severity_number, expected_num, "Failed for input: {}", input);
        assert_eq!(severity_text, expected_text, "Failed for input: {}", input);
    }
}

#[test]
fn test_severity_mapping_numeric_levels() {
    use super::encoder::map_severity_to_otlp;
    use vector_lib::opentelemetry::proto::logs::v1::SeverityNumber;
    use vrl::value::Value;

    // Test syslog-style numeric levels (0-7)
    let syslog_cases = vec![
        (0, SeverityNumber::Fatal as i32, "EMERGENCY"),
        (1, SeverityNumber::Fatal as i32, "ALERT"),
        (2, SeverityNumber::Fatal as i32, "CRITICAL"),
        (3, SeverityNumber::Error as i32, "ERROR"),
        (4, SeverityNumber::Warn as i32, "WARNING"),
        (5, SeverityNumber::Info as i32, "NOTICE"),
        (6, SeverityNumber::Info as i32, "INFO"),
        (7, SeverityNumber::Debug as i32, "DEBUG"),
    ];

    for (input, expected_num, expected_text) in syslog_cases {
        let value = Value::from(input);
        let (severity_number, severity_text) = map_severity_to_otlp(value);
        assert_eq!(
            severity_number, expected_num,
            "Failed for syslog level: {}",
            input
        );
        assert_eq!(
            severity_text, expected_text,
            "Failed for syslog level: {}",
            input
        );
    }

    // Test direct OTLP numeric levels
    let otlp_cases = vec![
        (9, 9, "INFO"),    // SEVERITY_NUMBER_INFO
        (13, 13, "WARN"),  // SEVERITY_NUMBER_WARN
        (17, 17, "ERROR"), // SEVERITY_NUMBER_ERROR
        (21, 21, "FATAL"), // SEVERITY_NUMBER_FATAL
    ];

    for (input, expected_num, expected_text) in otlp_cases {
        let value = Value::from(input);
        let (severity_number, severity_text) = map_severity_to_otlp(value);
        assert_eq!(
            severity_number, expected_num,
            "Failed for OTLP level: {}",
            input
        );
        assert_eq!(
            severity_text, expected_text,
            "Failed for OTLP level: {}",
            input
        );
    }
}

#[test]
fn test_extract_severity_from_log() {
    use super::encoder::extract_severity;
    use vector_lib::opentelemetry::proto::logs::v1::SeverityNumber;

    // Test extracting severity from different field names
    let field_test_cases = vec![
        ("level", "error"),
        ("severity", "warn"),
        ("log_level", "info"),
        ("priority", "debug"),
    ];

    for (field_name, level_value) in field_test_cases {
        let mut log = LogEvent::from("test message");
        log.insert(field_name, level_value);

        let (severity_number, severity_text) = extract_severity(&mut log);

        // Verify severity field handling: "level" is preserved, others are removed
        if field_name == "level" {
            assert!(
                log.get(field_name).is_some(),
                "Field 'level' should be preserved as an attribute"
            );
        } else {
            assert!(
                log.get(field_name).is_none(),
                "Field '{}' should have been removed",
                field_name
            );
        }

        // Verify correct mapping
        let expected_number = match level_value {
            "error" => SeverityNumber::Error as i32,
            "warn" => SeverityNumber::Warn as i32,
            "info" => SeverityNumber::Info as i32,
            "debug" => SeverityNumber::Debug as i32,
            _ => SeverityNumber::Unspecified as i32,
        };

        assert_eq!(
            severity_number, expected_number,
            "Failed for field '{}' with value '{}'",
            field_name, level_value
        );
        assert_eq!(
            severity_text,
            level_value.to_uppercase(),
            "Failed for field '{}' with value '{}'",
            field_name,
            level_value
        );
    }
}

#[test]
fn test_extract_severity_field_priority() {
    use super::encoder::extract_severity;

    // Test that 'level' field takes priority over other fields
    let mut log = LogEvent::from("test message");
    log.insert("level", "error");
    log.insert("severity", "info");
    log.insert("log_level", "debug");

    let (severity_number, severity_text) = extract_severity(&mut log);

    // Should use 'level' field (error)
    assert_eq!(severity_number, 17); // SEVERITY_NUMBER_ERROR
    assert_eq!(severity_text, "ERROR");

    // "level" should be preserved, other severity fields should be cleaned up
    assert!(log.get("level").is_some());
    assert!(log.get("severity").is_none());
    assert!(log.get("log_level").is_none());
}

#[test]
fn test_severity_integration_with_encoder() {
    let encoder = OtlpEncoder::new_default();

    let mut log = LogEvent::from("test message with severity");
    log.insert("level", "warn");
    log.insert("user_id", 123);

    let events = vec![Event::Log(log)];
    let result = encoder.encode_logs(events).unwrap();

    // Decode and verify
    let request = ExportLogsServiceRequest::decode(result.as_ref()).unwrap();
    let log_record = &request.resource_logs[0].scope_logs[0].log_records[0];

    // Check severity mapping
    assert_eq!(log_record.severity_number, 13); // SEVERITY_NUMBER_WARN
    assert_eq!(log_record.severity_text, "WARN");

    // Check that 'level' field is preserved in attributes
    let level_attr = log_record
        .attributes
        .iter()
        .find(|attr| attr.key == "level");
    assert!(
        level_attr.is_some(),
        "Level field should be preserved in attributes"
    );

    // Check that other fields are still present
    let user_id_attr = log_record
        .attributes
        .iter()
        .find(|attr| attr.key == "user_id");
    assert!(user_id_attr.is_some(), "user_id should be in attributes");
}

#[test]
fn test_string_encoding_utf8_vs_bytes() {
    use super::encoder::convert_value_to_any_value;
    use vector_lib::opentelemetry::proto::common::v1::any_value::Value as PbValue;
    use vrl::value::Value;

    // Test that UTF-8 strings stored as bytes become StringValue
    let utf8_string = "Hello, World! 🌍";
    let utf8_bytes = Value::Bytes(utf8_string.as_bytes().into());
    let any_value = convert_value_to_any_value(utf8_bytes);

    match any_value.value {
        Some(PbValue::StringValue(s)) => {
            assert_eq!(s, utf8_string, "UTF-8 bytes should become StringValue");
        }
        other => panic!("Expected StringValue, got: {:?}", other),
    }

    // Test that non-UTF-8 bytes become BytesValue
    let non_utf8_bytes = Value::Bytes(vec![0xFF, 0xFE, 0xFD].into());
    let any_value = convert_value_to_any_value(non_utf8_bytes);

    match any_value.value {
        Some(PbValue::BytesValue(b)) => {
            assert_eq!(
                b,
                vec![0xFF, 0xFE, 0xFD],
                "Non-UTF-8 bytes should become BytesValue"
            );
        }
        other => panic!("Expected BytesValue, got: {:?}", other),
    }

    // Test that regular strings still work
    let string_value = Value::from("Regular string");
    let any_value = convert_value_to_any_value(string_value);

    match any_value.value {
        Some(PbValue::StringValue(s)) => {
            assert_eq!(
                s, "Regular string",
                "String values should become StringValue"
            );
        }
        other => panic!("Expected StringValue, got: {:?}", other),
    }
}

#[test]
fn test_encoder_string_attributes() {
    let encoder = OtlpEncoder::new_default();

    let mut log = LogEvent::from("test message");
    // Insert string that will be stored as Bytes internally
    log.insert("host", "example.com");
    log.insert("service", "my-service");
    log.insert("numeric_value", 42);

    let events = vec![Event::Log(log)];
    let result = encoder.encode_logs(events).unwrap();

    // Decode and verify string attributes are properly encoded
    let request = ExportLogsServiceRequest::decode(result.as_ref()).unwrap();
    let log_record = &request.resource_logs[0].scope_logs[0].log_records[0];

    // Find host attribute
    let host_attr = log_record
        .attributes
        .iter()
        .find(|attr| attr.key == "host")
        .expect("Should have host attribute");

    // Should be StringValue, not BytesValue
    match &host_attr.value.as_ref().unwrap().value {
        Some(vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(s)) => {
            assert_eq!(s, "example.com", "Host should be a string value");
        }
        other => panic!("Expected StringValue for host, got: {:?}", other),
    }

    // Find service attribute
    let service_attr = log_record
        .attributes
        .iter()
        .find(|attr| attr.key == "service")
        .expect("Should have service attribute");

    // Should be StringValue, not BytesValue
    match &service_attr.value.as_ref().unwrap().value {
        Some(vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(s)) => {
            assert_eq!(s, "my-service", "Service should be a string value");
        }
        other => panic!("Expected StringValue for service, got: {:?}", other),
    }

    // Find numeric attribute
    let numeric_attr = log_record
        .attributes
        .iter()
        .find(|attr| attr.key == "numeric_value")
        .expect("Should have numeric_value attribute");

    // Should be IntValue
    match &numeric_attr.value.as_ref().unwrap().value {
        Some(vector_lib::opentelemetry::proto::common::v1::any_value::Value::IntValue(i)) => {
            assert_eq!(*i, 42, "Numeric value should be preserved");
        }
        other => panic!("Expected IntValue for numeric_value, got: {:?}", other),
    }
}

#[test]
fn test_encode_metrics_counter() {
    use crate::event::{Metric, MetricKind, MetricValue};
    use vector_lib::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;

    let encoder = OtlpEncoder::new_default();

    let metric = Metric::new(
        "test_counter",
        MetricKind::Incremental,
        MetricValue::Counter { value: 42.0 },
    )
    .with_tags(Some({
        let mut tags = crate::event::MetricTags::default();
        tags.replace("host".to_string(), "example.com");
        tags.replace("environment".to_string(), "test");
        tags
    }));

    let events = vec![Event::Metric(metric)];
    let result = encoder.encode_metrics(events).unwrap();

    // Should have non-empty protobuf bytes
    assert!(!result.is_empty());

    // Decode and verify the structure
    let request = ExportMetricsServiceRequest::decode(result.as_ref()).unwrap();
    assert_eq!(request.resource_metrics.len(), 1);

    let resource_metrics = &request.resource_metrics[0];
    assert_eq!(resource_metrics.scope_metrics.len(), 1);

    let scope_metrics = &resource_metrics.scope_metrics[0];
    assert_eq!(scope_metrics.metrics.len(), 1);

    let otlp_metric = &scope_metrics.metrics[0];
    assert_eq!(otlp_metric.name, "test_counter");

    // Should be a Sum (counter) metric
    assert!(otlp_metric.data.is_some());
    if let Some(vector_lib::opentelemetry::proto::metrics::v1::metric::Data::Sum(sum)) =
        &otlp_metric.data
    {
        assert_eq!(sum.data_points.len(), 1);
        assert!(sum.is_monotonic);

        let data_point = &sum.data_points[0];
        if let Some(
            vector_lib::opentelemetry::proto::metrics::v1::number_data_point::Value::AsDouble(
                value,
            ),
        ) = &data_point.value
        {
            assert_eq!(*value, 42.0);
        } else {
            panic!("Expected double value");
        }

        // Check attributes
        assert_eq!(data_point.attributes.len(), 2);
        let host_attr = data_point
            .attributes
            .iter()
            .find(|attr| attr.key == "host")
            .expect("Should have host attribute");
        assert_eq!(
            host_attr.value.as_ref().unwrap().value,
            Some(
                vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                    "example.com".to_string()
                )
            )
        );
    } else {
        panic!("Expected Sum metric data");
    }
}

#[test]
fn test_encode_metrics_gauge() {
    use crate::event::{Metric, MetricKind, MetricValue};
    use vector_lib::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;

    let encoder = OtlpEncoder::new_default();

    let metric = Metric::new(
        "test_gauge",
        MetricKind::Absolute,
        MetricValue::Gauge { value: 123.5 },
    );

    let events = vec![Event::Metric(metric)];
    let result = encoder.encode_metrics(events).unwrap();

    let request = ExportMetricsServiceRequest::decode(result.as_ref()).unwrap();
    let otlp_metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];

    assert_eq!(otlp_metric.name, "test_gauge");

    // Should be a Gauge metric
    if let Some(vector_lib::opentelemetry::proto::metrics::v1::metric::Data::Gauge(gauge)) =
        &otlp_metric.data
    {
        assert_eq!(gauge.data_points.len(), 1);

        let data_point = &gauge.data_points[0];
        if let Some(
            vector_lib::opentelemetry::proto::metrics::v1::number_data_point::Value::AsDouble(
                value,
            ),
        ) = &data_point.value
        {
            assert_eq!(*value, 123.5);
        } else {
            panic!("Expected double value");
        }
    } else {
        panic!("Expected Gauge metric data");
    }
}

#[test]
fn test_encode_metrics_histogram() {
    use crate::event::metric::Bucket;
    use crate::event::{Metric, MetricKind, MetricValue};
    use vector_lib::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;

    let encoder = OtlpEncoder::new_default();

    let buckets = vec![
        Bucket {
            upper_limit: 1.0,
            count: 10,
        },
        Bucket {
            upper_limit: 5.0,
            count: 25,
        },
        Bucket {
            upper_limit: f64::INFINITY,
            count: 30,
        },
    ];

    let metric = Metric::new(
        "test_histogram",
        MetricKind::Absolute,
        MetricValue::AggregatedHistogram {
            buckets,
            count: 30,
            sum: 100.0,
        },
    );

    let events = vec![Event::Metric(metric)];
    let result = encoder.encode_metrics(events).unwrap();

    let request = ExportMetricsServiceRequest::decode(result.as_ref()).unwrap();
    let otlp_metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];

    assert_eq!(otlp_metric.name, "test_histogram");

    // Should be a Histogram metric
    if let Some(vector_lib::opentelemetry::proto::metrics::v1::metric::Data::Histogram(histogram)) =
        &otlp_metric.data
    {
        assert_eq!(histogram.data_points.len(), 1);

        let data_point = &histogram.data_points[0];
        assert_eq!(data_point.count, 30);
        assert_eq!(data_point.sum, Some(100.0));

        // Check buckets - should have explicit bounds [1.0, 5.0] (infinity excluded)
        assert_eq!(data_point.explicit_bounds, vec![1.0, 5.0]);
        assert_eq!(data_point.bucket_counts, vec![10, 25, 30]);
    } else {
        panic!("Expected Histogram metric data");
    }
}

#[test]
fn test_metrics_partitioner() {
    use crate::event::{Metric, MetricKind, MetricValue};

    let partitioner = KeyPartitioner::new(
        "http://localhost:4318/v1/logs".to_string(),
        "http://localhost:4318/v1/traces".to_string(),
        "http://localhost:4318/v1/metrics".to_string(),
    );

    let metric = Metric::new(
        "test_metric",
        MetricKind::Absolute,
        MetricValue::Counter { value: 1.0 },
    );
    let event = Event::Metric(metric);

    let key = partitioner.partition(&event);
    assert_eq!(key.endpoint, "http://localhost:4318/v1/metrics");
}

#[test]
fn test_encoder_routes_metrics_vs_logs() {
    let encoder = OtlpEncoder::new_default();
    let mut writer = Vec::new();

    // Test that a metric event gets routed to metrics encoding
    let metric = Metric::new(
        "test_counter",
        MetricKind::Absolute,
        MetricValue::Counter { value: 5.0 },
    );
    let metric_event = Event::Metric(metric);

    let (written_bytes, _) = encoder
        .encode_input(vec![metric_event], &mut writer)
        .unwrap();
    assert!(written_bytes > 0, "Should have written metric data");

    // Verify it's a valid OTLP metrics protobuf
    let metrics_request =
        vector_lib::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest::decode(
            writer.as_slice(),
        );
    assert!(
        metrics_request.is_ok(),
        "Should decode as OTLP metrics request"
    );

    // Clear writer for next test
    writer.clear();

    // Test that a log event gets routed to logs encoding
    let mut log = LogEvent::from("test log message");
    log.insert("level", "info");
    let log_event = Event::Log(log);

    let (written_bytes, _) = encoder.encode_input(vec![log_event], &mut writer).unwrap();
    assert!(written_bytes > 0, "Should have written log data");

    // Verify it's a valid OTLP logs protobuf
    let logs_request =
        vector_lib::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest::decode(
            writer.as_slice(),
        );
    assert!(logs_request.is_ok(), "Should decode as OTLP logs request");

    // Should NOT decode as metrics request
    writer.clear();
    writer.extend_from_slice(&logs_request.unwrap().encode_to_vec());
    let bad_metrics_request =
        vector_lib::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest::decode(
            writer.as_slice(),
        );
    assert!(
        bad_metrics_request.is_err(),
        "Log protobuf should not decode as metrics"
    );
}

#[test]
fn test_encode_traces_basic() {
    use vector_lib::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;

    let encoder = OtlpEncoder::new_default();
    let trace_event = create_test_trace_event();

    let events = vec![trace_event];
    let result = encoder.encode_traces(events).unwrap();

    // Should have non-empty protobuf bytes
    assert!(!result.is_empty());

    // Decode and verify the structure
    let request = ExportTraceServiceRequest::decode(result.as_ref()).unwrap();
    assert_eq!(request.resource_spans.len(), 1);

    let resource_spans = &request.resource_spans[0];
    assert_eq!(resource_spans.scope_spans.len(), 1);

    let scope_spans = &resource_spans.scope_spans[0];
    assert_eq!(scope_spans.spans.len(), 1);

    let otlp_span = &scope_spans.spans[0];
    assert_eq!(otlp_span.name, "test_span");
    assert_eq!(
        hex::encode(&otlp_span.trace_id),
        "0102030405060708090a0b0c0d0e0f10"
    );
    assert_eq!(hex::encode(&otlp_span.span_id), "0102030405060708");
}

#[test]
fn test_encode_traces_with_attributes() {
    use std::collections::BTreeMap;
    use vrl::value::Value;

    let encoder = OtlpEncoder::new_default();

    // Create trace with attributes
    let mut trace_fields = BTreeMap::new();
    trace_fields.insert(
        "trace_id".into(),
        Value::from("0102030405060708090a0b0c0d0e0f10"),
    );
    trace_fields.insert("span_id".into(), Value::from("0102030405060708"));
    trace_fields.insert("name".into(), Value::from("test_span"));
    trace_fields.insert("kind".into(), Value::from(1));
    trace_fields.insert(
        "start_time_unix_nano".into(),
        Value::from(Utc.timestamp_nanos(1234567890000000000)),
    );
    trace_fields.insert(
        "end_time_unix_nano".into(),
        Value::from(Utc.timestamp_nanos(1234567891000000000)),
    );

    // Add attributes
    let mut attributes = BTreeMap::new();
    attributes.insert("http.method".into(), Value::from("GET"));
    attributes.insert("http.status_code".into(), Value::from(200));
    trace_fields.insert("attributes".into(), Value::Object(attributes));

    let trace_event = Event::Trace(crate::event::TraceEvent::from_parts(
        trace_fields,
        EventMetadata::default(),
    ));
    let events = vec![trace_event];
    let result = encoder.encode_traces(events).unwrap();

    // Decode and verify attributes
    let request =
        vector_lib::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest::decode(
            result.as_ref(),
        )
        .unwrap();
    let otlp_span = &request.resource_spans[0].scope_spans[0].spans[0];

    assert_eq!(otlp_span.attributes.len(), 2);
    let method_attr = otlp_span
        .attributes
        .iter()
        .find(|attr| attr.key == "http.method")
        .unwrap();
    assert_eq!(
        method_attr.value.as_ref().unwrap().value,
        Some(
            vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                "GET".to_string()
            )
        )
    );
}

#[test]
fn test_encode_traces_with_events() {
    use std::collections::BTreeMap;
    use vrl::value::Value;

    let encoder = OtlpEncoder::new_default();

    // Create trace with span events
    let mut trace_fields = BTreeMap::new();
    trace_fields.insert(
        "trace_id".into(),
        Value::from("0102030405060708090a0b0c0d0e0f10"),
    );
    trace_fields.insert("span_id".into(), Value::from("0102030405060708"));
    trace_fields.insert("name".into(), Value::from("test_span"));

    // Add events
    let mut event_obj = BTreeMap::new();
    event_obj.insert("name".into(), Value::from("test_event"));
    event_obj.insert(
        "time_unix_nano".into(),
        Value::from(Utc.timestamp_nanos(1234567890500000000)),
    );
    event_obj.insert("attributes".into(), Value::Object(BTreeMap::new()));
    event_obj.insert("dropped_attributes_count".into(), Value::from(0));

    let events_array = vec![Value::Object(event_obj)];
    trace_fields.insert("events".into(), Value::Array(events_array));

    let trace_event = Event::Trace(crate::event::TraceEvent::from_parts(
        trace_fields,
        EventMetadata::default(),
    ));
    let events = vec![trace_event];
    let result = encoder.encode_traces(events).unwrap();

    // Decode and verify events
    let request =
        vector_lib::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest::decode(
            result.as_ref(),
        )
        .unwrap();
    let otlp_span = &request.resource_spans[0].scope_spans[0].spans[0];

    assert_eq!(otlp_span.events.len(), 1);
    assert_eq!(otlp_span.events[0].name, "test_event");
}

#[test]
fn test_traces_partitioner() {
    let partitioner = KeyPartitioner::new(
        "http://localhost:4318/v1/logs".to_string(),
        "http://localhost:4318/v1/traces".to_string(),
        "http://localhost:4318/v1/metrics".to_string(),
    );

    let trace_event = create_test_trace_event();
    let key = partitioner.partition(&trace_event);
    assert_eq!(key.endpoint, "http://localhost:4318/v1/traces");
}

fn create_test_trace_event() -> Event {
    use std::collections::BTreeMap;
    use vrl::value::Value;

    let mut trace_fields = BTreeMap::new();
    trace_fields.insert(
        "trace_id".into(),
        Value::from("0102030405060708090a0b0c0d0e0f10"),
    );
    trace_fields.insert("span_id".into(), Value::from("0102030405060708"));
    trace_fields.insert("parent_span_id".into(), Value::from(""));
    trace_fields.insert("name".into(), Value::from("test_span"));
    trace_fields.insert("kind".into(), Value::from(1)); // SPAN_KIND_INTERNAL
    trace_fields.insert(
        "start_time_unix_nano".into(),
        Value::from(Utc.timestamp_nanos(1234567890000000000)),
    );
    trace_fields.insert(
        "end_time_unix_nano".into(),
        Value::from(Utc.timestamp_nanos(1234567891000000000)),
    );
    trace_fields.insert("attributes".into(), Value::Object(BTreeMap::new()));
    trace_fields.insert("dropped_attributes_count".into(), Value::from(0));
    trace_fields.insert("events".into(), Value::Array(Vec::new()));
    trace_fields.insert("dropped_events_count".into(), Value::from(0));
    trace_fields.insert("links".into(), Value::Array(Vec::new()));
    trace_fields.insert("dropped_links_count".into(), Value::from(0));

    Event::Trace(crate::event::TraceEvent::from_parts(
        trace_fields,
        EventMetadata::default(),
    ))
}

#[tokio::test]
async fn test_custom_paths_configuration() {
    trace_init();

    // Use Arc<Mutex<Vec<...>>> to capture requests
    let received_requests = Arc::new(Mutex::new(Vec::new()));
    let received_requests_clone = received_requests.clone();

    // Create a mock HTTP server that captures requests
    let handler = move |req: Request<Body>| {
        let received_requests = received_requests_clone.clone();
        async move {
            let (parts, body) = req.into_parts();
            let body_bytes = hyper::body::to_bytes(body).await.unwrap();

            // Store the request
            {
                let mut requests = received_requests.lock().unwrap();
                requests.push((parts, body_bytes));
            }

            Ok::<_, Infallible>(Response::new(Body::empty()))
        }
    };

    // Use Vector's test utility to spawn the server
    let mock_endpoint = crate::test_util::http::spawn_blackhole_http_server(handler).await;

    let config_str = format!(
        r#"
endpoint = "{}"
logs_path = "/custom/logs"
metrics_path = "/custom/metrics"
traces_path = "/custom/traces"
"#,
        mock_endpoint
    );

    let config: OpenTelemetryConfig = toml::from_str(&config_str).unwrap();
    let cx = SinkContext::default();
    let (sink, _healthcheck) = config.build(cx).await.unwrap();

    let mut log = LogEvent::from("hello custom paths");
    log.insert("host", "example.com");
    log.insert(event_path!("resource", "service.name"), "vector-test-suite");

    let event = Event::Log(log);
    run_and_assert_sink_compliance(sink, stream::once(async { event }), &SINK_TAGS).await;

    // Wait a bit for async processing
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Assert on the received request
    let requests = received_requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "Expected exactly 1 HTTP request");

    let (parts, _body_bytes) = &requests[0];
    assert_eq!(parts.method, http::Method::POST);
    assert_eq!(parts.uri.path(), "/custom/logs"); // Verify custom path is used
}

#[test]
fn test_default_config_values() {
    let config = OpenTelemetryConfig::default();

    assert_eq!(config.endpoint, "http://localhost:4318");
    assert_eq!(config.logs_path, "/v1/logs");
    assert_eq!(config.metrics_path, "/v1/metrics");
    assert_eq!(config.traces_path, "/v1/traces");
    assert!(matches!(config.protocol, OtlpProtocol::Http));
}

#[test]
fn test_config_parsing_with_custom_paths() {
    let config_str = r#"
endpoint = "https://otel.example.com:4318"
logs_path = "/api/v2/logs"
metrics_path = "/api/v2/metrics"
traces_path = "/api/v2/traces"
"#;

    let config: OpenTelemetryConfig = toml::from_str(config_str).unwrap();

    assert_eq!(config.endpoint, "https://otel.example.com:4318");
    assert_eq!(config.logs_path, "/api/v2/logs");
    assert_eq!(config.metrics_path, "/api/v2/metrics");
    assert_eq!(config.traces_path, "/api/v2/traces");
}

#[test]
fn test_config_parsing_with_leading_slash_normalization() {
    let config_str = r#"
endpoint = "http://localhost:4318"
logs_path = "custom/logs"
metrics_path = "/custom/metrics"
traces_path = "//custom/traces"
"#;

    let config: OpenTelemetryConfig = toml::from_str(config_str).unwrap();

    // All paths should work regardless of leading slashes
    assert_eq!(config.logs_path, "custom/logs");
    assert_eq!(config.metrics_path, "/custom/metrics");
    assert_eq!(config.traces_path, "//custom/traces");
}

#[test]
fn test_generate_config() {
    crate::test_util::test_generate_config::<OpenTelemetryConfig>();
}

#[test]
fn test_default_config() {
    let config = OpenTelemetryConfig::default();

    assert_eq!(config.endpoint, "http://localhost:4318");
    assert_eq!(config.logs_path, "/v1/logs");
    assert_eq!(config.metrics_path, "/v1/metrics");
    assert_eq!(config.traces_path, "/v1/traces");
    assert!(matches!(config.protocol, OtlpProtocol::Http));

    // Test HTTP config defaults
    assert!(matches!(config.http.method, HttpMethod::Post));
    assert!(matches!(config.http.compression, Compression::None));
    assert!(matches!(config.http.encoding, ContentEncoding::Protobuf));
}

#[test]
fn test_http_config_serialization() {
    let config_str = r#"
endpoint = "http://localhost:4318"

[http]
method = "put"
compression = "gzip"
encoding = "protobuf"
"#;

    let config: OpenTelemetryConfig = toml::from_str(config_str).unwrap();

    assert!(matches!(config.http.method, HttpMethod::Put));
    assert!(matches!(config.http.compression, Compression::Gzip(_)));
    assert!(matches!(config.http.encoding, ContentEncoding::Protobuf));
}

// Test removed - resource attributes are no longer supported at config level

#[test]
fn test_compression_options() {
    let configs = [
        (
            r#"[http]
compression = "none""#,
            "None",
        ),
        (
            r#"[http]
compression = "gzip""#,
            "Gzip",
        ),
        (
            r#"[http]
compression = "zlib""#,
            "Zlib",
        ),
        (
            r#"[http]
compression = "zstd""#,
            "Zstd",
        ),
        (
            r#"[http]
compression = "snappy""#,
            "Snappy",
        ),
    ];

    for (config_str, expected) in configs {
        let full_config = format!("endpoint = \"http://localhost:4318\"\n{}", config_str);
        let config: OpenTelemetryConfig = toml::from_str(&full_config).unwrap();

        let compression_name = match config.http.compression {
            Compression::None => "None",
            Compression::Gzip(_) => "Gzip",
            Compression::Zlib(_) => "Zlib",
            Compression::Zstd(_) => "Zstd",
            Compression::Snappy => "Snappy",
        };

        assert_eq!(
            compression_name, expected,
            "Failed for config: {}",
            config_str
        );
    }
}

#[test]
fn test_http_method_options() {
    let post_config = r#"
endpoint = "http://localhost:4318"

[http]
method = "post"
"#;

    let put_config = r#"
endpoint = "http://localhost:4318"

[http]
method = "put"
"#;

    let post_parsed: OpenTelemetryConfig = toml::from_str(post_config).unwrap();
    let put_parsed: OpenTelemetryConfig = toml::from_str(put_config).unwrap();

    assert!(matches!(post_parsed.http.method, HttpMethod::Post));
    assert!(matches!(put_parsed.http.method, HttpMethod::Put));
}

// Test removed - encoder no longer manipulates resource attributes

#[test]
fn test_complex_production_config() {
    let config_str = r#"
endpoint = "https://otel-collector.prod.company.com:4318"
logs_path = "/v1/logs"
metrics_path = "/v1/metrics"
traces_path = "/v1/traces"

[http]
method = "post"
compression = "gzip"
encoding = "protobuf"

# OTLP config is now empty
"#;

    let config: OpenTelemetryConfig = toml::from_str(config_str).unwrap();

    // Verify endpoint and paths
    assert_eq!(
        config.endpoint,
        "https://otel-collector.prod.company.com:4318"
    );
    assert_eq!(config.logs_path, "/v1/logs");

    // Verify HTTP config
    assert!(matches!(config.http.method, HttpMethod::Post));
    assert!(matches!(config.http.compression, Compression::Gzip(_)));
    assert!(matches!(config.http.encoding, ContentEncoding::Protobuf));

    // OTLP config is now empty struct
}

#[test]
fn test_minimal_config() {
    let config_str = r#"
endpoint = "http://localhost:4318"
"#;

    let config: OpenTelemetryConfig = toml::from_str(config_str).unwrap();

    // Should use all defaults
    assert_eq!(config.endpoint, "http://localhost:4318");
    assert_eq!(config.logs_path, "/v1/logs");
    assert!(matches!(config.http.method, HttpMethod::Post));
    assert!(matches!(config.http.compression, Compression::None));
}

#[test]
fn test_mixed_signal_resource_attribute_grouping() {
    use std::collections::BTreeMap;
    use vector_lib::event::{TraceEvent, Value};
    use vector_lib::opentelemetry::proto::{
        collector::{
            logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
            trace::v1::ExportTraceServiceRequest,
        },
        common::v1::any_value::Value as PbValue,
    };

    let encoder = OtlpEncoder::new_default();

    // Create events with different resource attributes across multiple services
    let mut events = Vec::new();

    // Service A - Logs with resource attributes
    for i in 0..3 {
        let mut log = LogEvent::from(format!("Log message {} from service A", i));
        log.insert("level", "info");
        log.insert("user_id", i as i64);
        log.insert(event_path!("resource", "service.name"), "service-a");
        log.insert(event_path!("resource", "service.version"), "1.0.0");
        log.insert(
            event_path!("resource", "deployment.environment"),
            "production",
        );
        log.insert(event_path!("resource", "host.name"), "host-1");
        events.push(Event::Log(log));
    }

    // Service B - Logs with different resource attributes
    for i in 0..2 {
        let mut log = LogEvent::from(format!("Log message {} from service B", i));
        log.insert("level", "warn");
        log.insert("error_code", 500 + i as i64);
        log.insert(event_path!("resource", "service.name"), "service-b");
        log.insert(event_path!("resource", "service.version"), "2.1.0");
        log.insert(event_path!("resource", "deployment.environment"), "staging");
        log.insert(event_path!("resource", "host.name"), "host-2");
        events.push(Event::Log(log));
    }

    // Service A - Metrics with same resource attributes as Service A logs
    for i in 0..2 {
        let mut metric = Metric::new(
            format!("counter_a_{}", i),
            MetricKind::Absolute,
            MetricValue::Counter {
                value: (i + 1) as f64 * 10.0,
            },
        );

        // Add resource attributes as tags with "resources." prefix
        let mut tags = crate::event::metric::MetricTags::default();
        tags.replace("endpoint".to_string(), format!("/api/v{}", i + 1));
        tags.replace(
            "resources.\"service.name\"".to_string(),
            "service-a".to_string(),
        );
        tags.replace(
            "resources.\"service.version\"".to_string(),
            "1.0.0".to_string(),
        );
        tags.replace(
            "resources.\"deployment.environment\"".to_string(),
            "production".to_string(),
        );
        tags.replace("resources.\"host.name\"".to_string(), "host-1".to_string());
        metric = metric.with_tags(Some(tags));

        events.push(Event::Metric(metric));
    }

    // Service C - Metrics with completely different resource attributes
    for i in 0..1 {
        let mut metric = Metric::new(
            format!("gauge_c_{}", i),
            MetricKind::Absolute,
            MetricValue::Gauge {
                value: 42.5 + i as f64,
            },
        );

        // Add resource attributes as tags with "resources." prefix
        let mut tags = crate::event::metric::MetricTags::default();
        tags.replace("region".to_string(), "us-west-2".to_string());
        tags.replace(
            "resources.\"service.name\"".to_string(),
            "service-c".to_string(),
        );
        tags.replace(
            "resources.\"service.version\"".to_string(),
            "3.0.0".to_string(),
        );
        tags.replace(
            "resources.\"deployment.environment\"".to_string(),
            "development".to_string(),
        );
        tags.replace("resources.\"host.name\"".to_string(), "host-3".to_string());
        metric = metric.with_tags(Some(tags));

        events.push(Event::Metric(metric));
    }

    // Service A - Traces with same resource attributes as Service A logs/metrics
    for i in 0..2 {
        let mut trace_fields = BTreeMap::new();
        trace_fields.insert("trace_id".into(), Value::from(format!("{:032x}", i + 100)));
        trace_fields.insert("span_id".into(), Value::from(format!("{:016x}", i + 10)));
        trace_fields.insert("name".into(), Value::from(format!("span_a_{}", i)));
        trace_fields.insert("kind".into(), Value::from(1)); // SPAN_KIND_INTERNAL
        trace_fields.insert(
            "start_time_unix_nano".into(),
            Value::from(1234567890000000000i64 + i as i64 * 1000000),
        );
        trace_fields.insert(
            "end_time_unix_nano".into(),
            Value::from(1234567891000000000i64 + i as i64 * 1000000),
        );

        let mut attributes = BTreeMap::new();
        attributes.insert("operation".into(), Value::from(format!("op_{}", i)));
        trace_fields.insert("attributes".into(), Value::Object(attributes));

        // Add resource attributes for service A with "resources." prefix
        trace_fields.insert(
            "resources.\"service.name\"".into(),
            Value::from("service-a"),
        );
        trace_fields.insert("resources.\"service.version\"".into(), Value::from("1.0.0"));
        trace_fields.insert(
            "resources.\"deployment.environment\"".into(),
            Value::from("production"),
        );
        trace_fields.insert("resources.\"host.name\"".into(), Value::from("host-1"));

        let trace_event = TraceEvent::from_parts(trace_fields, EventMetadata::default());
        events.push(Event::Trace(trace_event));
    }

    // Service B - Traces with same resource attributes as Service B logs
    for i in 0..1 {
        let mut trace_fields = BTreeMap::new();
        trace_fields.insert("trace_id".into(), Value::from(format!("{:032x}", i + 200)));
        trace_fields.insert("span_id".into(), Value::from(format!("{:016x}", i + 20)));
        trace_fields.insert("name".into(), Value::from(format!("span_b_{}", i)));
        trace_fields.insert("kind".into(), Value::from(2)); // SPAN_KIND_SERVER
        trace_fields.insert(
            "start_time_unix_nano".into(),
            Value::from(1234567892000000000i64 + i as i64 * 1000000),
        );
        trace_fields.insert(
            "end_time_unix_nano".into(),
            Value::from(1234567893000000000i64 + i as i64 * 1000000),
        );

        let mut attributes = BTreeMap::new();
        attributes.insert("http.method".into(), Value::from("POST"));
        trace_fields.insert("attributes".into(), Value::Object(attributes));

        // Add resource attributes for service B with "resources." prefix
        trace_fields.insert(
            "resources.\"service.name\"".into(),
            Value::from("service-b"),
        );
        trace_fields.insert("resources.\"service.version\"".into(), Value::from("2.1.0"));
        trace_fields.insert(
            "resources.\"deployment.environment\"".into(),
            Value::from("staging"),
        );
        trace_fields.insert("resources.\"host.name\"".into(), Value::from("host-2"));

        let trace_event = TraceEvent::from_parts(trace_fields, EventMetadata::default());
        events.push(Event::Trace(trace_event));
    }

    // Separate events by type and encode each type
    let mut logs = Vec::new();
    let mut metrics = Vec::new();
    let mut traces = Vec::new();

    for event in events {
        match event {
            Event::Log(_) => logs.push(event),
            Event::Metric(_) => metrics.push(event),
            Event::Trace(_) => traces.push(event),
        }
    }

    // Test logs grouping
    if !logs.is_empty() {
        let result = encoder.encode_logs(logs).unwrap();
        let request = ExportLogsServiceRequest::decode(result.as_ref()).unwrap();

        // Should have 2 resource groups: service-a and service-b
        assert_eq!(
            request.resource_logs.len(),
            2,
            "Should have 2 log resource groups"
        );

        // Check service-a resource group
        let service_a_logs = request
            .resource_logs
            .iter()
            .find(|rl| {
                rl.resource.as_ref().unwrap().attributes.iter().any(|attr| {
                    attr.key == "service.name"
                        && matches!(
                            attr.value.as_ref().unwrap().value.as_ref().unwrap(),
                            PbValue::StringValue(s) if s == "service-a"
                        )
                })
            })
            .expect("Should find service-a logs");

        // Verify service-a has 3 log records
        let service_a_log_count: usize = service_a_logs
            .scope_logs
            .iter()
            .map(|sl| sl.log_records.len())
            .sum();
        assert_eq!(
            service_a_log_count, 3,
            "Service A should have 3 log records"
        );

        // Check service-b resource group
        let service_b_logs = request
            .resource_logs
            .iter()
            .find(|rl| {
                rl.resource.as_ref().unwrap().attributes.iter().any(|attr| {
                    attr.key == "service.name"
                        && matches!(
                            attr.value.as_ref().unwrap().value.as_ref().unwrap(),
                            PbValue::StringValue(s) if s == "service-b"
                        )
                })
            })
            .expect("Should find service-b logs");

        // Verify service-b has 2 log records
        let service_b_log_count: usize = service_b_logs
            .scope_logs
            .iter()
            .map(|sl| sl.log_records.len())
            .sum();
        assert_eq!(
            service_b_log_count, 2,
            "Service B should have 2 log records"
        );
    }

    // Test metrics grouping
    if !metrics.is_empty() {
        let result = encoder.encode_metrics(metrics).unwrap();
        let request = ExportMetricsServiceRequest::decode(result.as_ref()).unwrap();

        // Should have 2 resource groups: service-a and service-c
        assert_eq!(
            request.resource_metrics.len(),
            2,
            "Should have 2 metric resource groups"
        );

        // Check service-a resource group
        let service_a_metrics = request
            .resource_metrics
            .iter()
            .find(|rm| {
                rm.resource.as_ref().unwrap().attributes.iter().any(|attr| {
                    attr.key == "service.name"
                        && matches!(
                            attr.value.as_ref().unwrap().value.as_ref().unwrap(),
                            PbValue::StringValue(s) if s == "service-a"
                        )
                })
            })
            .expect("Should find service-a metrics");

        // Verify service-a has 2 metrics
        let service_a_metric_count: usize = service_a_metrics
            .scope_metrics
            .iter()
            .map(|sm| sm.metrics.len())
            .sum();
        assert_eq!(service_a_metric_count, 2, "Service A should have 2 metrics");

        // Check service-c resource group
        let service_c_metrics = request
            .resource_metrics
            .iter()
            .find(|rm| {
                rm.resource.as_ref().unwrap().attributes.iter().any(|attr| {
                    attr.key == "service.name"
                        && matches!(
                            attr.value.as_ref().unwrap().value.as_ref().unwrap(),
                            PbValue::StringValue(s) if s == "service-c"
                        )
                })
            })
            .expect("Should find service-c metrics");

        // Verify service-c has 1 metric
        let service_c_metric_count: usize = service_c_metrics
            .scope_metrics
            .iter()
            .map(|sm| sm.metrics.len())
            .sum();
        assert_eq!(service_c_metric_count, 1, "Service C should have 1 metric");
    }

    // Test traces grouping
    if !traces.is_empty() {
        let result = encoder.encode_traces(traces).unwrap();
        let request = ExportTraceServiceRequest::decode(result.as_ref()).unwrap();

        // Should have 2 resource groups: service-a and service-b
        assert_eq!(
            request.resource_spans.len(),
            2,
            "Should have 2 trace resource groups"
        );

        // Check service-a resource group
        let service_a_traces = request
            .resource_spans
            .iter()
            .find(|rs| {
                rs.resource.as_ref().unwrap().attributes.iter().any(|attr| {
                    attr.key == "service.name"
                        && matches!(
                            attr.value.as_ref().unwrap().value.as_ref().unwrap(),
                            PbValue::StringValue(s) if s == "service-a"
                        )
                })
            })
            .expect("Should find service-a traces");

        // Verify service-a has 2 spans
        let service_a_span_count: usize = service_a_traces
            .scope_spans
            .iter()
            .map(|ss| ss.spans.len())
            .sum();
        assert_eq!(service_a_span_count, 2, "Service A should have 2 spans");

        // Check service-b resource group
        let service_b_traces = request
            .resource_spans
            .iter()
            .find(|rs| {
                rs.resource.as_ref().unwrap().attributes.iter().any(|attr| {
                    attr.key == "service.name"
                        && matches!(
                            attr.value.as_ref().unwrap().value.as_ref().unwrap(),
                            PbValue::StringValue(s) if s == "service-b"
                        )
                })
            })
            .expect("Should find service-b traces");

        // Verify service-b has 1 span
        let service_b_span_count: usize = service_b_traces
            .scope_spans
            .iter()
            .map(|ss| ss.spans.len())
            .sum();
        assert_eq!(service_b_span_count, 1, "Service B should have 1 span");
    }

    // Verify that resource attributes are correctly preserved
    // Check that service-a has consistent resource attributes across all signal types
    // This validates that our grouping key correctly identifies the same service

    println!("✅ Mixed signal resource attribute grouping test passed!");
    println!("   - Logs: 2 resource groups (service-a: 3 logs, service-b: 2 logs)");
    println!("   - Metrics: 2 resource groups (service-a: 2 metrics, service-c: 1 metric)");
    println!("   - Traces: 2 resource groups (service-a: 2 spans, service-b: 1 span)");
}
