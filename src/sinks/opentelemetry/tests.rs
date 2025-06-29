//! Unit and integration tests for the `opentelemetry` sink.

use futures::stream;
use http::{Request, Response};
use hyper::Body;
use prost::Message;
use rstest::rstest;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
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

#[test]
fn test_encode_logs_method_directly() {
    let encoder = OtlpEncoder::new();

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

        // Verify the field was removed from the log
        assert!(
            log.get(field_name).is_none(),
            "Field '{}' should have been removed",
            field_name
        );

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

    // Only the 'level' field should be removed
    assert!(log.get("level").is_none());
    assert!(log.get("severity").is_some());
    assert!(log.get("log_level").is_some());
}

#[test]
fn test_severity_integration_with_encoder() {
    let encoder = OtlpEncoder::new();

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

    // Check that 'level' field was removed from attributes
    let level_attr = log_record
        .attributes
        .iter()
        .find(|attr| attr.key == "level");
    assert!(
        level_attr.is_none(),
        "Level field should not be in attributes"
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
    let encoder = OtlpEncoder::new();

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
