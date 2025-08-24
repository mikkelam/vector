use crate::sinks::opentelemetry::encoder::OtlpEncoder;

use vector_lib::event::{Event, LogEvent};
use vrl::core::Value;

use vrl::value::ObjectMap;

#[test]
fn test_key_quoting_behavior() {
    use prost::Message;
    use vector_lib::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;

    let encoder = OtlpEncoder::new_default();

    // Test 1: Flat key with dots (should trigger quoting issue)
    let mut log1 = LogEvent::default();
    log1.insert("code.file.path", "/path/to/file.py");
    log1.insert("code.function.name", "my_function");
    log1.insert("message", "test log");

    // Test 2: Nested structure
    let mut log2 = LogEvent::default();
    let mut code_attrs = ObjectMap::new();
    code_attrs.insert("file.path".into(), Value::from("/path/to/file.py"));
    code_attrs.insert("function.name".into(), Value::from("my_function"));
    log2.insert("attributes", Value::Object(code_attrs));
    log2.insert("message", "test log nested");

    // Test 3: Resource attributes with dots
    let mut log3 = LogEvent::default();
    let mut res_attrs = ObjectMap::new();
    res_attrs.insert("service.name".into(), Value::from("test-service"));
    res_attrs.insert("host.name".into(), Value::from("test-host"));
    log3.insert("resources", Value::Object(res_attrs));
    log3.insert("message", "test log resource");

    let events = vec![Event::Log(log1), Event::Log(log2), Event::Log(log3)];
    let result = encoder.encode_logs(events).unwrap();

    // Decode and inspect the result
    let request = ExportLogsServiceRequest::decode(result.as_ref()).unwrap();

    println!("=== ENCODED OTLP REQUEST ===");
    for (i, resource_log) in request.resource_logs.iter().enumerate() {
        println!("ResourceLog {}: ", i);
        if let Some(resource) = &resource_log.resource {
            println!("  Resource attributes:");
            for attr in &resource.attributes {
                println!("    Key: {:?} = {:?}", attr.key, attr.value);
            }
        }

        for scope_log in &resource_log.scope_logs {
            for (j, log_record) in scope_log.log_records.iter().enumerate() {
                println!("  LogRecord {}: ", j);
                println!("    Log attributes:");
                for attr in &log_record.attributes {
                    println!("      Key: {:?} = {:?}", attr.key, attr.value);
                }
            }
        }
    }

    // Check for quoted keys in attributes
    let mut found_quoted_keys = Vec::new();
    for resource_log in &request.resource_logs {
        for scope_log in &resource_log.scope_logs {
            for log_record in &scope_log.log_records {
                for attr in &log_record.attributes {
                    if attr.key.starts_with('"') && attr.key.ends_with('"') {
                        found_quoted_keys.push(attr.key.clone());
                    }
                }
            }
        }
    }

    if !found_quoted_keys.is_empty() {
        panic!("Found quoted keys in OTLP output: {:?}", found_quoted_keys);
    }
}

#[test]
fn test_trace_id_hex_decoding() {
    let encoder = OtlpEncoder::new_default();

    // Create log event with hex string trace_id (as stored by Vector's OTLP source)
    let mut log = LogEvent::from("test message");
    log.insert("trace_id", "4ac52aadf321c2e531db005df08792f5"); // 32-char hex = 16 bytes
    log.insert("span_id", "0b9e4bda2a55530d"); // 16-char hex = 8 bytes

    let log_record = encoder.convert_log_event_to_log_record(log);

    // Should decode hex strings to proper byte lengths
    assert_eq!(log_record.trace_id.len(), 16);
    assert_eq!(log_record.span_id.len(), 8);

    // Verify actual decoded bytes
    assert_eq!(
        log_record.trace_id,
        hex::decode("4ac52aadf321c2e531db005df08792f5").unwrap()
    );
    assert_eq!(log_record.span_id, hex::decode("0b9e4bda2a55530d").unwrap());
}

#[test]
fn test_invalid_trace_id_hex() {
    let encoder = OtlpEncoder::new_default();

    let mut log = LogEvent::from("test message");
    log.insert("trace_id", "invalid_hex"); // Invalid hex
    log.insert("span_id", "too_short"); // Too short

    let log_record = encoder.convert_log_event_to_log_record(log);

    // Should result in empty arrays for invalid data
    assert_eq!(log_record.trace_id.len(), 0);
    assert_eq!(log_record.span_id.len(), 0);
}

#[test]
fn test_resources_object_to_resource_attributes() {
    use prost::Message;

    use vector_lib::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
    use vector_lib::opentelemetry::proto::common::v1::any_value;

    let encoder = OtlpEncoder::new_default();

    let mut res = ObjectMap::new();
    res.insert("service.name".into(), Value::from("svc-a"));
    res.insert("host.name".into(), Value::from("host-a"));
    res.insert("count".into(), Value::from(42));

    let mut nested = ObjectMap::new();
    nested.insert("k".into(), Value::from("v"));
    res.insert("nested".into(), Value::Object(nested));

    let mut log = LogEvent::from("body");
    log.insert("resources", Value::Object(res));
    log.insert("attr", "x");

    let bytes = encoder.encode_logs(vec![Event::Log(log)]).unwrap();
    let request = ExportLogsServiceRequest::decode(bytes.as_ref()).unwrap();

    let rl = &request.resource_logs[0];
    let resource = rl.resource.as_ref().unwrap();
    let scope_log = &rl.scope_logs[0];
    let lr = &scope_log.log_records[0];

    let has_attr = |k: &str, kvs: &Vec<vector_lib::opentelemetry::proto::common::v1::KeyValue>| {
        kvs.iter()
            .any(|kv: &vector_lib::opentelemetry::proto::common::v1::KeyValue| kv.key == k)
    };

    assert!(has_attr("service.name", &resource.attributes));
    assert!(has_attr("host.name", &resource.attributes));
    assert!(has_attr("count", &resource.attributes));
    assert!(has_attr("nested", &resource.attributes));

    // Ensure nested became a kvlist AnyValue
    let nested_val = resource
        .attributes
        .iter()
        .find(|kv| kv.key == "nested")
        .and_then(|kv| kv.value.as_ref().and_then(|v| v.value.clone()));
    match nested_val {
        Some(any_value::Value::KvlistValue(_)) => {}
        other => panic!("expected KvlistValue for nested, got {:?}", other),
    }

    // Resource attrs should not be duplicated into LogRecord attrs
    assert!(!has_attr("service.name", &lr.attributes));
    assert!(!has_attr("host.name", &lr.attributes));
    assert!(!has_attr("count", &lr.attributes));
    assert!(!has_attr("nested", &lr.attributes));
    // Non-resource attribute remains on the LogRecord
    assert!(has_attr("attr", &lr.attributes));
}

#[test]
fn test_attributes_object_goes_to_log_attributes_not_resource() {
    use prost::Message;

    use vector_lib::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
    use vector_lib::opentelemetry::proto::common::v1::any_value;

    let encoder = OtlpEncoder::new_default();

    let mut attrs = ObjectMap::new();
    attrs.insert("a".into(), Value::from("x"));
    attrs.insert("b".into(), Value::from(1));

    let mut nested = ObjectMap::new();
    nested.insert("n".into(), Value::from(true));
    attrs.insert("c".into(), Value::Object(nested));

    let mut log = LogEvent::from("body");
    log.insert("attributes", Value::Object(attrs));

    let bytes = encoder.encode_logs(vec![Event::Log(log)]).unwrap();
    let request = ExportLogsServiceRequest::decode(bytes.as_ref()).unwrap();

    let rl = &request.resource_logs[0];
    let resource = rl.resource.as_ref().unwrap();
    let scope_log = &rl.scope_logs[0];
    let lr = &scope_log.log_records[0];

    assert!(resource.attributes.is_empty());

    let has = |k: &str| lr.attributes.iter().any(|kv| kv.key == k);

    assert!(has("a"));
    assert!(has("b"));
    let c_val = lr
        .attributes
        .iter()
        .find(|kv| kv.key == "c")
        .and_then(|kv| kv.value.as_ref().and_then(|v| v.value.clone()));
    match c_val {
        Some(any_value::Value::KvlistValue(_)) => {}
        other => panic!("expected KvlistValue for nested attr c, got {:?}", other),
    }
}

#[test]
fn test_mixed_resources_object_and_dotted_keys_no_duplication() {
    use prost::Message;

    use vector_lib::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;

    let encoder = OtlpEncoder::new_default();

    let mut res = ObjectMap::new();
    res.insert("service.name".into(), Value::from("svc-c"));
    res.insert("region".into(), Value::from("eu-central-1"));
    res.insert("host.name".into(), Value::from("host-c"));

    let mut log = LogEvent::from("body");
    log.insert("resources", Value::Object(res));
    log.insert("foo", "bar");

    let bytes = encoder.encode_logs(vec![Event::Log(log)]).unwrap();
    let request = ExportLogsServiceRequest::decode(bytes.as_ref()).unwrap();

    let rl = &request.resource_logs[0];
    let resource = rl.resource.as_ref().unwrap();
    let scope_log = &rl.scope_logs[0];
    let lr = &scope_log.log_records[0];

    let has = |k: &str, kvs: &Vec<vector_lib::opentelemetry::proto::common::v1::KeyValue>| {
        kvs.iter()
            .any(|kv: &vector_lib::opentelemetry::proto::common::v1::KeyValue| kv.key == k)
    };

    assert!(has("service.name", &resource.attributes));
    assert!(has("region", &resource.attributes));
    assert!(has("host.name", &resource.attributes));

    assert!(has("foo", &lr.attributes));
    assert!(!has("service.name", &lr.attributes));
    assert!(!has("region", &lr.attributes));
    assert!(!has("host.name", &lr.attributes));
}

#[test]
fn test_no_quoted_keys_in_resource_or_log_attributes() {
    use prost::Message;
    use vector_lib::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;

    let encoder = OtlpEncoder::new_default();

    let mut res = ObjectMap::new();
    res.insert("file.path".into(), Value::from("/path"));
    res.insert("function.name".into(), Value::from("f"));

    let mut log = LogEvent::from("body");
    log.insert("resources", Value::Object(res));
    log.insert(
        "attributes",
        Value::Object({
            let mut m = ObjectMap::new();
            m.insert("log.attr.with.dots".into(), Value::from(1));
            m
        }),
    );

    let bytes = encoder.encode_logs(vec![Event::Log(log)]).unwrap();
    let request = ExportLogsServiceRequest::decode(bytes.as_ref()).unwrap();

    for resource_log in &request.resource_logs {
        if let Some(resource) = &resource_log.resource {
            for attr in &resource.attributes {
                assert!(
                    !(attr.key.starts_with('"') && attr.key.ends_with('"')),
                    "quoted resource key {:?}",
                    attr.key
                );
            }
        }
        for scope_log in &resource_log.scope_logs {
            for log_record in &scope_log.log_records {
                for attr in &log_record.attributes {
                    assert!(
                        !(attr.key.starts_with('"') && attr.key.ends_with('"')),
                        "quoted log key {:?}",
                        attr.key
                    );
                }
            }
        }
    }
}

#[test]
fn test_convert_log_event_to_log_record_fields() {
    use chrono::{TimeZone, Utc};
    use vector_lib::opentelemetry::proto::common::v1::any_value::Value as PbValue;

    let encoder = OtlpEncoder::new_default();

    // Shared values
    let body = "body msg";
    let ts = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 10).unwrap();
    let obs = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 9).unwrap();
    let expected_time_ns = ts.timestamp_nanos_opt().unwrap() as u64;
    let expected_observed_ns = obs.timestamp_nanos_opt().unwrap() as u64;

    let trace_id_hex = "4ac52aadf321c2e531db005df08792f5";
    let span_id_hex = "0b9e4bda2a55530d";
    let severity_text = "WARN";
    let severity_number = 9;
    let flags = 1;
    let dropped_count = 2;
    let extra_k = "foo";
    let extra_v = "bar";

    let mut log = LogEvent::from(body);
    log.insert("timestamp", Value::Timestamp(ts));
    log.insert("observed_timestamp", Value::Timestamp(obs));

    // Hex-encoded IDs stored as bytes (Vector commonly stores as bytes with hex content)
    log.insert("trace_id", trace_id_hex);
    log.insert("span_id", span_id_hex);

    // Severity and flags
    log.insert("severity_text", severity_text);
    log.insert("severity_number", severity_number);
    log.insert("flags", flags);
    log.insert("dropped_attributes_count", dropped_count);

    // Attributes object (merged into LogRecord.attributes)
    let mut nested = ObjectMap::new();
    nested.insert("x".into(), Value::from("y"));
    let mut attrs = ObjectMap::new();
    attrs.insert("a".into(), Value::from(1));
    attrs.insert("nested".into(), Value::Object(nested));
    log.insert("attributes", Value::Object(attrs));

    // Additional fields that should become attributes (except ones removed explicitly)
    log.insert(extra_k, extra_v);
    log.insert("source_type", "should_be_removed");

    // Convert
    let lr = encoder.convert_log_event_to_log_record(log);

    // Body
    assert_eq!(
        lr.body.as_ref().unwrap().value,
        Some(PbValue::StringValue(body.to_string()))
    );

    // Timestamps
    assert_eq!(lr.time_unix_nano, expected_time_ns);
    assert_eq!(lr.observed_time_unix_nano, expected_observed_ns);

    // Trace/span IDs
    assert_eq!(lr.trace_id, hex::decode(trace_id_hex).unwrap());
    assert_eq!(lr.span_id, hex::decode(span_id_hex).unwrap());

    // Severity, flags, dropped count
    assert_eq!(lr.severity_text, severity_text);
    assert_eq!(lr.severity_number, severity_number);
    assert_eq!(lr.flags, flags);
    assert_eq!(lr.dropped_attributes_count, dropped_count);

    // Attributes merged correctly
    let has = |k: &str| lr.attributes.iter().any(|kv| kv.key == k);
    assert!(has("a"));
    assert!(has("nested"));
    assert!(has(extra_k));

    // Ensure removed/special fields are not duplicated as attributes
    assert!(!has("source_type"));
    assert!(!has("severity_text"));
    assert!(!has("severity_number"));
    assert!(!has("flags"));
    assert!(!has("dropped_attributes_count"));

    // Nested attribute is encoded as kvlist
    let nested_val = lr
        .attributes
        .iter()
        .find(|kv| kv.key == "nested")
        .and_then(|kv| kv.value.as_ref().and_then(|v| v.value.clone()));
    match nested_val {
        Some(PbValue::KvlistValue(_)) => {}
        other => panic!("expected nested to be kvlist, got {:?}", other),
    }
}

#[test]
fn test_convert_log_event_to_log_record_without_otlp_fields() {
    use vector_lib::opentelemetry::proto::common::v1::any_value::Value as PbValue;

    let encoder = OtlpEncoder::new_default();

    let body = "non otlp log";
    let mut log = LogEvent::from(body);
    log.insert("level", "info");
    log.insert("host", "example.com");
    log.insert("count", 42);

    // Convert without any OTLP-native fields present
    let lr = encoder.convert_log_event_to_log_record(log);

    // Body present, basic defaults for other OTLP fields
    assert_eq!(
        lr.body.as_ref().unwrap().value,
        Some(PbValue::StringValue(body.to_string()))
    );
    assert!(lr.time_unix_nano > 0);
    assert_eq!(lr.observed_time_unix_nano, 0);
    assert_eq!(lr.trace_id.len(), 0);
    assert_eq!(lr.span_id.len(), 0);
    assert_eq!(lr.severity_text, "");
    assert_eq!(lr.severity_number, 0);
    assert_eq!(lr.flags, 0);
    assert_eq!(lr.dropped_attributes_count, 0);

    // Attributes should contain user fields and exclude removed/special ones
    let has = |k: &str| lr.attributes.iter().any(|kv| kv.key == k);
    assert!(has("level"));
    assert!(has("host"));
    assert!(has("count"));
    assert!(!has("message"));
    assert!(!has("timestamp"));
    assert!(!has("source_type"));
}
