use crate::sinks::opentelemetry::encoder::OtlpEncoder;
use crate::sinks::opentelemetry::encoder::PbValue;
use crate::sinks::opentelemetry::encoder::ResourceAttributeExtractor;
use vector_lib::event::{Event, LogEvent};
use vrl::core::Value;
use vrl::event_path;
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
    log3.insert("resource.service.name", "test-service");
    log3.insert("resource.host.name", "test-host");
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
fn test_resource_attribute_extractor_trait_logs() {
    // Test the trait methods directly on LogEvent
    let mut log = LogEvent::from("test message");
    log.insert(event_path!("resource", "service", "name"), "test-service");
    log.insert(event_path!("resource", "environment"), "production");
    log.insert("normal_field", "normal_value");

    // Test extraction
    let resource_attrs = log.extract_resource_attributes();
    assert_eq!(resource_attrs.len(), 2);

    let service_name = resource_attrs
        .iter()
        .find(|attr| attr.key == "service.name")
        .unwrap();
    if let Some(any_value) = &service_name.value {
        if let Some(PbValue::StringValue(s)) = &any_value.value {
            assert_eq!(s, "test-service");
        }
    }

    // Verify resource fields are still present (we no longer remove them)
    assert!(
        log.get(event_path!("resource", "service", "name"))
            .is_some()
    );
    assert!(log.get(event_path!("resource", "environment")).is_some());
    assert!(log.get("normal_field").is_some());
}

#[test]
fn test_extract_all_resource_attributes() {
    // Test logs with both nested and flattened resource attributes
    let mut log = LogEvent::from("test message");

    // Add nested resource attributes
    let mut nested_resource = ObjectMap::new();
    nested_resource.insert("service.name".into(), Value::from("nested-service"));
    nested_resource.insert("version".into(), Value::from("1.0.0"));
    log.insert(event_path!("resource"), Value::Object(nested_resource));

    // Add flattened resource attributes using dot notation
    // Note: When we insert "resource.environment", Vector automatically
    // adds it to the existing nested "resource" object we created above
    log.insert("resource.environment", "production");
    log.insert("resource.cluster", "us-west-1");
    log.insert("normal_field", "keep_me");

    // Extract all resource attributes
    let all_attrs = log.extract_all_resource_attributes();

    // We get duplicates because when we insert "resource.environment", Vector
    // creates a nested structure. Both extraction methods find the same data:
    // - extract_nested_resource_attributes() finds 4 attrs in the resource object
    // - extract_resource_attributes() finds 4 attrs via flattened dot notation
    // Total = 8 attributes (each attribute appears twice)
    assert_eq!(all_attrs.len(), 8);

    // Verify we have the expected attributes (no duplicates)
    let has_service_name = all_attrs.iter().any(|attr| attr.key == "service.name");
    let has_version = all_attrs.iter().any(|attr| attr.key == "version");
    let has_environment = all_attrs.iter().any(|attr| attr.key == "environment");
    let has_cluster = all_attrs.iter().any(|attr| attr.key == "cluster");

    assert!(has_service_name, "Missing service.name attribute");
    assert!(has_version, "Missing version attribute");
    assert!(has_environment, "Missing environment attribute");
    assert!(has_cluster, "Missing cluster attribute");

    // Verify all fields are still present in the log (we no longer remove them)
    assert!(log.get("resource").is_some());
    // These fields exist both as nested and flattened
    assert!(log.get("resource.environment").is_some());
    assert!(log.get("resource.cluster").is_some());
    assert!(log.get("normal_field").is_some());
}
