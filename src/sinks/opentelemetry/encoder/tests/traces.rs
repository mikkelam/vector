use crate::sinks::opentelemetry::encoder::OtlpEncoder;
use crate::sinks::opentelemetry::encoder::PbValue;
use crate::sinks::opentelemetry::encoder::ResourceAttributeExtractor;
use crate::sinks::util::encoding::Encoder;
use prost::Message;
use std::collections::BTreeMap;
use vector_lib::event::Event;
use vector_lib::event::TraceEvent;
use vector_lib::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
use vector_lib::opentelemetry::proto::common::v1::any_value;
use vector_lib::opentelemetry::proto::trace::v1::span::SpanKind;
use vrl::core::Value;
use vrl::value::ObjectMap;

#[test]
fn test_full_trace_conversion() {
    let trace_event = TraceEvent::from(BTreeMap::from([
        (
            "trace_id".into(),
            Value::from("7b5a5a5a5a5a5a5a7b5a5a5a5a5a5a5a"),
        ),
        ("span_id".into(), Value::from("7b5a5a5a5a5a5a5b")),
        ("parent_span_id".into(), Value::from("7b5a5a5a5a5a5a5c")),
        ("name".into(), Value::from("test-span")),
        (
            "timestamp".into(),
            Value::Timestamp(
                chrono::DateTime::parse_from_rfc3339("2023-01-01T00:00:10Z")
                    .unwrap()
                    .into(),
            ),
        ),
        ("duration_ns".into(), Value::from(1_000_000_000u64)), // 1 second
        ("kind".into(), Value::from("server")),
        (
            "tags".into(),
            Value::from(BTreeMap::from([
                ("http.method".into(), Value::from("GET")),
                ("otel.status_code".into(), Value::from("Error")),
                ("error".into(), Value::from("something went wrong")),
            ])),
        ),
        (
            "events".into(),
            Value::from(vec![Value::from(BTreeMap::from([
                ("name".into(), Value::from("test-event")),
                (
                    "timestamp".into(),
                    Value::Timestamp(
                        chrono::DateTime::parse_from_rfc3339("2023-01-01T00:00:09.5Z")
                            .unwrap()
                            .into(),
                    ),
                ),
            ]))]),
        ),
    ]));
    let event = Event::Trace(trace_event);

    let encoder = OtlpEncoder::new_default();
    let mut buf = Vec::new();
    let result = encoder.encode_input(vec![event], &mut buf);
    assert!(result.is_ok());

    let request = ExportTraceServiceRequest::decode(buf.as_slice()).unwrap();
    let resource_spans = request.resource_spans.first().unwrap();
    let scope_spans = resource_spans.scope_spans.first().unwrap();
    let span = scope_spans.spans.first().unwrap();

    assert_eq!(
        hex::encode(&span.trace_id),
        "7b5a5a5a5a5a5a5a7b5a5a5a5a5a5a5a"
    );
    assert_eq!(hex::encode(&span.span_id), "7b5a5a5a5a5a5a5b");
    assert_eq!(hex::encode(&span.parent_span_id), "7b5a5a5a5a5a5a5c");
    assert_eq!(span.name, "test-span");
    assert_eq!(span.end_time_unix_nano, 1672531210000000000);
    assert_eq!(span.start_time_unix_nano, 1672531209000000000);
    assert_eq!(span.kind, SpanKind::Server as i32);

    if let Some(status) = span.status.as_ref() {
        // The protobuf uses the numeric code
        assert_eq!(status.code, 2); // ERROR = 2
        assert_eq!(status.message, "something went wrong");
    } else {
        panic!("Expected span to have status but it was None");
    }

    // Verify events are properly converted
    assert_eq!(span.events.len(), 1);
    let event = &span.events[0];
    assert_eq!(event.name, "test-event");
    assert_eq!(event.time_unix_nano, 1672531209500000000); // 2023-01-01T00:00:09.5Z

    let http_method_attr = span
        .attributes
        .iter()
        .find(|attr| attr.key == "http.method")
        .unwrap();
    assert_eq!(
        http_method_attr.value.as_ref().unwrap().value,
        Some(any_value::Value::StringValue("GET".to_string()))
    );
}

#[test]
fn test_trace_resource_extraction() {
    use prost::Message;
    use vector_lib::event::EventMetadata;

    use vector_lib::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;

    let encoder = OtlpEncoder::new_default();

    // Create trace with flattened resource attributes
    let mut trace_fields = ObjectMap::new();
    trace_fields.insert("trace_id".into(), Value::from("test_trace_id"));
    trace_fields.insert("span_id".into(), Value::from("test_span_id"));
    trace_fields.insert("name".into(), Value::from("test_span"));

    // Add resource attributes with "resource." prefix
    trace_fields.insert("resource.service.name".into(), Value::from("trace-service"));
    trace_fields.insert("resource.service.version".into(), Value::from("1.0.0"));
    trace_fields.insert("resource.k8s.cluster".into(), Value::from("test-cluster"));

    let trace_event = Event::Trace(TraceEvent::from_parts(
        trace_fields,
        EventMetadata::default(),
    ));

    let events = vec![trace_event];
    let result = encoder.encode_traces(events).unwrap();

    // Decode and verify resource attributes are extracted
    let request = ExportTraceServiceRequest::decode(result.as_ref()).unwrap();
    let resource_spans = &request.resource_spans[0];
    let resource = resource_spans.resource.as_ref().unwrap();

    let resource_attrs: std::collections::HashMap<String, String> = resource
        .attributes
        .iter()
        .map(|kv| {
            let value = kv.value.as_ref().unwrap().value.as_ref().unwrap();
            if let vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(s) =
                value
            {
                (kv.key.clone(), s.clone())
            } else {
                (kv.key.clone(), "".to_string())
            }
        })
        .collect();

    // Verify resource attributes were extracted
    assert_eq!(
        resource_attrs.get("service.name"),
        Some(&"trace-service".to_string())
    );
    assert_eq!(
        resource_attrs.get("service.version"),
        Some(&"1.0.0".to_string())
    );
    assert_eq!(
        resource_attrs.get("k8s.cluster"),
        Some(&"test-cluster".to_string())
    );
}

#[test]
fn test_resource_attribute_extractor_trait_traces() {
    // Test the trait methods directly on TraceEvent
    let mut trace_fields = ObjectMap::new();
    trace_fields.insert("trace_id".into(), Value::from("test_trace"));
    trace_fields.insert("resource.service.name".into(), Value::from("trace-service"));
    trace_fields.insert("resource.cluster".into(), Value::from("test-cluster"));
    trace_fields.insert("span_name".into(), Value::from("test-span"));

    let mut trace_event =
        TraceEvent::from_parts(trace_fields, vector_lib::event::EventMetadata::default());

    // Test extraction
    let resource_attrs = trace_event.extract_resource_attributes();
    assert_eq!(resource_attrs.len(), 2);

    let service_name = resource_attrs
        .iter()
        .find(|attr| attr.key == "service.name")
        .unwrap();
    if let Some(any_value) = &service_name.value {
        if let Some(PbValue::StringValue(s)) = &any_value.value {
            assert_eq!(s, "trace-service");
        }
    }

    // Verify resource fields are still present (we no longer remove them)
    // TraceEvent uses flattened field names with dots
    assert!(trace_event.as_map().contains_key("resource.service.name"));
    assert!(trace_event.as_map().contains_key("resource.cluster"));
    assert!(trace_event.as_map().contains_key("span_name"));
}
