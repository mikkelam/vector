use crate::sinks::opentelemetry::encoder::OtlpEncoder;
use crate::sinks::util::encoding::Encoder;
use prost::Message;
use std::collections::BTreeMap;
use vector_lib::event::Event;
use vector_lib::event::TraceEvent;
use vector_lib::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
use vector_lib::opentelemetry::proto::common::v1::any_value;
use vector_lib::opentelemetry::proto::trace::v1::span::SpanKind;
use vrl::core::Value;

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
fn test_span_ids_kind_and_shape() {
    use vector_lib::opentelemetry::proto::trace::v1::span::SpanKind;

    // Case 1: IDs as hex strings, kind = server, spans array present (ignored)
    let mut root = BTreeMap::new();
    root.insert(
        "trace_id".into(),
        Value::from("0102030405060708090a0b0c0d0e0f10"),
    );
    root.insert("span_id".into(), Value::from("0102030405060708"));
    root.insert("parent_span_id".into(), Value::from("0203040506070809"));
    root.insert("kind".into(), Value::from("SeRvEr"));
    root.insert(
        "spans".into(),
        Value::from(vec![Value::from(BTreeMap::from([(
            "ignored".into(),
            Value::from(1),
        )]))]),
    );

    let e1 = Event::Trace(TraceEvent::from(root));

    // Case 2: IDs as bytes and invalid hex for parent (should be empty), unknown kind -> internal
    let mut bytes_case = BTreeMap::new();
    let trace_id_vec = hex::decode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    bytes_case.insert("trace_id".into(), Value::Bytes(trace_id_vec.into()));
    let span_id_vec = hex::decode("bbbbbbbbbbbbbbbb").unwrap();
    bytes_case.insert("span_id".into(), Value::Bytes(span_id_vec.into()));
    bytes_case.insert("parent_span_id".into(), Value::from("not-hex"));
    bytes_case.insert("name".into(), Value::Null); // force default later
    bytes_case.insert("kind".into(), Value::from("unknown"));

    let e2 = Event::Trace(TraceEvent::from(bytes_case));

    let encoder = OtlpEncoder::new_default();
    let mut buf = Vec::new();
    let res = encoder.encode_input(vec![e1, e2], &mut buf);
    assert!(res.is_ok());

    let req = ExportTraceServiceRequest::decode(buf.as_slice()).unwrap();
    assert_eq!(req.resource_spans.len(), 2);

    // First span
    {
        let span = &req.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(
            hex::encode(&span.trace_id),
            "0102030405060708090a0b0c0d0e0f10"
        );
        assert_eq!(hex::encode(&span.span_id), "0102030405060708");
        assert_eq!(hex::encode(&span.parent_span_id), "0203040506070809");
        assert_eq!(span.kind, SpanKind::Server as i32);
        // spans array ignored and does not produce extra spans
        assert_eq!(req.resource_spans[0].scope_spans[0].spans.len(), 1);
    }

    // Second span
    {
        let span = &req.resource_spans[1].scope_spans[0].spans[0];
        assert_eq!(
            hex::encode(&span.trace_id),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(hex::encode(&span.span_id), "bbbbbbbbbbbbbbbb");
        // invalid parent -> empty
        assert!(span.parent_span_id.is_empty());
        // default kind -> internal
        assert_eq!(span.kind, SpanKind::Internal as i32);
        // default name when omitted/invalid
        assert_eq!(span.name, "unknown");
    }
}

#[test]
fn test_timestamp_parsing_and_fallbacks() {
    use chrono::{TimeZone, Utc};

    // Case 1: start/end as Timestamp
    let mut t1 = BTreeMap::new();
    t1.insert(
        "start_time_unix_nano".into(),
        Value::Timestamp(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 1).unwrap()),
    );
    t1.insert(
        "end_time_unix_nano".into(),
        Value::Timestamp(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 2).unwrap()),
    );
    t1.insert("trace_id".into(), Value::from("0".repeat(32)));
    t1.insert("span_id".into(), Value::from("0".repeat(16)));

    // Case 2: start as integer nanos; end missing but timestamp present (fallback)
    let mut t2 = BTreeMap::new();
    t2.insert(
        "start_time_unix_nano".into(),
        Value::from(1672531203000000000u64),
    );
    t2.insert(
        "timestamp".into(),
        Value::Timestamp(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 5).unwrap()),
    );
    t2.insert("trace_id".into(), Value::from("1".repeat(32)));
    t2.insert("span_id".into(), Value::from("1".repeat(16)));

    // Case 3: neither start nor fallback -> zeros
    let mut t3 = BTreeMap::new();
    t3.insert("trace_id".into(), Value::from("2".repeat(32)));
    t3.insert("span_id".into(), Value::from("2".repeat(16)));

    let e1 = Event::Trace(TraceEvent::from(t1));
    let e2 = Event::Trace(TraceEvent::from(t2));
    let e3 = Event::Trace(TraceEvent::from(t3));

    let encoder = OtlpEncoder::new_default();
    let mut buf = Vec::new();
    assert!(encoder.encode_input(vec![e1, e2, e3], &mut buf).is_ok());

    let req = ExportTraceServiceRequest::decode(buf.as_slice()).unwrap();
    assert_eq!(req.resource_spans.len(), 3);

    // Case 1
    {
        let span = &req.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.start_time_unix_nano, 1672531201000000000);
        assert_eq!(span.end_time_unix_nano, 1672531202000000000);
    }

    // Case 2
    {
        let span = &req.resource_spans[1].scope_spans[0].spans[0];
        assert_eq!(span.start_time_unix_nano, 1672531203000000000);
        assert_eq!(span.end_time_unix_nano, 1672531205000000000);
    }

    // Case 3
    {
        let span = &req.resource_spans[2].scope_spans[0].spans[0];
        assert_eq!(span.start_time_unix_nano, 0);
        assert_eq!(span.end_time_unix_nano, 0);
    }
}
#[test]
fn numeric_kind_and_status_object_precedence() {
    // Build a trace event with numeric kind and a status object
    let mut m = BTreeMap::new();
    // valid IDs (hex strings)
    m.insert(
        "trace_id".into(),
        Value::from("00112233445566778899aabbccddeeff"),
    );
    m.insert("span_id".into(), Value::from("0011223344556677"));
    // numeric kind = Server (2)
    m.insert("kind".into(), Value::from(2));
    // conflicting tag says OK, but status object should take precedence to ERROR
    m.insert(
        "tags".into(),
        Value::from(BTreeMap::from([(
            "otel.status_code".into(),
            Value::from("ok"),
        )])),
    );
    m.insert(
        "status".into(),
        Value::from(BTreeMap::from([
            ("code".into(), Value::from(2)),
            ("message".into(), Value::from("boom")),
        ])),
    );

    let event = Event::Trace(TraceEvent::from(m));
    let enc = OtlpEncoder::new_default();
    let mut buf = Vec::new();
    assert!(enc.encode_input(vec![event], &mut buf).is_ok());

    let req = ExportTraceServiceRequest::decode(buf.as_slice()).unwrap();
    let span = &req.resource_spans[0].scope_spans[0].spans[0];

    // Numeric kind passed through
    assert_eq!(span.kind, SpanKind::Server as i32);

    // Status object takes precedence over tag-derived status
    let status = span.status.as_ref().expect("status must be set");
    assert_eq!(status.code, 2);
    assert_eq!(status.message, "boom");
}

#[test]
fn test_attributes_resources_events_links() {
    use chrono::{TimeZone, Utc};
    use vector_lib::opentelemetry::proto::common::v1::any_value;

    let mut trace = BTreeMap::new();
    trace.insert("name".into(), Value::from("attrs-span"));
    trace.insert(
        "trace_id".into(),
        Value::from("abcdefabcdefabcdefabcdefabcdefab"),
    );
    trace.insert("span_id".into(), Value::from("abcdefabcdefabcd"));

    // tags and attributes (overlapping keys + nested)
    let mut tags = BTreeMap::new();
    tags.insert("http.method".into(), Value::from("POST"));
    tags.insert("overlap".into(), Value::from("tag"));
    let mut attrs = BTreeMap::new();
    attrs.insert("overlap".into(), Value::from("attr"));
    let mut nested = BTreeMap::new();
    nested.insert("n".into(), Value::from(true));
    attrs.insert("nested".into(), Value::from(nested));
    trace.insert("tags".into(), Value::from(tags));
    trace.insert("attributes".into(), Value::from(attrs));

    // resources object
    let mut resources = BTreeMap::new();
    resources.insert("service.name".into(), Value::from("svc-x"));
    resources.insert("host.name".into(), Value::from("host-x"));
    trace.insert("resources".into(), Value::from(resources));

    // events: one with timestamp, one with time_unix_nano
    let ev1 = BTreeMap::from([
        ("name".into(), Value::from("ev1")),
        (
            "timestamp".into(),
            Value::Timestamp(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 1).unwrap()),
        ),
        (
            "attributes".into(),
            Value::from(BTreeMap::from([("k1".into(), Value::from(1))])),
        ),
        ("dropped_attributes_count".into(), Value::from(1)),
    ]);
    let ev2 = BTreeMap::from([
        ("name".into(), Value::from("ev2")),
        ("time_unix_nano".into(), Value::from(1672531202000000000u64)),
        (
            "attributes".into(),
            Value::from(BTreeMap::from([("k2".into(), Value::from("v2"))])),
        ),
        ("dropped_attributes_count".into(), Value::from(2)),
    ]);
    trace.insert(
        "events".into(),
        Value::from(vec![Value::from(ev1), Value::from(ev2)]),
    );

    // links: hex and bytes
    let link1 = BTreeMap::from([
        (
            "trace_id".into(),
            Value::from("00112233445566778899aabbccddeeff"),
        ),
        ("span_id".into(), Value::from("0011223344556677")),
        (
            "attributes".into(),
            Value::from(BTreeMap::from([("lk".into(), Value::from("lv"))])),
        ),
        ("dropped_attributes_count".into(), Value::from(1)),
    ]);
    let link2 = {
        let tid_vec = hex::decode("ffeeddccbbaa99887766554433221100").unwrap();
        let sid_vec = hex::decode("7766554433221100").unwrap();
        BTreeMap::from([
            ("trace_id".into(), Value::Bytes(tid_vec.into())),
            ("span_id".into(), Value::Bytes(sid_vec.into())),
            (
                "attributes".into(),
                Value::from(BTreeMap::from([("lk2".into(), Value::from(2))])),
            ),
            ("dropped_attributes_count".into(), Value::from(0)),
        ])
    };
    trace.insert(
        "links".into(),
        Value::from(vec![Value::from(link1), Value::from(link2)]),
    );

    let event = Event::Trace(TraceEvent::from(trace));
    let encoder = OtlpEncoder::new_default();
    let mut buf = Vec::new();
    assert!(encoder.encode_input(vec![event], &mut buf).is_ok());

    let req = ExportTraceServiceRequest::decode(buf.as_slice()).unwrap();
    let rs = &req.resource_spans[0];
    let span = &rs.scope_spans[0].spans[0];

    // Resource attributes present and not duplicated as span attributes
    {
        let rkeys = rs
            .resource
            .as_ref()
            .map(|r| {
                r.attributes
                    .iter()
                    .map(|kv| kv.key.as_str())
                    .collect::<std::collections::HashSet<_>>()
            })
            .unwrap_or_default();
        assert!(rkeys.contains("service.name"));
        assert!(rkeys.contains("host.name"));

        let has = |k: &str| span.attributes.iter().any(|kv| kv.key == k);
        assert!(!has("service.name"));
        assert!(!has("host.name"));
    }

    // Attributes merged; nested is kvlist; overlapping keys may appear twice (tags + attributes)
    {
        let mut found_nested_kvlist = false;
        let mut overlap_seen = 0;
        for kv in &span.attributes {
            if kv.key == "nested" {
                if let Some(any_value::Value::KvlistValue(_)) =
                    kv.value.as_ref().and_then(|v| v.value.clone())
                {
                    found_nested_kvlist = true;
                }
            }
            if kv.key == "overlap" {
                overlap_seen += 1;
            }
        }
        assert!(found_nested_kvlist);
        assert_eq!(overlap_seen, 2);
    }

    // Events parsed
    {
        assert_eq!(span.events.len(), 2);
        let e1 = &span.events[0];
        let e2 = &span.events[1];
        assert_eq!(e1.name, "ev1");
        assert_eq!(e1.time_unix_nano, 1672531201000000000);
        assert_eq!(e2.name, "ev2");
        assert_eq!(e2.time_unix_nano, 1672531202000000000);
        // dropped counts carried over
        assert_eq!(e1.dropped_attributes_count, 1);
        assert_eq!(e2.dropped_attributes_count, 2);
    }

    // Links parsed with IDs via same rules
    {
        assert_eq!(span.links.len(), 2);
        let l1 = &span.links[0];
        let l2 = &span.links[1];
        assert_eq!(
            hex::encode(&l1.trace_id),
            "00112233445566778899aabbccddeeff"
        );
        assert_eq!(hex::encode(&l1.span_id), "0011223344556677");
        assert_eq!(
            hex::encode(&l2.trace_id),
            "ffeeddccbbaa99887766554433221100"
        );
        assert_eq!(hex::encode(&l2.span_id), "7766554433221100");
    }
}
