use crate::sinks::opentelemetry::encoder::ResourceAttributeExtractor;
use crate::sinks::opentelemetry::encoder::VectorMetric;
use vector_lib::event::LogEvent;

#[test]
fn test_resource_attribute_edge_cases() {
    // Test empty resource attributes
    let mut log = LogEvent::from("test");
    let attrs = log.extract_resource_attributes();
    assert_eq!(attrs.len(), 0);

    // Test metric without tags
    let mut metric = VectorMetric::new(
        "test",
        crate::event::metric::MetricKind::Absolute,
        crate::event::metric::MetricValue::Counter { value: 1.0 },
    );
    let attrs = metric.extract_resource_attributes();
    assert_eq!(attrs.len(), 0);

    // Test partial "resources" prefix (should not match)
    let mut log = LogEvent::from("test");
    log.insert("resource", "not-extracted");
    log.insert("resources", "also-not-extracted");
    let attrs = log.extract_resource_attributes();
    assert_eq!(attrs.len(), 0);
    assert!(log.get("resource").is_some());
    assert!(log.get("resources").is_some());
}
