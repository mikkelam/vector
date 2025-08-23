use crate::sinks::opentelemetry::encoder::NumberDataPointValue;
use crate::sinks::opentelemetry::encoder::OtlpEncoder;
use crate::sinks::opentelemetry::encoder::PbValue;
use crate::sinks::opentelemetry::encoder::ResourceAttributeExtractor;
use crate::sinks::opentelemetry::encoder::VectorMetric;
use crate::sinks::util::encoding::Encoder;

use prost::Message;
use vector_lib::event::Event;

#[test]
fn test_set_distribution_sketch_metrics() {
    use crate::event::metric::{MetricSketch, Sample, StatisticKind};
    use crate::event::{Metric, MetricKind, MetricValue};
    use crate::metrics::AgentDDSketch;
    use chrono::Utc;
    use std::collections::BTreeSet;
    use vector_lib::opentelemetry::proto::{
        collector::metrics::v1::ExportMetricsServiceRequest, metrics::v1::metric::Data,
    };

    let encoder = OtlpEncoder::new_default();

    // Test Set metric (converted to gauge with cardinality)
    let mut set_values = BTreeSet::new();
    set_values.insert("value1".to_string());
    set_values.insert("value2".to_string());
    set_values.insert("value3".to_string());

    let set_metric = Event::Metric(
        Metric::new(
            "test_set",
            MetricKind::Absolute,
            MetricValue::Set { values: set_values },
        )
        .with_timestamp(Some(Utc::now())),
    );

    // Test Distribution metric (converted to histogram)
    let distribution_metric = Event::Metric(
        Metric::new(
            "test_distribution",
            MetricKind::Absolute,
            MetricValue::Distribution {
                samples: vec![
                    Sample {
                        value: 1.0,
                        rate: 1,
                    },
                    Sample {
                        value: 5.0,
                        rate: 1,
                    },
                    Sample {
                        value: 10.0,
                        rate: 1,
                    },
                ],
                statistic: StatisticKind::Histogram,
            },
        )
        .with_timestamp(Some(Utc::now())),
    );

    // Test Sketch metric (converted to histogram)
    let mut ddsketch = AgentDDSketch::with_agent_defaults();
    ddsketch.insert_many(&[1.0, 2.0, 3.0, 4.0, 5.0]);

    let sketch_metric = Event::Metric(
        Metric::new(
            "test_sketch",
            MetricKind::Absolute,
            MetricValue::Sketch {
                sketch: MetricSketch::AgentDDSketch(ddsketch),
            },
        )
        .with_timestamp(Some(Utc::now())),
    );

    let mut buf = Vec::new();
    let result = encoder.encode_input(
        vec![set_metric, distribution_metric, sketch_metric],
        &mut buf,
    );
    assert!(result.is_ok());

    let request = ExportMetricsServiceRequest::decode(buf.as_slice()).unwrap();

    assert_eq!(request.resource_metrics.len(), 3);

    // Find each metric by name since order isn't guaranteed
    let mut set_metric = None;
    let mut distribution_metric = None;
    let mut sketch_metric = None;

    for resource_metric in &request.resource_metrics {
        assert_eq!(resource_metric.scope_metrics.len(), 1);
        let scope_metrics = &resource_metric.scope_metrics[0];
        assert_eq!(scope_metrics.metrics.len(), 1);
        let metric = &scope_metrics.metrics[0];

        match metric.name.as_str() {
            "test_set" => set_metric = Some(metric),
            "test_distribution" => distribution_metric = Some(metric),
            "test_sketch" => sketch_metric = Some(metric),
            _ => panic!("Unexpected metric name: {}", metric.name),
        }
    }

    // Validate set metric (converted to gauge)
    let set_metric = set_metric.unwrap();
    assert_eq!(set_metric.name, "test_set");
    if let Some(Data::Gauge(gauge)) = &set_metric.data {
        assert_eq!(gauge.data_points.len(), 1);
        let data_point = &gauge.data_points[0];
        // Should have cardinality of 3
        if let Some(NumberDataPointValue::AsDouble(val)) = &data_point.value {
            assert_eq!(*val, 3.0);
        }
    } else {
        panic!("Expected set to have Gauge data");
    }

    // Validate distribution metric (converted to histogram)
    let distribution_metric = distribution_metric.unwrap();
    assert_eq!(distribution_metric.name, "test_distribution");
    if let Some(Data::Histogram(hist)) = &distribution_metric.data {
        assert_eq!(hist.data_points.len(), 1);
        let data_point = &hist.data_points[0];
        assert_eq!(data_point.count, 3);
        assert_eq!(data_point.sum, Some(16.0)); // 1.0 + 5.0 + 10.0
    } else {
        panic!("Expected distribution to have Histogram data");
    }

    // Validate sketch metric (converted to histogram)
    let sketch_metric = sketch_metric.unwrap();
    assert_eq!(sketch_metric.name, "test_sketch");
    if let Some(Data::Histogram(hist)) = &sketch_metric.data {
        assert_eq!(hist.data_points.len(), 1);
        let data_point = &hist.data_points[0];
        assert_eq!(data_point.count, 5);
        assert_eq!(data_point.sum, Some(15.0)); // 1.0 + 2.0 + 3.0 + 4.0 + 5.0
    } else {
        panic!("Expected sketch to have Histogram data");
    }
}

#[test]
fn test_incremental_metric_normalization() {
    use crate::event::{Metric, MetricKind, MetricValue};
    use chrono::Utc;
    use vector_lib::opentelemetry::proto::{
        collector::metrics::v1::ExportMetricsServiceRequest,
        metrics::v1::{AggregationTemporality, metric::Data},
    };

    let encoder = OtlpEncoder::new_default();

    // Test that absolute metrics pass through unchanged
    let absolute_counter = Event::Metric(
        Metric::new(
            "test_counter",
            MetricKind::Absolute,
            MetricValue::Counter { value: 42.0 },
        )
        .with_timestamp(Some(Utc::now())),
    );

    // Test that incremental metrics get converted to absolute via normalization
    let incremental_counter = Event::Metric(
        Metric::new(
            "test_incremental",
            MetricKind::Incremental,
            MetricValue::Counter { value: 10.0 },
        )
        .with_timestamp(Some(Utc::now())),
    );

    let mut buf = Vec::new();
    let result = encoder.encode_input(vec![absolute_counter, incremental_counter], &mut buf);
    assert!(result.is_ok());

    let request = ExportMetricsServiceRequest::decode(buf.as_slice()).unwrap();

    assert_eq!(request.resource_metrics.len(), 2);

    // Check each ResourceMetrics has the expected structure
    for resource_metric in &request.resource_metrics {
        assert_eq!(resource_metric.scope_metrics.len(), 1);
        let scope_metrics = &resource_metric.scope_metrics[0];
        assert_eq!(scope_metrics.metrics.len(), 1);

        // All metrics should use cumulative temporality
        let metric = &scope_metrics.metrics[0];
        if let Some(Data::Sum(sum)) = &metric.data {
            assert_eq!(
                sum.aggregation_temporality,
                AggregationTemporality::Cumulative as i32
            );
        }
    }
}

#[test]
fn test_metrics_conversion() {
    use crate::event::metric::Bucket;
    use crate::event::{Metric, MetricKind, MetricValue};
    use chrono::Utc;
    use vector_lib::opentelemetry::proto::{
        collector::metrics::v1::ExportMetricsServiceRequest,
        metrics::v1::{AggregationTemporality, metric::Data},
    };

    let encoder = OtlpEncoder::new_default();

    // Test Counter metric (using Absolute to avoid normalization dropping)
    let counter = Event::Metric(
        Metric::new(
            "test_counter",
            MetricKind::Absolute,
            MetricValue::Counter { value: 42.0 },
        )
        .with_timestamp(Some(Utc::now())),
    );

    // Test Gauge metric
    let gauge = Event::Metric(
        Metric::new(
            "test_gauge",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 123.45 },
        )
        .with_timestamp(Some(Utc::now())),
    );

    // Test Histogram metric
    let histogram = Event::Metric(
        Metric::new(
            "test_histogram",
            MetricKind::Absolute,
            MetricValue::AggregatedHistogram {
                buckets: vec![
                    Bucket {
                        upper_limit: 1.0,
                        count: 5,
                    },
                    Bucket {
                        upper_limit: 5.0,
                        count: 10,
                    },
                    Bucket {
                        upper_limit: f64::INFINITY,
                        count: 2,
                    },
                ],
                count: 17,
                sum: 25.5,
            },
        )
        .with_timestamp(Some(Utc::now())),
    );

    let mut buf = Vec::new();
    let result = encoder.encode_input(vec![counter, gauge, histogram], &mut buf);
    assert!(result.is_ok());

    let request = ExportMetricsServiceRequest::decode(buf.as_slice()).unwrap();

    assert_eq!(request.resource_metrics.len(), 3);

    // Find each metric by name since order isn't guaranteed
    let mut counter_metric = None;
    let mut gauge_metric = None;
    let mut histogram_metric = None;

    for resource_metric in &request.resource_metrics {
        assert_eq!(resource_metric.scope_metrics.len(), 1);
        let scope_metrics = &resource_metric.scope_metrics[0];
        assert_eq!(scope_metrics.metrics.len(), 1);
        let metric = &scope_metrics.metrics[0];

        match metric.name.as_str() {
            "test_counter" => counter_metric = Some(metric),
            "test_gauge" => gauge_metric = Some(metric),
            "test_histogram" => histogram_metric = Some(metric),
            _ => panic!("Unexpected metric name: {}", metric.name),
        }
    }

    // Validate counter metric
    let counter_metric = counter_metric.unwrap();
    assert_eq!(counter_metric.name, "test_counter");
    if let Some(Data::Sum(sum)) = &counter_metric.data {
        assert_eq!(
            sum.aggregation_temporality,
            AggregationTemporality::Cumulative as i32
        );
        assert!(sum.is_monotonic);
        assert_eq!(sum.data_points.len(), 1);
    } else {
        panic!("Expected counter to have Sum data");
    }

    // Validate gauge metric
    let gauge_metric = gauge_metric.unwrap();
    assert_eq!(gauge_metric.name, "test_gauge");
    if let Some(Data::Gauge(_)) = &gauge_metric.data {
        // Gauge validation passed
    } else {
        panic!("Expected gauge to have Gauge data");
    }

    // Validate histogram metric
    let histogram_metric = histogram_metric.unwrap();
    assert_eq!(histogram_metric.name, "test_histogram");
    if let Some(Data::Histogram(hist)) = &histogram_metric.data {
        assert_eq!(hist.data_points.len(), 1);
        let data_point = &hist.data_points[0];
        assert_eq!(data_point.count, 17);
        assert_eq!(data_point.sum, Some(25.5));
        assert_eq!(data_point.explicit_bounds, vec![1.0, 5.0]);
        assert_eq!(data_point.bucket_counts, vec![5, 10, 2]);
    } else {
        panic!("Expected histogram to have Histogram data");
    }
}

#[test]
fn test_metric_resource_extraction() {
    use prost::Message;

    use vector_lib::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;

    let encoder = OtlpEncoder::new_default();

    // Create metric with flattened resource attributes
    let mut metric = VectorMetric::new(
        "test_counter",
        crate::event::metric::MetricKind::Incremental,
        crate::event::metric::MetricValue::Counter { value: 1.0 },
    );

    // Add resource attributes as tags with "resource." prefix
    let mut tags = crate::event::metric::MetricTags::default();
    tags.replace(
        "resource.service.name".to_string(),
        "test-service".to_string(),
    );
    tags.replace(
        "resource.deployment.environment".to_string(),
        "production".to_string(),
    );
    tags.replace("resource.host.name".to_string(), "test-host".to_string());
    tags.replace("normal.tag".to_string(), "normal-value".to_string());
    metric = metric.with_tags(Some(tags));

    let events = vec![Event::Metric(metric)];
    let result = encoder.encode_metrics(events).unwrap();

    // Decode and verify resource attributes are extracted
    let request = ExportMetricsServiceRequest::decode(result.as_ref()).unwrap();
    let resource_metrics = &request.resource_metrics[0];
    let resource = resource_metrics.resource.as_ref().unwrap();

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
        Some(&"test-service".to_string())
    );
    assert_eq!(
        resource_attrs.get("deployment.environment"),
        Some(&"production".to_string())
    );
    assert_eq!(
        resource_attrs.get("host.name"),
        Some(&"test-host".to_string())
    );

    // Verify normal tags were not extracted as resource attributes
    assert!(!resource_attrs.contains_key("normal.tag"));
}

#[test]
fn test_resource_attribute_extractor_trait_metrics() {
    // Test the trait methods directly on VectorMetric
    let mut metric = VectorMetric::new(
        "test_metric",
        crate::event::metric::MetricKind::Absolute,
        crate::event::metric::MetricValue::Counter { value: 1.0 },
    );

    let mut tags = crate::event::metric::MetricTags::default();
    tags.replace(
        "resource.service.name".to_string(),
        "metric-service".to_string(),
    );
    tags.replace("resource.version".to_string(), "2.0.0".to_string());
    tags.replace("normal_tag".to_string(), "normal_value".to_string());
    metric = metric.with_tags(Some(tags));

    // Test extraction
    let resource_attrs = metric.extract_resource_attributes();
    assert_eq!(resource_attrs.len(), 2);

    let service_name = resource_attrs
        .iter()
        .find(|attr| attr.key == "service.name")
        .unwrap();
    if let Some(any_value) = &service_name.value {
        if let Some(PbValue::StringValue(s)) = &any_value.value {
            assert_eq!(s, "metric-service");
        }
    }

    // Verify resource tags are still present (we no longer remove them)
    if let Some(tags) = metric.tags() {
        assert!(tags.contains_key("resource.service.name"));
        assert!(tags.contains_key("resource.version"));
        assert!(tags.contains_key("normal_tag"));
    }
}
