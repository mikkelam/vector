use crate::sinks::opentelemetry::encoder::OtlpEncoder;

use crate::sinks::util::encoding::Encoder;

use prost::Message;
use vector_lib::event::Event;

#[test]
fn test_metric_resource_attributes_not_duplicated() {
    use crate::event::{Metric, MetricKind, MetricValue};
    use chrono::Utc;
    use vector_lib::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;

    let encoder = OtlpEncoder::new_default();

    // Build tags with resource.* and a normal tag
    let mut tags = crate::event::MetricTags::default();
    tags.insert(
        "resource.service.name".to_string(),
        crate::event::metric::TagValue::from("svc-x"),
    );
    tags.insert(
        "resource.host.name".to_string(),
        crate::event::metric::TagValue::from("host-x"),
    );
    tags.insert(
        "env".to_string(),
        crate::event::metric::TagValue::from("prod"),
    );

    let metric = Event::Metric(
        Metric::new(
            "resource_test_gauge",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 1.0 },
        )
        .with_tags(Some(tags))
        .with_timestamp(Some(Utc::now())),
    );

    let mut buf = Vec::new();
    let result = encoder.encode_input(vec![metric], &mut buf);
    assert!(result.is_ok());

    let request = ExportMetricsServiceRequest::decode(buf.as_slice()).unwrap();
    assert_eq!(request.resource_metrics.len(), 1);

    let rm = &request.resource_metrics[0];
    let resource = rm.resource.as_ref().expect("resource present");
    let has_res = |k: &str| resource.attributes.iter().any(|kv| kv.key == k);
    // Resource-level attributes should contain stripped keys
    assert!(has_res("service.name"));
    assert!(has_res("host.name"));

    // Data point attributes should not include resource.* or stripped resource keys
    let scope_metrics = &rm.scope_metrics[0];
    let metric = &scope_metrics.metrics[0];
    if let Some(vector_lib::opentelemetry::proto::metrics::v1::metric::Data::Gauge(g)) =
        &metric.data
    {
        let attrs = &g.data_points[0].attributes;
        let has = |k: &str| attrs.iter().any(|kv| kv.key == k);

        // Normal tag remains at point level
        assert!(has("env"));

        // No resource.* keys leaked
        assert!(!has("resource.service.name"));
        assert!(!has("resource.host.name"));

        // And not duplicated as stripped keys at point level
        assert!(!has("service.name"));
        assert!(!has("host.name"));
    } else {
        panic!("Expected Gauge metric data");
    }
}

#[test]
fn test_counter_sum_encoding_temporality_monotonic_timestamps_and_attributes() {
    use crate::event::{Metric, MetricKind, MetricValue};
    use chrono::Utc;
    use vector_lib::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
    use vector_lib::opentelemetry::proto::metrics::v1::{
        AggregationTemporality, metric::Data, number_data_point::Value as NumberValue,
    };

    let encoder = OtlpEncoder::new_default();

    // Build tags with a resource attribute and a normal tag
    let mut tags_abs = crate::event::MetricTags::default();
    tags_abs.insert(
        "resource.service.name".to_string(),
        crate::event::metric::TagValue::from("svc-a"),
    );
    tags_abs.insert(
        "env".to_string(),
        crate::event::metric::TagValue::from("prod"),
    );

    let mut tags_delta = crate::event::MetricTags::default();
    tags_delta.insert(
        "resource.service.name".to_string(),
        crate::event::metric::TagValue::from("svc-b"),
    );
    tags_delta.insert(
        "env".to_string(),
        crate::event::metric::TagValue::from("stage"),
    );

    let ts_abs = Utc::now();
    let ts_delta = Utc::now();

    // Absolute counter -> Cumulative Sum
    let abs_counter = Event::Metric(
        Metric::new(
            "abs_counter",
            MetricKind::Absolute,
            MetricValue::Counter { value: 42.0 },
        )
        .with_tags(Some(tags_abs))
        .with_timestamp(Some(ts_abs)),
    );

    // Incremental counter -> Delta Sum
    let delta_counter = Event::Metric(
        Metric::new(
            "delta_counter",
            MetricKind::Incremental,
            MetricValue::Counter { value: 5.0 },
        )
        .with_tags(Some(tags_delta))
        .with_timestamp(Some(ts_delta)),
    );

    let mut buf = Vec::new();
    let result = encoder.encode_input(vec![abs_counter, delta_counter], &mut buf);
    assert!(result.is_ok());

    let request = ExportMetricsServiceRequest::decode(buf.as_slice()).unwrap();
    assert_eq!(request.resource_metrics.len(), 2);

    // Collect by metric name
    let mut abs = None;
    let mut delta = None;
    for rm in &request.resource_metrics {
        assert_eq!(rm.scope_metrics.len(), 1);
        let metric = &rm.scope_metrics[0].metrics[0];
        match metric.name.as_str() {
            "abs_counter" => abs = Some((rm, metric)),
            "delta_counter" => delta = Some((rm, metric)),
            other => panic!("unexpected metric: {}", other),
        }
    }

    // Validate absolute counter => Cumulative Sum, monotonic, timestamps, attributes
    let (rm_abs, m_abs) = abs.expect("abs_counter present");
    // Resource attribute present and stripped
    let res_abs = rm_abs.resource.as_ref().expect("resource present");
    assert!(res_abs.attributes.iter().any(|kv| kv.key == "service.name"));

    if let Some(Data::Sum(sum)) = &m_abs.data {
        assert_eq!(
            sum.aggregation_temporality,
            AggregationTemporality::Cumulative as i32
        );
        assert!(sum.is_monotonic);
        assert_eq!(sum.data_points.len(), 1);
        let dp = &sum.data_points[0];

        // time_unix_nano matches, start_time_unix_nano is 0
        assert_eq!(
            dp.time_unix_nano,
            ts_abs.timestamp_nanos_opt().unwrap() as u64
        );
        assert_eq!(dp.start_time_unix_nano, 0);

        // value is correct
        match dp.value.as_ref() {
            Some(NumberValue::AsDouble(v)) => assert_eq!(*v, 42.0),
            other => panic!("expected AsDouble(42.0), got {:?}", other),
        }

        // point attributes contain normal tag, not resource
        let has = |k: &str| dp.attributes.iter().any(|kv| kv.key == k);
        assert!(has("env"));
        assert!(!has("resource.service.name"));
        assert!(!has("service.name"));
    } else {
        panic!("expected Sum for abs_counter");
    }

    // Validate incremental counter => Delta Sum, monotonic, timestamps, attributes
    let (rm_delta, m_delta) = delta.expect("delta_counter present");
    // Resource attribute present and stripped
    let res_delta = rm_delta.resource.as_ref().expect("resource present");
    assert!(
        res_delta
            .attributes
            .iter()
            .any(|kv| kv.key == "service.name")
    );

    if let Some(Data::Sum(sum)) = &m_delta.data {
        assert_eq!(
            sum.aggregation_temporality,
            AggregationTemporality::Delta as i32
        );
        assert!(sum.is_monotonic);
        assert_eq!(sum.data_points.len(), 1);
        let dp = &sum.data_points[0];

        // time_unix_nano matches, start_time_unix_nano is 0
        assert_eq!(
            dp.time_unix_nano,
            ts_delta.timestamp_nanos_opt().unwrap() as u64
        );
        assert_eq!(dp.start_time_unix_nano, 0);

        // value is correct
        match dp.value.as_ref() {
            Some(NumberValue::AsDouble(v)) => assert_eq!(*v, 5.0),
            other => panic!("expected AsDouble(5.0), got {:?}", other),
        }

        // point attributes contain normal tag, not resource
        let has = |k: &str| dp.attributes.iter().any(|kv| kv.key == k);
        assert!(has("env"));
        assert!(!has("resource.service.name"));
        assert!(!has("service.name"));
    } else {
        panic!("expected Sum for delta_counter");
    }
}

#[test]
fn test_gauge_encoding_timestamps_and_attributes() {
    use crate::event::{Metric, MetricKind, MetricValue};
    use chrono::Utc;
    use vector_lib::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
    use vector_lib::opentelemetry::proto::metrics::v1::{
        metric::Data, number_data_point::Value as NumberValue,
    };

    let encoder = OtlpEncoder::new_default();

    // Build tags with a resource attribute and a normal tag
    let mut tags = crate::event::MetricTags::default();
    tags.insert(
        "resource.service.name".to_string(),
        crate::event::metric::TagValue::from("svc-g"),
    );
    tags.insert(
        "env".to_string(),
        crate::event::metric::TagValue::from("prod"),
    );

    let ts = Utc::now();

    let gauge = Event::Metric(
        Metric::new(
            "g1",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 123.45 },
        )
        .with_tags(Some(tags))
        .with_timestamp(Some(ts)),
    );

    let mut buf = Vec::new();
    let result = encoder.encode_input(vec![gauge], &mut buf);
    assert!(result.is_ok());

    let request = ExportMetricsServiceRequest::decode(buf.as_slice()).unwrap();
    assert_eq!(request.resource_metrics.len(), 1);

    let rm = &request.resource_metrics[0];
    // Resource has stripped attribute
    let resource = rm.resource.as_ref().expect("resource present");
    assert!(
        resource
            .attributes
            .iter()
            .any(|kv| kv.key == "service.name")
    );

    let metric = &rm.scope_metrics[0].metrics[0];
    if let Some(Data::Gauge(g)) = &metric.data {
        assert_eq!(g.data_points.len(), 1);
        let dp = &g.data_points[0];

        // Timestamps: time set, no start time
        assert_eq!(dp.time_unix_nano, ts.timestamp_nanos_opt().unwrap() as u64);
        assert_eq!(dp.start_time_unix_nano, 0);

        // Attributes: normal tag present, resource tags not duplicated
        let has = |k: &str| dp.attributes.iter().any(|kv| kv.key == k);
        assert!(has("env"));
        assert!(!has("resource.service.name"));
        assert!(!has("service.name"));

        // Value is encoded as double
        match dp.value.as_ref() {
            Some(NumberValue::AsDouble(v)) => assert_eq!(*v, 123.45),
            other => panic!("expected AsDouble(123.45), got {:?}", other),
        }
    } else {
        panic!("expected Gauge for g1");
    }
}

#[test]
fn test_histogram_bucket_counts_sum_equals_count() {
    use crate::event::metric::Bucket;
    use crate::event::{Metric, MetricKind, MetricValue};
    use chrono::Utc;
    use vector_lib::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
    use vector_lib::opentelemetry::proto::metrics::v1::metric::Data;

    let encoder = OtlpEncoder::new_default();

    // Aggregated histogram with explicit +Inf bucket
    let histogram = Event::Metric(
        Metric::new(
            "h1",
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
    let result = encoder.encode_input(vec![histogram], &mut buf);
    assert!(result.is_ok());

    let request = ExportMetricsServiceRequest::decode(buf.as_slice()).unwrap();
    assert_eq!(request.resource_metrics.len(), 1);

    let rm = &request.resource_metrics[0];
    let metric = &rm.scope_metrics[0].metrics[0];

    if let Some(Data::Histogram(hist)) = &metric.data {
        assert_eq!(hist.data_points.len(), 1);
        let dp = &hist.data_points[0];

        // Invariant: sum(bucket_counts) == count
        let sum_bucket_counts: u64 = dp.bucket_counts.iter().copied().sum();
        assert_eq!(sum_bucket_counts, dp.count);

        // Invariant: bucket_counts.len() == explicit_bounds.len() + 1
        assert_eq!(dp.bucket_counts.len(), dp.explicit_bounds.len() + 1);

        // Bounds mapping: Vector upper limits excluding +inf become explicit_bounds
        assert_eq!(dp.explicit_bounds, vec![1.0, 5.0]);

        // Optional: sanity check the exact counts and total sum
        assert_eq!(dp.bucket_counts, vec![5, 10, 2]);
        assert_eq!(dp.count, 17);
        assert_eq!(dp.sum, Some(25.5));
    } else {
        panic!("expected Histogram for h1");
    }
}
