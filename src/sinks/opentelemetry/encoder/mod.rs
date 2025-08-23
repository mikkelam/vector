//! Encoder for the `opentelemetry` sink.
//!
//! This module is responsible for converting Vector `Event`s into OTLP Protobuf messages.

use std::io;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use hex;

use prost::Message;
use vector_lib::opentelemetry::{
    logs::{
        ATTRIBUTES_KEY, DROPPED_ATTRIBUTES_COUNT_KEY, FLAGS_KEY, OBSERVED_TIMESTAMP_KEY,
        SEVERITY_NUMBER_KEY, SEVERITY_TEXT_KEY, SPAN_ID_KEY, TRACE_ID_KEY,
    },
    proto::{
        collector::{
            logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
            trace::v1::ExportTraceServiceRequest,
        },
        common::v1::{AnyValue, KeyValue, KeyValueList, any_value::Value as PbValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{
            AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric as OtlpMetric,
            NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
            metric::Data, number_data_point::Value as NumberDataPointValue,
            summary_data_point::ValueAtQuantile,
        },
        resource::v1::Resource,
        trace::v1::{
            ResourceSpans, ScopeSpans, Span, Status as SpanStatus,
            span::{Event as SpanEvent, Link, SpanKind},
        },
    },
};
use vrl::{
    event_path,
    value::{ObjectMap, Value},
};

use crate::{
    event::{
        Event, LogEvent, Metric as VectorMetric, MetricTags, MetricValue, TraceEvent,
        metric::{Bucket, MetricSketch, Quantile, Sample},
    },
    sinks::{
        prelude::*,
        util::buffer::metrics::{MetricNormalize, MetricSet},
    },
};

use super::config::OpenTelemetryConfig;

/// The encoder for OTLP, responsible for converting batches of events
/// into Protobuf byte payloads.
#[derive(Debug, Clone)]
pub(super) struct OtlpEncoder {}

/// Normalizer that converts all metrics to absolute (cumulative) values
/// This follows Vector's standard pattern used by Prometheus and other sinks
#[derive(Default)]
struct OtlpMetricNormalize;

impl MetricNormalize for OtlpMetricNormalize {
    fn normalize(&mut self, state: &mut MetricSet, metric: VectorMetric) -> Option<VectorMetric> {
        // Convert all metrics to absolute (cumulative) for consistent OTLP semantics
        // This matches the behavior of Prometheus sink and provides proper start_time
        state.make_absolute(metric)
    }
}

impl OtlpEncoder {
    /// Creates a new `OtlpEncoder`.
    pub(super) fn new(_otlp_config: OpenTelemetryConfig) -> Self {
        Self {}
    }

    /// Creates a new `OtlpEncoder` with default configuration for testing.
    #[cfg(test)]
    pub(super) const fn new_default() -> Self {
        Self {}
    }

    /// Encodes a batch of log events into an `ExportLogsServiceRequest` Protobuf message.
    pub fn encode_logs(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        let resource_logs = events
            .into_iter()
            .map(|e| {
                let mut log = e.into_log();

                // Extract resources before converting to log record
                let resource_attrs = if let Some(resources) = log.remove(event_path!("resources")) {
                    if let Value::Object(map) = resources {
                        convert_object_map_to_key_value_vec(map)
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

                ResourceLogs {
                    resource: Some(Resource {
                        attributes: resource_attrs,
                        dropped_attributes_count: 0,
                    }),
                    scope_logs: vec![
                        self.create_scope_logs(vec![self.convert_log_event_to_log_record(log)]),
                    ],
                    schema_url: String::new(),
                }
            })
            .collect();

        let request = ExportLogsServiceRequest { resource_logs };

        Ok(request.encode_to_vec().into())
    }

    /// Converts a single Vector `LogEvent` into an OTLP `LogRecord`.
    fn convert_log_event_to_log_record(&self, mut log: LogEvent) -> LogRecord {
        let mut log_record = LogRecord::default();

        // Handle body/message
        if let Some(msg) = log.get_message() {
            log_record.body = Some(convert_value_to_any_value(msg.clone()));
        }

        // Handle timestamp
        if let Some(Value::Timestamp(timestamp)) = log.get_timestamp() {
            log_record.time_unix_nano = timestamp.timestamp_nanos_opt().unwrap_or(0) as u64;
        }

        // Handle observed timestamp
        if let Some(Value::Timestamp(timestamp)) = log.remove(event_path!(OBSERVED_TIMESTAMP_KEY)) {
            log_record.observed_time_unix_nano =
                timestamp.timestamp_nanos_opt().unwrap_or(0) as u64;
        }

        // Handle trace_id
        if let Some(trace_id) = log.remove(event_path!(TRACE_ID_KEY)) {
            match trace_id {
                Value::Bytes(bytes) => {
                    if bytes.len() == 16 {
                        log_record.trace_id = bytes.into();
                    } else if let Ok(decoded) = hex::decode(bytes.as_ref()) {
                        if decoded.len() == 16 {
                            log_record.trace_id = decoded;
                        }
                    }
                }
                _ => {}
            }
        }

        // Handle span_id
        if let Some(span_id) = log.remove(event_path!(SPAN_ID_KEY)) {
            match span_id {
                Value::Bytes(bytes) => {
                    if bytes.len() == 8 {
                        log_record.span_id = bytes.into();
                    } else if let Ok(decoded) = hex::decode(bytes.as_ref()) {
                        if decoded.len() == 8 {
                            log_record.span_id = decoded;
                        }
                    }
                }
                _ => {}
            }
        }

        // Handle severity
        if let Some(severity_text) = log.remove(event_path!(SEVERITY_TEXT_KEY)) {
            if let Value::Bytes(text) = severity_text {
                log_record.severity_text = String::from_utf8_lossy(&text).to_string();
            }
        }

        if let Some(severity_number) = log.remove(event_path!(SEVERITY_NUMBER_KEY)) {
            if let Value::Integer(num) = severity_number {
                log_record.severity_number = num as i32;
            }
        }

        // Handle flags
        if let Some(flags) = log.remove(event_path!(FLAGS_KEY)) {
            if let Value::Integer(f) = flags {
                log_record.flags = f as u32;
            }
        }

        // Handle dropped_attributes_count
        if let Some(count) = log.remove(event_path!(DROPPED_ATTRIBUTES_COUNT_KEY)) {
            if let Value::Integer(c) = count {
                log_record.dropped_attributes_count = c as u32;
            }
        }

        // Handle attributes
        if let Some(attrs) = log.remove(event_path!(ATTRIBUTES_KEY)) {
            match attrs {
                Value::Object(map) => {
                    log_record
                        .attributes
                        .extend(convert_object_map_to_key_value_vec(map));
                }
                _ => {}
            }
        }

        // Remove core Vector fields that shouldn't be attributes
        log.remove(event_path!("message"));
        log.remove(event_path!("timestamp"));
        log.remove(event_path!("source_type"));

        // Convert remaining fields to attributes
        if let Some(map) = log.as_map() {
            log_record
                .attributes
                .extend(convert_object_map_to_key_value_vec(map.clone()));
        }

        log_record
    }

    /// Encodes a batch of metric events into an `ExportMetricsServiceRequest` Protobuf message.
    pub(super) fn encode_metrics(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        let resource_metrics = events
            .into_iter()
            .filter_map(|e| {
                let mut metric = e.into_metric();
                let resource_attrs = metric.extract_all_resource_attributes();

                // Apply normalization to convert incremental metrics to absolute
                let mut normalizer = OtlpMetricNormalize;
                let mut metric_state = MetricSet::default();

                normalizer
                    .normalize(&mut metric_state, metric)
                    .map(|normalized_metric| {
                        let otlp_metric = convert_vector_metric_to_otlp(normalized_metric);
                        ResourceMetrics {
                            resource: Some(Resource {
                                attributes: resource_attrs,
                                dropped_attributes_count: 0,
                            }),
                            scope_metrics: vec![self.create_scope_metrics(vec![otlp_metric])],
                            schema_url: String::new(),
                        }
                    })
            })
            .collect();

        let request = ExportMetricsServiceRequest { resource_metrics };
        Ok(request.encode_to_vec().into())
    }

    pub fn encode_traces(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        let resource_spans = events
            .into_iter()
            .map(|e| {
                let mut trace = e.into_trace();
                let resource_attrs = trace.extract_all_resource_attributes();

                ResourceSpans {
                    resource: Some(Resource {
                        attributes: resource_attrs,
                        dropped_attributes_count: 0,
                    }),
                    scope_spans: vec![
                        self.create_scope_spans(vec![convert_vector_trace_to_otlp_span(trace)]),
                    ],
                    schema_url: String::new(),
                }
            })
            .collect();

        let request = ExportTraceServiceRequest { resource_spans };
        Ok(request.encode_to_vec().into())
    }

    const fn create_scope_logs(&self, log_records: Vec<LogRecord>) -> ScopeLogs {
        ScopeLogs {
            scope: None,
            log_records,
            schema_url: String::new(),
        }
    }

    const fn create_scope_metrics(&self, metrics: Vec<OtlpMetric>) -> ScopeMetrics {
        ScopeMetrics {
            scope: None,
            metrics,
            schema_url: String::new(),
        }
    }

    const fn create_scope_spans(&self, spans: Vec<Span>) -> ScopeSpans {
        ScopeSpans {
            scope: None,
            spans,
            schema_url: String::new(),
        }
    }
}

impl crate::sinks::util::encoding::Encoder<Vec<Event>> for OtlpEncoder {
    fn encode_input(
        &self,
        events: Vec<Event>,
        writer: &mut dyn io::Write,
    ) -> io::Result<(usize, GroupedCountByteSize)> {
        if events.is_empty() {
            return Ok((0, GroupedCountByteSize::default()));
        }

        let byte_size = GroupedCountByteSize::new_untagged();

        // The partitioner ensures that all events in this batch are of the same type.
        // We can safely check the type of the first event to decide which encoder to use.
        let payload_result = match events.first().unwrap() {
            Event::Log(_) => self.encode_logs(events),
            Event::Metric(_) => self.encode_metrics(events),
            Event::Trace(_) => self.encode_traces(events),
        };

        match payload_result {
            Ok(payload) => {
                let len = payload.len();
                writer.write_all(&payload)?;
                Ok((len, byte_size))
            }
            Err(()) => {
                // The specific error was already emitted within the encoding function.
                // We return 0 bytes written to signal that the batch should be dropped.
                Ok((0, GroupedCountByteSize::default()))
            }
        }
    }
}

/// Trait for extracting and removing resource attributes from events
trait ResourceAttributeExtractor {
    fn extract_resource_attributes(&mut self) -> Vec<KeyValue>;

    /// Extract nested resource attributes (default: empty, override for logs/traces)
    fn extract_nested_resource_attributes(&mut self) -> Vec<KeyValue> {
        Vec::new()
    }

    /// Extract all resource attributes (both nested and flattened formats)
    fn extract_all_resource_attributes(&mut self) -> Vec<KeyValue> {
        let mut resource_attributes = Vec::new();

        // Try nested format first (if supported by event type)
        resource_attributes.extend(self.extract_nested_resource_attributes());

        // Then extract flattened fields for Vector OTLP source compatibility
        resource_attributes.extend(self.extract_resource_attributes());

        resource_attributes
    }
}

impl ResourceAttributeExtractor for LogEvent {
    fn extract_resource_attributes(&mut self) -> Vec<KeyValue> {
        // Extract resource attributes from nested "resources" object
        if let Some(resources) = self.remove(event_path!("resources")) {
            if let Value::Object(map) = resources {
                return convert_object_map_to_key_value_vec(map);
            }
        }
        Vec::new()
    }

    fn extract_nested_resource_attributes(&mut self) -> Vec<KeyValue> {
        // This is now handled by extract_resource_attributes
        Vec::new()
    }
}

impl ResourceAttributeExtractor for VectorMetric {
    fn extract_resource_attributes(&mut self) -> Vec<KeyValue> {
        let mut resource_attributes = Vec::new();

        if let Some(tags) = self.tags() {
            for (key, value) in tags.iter_single() {
                if let Some(attr_key) = key.strip_prefix("resource.") {
                    resource_attributes.push(KeyValue {
                        key: attr_key.to_string(),
                        value: Some(convert_value_to_any_value(Value::from(value.to_string()))),
                    });
                }
            }
        }

        resource_attributes
    }
}

impl ResourceAttributeExtractor for TraceEvent {
    fn extract_resource_attributes(&mut self) -> Vec<KeyValue> {
        let mut resource_attributes = Vec::new();

        for (key, value) in self.as_map().iter() {
            let key_str = key.to_string();
            if let Some(attr_key) = key_str.strip_prefix("resource.") {
                resource_attributes.push(KeyValue {
                    key: attr_key.to_string(),
                    value: Some(convert_value_to_any_value(value.clone())),
                });
            }
        }

        resource_attributes
    }

    fn extract_nested_resource_attributes(&mut self) -> Vec<KeyValue> {
        if let Some(resource_map) = self.get(event_path!("resource")) {
            if let Some(resource_obj) = resource_map.as_object() {
                // Check if there's an "attributes" key containing the actual attributes
                if let Some(attributes_value) = resource_obj.get("attributes") {
                    if let Some(attributes_obj) = attributes_value.as_object() {
                        return convert_object_map_to_key_value_vec(attributes_obj.clone());
                    }
                }
                // Fallback: treat the entire resource object as attributes
                return convert_object_map_to_key_value_vec(resource_obj.clone());
            }
        }
        Vec::new()
    }
}

// Helper functions for converting Vector `Value`s to OTLP Protobuf types.

pub(super) fn convert_value_to_any_value(value: Value) -> AnyValue {
    let value = match value {
        Value::Null => return AnyValue { value: None },
        Value::Boolean(b) => PbValue::BoolValue(b),
        Value::Integer(i) => PbValue::IntValue(i),
        Value::Float(f) => PbValue::DoubleValue(f.into()),
        Value::Bytes(b) => {
            // Try to convert bytes to UTF-8 string first, fallback to binary if not valid UTF-8
            if let Ok(s) = String::from_utf8(b.to_vec()) {
                PbValue::StringValue(s)
            } else {
                PbValue::BytesValue(b.into())
            }
        }
        Value::Timestamp(ts) => PbValue::StringValue(ts.to_string()),
        Value::Object(map) => PbValue::KvlistValue(convert_object_map_to_key_value_list(map)),
        Value::Array(arr) => {
            PbValue::ArrayValue(vector_lib::opentelemetry::proto::common::v1::ArrayValue {
                values: arr.into_iter().map(convert_value_to_any_value).collect(),
            })
        }
        // Fallback for other types
        other => PbValue::StringValue(other.to_string_lossy().into_owned()),
    };
    AnyValue { value: Some(value) }
}

fn convert_object_map_to_key_value_list(map: ObjectMap) -> KeyValueList {
    KeyValueList {
        values: convert_object_map_to_key_value_vec(map),
    }
}

fn convert_object_map_to_key_value_vec(map: ObjectMap) -> Vec<KeyValue> {
    map.into_iter()
        .map(|(key, value)| KeyValue {
            key: key.to_string(),
            value: Some(convert_value_to_any_value(value)),
        })
        .collect()
}

/// Converts a Vector Metric into an OTLP Metric.
fn convert_vector_metric_to_otlp(metric: VectorMetric) -> OtlpMetric {
    let name = metric.name().to_string();
    let description = String::new(); // Vector metrics don't have descriptions
    let unit = String::new(); // Vector metrics don't have units

    let attributes = convert_metric_tags_to_attributes(metric.tags());
    let time_unix_nano = convert_timestamp_to_nanos(metric.timestamp());

    let data = match metric.value() {
        MetricValue::Counter { value } => {
            convert_counter_to_otlp(&attributes, time_unix_nano, *value)
        }
        MetricValue::Gauge { value } => convert_gauge_to_otlp(&attributes, time_unix_nano, *value),
        MetricValue::AggregatedHistogram {
            buckets,
            count,
            sum,
        } => convert_histogram_to_otlp(&attributes, time_unix_nano, buckets, *count, *sum),
        MetricValue::AggregatedSummary {
            quantiles,
            count,
            sum,
        } => convert_summary_to_otlp(&attributes, time_unix_nano, quantiles, *count, *sum),
        MetricValue::Set { values } => {
            convert_set_to_otlp(&attributes, time_unix_nano, values.len())
        }
        MetricValue::Distribution {
            samples,
            statistic: _,
        } => convert_distribution_to_otlp(&attributes, time_unix_nano, samples),
        MetricValue::Sketch { sketch } => {
            convert_sketch_to_otlp(&attributes, time_unix_nano, sketch)
        }
    };

    OtlpMetric {
        name,
        description,
        unit,
        data,
    }
}

fn convert_metric_tags_to_attributes(tags: Option<&MetricTags>) -> Vec<KeyValue> {
    if let Some(tags) = tags {
        tags.iter_all()
            .map(|(key, value)| KeyValue {
                key: key.to_string(),
                value: Some(AnyValue {
                    value: Some(PbValue::StringValue(value.unwrap_or("").to_string())),
                }),
            })
            .collect()
    } else {
        Vec::new()
    }
}

fn convert_timestamp_to_nanos(timestamp: Option<DateTime<Utc>>) -> u64 {
    timestamp
        .and_then(|ts| ts.timestamp_nanos_opt())
        .unwrap_or(0) as u64
}

fn convert_counter_to_otlp(
    attributes: &[KeyValue],
    time_unix_nano: u64,
    value: f64,
) -> Option<Data> {
    // NOTE: All metrics are normalized to absolute (cumulative) values using Vector's
    // standard MetricSet normalization, so we always use Cumulative temporality.
    // start_time_unix_nano = 0 indicates the metric has been cumulative since an
    // unknown start time, which is semantically correct for normalized counters.
    let data_point = NumberDataPoint {
        attributes: attributes.to_vec(),
        time_unix_nano,
        start_time_unix_nano: 0, // Cumulative since unknown start time
        value: Some(NumberDataPointValue::AsDouble(value)),
        exemplars: Vec::new(),
        flags: 0,
    };

    Some(Data::Sum(Sum {
        data_points: vec![data_point],
        aggregation_temporality: AggregationTemporality::Cumulative as i32,
        is_monotonic: true, // Counters are always monotonic
    }))
}

fn convert_gauge_to_otlp(attributes: &[KeyValue], time_unix_nano: u64, value: f64) -> Option<Data> {
    let data_point = NumberDataPoint {
        attributes: attributes.to_vec(),
        time_unix_nano,
        start_time_unix_nano: 0, // Gauges don't have a start time concept
        value: Some(NumberDataPointValue::AsDouble(value)),
        exemplars: Vec::new(),
        flags: 0,
    };

    Some(Data::Gauge(Gauge {
        data_points: vec![data_point],
    }))
}

fn convert_histogram_to_otlp(
    attributes: &[KeyValue],
    time_unix_nano: u64,
    buckets: &[Bucket],
    count: u64,
    sum: f64,
) -> Option<Data> {
    let mut explicit_bounds = Vec::new();
    let mut bucket_counts = Vec::new();

    for bucket in buckets {
        if bucket.upper_limit != f64::INFINITY {
            explicit_bounds.push(bucket.upper_limit);
        }
        bucket_counts.push(bucket.count);
    }

    let data_point = HistogramDataPoint {
        attributes: attributes.to_vec(),
        time_unix_nano,
        start_time_unix_nano: 0, // Cumulative since unknown start time
        count,
        sum: Some(sum),
        bucket_counts,
        explicit_bounds,
        exemplars: Vec::new(),
        flags: 0,
        min: None, // TODO: Track min/max if available
        max: None,
    };

    Some(Data::Histogram(Histogram {
        data_points: vec![data_point],
        aggregation_temporality: AggregationTemporality::Cumulative as i32,
    }))
}

fn convert_summary_to_otlp(
    attributes: &[KeyValue],
    time_unix_nano: u64,
    quantiles: &[Quantile],
    count: u64,
    sum: f64,
) -> Option<Data> {
    let quantile_values = quantiles
        .iter()
        .map(|q| ValueAtQuantile {
            quantile: q.quantile,
            value: q.value,
        })
        .collect();

    let data_point = SummaryDataPoint {
        attributes: attributes.to_vec(),
        time_unix_nano,
        start_time_unix_nano: 0, // Cumulative since unknown start time
        count,
        sum,
        quantile_values,
        flags: 0,
    };

    Some(Data::Summary(Summary {
        data_points: vec![data_point],
    }))
}

fn convert_set_to_otlp(
    attributes: &[KeyValue],
    time_unix_nano: u64,
    cardinality: usize,
) -> Option<Data> {
    // Convert set to gauge representing cardinality (number of unique values)
    let data_point = NumberDataPoint {
        attributes: attributes.to_vec(),
        time_unix_nano,
        start_time_unix_nano: 0, // Gauges don't have start times
        value: Some(NumberDataPointValue::AsDouble(cardinality as f64)),
        exemplars: Vec::new(),
        flags: 0,
    };

    Some(Data::Gauge(Gauge {
        data_points: vec![data_point],
    }))
}

fn convert_distribution_to_otlp(
    attributes: &[KeyValue],
    time_unix_nano: u64,
    samples: &[Sample],
) -> Option<Data> {
    // Convert distribution to histogram by creating buckets
    // This is a basic conversion - in practice you might want more sophisticated bucketing
    let mut bucket_counts = vec![0u64; 10]; // 10 buckets
    let mut explicit_bounds = Vec::new();

    // Create exponential buckets: 0.1, 1, 10, 100, 1000, etc.
    for i in 0..9 {
        explicit_bounds.push(10_f64.powi(i - 1));
    }

    let mut sum = 0.0;
    let count = samples.len() as u64;

    // Distribute samples into buckets
    for sample in samples {
        sum += sample.value;

        // Find which bucket this sample falls into
        let mut bucket_index = explicit_bounds.len();
        for (i, &bound) in explicit_bounds.iter().enumerate() {
            if sample.value <= bound {
                bucket_index = i;
                break;
            }
        }

        // Increment all buckets up to and including the target bucket
        for bucket_count in bucket_counts.iter_mut().skip(bucket_index) {
            *bucket_count += 1;
        }
    }

    let data_point = HistogramDataPoint {
        attributes: attributes.to_vec(),
        time_unix_nano,
        start_time_unix_nano: 0, // Cumulative since unknown start time
        count,
        sum: Some(sum),
        bucket_counts,
        explicit_bounds,
        exemplars: Vec::new(),
        flags: 0,
        min: samples
            .iter()
            .map(|s| s.value)
            .fold(f64::INFINITY, f64::min)
            .into(),
        max: samples
            .iter()
            .map(|s| s.value)
            .fold(f64::NEG_INFINITY, f64::max)
            .into(),
    };

    Some(Data::Histogram(Histogram {
        data_points: vec![data_point],
        aggregation_temporality: AggregationTemporality::Cumulative as i32,
    }))
}

fn convert_sketch_to_otlp(
    attributes: &[KeyValue],
    time_unix_nano: u64,
    sketch: &MetricSketch,
) -> Option<Data> {
    // Convert sketch to histogram using sketch's quantile information
    // Use the sketch's internal bucket structure if available
    match sketch {
        MetricSketch::AgentDDSketch(ddsketch) => {
            let mut bucket_counts = Vec::new();
            let mut explicit_bounds = Vec::new();

            // Create buckets based on sketch quantiles
            let quantiles = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99];
            let mut previous_value = 0.0;

            for &q in &quantiles {
                if let Some(value) = ddsketch.quantile(q) {
                    explicit_bounds.push(value);
                    // Estimate count in this bucket (this is approximate)
                    let estimated_count = ((q - previous_value) * ddsketch.count() as f64) as u64;
                    bucket_counts.push(estimated_count);
                    previous_value = q;
                }
            }

            // Add final bucket for remaining samples
            bucket_counts.push(((1.0 - previous_value) * ddsketch.count() as f64) as u64);

            let data_point = HistogramDataPoint {
                attributes: attributes.to_vec(),
                time_unix_nano,
                start_time_unix_nano: 0, // Cumulative since unknown start time
                count: ddsketch.count() as u64,
                sum: ddsketch.sum(),
                bucket_counts,
                explicit_bounds,
                exemplars: Vec::new(),
                flags: 0,
                min: ddsketch.min(),
                max: ddsketch.max(),
            };

            Some(Data::Histogram(Histogram {
                data_points: vec![data_point],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            }))
        }
    }
}

fn convert_vector_trace_to_otlp_span(trace: TraceEvent) -> Span {
    let trace_map = trace.as_map();

    // Extract required fields with fallbacks
    let trace_id = trace_map
        .get("trace_id")
        .and_then(|v| v.as_str())
        .map(|s| hex_string_to_bytes(s.as_ref()))
        .unwrap_or_default();

    let span_id = trace_map
        .get("span_id")
        .and_then(|v| v.as_str())
        .map(|s| hex_string_to_bytes(s.as_ref()))
        .unwrap_or_default();

    let parent_span_id = trace_map
        .get("parent_span_id")
        .and_then(|v| v.as_str())
        .map(|s| hex_string_to_bytes(s.as_ref()))
        .unwrap_or_default();

    let name = trace_map
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Map string kind to OTLP span kind enum
    let kind = trace_map
        .get("kind")
        .and_then(|v| v.as_str())
        .map(|kind_str| match kind_str.to_lowercase().as_str() {
            "server" => SpanKind::Server as i32,
            "client" => SpanKind::Client as i32,
            "producer" => SpanKind::Producer as i32,
            "consumer" => SpanKind::Consumer as i32,
            "internal" => SpanKind::Internal as i32,
            _ => SpanKind::Internal as i32,
        })
        .unwrap_or(SpanKind::Internal as i32);

    let trace_state = trace_map
        .get("trace_state")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default();

    // Handle timestamps - either direct fields or calculated from timestamp/duration
    let end_time_unix_nano = if let Some(end_time) =
        trace_map.get("end_time_unix_nano").and_then(|v| {
            if let Some(ts) = v.as_timestamp() {
                ts.timestamp_nanos_opt()
            } else {
                v.as_integer()
            }
        }) {
        end_time as u64
    } else if let Some(timestamp) = trace_map
        .get("timestamp")
        .and_then(|v| v.as_timestamp())
        .and_then(|ts| ts.timestamp_nanos_opt())
    {
        timestamp as u64
    } else {
        0
    };

    let start_time_unix_nano = if let Some(start_time) =
        trace_map.get("start_time_unix_nano").and_then(|v| {
            if let Some(ts) = v.as_timestamp() {
                ts.timestamp_nanos_opt()
            } else {
                v.as_integer()
            }
        }) {
        start_time as u64
    } else if end_time_unix_nano > 0 {
        if let Some(duration_ns) = trace_map.get("duration_ns").and_then(|v| v.as_integer()) {
            end_time_unix_nano.saturating_sub(duration_ns as u64)
        } else {
            0
        }
    } else {
        0
    };

    // Extract tags and convert to attributes, also look for status info
    let mut attributes = Vec::new();
    let mut status_code = 1; // OK = 1
    let mut status_message = String::new();

    // Process both tags and attributes fields
    for field_name in ["tags", "attributes"] {
        if let Some(attrs) = trace_map.get(field_name).and_then(|v| v.as_object()) {
            for (key, value) in attrs {
                match key.as_str() {
                    "otel.status_code" => {
                        if let Some(code_str) = value.as_str() {
                            status_code = match code_str.to_lowercase().as_str() {
                                "error" => 2, // ERROR = 2
                                "ok" => 1,    // OK = 1
                                _ => 0,       // UNSET = 0
                            };
                        }
                    }
                    "error" => {
                        if let Some(msg) = value.as_str() {
                            status_message = msg.to_string();
                        }
                    }
                    _ => {
                        attributes.push(KeyValue {
                            key: key.to_string(),
                            value: Some(convert_value_to_any_value(value.clone())),
                        });
                    }
                }
            }
        }
    }

    let dropped_attributes_count = trace_map
        .get("dropped_attributes_count")
        .and_then(|v| v.as_integer())
        .unwrap_or(0) as u32;

    // Convert events
    let events = trace_map
        .get("events")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(convert_value_to_span_event).collect())
        .unwrap_or_default();

    let dropped_events_count = trace_map
        .get("dropped_events_count")
        .and_then(|v| v.as_integer())
        .unwrap_or(0) as u32;

    // Convert links
    let links = trace_map
        .get("links")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(convert_value_to_span_link).collect())
        .unwrap_or_default();

    let dropped_links_count = trace_map
        .get("dropped_links_count")
        .and_then(|v| v.as_integer())
        .unwrap_or(0) as u32;

    // Build status if we found error information
    let status = if status_code != 1 || !status_message.is_empty() {
        // OK = 1
        Some(SpanStatus {
            code: status_code,
            message: status_message,
        })
    } else {
        None
    };

    Span {
        trace_id,
        span_id,
        trace_state,
        parent_span_id,
        name,
        kind,
        start_time_unix_nano,
        end_time_unix_nano,
        attributes,
        dropped_attributes_count,
        events,
        dropped_events_count,
        links,
        dropped_links_count,
        status,
    }
}

fn hex_string_to_bytes(hex_str: &str) -> Vec<u8> {
    hex::decode(hex_str).unwrap_or_default()
}

fn convert_value_to_span_event(value: &Value) -> Option<SpanEvent> {
    let obj = value.as_object()?;

    let name = obj.get("name")?.as_str()?.to_string();
    let time_unix_nano =
        if let Some(timestamp) = obj.get("timestamp").and_then(|v| v.as_timestamp()) {
            timestamp.timestamp_nanos_opt().unwrap_or(0) as u64
        } else if let Some(time_nano) = obj.get("time_unix_nano").and_then(|v| v.as_integer()) {
            time_nano as u64
        } else {
            0
        };

    let attributes = obj
        .get("attributes")
        .and_then(|v| v.as_object())
        .map(|obj| convert_object_map_to_key_value_vec(obj.clone()))
        .unwrap_or_default();

    let dropped_attributes_count = obj
        .get("dropped_attributes_count")
        .and_then(|v| v.as_integer())
        .unwrap_or(0) as u32;

    Some(SpanEvent {
        time_unix_nano,
        name,
        attributes,
        dropped_attributes_count,
    })
}

fn convert_value_to_span_link(value: &Value) -> Option<Link> {
    let obj = value.as_object()?;

    let trace_id = obj
        .get("trace_id")?
        .as_str()
        .map(|s| hex_string_to_bytes(s.as_ref()))?;

    let span_id = obj
        .get("span_id")?
        .as_str()
        .map(|s| hex_string_to_bytes(s.as_ref()))?;

    let trace_state = obj
        .get("trace_state")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "".to_string());

    let attributes = obj
        .get("attributes")
        .and_then(|v| v.as_object())
        .map(|obj| convert_object_map_to_key_value_vec(obj.clone()))
        .unwrap_or_default();

    let dropped_attributes_count = obj
        .get("dropped_attributes_count")
        .and_then(|v| v.as_integer())
        .unwrap_or(0) as u32;

    Some(Link {
        trace_id,
        span_id,
        trace_state,
        attributes,
        dropped_attributes_count,
    })
}

#[cfg(test)]
mod tests;
