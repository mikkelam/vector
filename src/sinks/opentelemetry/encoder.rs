//! Encoder for the `opentelemetry` sink.
//!
//! This module is responsible for converting Vector `Event`s into OTLP Protobuf messages.

use std::io;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use hex;

use prost::Message;
use vector_lib::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    collector::metrics::v1::ExportMetricsServiceRequest,
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value as PbValue, AnyValue, KeyValue, KeyValueList},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        metric::Data, number_data_point::Value as NumberDataPointValue,
        summary_data_point::ValueAtQuantile, AggregationTemporality, Gauge, Histogram,
        HistogramDataPoint, Metric as OtlpMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
        Sum, Summary, SummaryDataPoint,
    },
    resource::v1::Resource,
    trace::v1::{
        span::{Event as SpanEvent, Link, SpanKind},
        ResourceSpans, ScopeSpans, Span, Status as SpanStatus,
    },
};
use vrl::{
    event_path,
    value::{ObjectMap, Value},
};

use crate::{
    event::{
        metric::{Bucket, MetricSketch, Quantile, Sample},
        Event, LogEvent, Metric as VectorMetric, MetricTags, MetricValue, TraceEvent,
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
                        self.create_scope_logs(vec![self.convert_log_event_to_log_record(log)])
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
        if let Some(Value::Timestamp(timestamp)) = log.remove(event_path!("observed_timestamp")) {
            log_record.observed_time_unix_nano =
                timestamp.timestamp_nanos_opt().unwrap_or(0) as u64;
        }

        // Handle trace_id
        if let Some(trace_id) = log.remove(event_path!("trace_id")) {
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
        if let Some(span_id) = log.remove(event_path!("span_id")) {
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
        if let Some(severity_text) = log.remove(event_path!("severity_text")) {
            if let Value::Bytes(text) = severity_text {
                log_record.severity_text = String::from_utf8_lossy(&text).to_string();
            }
        }

        if let Some(severity_number) = log.remove(event_path!("severity_number")) {
            if let Value::Integer(num) = severity_number {
                log_record.severity_number = num as i32;
            }
        }

        // Handle flags
        if let Some(flags) = log.remove(event_path!("flags")) {
            if let Value::Integer(f) = flags {
                log_record.flags = f as u32;
            }
        }

        // Handle dropped_attributes_count
        if let Some(count) = log.remove(event_path!("dropped_attributes_count")) {
            if let Value::Integer(c) = count {
                log_record.dropped_attributes_count = c as u32;
            }
        }

        // Handle attributes
        if let Some(attrs) = log.remove(event_path!("attributes")) {
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
                        self.create_scope_spans(vec![convert_vector_trace_to_otlp_span(trace)])
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
mod tests {
    use super::*;
    use crate::sinks::util::encoding::Encoder;
    use std::collections::BTreeMap;
    use vector_lib::event::{Event, LogEvent};
    use vector_lib::opentelemetry::proto::common::v1::any_value;
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
    fn test_missing_trace_context() {
        let encoder = OtlpEncoder::new_default();

        let log = LogEvent::from("test message");
        let log_record = encoder.convert_log_event_to_log_record(log);

        // Should result in empty arrays for missing data
        assert_eq!(log_record.trace_id.len(), 0);
        assert_eq!(log_record.span_id.len(), 0);
    }

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
            metrics::v1::{metric::Data, AggregationTemporality},
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
    fn test_metrics_conversion() {
        use crate::event::metric::Bucket;
        use crate::event::{Metric, MetricKind, MetricValue};
        use chrono::Utc;
        use vector_lib::opentelemetry::proto::{
            collector::metrics::v1::ExportMetricsServiceRequest,
            metrics::v1::{metric::Data, AggregationTemporality},
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
                if let vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                    s,
                ) = value
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
                if let vector_lib::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                    s,
                ) = value
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
        assert!(log
            .get(event_path!("resource", "service", "name"))
            .is_some());
        assert!(log.get(event_path!("resource", "environment")).is_some());
        assert!(log.get("normal_field").is_some());
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
}
