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
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
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

/// Trait for extracting and removing resource attributes from events
trait ResourceAttributeExtractor {
    fn extract_resource_attributes(&mut self) -> Vec<KeyValue>;
    fn remove_resource_attributes(&mut self);

    /// Extract all resource attributes (both nested and flattened formats)
    fn extract_all_resource_attributes(&mut self) -> Vec<KeyValue> {
        let mut resource_attributes = Vec::new();

        // Try nested format first (if supported by event type)
        resource_attributes.extend(self.extract_nested_resource_attributes());

        // Then extract flattened fields for Vector OTLP source compatibility
        resource_attributes.extend(self.extract_resource_attributes());

        resource_attributes
    }

    /// Remove all resource attributes (both nested and flattened formats)
    fn remove_all_resource_attributes(&mut self) {
        self.remove_nested_resource_attributes();
        self.remove_resource_attributes();
    }

    /// Extract nested resource attributes (default: empty, override for logs/traces)
    fn extract_nested_resource_attributes(&mut self) -> Vec<KeyValue> {
        Vec::new()
    }

    /// Remove nested resource attributes (default: no-op, override for logs/traces)
    fn remove_nested_resource_attributes(&mut self) {
        // Default implementation does nothing
    }
}

impl ResourceAttributeExtractor for LogEvent {
    fn extract_resource_attributes(&mut self) -> Vec<KeyValue> {
        let mut resource_attributes = Vec::new();
        let mut keys_to_remove = Vec::new();

        for (key, value) in self.all_event_fields().unwrap() {
            if let Some(attr_key) = key.strip_prefix("resources.") {
                let clean_key = attr_key.trim_matches('"');
                resource_attributes.push(KeyValue {
                    key: clean_key.to_string(),
                    value: Some(convert_value_to_any_value(value.clone())),
                });
                keys_to_remove.push(key.clone());
            }
        }

        for key in &keys_to_remove {
            self.remove(key.as_str());
        }

        resource_attributes
    }

    fn remove_resource_attributes(&mut self) {
        let mut keys_to_remove = Vec::new();

        for (key, _) in self.all_event_fields().unwrap() {
            if key.starts_with("resources.") {
                keys_to_remove.push(key.clone());
            }
        }

        for key in &keys_to_remove {
            self.remove(key.as_str());
        }
    }

    fn extract_nested_resource_attributes(&mut self) -> Vec<KeyValue> {
        if let Some(resource_map) = self.remove(event_path!("resource")) {
            if let Some(resource_obj) = resource_map.as_object() {
                return convert_object_map_to_key_value_vec(resource_obj.clone());
            }
        }
        Vec::new()
    }

    fn remove_nested_resource_attributes(&mut self) {
        let _ = self.remove(event_path!("resource"));
    }
}

impl ResourceAttributeExtractor for VectorMetric {
    fn extract_resource_attributes(&mut self) -> Vec<KeyValue> {
        let mut resource_attributes = Vec::new();
        let mut keys_to_remove = Vec::new();

        if let Some(tags) = self.tags_mut() {
            for (key, value) in tags.iter_single() {
                if let Some(attr_key) = key.strip_prefix("resources.") {
                    let clean_key = attr_key.trim_matches('"');
                    resource_attributes.push(KeyValue {
                        key: clean_key.to_string(),
                        value: Some(convert_value_to_any_value(Value::from(value.to_string()))),
                    });
                    keys_to_remove.push(key.to_string());
                }
            }

            for key in &keys_to_remove {
                tags.remove(key);
            }
        }

        resource_attributes
    }

    fn remove_resource_attributes(&mut self) {
        let mut keys_to_remove = Vec::new();

        if let Some(tags) = self.tags_mut() {
            for key in tags.keys() {
                if key.starts_with("resources.") {
                    keys_to_remove.push(key.to_string());
                }
            }

            for key in &keys_to_remove {
                tags.remove(&key);
            }
        }
    }
}

impl ResourceAttributeExtractor for TraceEvent {
    fn extract_resource_attributes(&mut self) -> Vec<KeyValue> {
        let mut resource_attributes = Vec::new();
        let mut keys_to_remove = Vec::new();

        for (key_path, value) in self.as_map().iter() {
            let key = key_path.to_string();
            if let Some(attr_key) = key.strip_prefix("resources.") {
                let clean_key = attr_key.trim_matches('"');
                resource_attributes.push(KeyValue {
                    key: clean_key.to_string(),
                    value: Some(convert_value_to_any_value(value.clone())),
                });
                keys_to_remove.push(key);
            }
        }

        for key in &keys_to_remove {
            self.remove(key.as_str());
        }

        resource_attributes
    }

    fn remove_resource_attributes(&mut self) {
        let mut keys_to_remove = Vec::new();

        for (key_path, _) in self.as_map().iter() {
            let key = key_path.to_string();
            if key.starts_with("resources.") {
                keys_to_remove.push(key);
            }
        }

        for key in &keys_to_remove {
            self.remove(key.as_str());
        }
    }

    fn extract_nested_resource_attributes(&mut self) -> Vec<KeyValue> {
        if let Some(resource_map) = self.remove(event_path!("resource")) {
            if let Some(resource_obj) = resource_map.as_object() {
                return convert_object_map_to_key_value_vec(resource_obj.clone());
            }
        }
        Vec::new()
    }

    fn remove_nested_resource_attributes(&mut self) {
        let _ = self.remove(event_path!("resource"));
    }
}

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
    pub(super) fn new_default() -> Self {
        Self {}
    }

    /// Groups events by their resource attributes - reusable across logs/metrics/traces
    fn group_events_by_resource_attributes<T>(
        &self,
        events: Vec<T>,
    ) -> std::collections::HashMap<String, (Vec<KeyValue>, Vec<T>)>
    where
        T: ResourceAttributeExtractor,
    {
        let mut groups: std::collections::HashMap<String, (Vec<KeyValue>, Vec<T>)> =
            std::collections::HashMap::new();

        for mut event in events {
            let attrs = event.extract_all_resource_attributes();
            // Create a stable string key from the attributes
            let key = attrs
                .iter()
                .map(|kv| {
                    let value_str = kv.value.as_ref().map_or(String::new(), |v| match &v.value {
                        Some(value) => match value {
                            PbValue::StringValue(s) => s.clone(),
                            PbValue::IntValue(i) => i.to_string(),
                            PbValue::DoubleValue(d) => d.to_string(),
                            PbValue::BoolValue(b) => b.to_string(),
                            _ => String::new(),
                        },
                        None => String::new(),
                    });
                    format!("{}={}", kv.key, value_str)
                })
                .collect::<Vec<_>>()
                .join(",");

            let attrs_clone = attrs.clone();
            groups
                .entry(key)
                .or_insert_with(|| (attrs_clone, Vec::new()))
                .1
                .push(event);
        }

        groups
    }

    /// Encodes a batch of log events into an `ExportLogsServiceRequest` Protobuf message.
    pub fn encode_logs(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        let log_events: Vec<LogEvent> = events.into_iter().map(|e| e.into_log()).collect();
        let resource_groups = self.group_events_by_resource_attributes(log_events);

        let resource_logs = resource_groups
            .into_iter()
            .map(|(_key, (resource_attrs, mut events))| {
                // Remove resource attributes from events since they're now at the resource level
                events
                    .iter_mut()
                    .for_each(|event| event.remove_all_resource_attributes());

                let log_records = events
                    .into_iter()
                    .map(|event| self.convert_log_event_to_log_record(event))
                    .collect();

                ResourceLogs {
                    resource: Some(Resource {
                        attributes: resource_attrs,
                        dropped_attributes_count: 0,
                    }),
                    scope_logs: vec![self.create_scope_logs(log_records)],
                    schema_url: String::new(),
                }
            })
            .collect();

        let request = ExportLogsServiceRequest { resource_logs };
        Ok(request.encode_to_vec().into())
    }

    /// Converts a single Vector `LogEvent` into an OTLP `LogRecord`.
    fn convert_log_event_to_log_record(&self, mut log: LogEvent) -> LogRecord {
        let body = log
            .get_message()
            .map(|msg| convert_value_to_any_value(msg.clone()));

        let time_unix_nano = log
            .get_timestamp()
            .and_then(|v| v.as_timestamp())
            .and_then(|ts| ts.timestamp_nanos_opt())
            .unwrap_or(0) as u64;

        // Value of 0 indicates unknown or missing timestamp
        let observed_time_unix_nano = log
            .remove(event_path!("observed_timestamp"))
            .and_then(|v| v.as_timestamp().copied())
            .and_then(|ts| ts.timestamp_nanos_opt())
            .map(|ns| ns as u64)
            .unwrap_or(0);

        let trace_id = log
            .remove(event_path!("trace_id"))
            .and_then(|v| {
                // Vector's OTLP source stores trace_id as hex string, decode it back to bytes
                if let Some(hex_str) = v.as_str() {
                    hex::decode(hex_str.as_ref())
                        .ok()
                        .filter(|bytes| bytes.len() == 16)
                } else if let Some(bytes) = v.as_bytes() {
                    // Raw bytes
                    Some(bytes.to_vec()).filter(|bytes| bytes.len() == 16)
                } else if let Some(array) = v.as_array() {
                    // Array of integers (converted to bytes)
                    let bytes: Result<Vec<u8>, _> = array
                        .iter()
                        .map(|v| v.as_integer().and_then(|i| u8::try_from(i).ok()))
                        .collect::<Option<Vec<u8>>>()
                        .ok_or(());
                    bytes.ok().filter(|bytes| bytes.len() == 16)
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let span_id = log
            .remove(event_path!("span_id"))
            .and_then(|v| {
                // Vector's OTLP source stores span_id as hex string, decode it back to bytes
                if let Some(hex_str) = v.as_str() {
                    hex::decode(hex_str.as_ref())
                        .ok()
                        .filter(|bytes| bytes.len() == 8)
                } else if let Some(bytes) = v.as_bytes() {
                    // Raw bytes
                    Some(bytes.to_vec()).filter(|bytes| bytes.len() == 8)
                } else if let Some(array) = v.as_array() {
                    // Array of integers (converted to bytes)
                    let bytes: Result<Vec<u8>, _> = array
                        .iter()
                        .map(|v| v.as_integer().and_then(|i| u8::try_from(i).ok()))
                        .collect::<Option<Vec<u8>>>()
                        .ok_or(());
                    bytes.ok().filter(|bytes| bytes.len() == 8)
                } else {
                    None
                }
            })
            .unwrap_or_default();

        // Extract severity information
        let (severity_number, severity_text) = extract_severity(&mut log);

        // Extract attributes from flattened fields (attributes.*) and remaining fields
        let attributes = self.extract_log_attributes(&mut log);

        LogRecord {
            time_unix_nano,
            observed_time_unix_nano,
            severity_number,
            severity_text,
            body,
            attributes,
            dropped_attributes_count: 0,
            flags: 0,
            trace_id,
            span_id,
        }
    }

    /// Encodes a batch of metric events into an `ExportMetricsServiceRequest` Protobuf message.
    pub(super) fn encode_metrics(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        let metrics: Vec<VectorMetric> = events.into_iter().map(|e| e.into_metric()).collect();
        let resource_groups = self.group_events_by_resource_attributes(metrics);

        let resource_metrics = resource_groups
            .into_iter()
            .map(|(_key, (resource_attrs, mut metrics))| {
                // Remove resource attributes from metrics since they're now at the resource level
                metrics
                    .iter_mut()
                    .for_each(|metric| metric.remove_all_resource_attributes());

                // Apply normalization to convert incremental metrics to absolute
                let mut normalizer = OtlpMetricNormalize::default();
                let mut metric_state = MetricSet::default();

                let metric_records: Vec<_> = metrics
                    .into_iter()
                    .filter_map(|metric| {
                        normalizer
                            .normalize(&mut metric_state, metric)
                            .map(convert_vector_metric_to_otlp)
                    })
                    .collect();

                ResourceMetrics {
                    resource: Some(Resource {
                        attributes: resource_attrs,
                        dropped_attributes_count: 0,
                    }),
                    scope_metrics: vec![self.create_scope_metrics(metric_records)],
                    schema_url: String::new(),
                }
            })
            .collect();

        let request = ExportMetricsServiceRequest { resource_metrics };
        Ok(request.encode_to_vec().into())
    }

    pub fn encode_traces(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        let traces: Vec<TraceEvent> = events.into_iter().map(|e| e.into_trace()).collect();
        let resource_groups = self.group_events_by_resource_attributes(traces);

        let resource_spans = resource_groups
            .into_iter()
            .map(|(_key, (resource_attrs, mut traces))| {
                // Remove resource attributes from traces since they're now at the resource level
                traces
                    .iter_mut()
                    .for_each(|trace| trace.remove_all_resource_attributes());

                let spans: Vec<_> = traces
                    .into_iter()
                    .map(convert_vector_trace_to_otlp_span)
                    .collect();

                ResourceSpans {
                    resource: Some(Resource {
                        attributes: resource_attrs,
                        dropped_attributes_count: 0,
                    }),
                    scope_spans: vec![self.create_scope_spans(spans)],
                    schema_url: String::new(),
                }
            })
            .collect();

        let request = ExportTraceServiceRequest { resource_spans };
        Ok(request.encode_to_vec().into())
    }

    fn create_scope_logs(&self, log_records: Vec<LogRecord>) -> ScopeLogs {
        ScopeLogs {
            scope: None,
            log_records,
            schema_url: String::new(),
        }
    }

    fn create_scope_metrics(&self, metrics: Vec<OtlpMetric>) -> ScopeMetrics {
        ScopeMetrics {
            scope: None,
            metrics,
            schema_url: String::new(),
        }
    }

    fn create_scope_spans(&self, spans: Vec<Span>) -> ScopeSpans {
        ScopeSpans {
            scope: None,
            spans,
            schema_url: String::new(),
        }
    }

    /// Extracts log record attributes from flattened fields and remaining fields.
    fn extract_log_attributes(&self, log: &mut LogEvent) -> Vec<KeyValue> {
        let mut attributes = Vec::new();
        let mut keys_to_remove = Vec::new();

        // First, extract flattened attribute fields with "attributes." prefix
        for (key, value) in log.all_event_fields().unwrap() {
            if let Some(attr_key) = key.strip_prefix("attributes.") {
                // Remove surrounding quotes if present
                let clean_key = attr_key.trim_matches('"');
                attributes.push(KeyValue {
                    key: clean_key.to_string(),
                    value: Some(convert_value_to_any_value(value.clone())),
                });
                keys_to_remove.push(key.clone());
            }
        }

        // Remove the flattened attribute fields
        for key in &keys_to_remove {
            log.remove(key.as_str());
        }

        // Clean up empty parent objects that may have been left behind
        if log
            .get("attributes")
            .and_then(|v| v.as_object())
            .map_or(false, |obj| obj.is_empty())
        {
            log.remove("attributes");
        }

        // Add remaining fields as attributes (excluding core OTLP fields that have dedicated protobuf fields)
        let remaining_fields: ObjectMap = log
            .all_event_fields()
            .unwrap()
            .filter(|(key, _)| {
                !matches!(
                    key.as_str(),
                    "severity_number"
                        | "severity_text"
                        | "trace_id"
                        | "span_id"
                        | "observed_timestamp"
                        | "source_type"
                        | "flags"
                        | "dropped_attributes_count"
                        | "attributes"
                        | "resources"
                ) && !key.starts_with("resources.")
                    && !key.starts_with("attributes.")
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        attributes.extend(convert_object_map_to_key_value_vec(remaining_fields));
        attributes
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

/// Extract severity number and text from a log event.
///
/// This function looks for common severity/level fields and maps them to OTLP severity values.
/// It removes the severity fields from the log so they don't appear as attributes.
pub(super) fn extract_severity(log: &mut LogEvent) -> (i32, String) {
    // Try common field names for severity/level
    let severity_fields = [
        "level",
        "severity",
        "severity_text",
        "log_level",
        "priority",
    ];

    for field_name in &severity_fields {
        if let Some(severity_value) = log.get(*field_name) {
            let result = map_severity_to_otlp(severity_value.clone());

            // Clean up severity fields: keep "level" as attribute, remove others
            for cleanup_field in &severity_fields {
                if *cleanup_field != "level" {
                    log.remove(*cleanup_field);
                }
            }

            return result;
        }
    }

    // Default to unspecified if no severity field found
    (SeverityNumber::Unspecified as i32, String::new())
}

/// Map a Vector Value to OTLP severity number and text.
pub(super) fn map_severity_to_otlp(value: Value) -> (i32, String) {
    let severity_str = match &value {
        Value::Bytes(b) => String::from_utf8_lossy(b).to_lowercase(),
        Value::Integer(i) => {
            // Handle numeric severity levels (0-7 syslog style or direct OTLP numbers)
            return map_numeric_severity(*i);
        }
        other => other.to_string_lossy().to_lowercase(),
    };

    let severity_text = severity_str.to_uppercase();

    let severity_number = match severity_str.as_str() {
        // TRACE levels (1-4)
        "trace" | "finest" => SeverityNumber::Trace,
        "trace1" => SeverityNumber::Trace,
        "trace2" => SeverityNumber::Trace2,
        "trace3" => SeverityNumber::Trace3,
        "trace4" => SeverityNumber::Trace4,

        // DEBUG levels (5-8)
        "debug" | "fine" => SeverityNumber::Debug,
        "debug1" => SeverityNumber::Debug,
        "debug2" => SeverityNumber::Debug2,
        "debug3" => SeverityNumber::Debug3,
        "debug4" => SeverityNumber::Debug4,

        // INFO levels (9-12)
        "info" | "information" | "notice" => SeverityNumber::Info,
        "info1" => SeverityNumber::Info,
        "info2" => SeverityNumber::Info2,
        "info3" => SeverityNumber::Info3,
        "info4" => SeverityNumber::Info4,

        // WARN levels (13-16)
        "warn" | "warning" => SeverityNumber::Warn,
        "warn1" => SeverityNumber::Warn,
        "warn2" => SeverityNumber::Warn2,
        "warn3" => SeverityNumber::Warn3,
        "warn4" => SeverityNumber::Warn4,

        // ERROR levels (17-20)
        "error" | "err" => SeverityNumber::Error,
        "error1" => SeverityNumber::Error,
        "error2" => SeverityNumber::Error2,
        "error3" => SeverityNumber::Error3,
        "error4" => SeverityNumber::Error4,

        // FATAL levels (21-24)
        "fatal" | "critical" | "crit" | "emergency" | "emerg" | "panic" => SeverityNumber::Fatal,
        "fatal1" => SeverityNumber::Fatal,
        "fatal2" => SeverityNumber::Fatal2,
        "fatal3" => SeverityNumber::Fatal3,
        "fatal4" => SeverityNumber::Fatal4,

        // Default to unspecified for unknown levels
        _ => SeverityNumber::Unspecified,
    };

    (severity_number as i32, severity_text)
}

/// Map numeric severity values to OTLP severity.
/// Handles both syslog-style (0-7) and direct OTLP numbering (1-24).
fn map_numeric_severity(level: i64) -> (i32, String) {
    match level {
        // Syslog style (RFC 5424) - prioritize these first since they're 0-7
        0 => (SeverityNumber::Fatal as i32, "EMERGENCY".to_string()),
        1 => (SeverityNumber::Fatal as i32, "ALERT".to_string()),
        2 => (SeverityNumber::Fatal as i32, "CRITICAL".to_string()),
        3 => (SeverityNumber::Error as i32, "ERROR".to_string()),
        4 => (SeverityNumber::Warn as i32, "WARNING".to_string()),
        5 => (SeverityNumber::Info as i32, "NOTICE".to_string()),
        6 => (SeverityNumber::Info as i32, "INFO".to_string()),
        7 => (SeverityNumber::Debug as i32, "DEBUG".to_string()),

        // Direct OTLP numeric levels (8-24, avoiding syslog overlap)
        8 => (level as i32, "DEBUG".to_string()),
        9..=12 => (level as i32, "INFO".to_string()),
        13..=16 => (level as i32, "WARN".to_string()),
        17..=20 => (level as i32, "ERROR".to_string()),
        21..=24 => (level as i32, "FATAL".to_string()),

        // Out of range - default to unspecified
        _ => (
            SeverityNumber::Unspecified as i32,
            format!("LEVEL_{}", level),
        ),
    }
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
        for i in bucket_index..bucket_counts.len() {
            bucket_counts[i] += 1;
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
            "internal" | _ => SpanKind::Internal as i32,
        })
        .unwrap_or(SpanKind::Internal as i32);

    let trace_state = trace_map
        .get("trace_state")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "".to_string());

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
        .map(|arr| {
            arr.iter()
                .filter_map(|event_val| convert_value_to_span_event(event_val))
                .collect()
        })
        .unwrap_or_default();

    let dropped_events_count = trace_map
        .get("dropped_events_count")
        .and_then(|v| v.as_integer())
        .unwrap_or(0) as u32;

    // Convert links
    let links = trace_map
        .get("links")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|link_val| convert_value_to_span_link(link_val))
                .collect()
        })
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
    use crate::event::{LogEvent, TraceEvent};
    use crate::sinks::util::encoding::Encoder;
    use std::collections::BTreeMap;
    use vector_lib::opentelemetry::proto::{
        collector::trace::v1::ExportTraceServiceRequest, common::v1::any_value,
        trace::v1::span::SpanKind,
    };

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
    fn test_hex_strings_from_source() {
        let encoder = OtlpEncoder::new_default();

        let mut log = LogEvent::from("test message");
        // Insert hex strings as they come from Vector's OTLP source (legacy namespace)
        log.insert("trace_id", "4ac52aadf321c2e531db005df08792f5"); // 32-char hex = 16 bytes
        log.insert("span_id", "0b9e4bda2a55530d"); // 16-char hex = 8 bytes

        let log_record = encoder.convert_log_event_to_log_record(log);

        // Should decode hex strings to proper byte lengths
        assert_eq!(log_record.trace_id.len(), 16);
        assert_eq!(log_record.span_id.len(), 8);

        // Verify actual decoded bytes match expected
        assert_eq!(
            log_record.trace_id,
            hex::decode("4ac52aadf321c2e531db005df08792f5").unwrap()
        );
        assert_eq!(log_record.span_id, hex::decode("0b9e4bda2a55530d").unwrap());
    }

    #[test]
    fn test_flattened_otlp_structure() {
        let encoder = OtlpEncoder::new_default();

        // Create log event with flattened OTLP structure as Vector's source provides
        let mut log = LogEvent::from("Processing DELETE /api/orders/789");

        // Resource attributes (flattened with resources. prefix)
        log.insert("resources.\"deployment.environment\"", "test");
        log.insert("resources.\"host.name\"", "test-host");
        log.insert("resources.\"service.name\"", "otel-test-generator");
        log.insert("resources.\"service.version\"", "1.0.0");

        // Log record attributes (flattened with attributes. prefix)
        log.insert(
            "attributes.\"code.file.path\"",
            "/home/user/generate_otel_signals.py",
        );
        log.insert(
            "attributes.\"code.function.name\"",
            "generate_request_trace",
        );
        log.insert("attributes.\"http.method\"", "DELETE");
        log.insert("attributes.\"request.id\"", "req_497271");

        // Other log fields
        log.insert("severity_number", 9);
        log.insert("level", "INFO");

        let events = vec![Event::Log(log)];
        let result = encoder.encode_logs(events).unwrap();

        // Decode the protobuf to verify structure
        use prost::Message;
        use vector_lib::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;

        let request = ExportLogsServiceRequest::decode(result.as_ref()).unwrap();
        let resource_logs = &request.resource_logs[0];
        let resource = resource_logs.resource.as_ref().unwrap();
        let log_record = &resource_logs.scope_logs[0].log_records[0];

        // Verify resource attributes are properly extracted and structured
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

        assert_eq!(
            resource_attrs.get("deployment.environment"),
            Some(&"test".to_string())
        );
        assert_eq!(
            resource_attrs.get("service.name"),
            Some(&"otel-test-generator".to_string())
        );
        assert_eq!(
            resource_attrs.get("host.name"),
            Some(&"test-host".to_string())
        );

        // Verify log record attributes are properly extracted
        let log_attrs: std::collections::HashMap<String, String> = log_record
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

        assert_eq!(
            log_attrs.get("code.file.path"),
            Some(&"/home/user/generate_otel_signals.py".to_string())
        );
        assert_eq!(log_attrs.get("http.method"), Some(&"DELETE".to_string()));
        assert_eq!(log_attrs.get("request.id"), Some(&"req_497271".to_string()));

        // Verify other fields are preserved as log attributes (not flattened with prefixes)
        assert_eq!(log_attrs.get("level"), Some(&"INFO".to_string()));

        // Verify severity is properly extracted (not in attributes)
        assert_eq!(log_record.severity_number, 9);
        assert!(!log_attrs.contains_key("severity_number"));
    }

    #[test]
    fn test_quoted_vs_unquoted_attribute_keys() {
        let encoder = OtlpEncoder::new_default();

        // Test with quoted keys (as seen from Vector's OTLP source)
        let mut log1 = LogEvent::from("test message 1");
        log1.insert("attributes.\"code.file.path\"", "/path/to/file.py");
        log1.insert("resources.\"service.name\"", "test-service");

        // Test with unquoted keys (edge case)
        let mut log2 = LogEvent::from("test message 2");
        log2.insert("attributes.simple_key", "simple_value");
        log2.insert("resources.service_name", "test-service-2");

        let events = vec![Event::Log(log1), Event::Log(log2)];
        let result = encoder.encode_logs(events).unwrap();

        use prost::Message;
        use vector_lib::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;

        let request = ExportLogsServiceRequest::decode(result.as_ref()).unwrap();
        let resource_logs = &request.resource_logs[0];
        let resource = resource_logs.resource.as_ref().unwrap();
        let log_records = &resource_logs.scope_logs[0].log_records;

        // Check resource attributes (should handle both quoted and unquoted)
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

        // Should have both service names without quotes in the keys
        assert!(
            resource_attrs.contains_key("service.name")
                || resource_attrs.contains_key("service_name")
        );

        // Check log record attributes
        for log_record in log_records {
            let log_attrs: std::collections::HashMap<String, String> = log_record
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

            // Should have attribute keys without quotes
            if log_attrs.contains_key("code.file.path") {
                assert_eq!(
                    log_attrs.get("code.file.path"),
                    Some(&"/path/to/file.py".to_string())
                );
            }
            if log_attrs.contains_key("simple_key") {
                assert_eq!(
                    log_attrs.get("simple_key"),
                    Some(&"simple_value".to_string())
                );
            }
        }
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
        assert!(!request.resource_metrics.is_empty());

        let resource_metrics = request.resource_metrics.first().unwrap();
        let scope_metrics = resource_metrics.scope_metrics.first().unwrap();
        assert_eq!(scope_metrics.metrics.len(), 3);

        // Validate set metric (converted to gauge)
        let set_metric = &scope_metrics.metrics[0];
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
        let distribution_metric = &scope_metrics.metrics[1];
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
        let sketch_metric = &scope_metrics.metrics[2];
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
        let resource_metrics = request.resource_metrics.first().unwrap();
        let scope_metrics = resource_metrics.scope_metrics.first().unwrap();

        // Should have both metrics (absolute passes through, incremental gets normalized)
        assert_eq!(scope_metrics.metrics.len(), 2);

        // All metrics should use cumulative temporality
        for metric in &scope_metrics.metrics {
            if let Some(Data::Sum(sum)) = &metric.data {
                assert_eq!(
                    sum.aggregation_temporality,
                    AggregationTemporality::Cumulative as i32
                );
                assert!(sum.is_monotonic);
            } else {
                panic!("Expected counter to have Sum data");
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
        assert!(!request.resource_metrics.is_empty());

        let resource_metrics = request.resource_metrics.first().unwrap();
        let scope_metrics = resource_metrics.scope_metrics.first().unwrap();
        assert_eq!(scope_metrics.metrics.len(), 3);

        // Validate counter metric
        let counter_metric = &scope_metrics.metrics[0];
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
        let gauge_metric = &scope_metrics.metrics[1];
        assert_eq!(gauge_metric.name, "test_gauge");
        if let Some(Data::Gauge(_)) = &gauge_metric.data {
            // Gauge validation passed
        } else {
            panic!("Expected gauge to have Gauge data");
        }

        // Validate histogram metric
        let histogram_metric = &scope_metrics.metrics[2];
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

        // Add resource attributes as tags with "resources." prefix
        let mut tags = crate::event::metric::MetricTags::default();
        tags.replace(
            "resources.\"service.name\"".to_string(),
            "test-service".to_string(),
        );
        tags.replace(
            "resources.\"deployment.environment\"".to_string(),
            "production".to_string(),
        );
        tags.replace(
            "resources.\"host.name\"".to_string(),
            "test-host".to_string(),
        );
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

        // Add resource attributes with "resources." prefix
        trace_fields.insert(
            "resources.\"service.name\"".into(),
            Value::from("trace-service"),
        );
        trace_fields.insert("resources.\"service.version\"".into(), Value::from("1.0.0"));
        trace_fields.insert(
            "resources.\"k8s.cluster\"".into(),
            Value::from("test-cluster"),
        );

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
        log.insert("resources.\"service.name\"", "test-service");
        log.insert("resources.environment", "production");
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

        // Verify resource fields were removed
        assert!(log.get("resources.\"service.name\"").is_none());
        assert!(log.get("resources.environment").is_none());
        // Normal field should remain
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
            "resources.service.name".to_string(),
            "metric-service".to_string(),
        );
        tags.replace("resources.version".to_string(), "2.0.0".to_string());
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

        // Verify resource tags were removed
        if let Some(tags) = metric.tags() {
            assert!(!tags.contains_key("resources.service.name"));
            assert!(!tags.contains_key("resources.version"));
            assert!(tags.contains_key("normal_tag"));
        }
    }

    #[test]
    fn test_resource_attribute_extractor_trait_traces() {
        // Test the trait methods directly on TraceEvent
        let mut trace_fields = ObjectMap::new();
        trace_fields.insert("trace_id".into(), Value::from("test_trace"));
        trace_fields.insert(
            "resources.service.name".into(),
            Value::from("trace-service"),
        );
        trace_fields.insert("resources.cluster".into(), Value::from("test-cluster"));
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

        // Verify resource fields were removed
        assert!(trace_event.get("resources.service.name").is_none());
        assert!(trace_event.get("resources.cluster").is_none());
        // Normal field should remain
        assert!(trace_event.get("span_name").is_some());
    }

    #[test]
    fn test_resource_attribute_removal_only() {
        // Test removal without extraction for logs
        let mut log = LogEvent::from("test message");
        log.insert("resources.service", "test");
        log.insert("resources.environment", "prod");
        log.insert("keep_me", "value");

        log.remove_resource_attributes();

        assert!(log.get("resources.service").is_none());
        assert!(log.get("resources.environment").is_none());
        assert!(log.get("keep_me").is_some());

        // Test removal for metrics
        let mut metric = VectorMetric::new(
            "test_metric",
            crate::event::metric::MetricKind::Absolute,
            crate::event::metric::MetricValue::Counter { value: 1.0 },
        );

        let mut tags = crate::event::metric::MetricTags::default();
        tags.replace("resources.service".to_string(), "test".to_string());
        tags.replace("keep_tag".to_string(), "value".to_string());
        metric = metric.with_tags(Some(tags));

        metric.remove_resource_attributes();

        if let Some(tags) = metric.tags() {
            assert!(!tags.contains_key("resources.service"));
            assert!(tags.contains_key("keep_tag"));
        }
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

        // Add flattened resource attributes
        log.insert("resources.environment", "production");
        log.insert("resources.cluster", "us-west-1");
        log.insert("normal_field", "keep_me");

        // Extract all resource attributes
        let all_attrs = log.extract_all_resource_attributes();

        // Should have 4 total attributes (2 nested + 2 flattened)
        assert_eq!(all_attrs.len(), 4);

        // Verify nested attributes were extracted
        let service_name = all_attrs
            .iter()
            .find(|attr| attr.key == "service.name")
            .unwrap();
        if let Some(any_value) = &service_name.value {
            if let Some(PbValue::StringValue(s)) = &any_value.value {
                assert_eq!(s, "nested-service");
            }
        }

        // Verify flattened attributes were extracted
        let environment = all_attrs
            .iter()
            .find(|attr| attr.key == "environment")
            .unwrap();
        if let Some(any_value) = &environment.value {
            if let Some(PbValue::StringValue(s)) = &any_value.value {
                assert_eq!(s, "production");
            }
        }

        // Verify both nested and flattened resources were removed
        assert!(log.get(event_path!("resource")).is_none());
        assert!(log.get("resources.environment").is_none());
        assert!(log.get("resources.cluster").is_none());

        // Normal field should remain
        assert!(log.get("normal_field").is_some());
    }

    #[test]
    fn test_remove_all_resource_attributes() {
        // Test comprehensive removal for logs
        let mut log = LogEvent::from("test message");

        // Add both formats
        let mut nested_resource = ObjectMap::new();
        nested_resource.insert("service.name".into(), Value::from("test"));
        log.insert(event_path!("resource"), Value::Object(nested_resource));
        log.insert("resources.environment", "test");
        log.insert("keep_field", "value");

        log.remove_all_resource_attributes();

        assert!(log.get(event_path!("resource")).is_none());
        assert!(log.get("resources.environment").is_none());
        assert!(log.get("keep_field").is_some());

        // Test metrics (only flattened format)
        let mut metric = VectorMetric::new(
            "test_metric",
            crate::event::metric::MetricKind::Absolute,
            crate::event::metric::MetricValue::Counter { value: 1.0 },
        );

        let mut tags = crate::event::metric::MetricTags::default();
        tags.replace("resources.service".to_string(), "test".to_string());
        tags.replace("keep_tag".to_string(), "value".to_string());
        metric = metric.with_tags(Some(tags));

        metric.remove_all_resource_attributes();

        if let Some(tags) = metric.tags() {
            assert!(!tags.contains_key("resources.service"));
            assert!(tags.contains_key("keep_tag"));
        }
    }
}
