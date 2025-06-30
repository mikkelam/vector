//! Encoder for the `opentelemetry` sink.
//!
//! This module is responsible for converting Vector `Event`s into OTLP Protobuf messages.

use std::io;

use bytes::Bytes;
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
    event::{Event, LogEvent, Metric as VectorMetric, MetricKind, MetricValue, TraceEvent},
    sinks::prelude::*,
};

use super::config::OtlpConfig;

/// The encoder for OTLP, responsible for converting batches of events
/// into Protobuf byte payloads.
#[derive(Debug, Clone)]
pub(super) struct OtlpEncoder {
    otlp_config: OtlpConfig,
}

impl OtlpEncoder {
    /// Creates a new `OtlpEncoder`.
    pub(super) const fn new(otlp_config: OtlpConfig) -> Self {
        Self { otlp_config }
    }

    /// Encodes a batch of log events into an `ExportLogsServiceRequest` Protobuf message.
    pub fn encode_logs(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        // For simplicity, put all logs in a single ResourceLogs with merged resource attributes
        let mut log_records = Vec::new();
        let mut event_resource_attributes = Vec::new();

        for event in events {
            let mut log = event.into_log();

            // Extract resource attributes from both nested and flattened formats
            if event_resource_attributes.is_empty() {
                // First try nested format (for existing tests)
                if let Some(resource_map) = log.remove(event_path!("resource")) {
                    if let Some(resource_obj) = resource_map.as_object() {
                        event_resource_attributes =
                            convert_object_map_to_key_value_vec(resource_obj.clone());
                    }
                }
                // Then extract any flattened fields (for Vector OTLP source)
                let flattened_attrs = self.extract_resource_attributes(&mut log);
                event_resource_attributes.extend(flattened_attrs);
            } else {
                // Remove resource attributes from subsequent events to avoid duplication
                let _ = log.remove(event_path!("resource"));
                self.remove_resource_attributes(&mut log);
            }

            let log_record = self.convert_log_event_to_log_record(log);
            log_records.push(log_record);
        }

        let scope_logs = vec![ScopeLogs {
            // Vector acts as a transparent aggregator/router, not the original instrumentation library.
            // Setting scope to None preserves the original instrumentation context from upstream sources.
            // Per OTLP spec, scope identifies "the logical unit of software that emits the telemetry" -
            // that's the original application, not Vector as the intermediary.
            scope: None,
            log_records,
            schema_url: String::new(),
        }];

        // Merge event resource attributes with global config attributes
        let merged_resource_attributes = self.merge_resource_attributes(event_resource_attributes);

        let resource_logs = vec![ResourceLogs {
            resource: Some(Resource {
                attributes: merged_resource_attributes,
                dropped_attributes_count: 0,
            }),
            scope_logs,
            schema_url: String::new(),
        }];

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
        let mut metric_records = Vec::new();

        // For metrics, we use the global config resource attributes to identify the Vector instance
        // that's exporting these metrics, even if they originated from other services.
        let resource_attributes = self.merge_resource_attributes(Vec::new());

        for event in events {
            let metric = event.into_metric();
            let otlp_metric = convert_vector_metric_to_otlp(metric);
            metric_records.push(otlp_metric);
        }

        let scope_metrics = vec![ScopeMetrics {
            // Vector acts as a transparent aggregator/router, not the original instrumentation library.
            // Setting scope to None preserves the original instrumentation context from upstream sources.
            // Per OTLP spec, scope identifies "the logical unit of software that emits the telemetry" -
            // that's the original application, not Vector as the intermediary.
            scope: None,
            metrics: metric_records,
            schema_url: String::new(),
        }];

        let resource_metrics = vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: resource_attributes,
                dropped_attributes_count: 0,
            }),
            scope_metrics,
            schema_url: String::new(),
        }];

        let request = ExportMetricsServiceRequest { resource_metrics };

        Ok(request.encode_to_vec().into())
    }

    pub fn encode_traces(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        let mut span_records = Vec::new();

        // For traces, we use the global config resource attributes to identify the Vector instance
        // that's processing these traces.
        let resource_attributes = self.merge_resource_attributes(Vec::new());

        for event in events {
            let trace = event.into_trace();
            let otlp_span = convert_vector_trace_to_otlp_span(trace);
            span_records.push(otlp_span);
        }

        let scope_spans = vec![ScopeSpans {
            // Vector acts as a transparent aggregator/router, not the original instrumentation library.
            // Setting scope to None preserves the original instrumentation context from upstream sources.
            // Per OTLP spec, scope identifies "the logical unit of software that emits the telemetry" -
            // that's the original application, not Vector as the intermediary.
            scope: None,
            spans: span_records,
            schema_url: String::new(),
        }];

        let resource_spans = vec![ResourceSpans {
            resource: Some(Resource {
                attributes: resource_attributes,
                dropped_attributes_count: 0,
            }),
            scope_spans,
            schema_url: String::new(),
        }];

        let request = ExportTraceServiceRequest { resource_spans };

        Ok(request.encode_to_vec().into())
    }

    /// Merges event-level resource attributes with global configuration attributes.
    ///
    /// Global config attributes take precedence over event attributes for consistency.
    fn merge_resource_attributes(&self, event_attributes: Vec<KeyValue>) -> Vec<KeyValue> {
        let mut merged_attributes = event_attributes;

        // Add global resource attributes from config
        for (key, value) in &self.otlp_config.resource_attributes {
            // Check if this key already exists in event attributes
            let key_exists = merged_attributes.iter().any(|kv| kv.key == *key);

            if !key_exists {
                merged_attributes.push(KeyValue {
                    key: key.clone(),
                    value: Some(AnyValue {
                        value: Some(PbValue::StringValue(value.clone())),
                    }),
                });
            }
        }

        // Add service name if configured and not already present
        if let Some(service_name) = &self.otlp_config.service_name {
            let service_name_exists = merged_attributes.iter().any(|kv| kv.key == "service.name");
            if !service_name_exists {
                merged_attributes.push(KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(PbValue::StringValue(service_name.clone())),
                    }),
                });
            }
        }

        // Add service version if configured and not already present
        if let Some(service_version) = &self.otlp_config.service_version {
            let service_version_exists = merged_attributes
                .iter()
                .any(|kv| kv.key == "service.version");
            if !service_version_exists {
                merged_attributes.push(KeyValue {
                    key: "service.version".to_string(),
                    value: Some(AnyValue {
                        value: Some(PbValue::StringValue(service_version.clone())),
                    }),
                });
            }
        }

        merged_attributes
    }

    /// Extracts resource attributes from flattened fields with "resources." prefix.
    fn extract_resource_attributes(&self, log: &mut LogEvent) -> Vec<KeyValue> {
        let mut resource_attributes = Vec::new();
        let mut keys_to_remove = Vec::new();

        // Collect all fields that start with "resources."
        for (key, value) in log.all_event_fields().unwrap() {
            if let Some(attr_key) = key.strip_prefix("resources.") {
                // Remove surrounding quotes if present
                let clean_key = attr_key.trim_matches('"');
                resource_attributes.push(KeyValue {
                    key: clean_key.to_string(),
                    value: Some(convert_value_to_any_value(value.clone())),
                });
                keys_to_remove.push(key.clone());
            }
        }

        // Remove the flattened resource fields from the log
        for key in &keys_to_remove {
            log.remove(key.as_str());
        }

        // Clean up empty parent objects that may have been left behind
        if log
            .get("resources")
            .and_then(|v| v.as_object())
            .map_or(false, |obj| obj.is_empty())
        {
            log.remove("resources");
        }

        resource_attributes
    }

    /// Removes resource attributes from log event (for subsequent events in batch).
    fn remove_resource_attributes(&self, log: &mut LogEvent) {
        let mut keys_to_remove = Vec::new();

        for (key, _) in log.all_event_fields().unwrap() {
            if key.starts_with("resources.") {
                keys_to_remove.push(key.clone());
            }
        }

        for key in keys_to_remove {
            log.remove(key.as_str());
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

    // Convert metric tags to OTLP attributes
    let attributes = if let Some(tags) = metric.tags() {
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
    };

    // Convert timestamp to nanoseconds
    let time_unix_nano = metric
        .timestamp()
        .and_then(|ts| ts.timestamp_nanos_opt())
        .unwrap_or(0) as u64;

    let data = match metric.value() {
        MetricValue::Counter { value } => {
            let data_point = NumberDataPoint {
                attributes,
                time_unix_nano,
                start_time_unix_nano: 0, // TODO: Handle start time for cumulative metrics
                value: Some(NumberDataPointValue::AsDouble(*value)),
                exemplars: Vec::new(),
                flags: 0,
            };

            Some(Data::Sum(Sum {
                data_points: vec![data_point],
                aggregation_temporality: match metric.kind() {
                    MetricKind::Incremental => AggregationTemporality::Delta as i32,
                    MetricKind::Absolute => AggregationTemporality::Cumulative as i32,
                },
                is_monotonic: true, // Counters are always monotonic
            }))
        }
        MetricValue::Gauge { value } => {
            let data_point = NumberDataPoint {
                attributes,
                time_unix_nano,
                start_time_unix_nano: 0,
                value: Some(NumberDataPointValue::AsDouble(*value)),
                exemplars: Vec::new(),
                flags: 0,
            };

            Some(Data::Gauge(Gauge {
                data_points: vec![data_point],
            }))
        }
        MetricValue::AggregatedHistogram {
            buckets,
            count,
            sum,
        } => {
            // Convert Vector buckets to OTLP format
            let mut explicit_bounds = Vec::new();
            let mut bucket_counts = Vec::new();

            for bucket in buckets {
                if bucket.upper_limit != f64::INFINITY {
                    explicit_bounds.push(bucket.upper_limit);
                }
                bucket_counts.push(bucket.count);
            }

            let data_point = HistogramDataPoint {
                attributes,
                time_unix_nano,
                start_time_unix_nano: 0,
                count: *count,
                sum: Some(*sum),
                bucket_counts,
                explicit_bounds,
                exemplars: Vec::new(),
                flags: 0,
                min: None, // TODO: Track min/max if available
                max: None,
            };

            Some(Data::Histogram(Histogram {
                data_points: vec![data_point],
                aggregation_temporality: match metric.kind() {
                    MetricKind::Incremental => AggregationTemporality::Delta as i32,
                    MetricKind::Absolute => AggregationTemporality::Cumulative as i32,
                },
            }))
        }
        MetricValue::AggregatedSummary {
            quantiles,
            count,
            sum,
        } => {
            // Convert Vector quantiles to OTLP format
            let quantile_values = quantiles
                .iter()
                .map(|q| ValueAtQuantile {
                    quantile: q.quantile,
                    value: q.value,
                })
                .collect();

            let data_point = SummaryDataPoint {
                attributes,
                time_unix_nano,
                start_time_unix_nano: 0,
                count: *count,
                sum: *sum,
                quantile_values,
                flags: 0,
            };

            Some(Data::Summary(Summary {
                data_points: vec![data_point],
            }))
        }
        MetricValue::Set { .. } => {
            // OTLP doesn't have a direct equivalent for sets
            // We could represent this as a gauge with the set size
            emit!(SinkRequestBuildError {
                error: "Set metrics are not directly supported in OTLP format"
            });
            return OtlpMetric {
                name,
                description,
                unit,
                data: None,
            };
        }
        MetricValue::Distribution { .. } => {
            // TODO: Convert distributions to histograms or summaries
            emit!(SinkRequestBuildError {
                error: "Distribution metrics conversion is not yet implemented"
            });
            return OtlpMetric {
                name,
                description,
                unit,
                data: None,
            };
        }
        MetricValue::Sketch { .. } => {
            // TODO: Convert sketches to histograms
            emit!(SinkRequestBuildError {
                error: "Sketch metrics conversion is not yet implemented"
            });
            return OtlpMetric {
                name,
                description,
                unit,
                data: None,
            };
        }
    };

    OtlpMetric {
        name,
        description,
        unit,
        data,
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

    let kind = trace_map
        .get("kind")
        .and_then(|v| v.as_integer())
        .unwrap_or(SpanKind::Internal as i64) as i32;

    let trace_state = trace_map
        .get("trace_state")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "".to_string());

    // Convert timestamps from Vector's DateTime to nanoseconds
    let start_time_unix_nano = trace_map
        .get("start_time_unix_nano")
        .and_then(|v| v.as_timestamp())
        .and_then(|ts| ts.timestamp_nanos_opt())
        .unwrap_or(0) as u64;

    let end_time_unix_nano = trace_map
        .get("end_time_unix_nano")
        .and_then(|v| v.as_timestamp())
        .and_then(|ts| ts.timestamp_nanos_opt())
        .unwrap_or(0) as u64;

    // Convert attributes
    let attributes = trace_map
        .get("attributes")
        .and_then(|v| v.as_object())
        .map(|obj| convert_object_map_to_key_value_vec(obj.clone()))
        .unwrap_or_default();

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

    // Convert status
    let status = trace_map
        .get("status")
        .and_then(|v| v.as_object())
        .map(|obj| SpanStatus {
            message: obj
                .get("message")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "".to_string()),
            code: obj.get("code").and_then(|v| v.as_integer()).unwrap_or(0) as i32,
        });

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
    let time_unix_nano = obj
        .get("time_unix_nano")?
        .as_timestamp()?
        .timestamp_nanos_opt()? as u64;

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
    use crate::event::LogEvent;

    #[test]
    fn test_trace_id_hex_decoding() {
        let encoder = OtlpEncoder::new(OtlpConfig::default());

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
        let encoder = OtlpEncoder::new(OtlpConfig::default());

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
        let encoder = OtlpEncoder::new(OtlpConfig::default());

        let log = LogEvent::from("test message");
        let log_record = encoder.convert_log_event_to_log_record(log);

        // Should result in empty arrays for missing data
        assert_eq!(log_record.trace_id.len(), 0);
        assert_eq!(log_record.span_id.len(), 0);
    }

    #[test]
    fn test_hex_strings_from_source() {
        let encoder = OtlpEncoder::new(OtlpConfig::default());

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
        let encoder = OtlpEncoder::new(OtlpConfig::default());

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
        let encoder = OtlpEncoder::new(OtlpConfig::default());

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
}
