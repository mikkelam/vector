//! Encoder for the `opentelemetry` sink.
//!
//! This module is responsible for converting Vector `Event`s into OTLP Protobuf messages.

use std::io;

use bytes::Bytes;

use prost::Message;
use vector_lib::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{any_value::Value as PbValue, AnyValue, KeyValue, KeyValueList},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    resource::v1::Resource,
};
use vrl::{
    event_path,
    value::{ObjectMap, Value},
};

use crate::{
    event::{Event, LogEvent},
    sinks::prelude::*,
};

/// The encoder for OTLP, responsible for converting batches of events
/// into Protobuf byte payloads.
#[derive(Debug, Clone)]
pub(super) struct OtlpEncoder;

impl OtlpEncoder {
    /// Creates a new `OtlpEncoder`.
    pub(super) const fn new() -> Self {
        Self
    }

    /// Encodes a batch of log events into an `ExportLogsServiceRequest` Protobuf message.
    pub fn encode_logs(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        // For simplicity, put all logs in a single ResourceLogs with empty resource
        // TODO: Group by actual resource attributes later
        let mut log_records = Vec::new();
        let mut resource_attributes = Vec::new();

        for event in events {
            let mut log = event.into_log();

            // Extract resource attributes from the first event
            if resource_attributes.is_empty() {
                if let Some(resource_map) = log.remove(event_path!("resource")) {
                    if let Some(resource_obj) = resource_map.as_object() {
                        resource_attributes =
                            convert_object_map_to_key_value_vec(resource_obj.clone());
                    }
                }
            } else {
                // Remove resource attributes from subsequent events to avoid duplication
                let _ = log.remove(event_path!("resource"));
            }

            let log_record = self.convert_log_event_to_log_record(log);
            log_records.push(log_record);
        }

        let scope_logs = vec![ScopeLogs {
            scope: None, // TODO: Add scope support
            log_records,
            schema_url: String::new(),
        }];

        let resource_logs = vec![ResourceLogs {
            resource: Some(Resource {
                attributes: resource_attributes,
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
            .and_then(|v| v.as_bytes().map(|b| b.to_vec()))
            .unwrap_or_default();

        let span_id = log
            .remove(event_path!("span_id"))
            .and_then(|v| v.as_bytes().map(|b| b.to_vec()))
            .unwrap_or_default();

        // Extract severity information
        let (severity_number, severity_text) = extract_severity(&mut log);

        // All remaining fields are treated as attributes.
        let attributes = convert_object_map_to_key_value_vec(
            log.all_event_fields()
                .unwrap()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );

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

    #[allow(dead_code, unused_variables)]
    fn encode_metrics(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        // TODO: Implement metric encoding
        emit!(SinkRequestBuildError {
            error: "Metric encoding is not yet implemented for the OTLP sink."
        });
        Err(())
    }

    #[allow(dead_code, unused_variables)]
    fn encode_traces(&self, events: Vec<Event>) -> Result<Bytes, ()> {
        // TODO: Implement trace encoding
        emit!(SinkRequestBuildError {
            error: "Trace encoding is not yet implemented for the OTLP sink."
        });
        Err(())
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

fn try_convert_value_to_key_value_vec(value: Value) -> Result<Vec<KeyValue>, ()> {
    if let Value::Object(map) = value {
        Ok(convert_object_map_to_key_value_vec(map))
    } else {
        // OTLP resources and attributes must be objects.
        // Emit an event and return an error.
        emit!(SinkRequestBuildError {
            error: format!(
                "Expected object for resource/attributes, got: {}",
                value.kind()
            )
        });
        Err(())
    }
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
        if let Some(severity_value) = log.remove(*field_name) {
            return map_severity_to_otlp(severity_value);
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
        // TRACE levels
        "trace" | "finest" => SeverityNumber::Trace,
        "trace1" | "trace2" | "trace3" | "trace4" => SeverityNumber::Trace4,

        // DEBUG levels
        "debug" | "fine" => SeverityNumber::Debug,
        "debug1" | "debug2" | "debug3" | "debug4" => SeverityNumber::Debug4,

        // INFO levels
        "info" | "information" | "notice" => SeverityNumber::Info,
        "info1" | "info2" | "info3" | "info4" => SeverityNumber::Info4,

        // WARN levels
        "warn" | "warning" => SeverityNumber::Warn,
        "warn1" | "warn2" | "warn3" | "warn4" => SeverityNumber::Warn4,

        // ERROR levels
        "error" | "err" => SeverityNumber::Error,
        "error1" | "error2" | "error3" | "error4" => SeverityNumber::Error4,

        // FATAL levels
        "fatal" | "critical" | "crit" | "emergency" | "emerg" | "panic" => SeverityNumber::Fatal,
        "fatal1" | "fatal2" | "fatal3" | "fatal4" => SeverityNumber::Fatal4,

        // Default to unspecified for unknown levels
        _ => SeverityNumber::Unspecified,
    };

    (severity_number as i32, severity_text)
}

/// Map numeric severity values to OTLP severity.
/// Handles both syslog-style (0-7) and direct OTLP numbering (1-24).
fn map_numeric_severity(level: i64) -> (i32, String) {
    match level {
        // Syslog style (RFC 5424) - prioritize these first
        0 => (SeverityNumber::Fatal as i32, "EMERGENCY".to_string()),
        1 => (SeverityNumber::Fatal as i32, "ALERT".to_string()),
        2 => (SeverityNumber::Fatal as i32, "CRITICAL".to_string()),
        3 => (SeverityNumber::Error as i32, "ERROR".to_string()),
        4 => (SeverityNumber::Warn as i32, "WARNING".to_string()),
        5 => (SeverityNumber::Info as i32, "NOTICE".to_string()),
        6 => (SeverityNumber::Info as i32, "INFO".to_string()),
        7 => (SeverityNumber::Debug as i32, "DEBUG".to_string()),

        // Direct OTLP numeric levels (8+ to avoid syslog overlap)
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
