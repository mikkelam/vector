//! The `opentelemetry` sink.
//!
//! This sink sends observability data to an OpenTelemetry-compatible collector
//! over HTTP. It handles logs, metrics, and traces, routing them to the
//! appropriate endpoints as Protobuf-encoded messages.

// #############################################################################
// ## TODO: Remove the `dead_code` allow once the modules are implemented.   ##
// #############################################################################
#![allow(dead_code)]

mod config;
mod encoder;
mod request_builder;
mod service;
mod sink;

#[cfg(test)]
mod tests;
