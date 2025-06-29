The `opentelemetry` sink now supports comprehensive OpenTelemetry Protocol (OTLP) HTTP compliance for logs, metrics, and traces. This includes native HTTP protocol support, automatic signal routing to appropriate endpoints (/v1/logs, /v1/metrics, /v1/traces), and encoding that eliminates the need for complex VRL transformations.

authors: mikkelam
