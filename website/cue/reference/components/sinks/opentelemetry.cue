package metadata

components: sinks: opentelemetry: {
	title: "OpenTelemetry (OTLP)"

	classes: {
		commonly_used: false
		delivery:      "at_least_once"
		development:   "beta"
		egress_method: "batch"
		stateful:      false
	}

	features: {
		auto_generated:   true
		acknowledgements: true
		healthcheck: enabled: false
	}

	input: {
		logs: true
		metrics: {
			counter:      true
			distribution: true
			gauge:        true
			histogram:    true
			set:          true
			summary:      true
		}
		traces: true
	}

	support: {
		requirements: ["This sink sends data using the native OTLP (OpenTelemetry Protocol) over HTTP with binary Protocol Buffers encoding."]
		warnings: []
		notices: ["Currently supports HTTP transport only. gRPC support is planned for future releases."]
	}

	configuration: base.components.sinks.opentelemetry.configuration
	how_it_works: {
		quickstart: {
			title: "Quickstart"
			body: """
				This sink sends observability data using the native OTLP (OpenTelemetry Protocol) over HTTP. It automatically handles logs, metrics, and traces, routing them to the appropriate endpoints.

				## Basic Configuration

				```yaml
				# Simple setup for all signal types
				[sinks.otlp]
				type = "opentelemetry"
				endpoint = "http://localhost:4318"
				inputs = ["logs", "metrics", "traces"]
				```

				## Custom Paths

				```yaml
				# Custom endpoint paths for enterprise setups
				[sinks.otlp_custom]
				type = "opentelemetry"
				endpoint = "https://otel.company.com"
				logs_path = "/api/v2/logs"
				metrics_path = "/api/v2/metrics"
				traces_path = "/api/v2/traces"
				```

				## Complete Example with OTEL Collector

				1. **Vector Configuration:**

				```yaml
				sources:
				  demo_logs:
				    type: "demo_logs"
				    format: "syslog"
				    count: 1000
				    interval: 1

				  host_metrics:
				    type: "host_metrics"
				    collectors: ["cpu", "memory"]

				sinks:
				  otlp_sink:
				    type: "opentelemetry"
				    inputs: ["demo_logs", "host_metrics"]
				    endpoint: "http://localhost:4318"
				```

				2. **OTEL Collector Configuration:**

				```yaml
				receivers:
				  otlp:
				    protocols:
				      http:
				        endpoint: "0.0.0.0:4318"

				exporters:
				  debug:
				    verbosity: detailed
				  jaeger:
				    endpoint: localhost:14250
				    tls:
				      insecure: true

				service:
				  pipelines:
				    logs:
				      receivers: [otlp]
				      exporters: [debug]
				    metrics:
				      receivers: [otlp]
				      exporters: [debug]
				    traces:
				      receivers: [otlp]
				      exporters: [debug, jaeger]
				```

				## How It Works

				- **Automatic Routing**: Data is automatically sent to `/v1/logs`, `/v1/metrics`, or `/v1/traces` based on signal type
				- **Native OTLP**: Uses binary Protocol Buffers encoding for optimal performance
				- **Log Level Parsing**: Automatically extracts severity from `level`, `severity`, or `log_level` fields
				- **Resource Handling**: For logs, extracts resource attributes from `resource` field if present. Metrics and traces use empty resources for transparency.

				## Current Limitations

				- **Transport**: HTTP only (gRPC support planned for future releases)
				- **Encoding**: Binary Protocol Buffers only
				- **Status**: Beta - configuration may change in future versions

				"""
		}
	}
}
