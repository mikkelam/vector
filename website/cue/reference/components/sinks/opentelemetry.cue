package metadata

components: sinks: opentelemetry: {
	title: "OpenTelemetry"

	classes: {
		commonly_used: true
		delivery:      "at_least_once"
		development:   "beta"
		egress_method: "batch"
		service_providers: ["OpenTelemetry"]
		stateful: false
	}

	features: {
		acknowledgements: true
		auto_generated:   true
		healthcheck: {
			enabled: true
		}
		send: {
			batch: {
				enabled:      true
				common:       true
				max_bytes:    null
				timeout_secs: 1.0
			}
			compression: {
				enabled: true
				default: "none"
				algorithms: ["none", "gzip"]
				levels: ["none", "fast", "default", "best", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
			}
			encoding: {
				enabled: false
			}
			request: {
				enabled: true
				headers: true
			}
			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        false
				enabled_by_scheme:      true
			}
			to: {
				service: services.opentelemetry

				interface: {
					socket: {
						api: {
							title: "OpenTelemetry Protocol (OTLP)"
							url:   urls.opentelemetry_protocol
						}
						direction: "outgoing"
						protocols: ["http"]
						ssl: "optional"
					}
				}
			}
		}
	}

	support: {
		requirements: []
		warnings: []
		notices: []
	}

	configuration: base.components.sinks.opentelemetry.configuration

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

	how_it_works: {
		resource_extraction: {
			title: "Resource Attribute Extraction"
			body: """
				The OpenTelemetry sink automatically extracts resource attributes from events based on the OpenTelemetry semantic conventions.

				For **logs and traces**, resource attributes are extracted from fields matching the pattern `resource.*`.
				For example, a field named `resource.service.name` will be extracted as a resource attribute with key `service.name`.

				For **metrics**, resource attributes are extracted from metric tags that start with `resource.`.
				The `resource.` prefix is stripped from the attribute name in the final OTLP payload.

				After extraction, these fields are removed from the original event to avoid duplication in the OTLP output.

				### Setting Resource Attributes manually
				If your sinks do not attach resource attributes, you should set them manually.
				You can set resource attributes for your sources using a transform before the OpenTelemetry sink:

				```yaml
				transforms:
				  otel_enrichment:
				    type: remap
				    source: |
				      .resource.attributes."host.name" = get_hostname!()
				      .resource.attributes."service.name" = "my amazing service"
				      .resource.attributes."service.version" = "1.33.7"

				sinks:
				  opentelemetry:
				    type: opentelemetry
				    inputs: ["otel_enrichment"]
				    endpoint: "http://localhost:4318"
				```

				This approach ensures that all events sent to the OpenTelemetry sink include consistent resource attributes that identify your service and deployment environment.
				"""
		}

		protocol_support: {
			title: "Protocol Support"
			body: """
				Currently only HTTP/protobuf is supported.
				"""
		}

		data_encoding: {
			title: "Data Encoding and Conversion"
			body: """
				Vector converts different data types to their appropriate OpenTelemetry representations:

				**Logs**: Log events are converted to OTLP LogRecord format with automatic severity level mapping.
				Vector maps standard log levels (error, warn, info, debug, trace) to their corresponding OTLP severity numbers.

				**Metrics**: All Vector metric types are supported and converted to appropriate OTLP metric types. Note that this conversion can be lossy for some types.

				**Traces**: Trace events are converted to OTLP Span format with automatic trace/span ID handling.
				"""
		}

	}
}
