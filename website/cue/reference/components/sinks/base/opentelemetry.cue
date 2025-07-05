package metadata

base: components: sinks: opentelemetry: configuration: {
	endpoint: {
		description: """
			The base endpoint for the OTLP receiver.

			The path values are appended to this base URL. For example, if the endpoint is
			`http://localhost:4318` and the logs_path is `/v1/logs`, logs will be sent to
			`http://localhost:4318/v1/logs`.
			"""
		required: true
		type: string: {
			examples: ["http://localhost:4318", "https://otel.company.com:4318"]
		}
	}

	logs_path: {
		description: """
			The path to use for logs.

			This path is appended to the base endpoint URL when sending log data.
			"""
		required: false

		type: string: {
			default: "/v1/logs"
			examples: ["/v1/logs", "/api/v2/logs", "/custom/logs"]
		}
	}

	metrics_path: {
		description: """
			The path to use for metrics.

			This path is appended to the base endpoint URL when sending metric data.
			"""
		required: false
		type: string: {
			default: "/v1/metrics"
			examples: ["/v1/metrics", "/api/v2/metrics", "/custom/metrics"]
		}
	}

	traces_path: {
		description: """
			The path to use for traces.

			This path is appended to the base endpoint URL when sending trace data.
			"""
		required: false
		type: string: {
			default: "/v1/traces"
			examples: ["/v1/traces", "/api/v2/traces", "/custom/traces"]
		}
	}

	protocol: {
		description: "The protocol to use for sending data."
		required:    false
		type: string: {
			default: "http"
			examples: ["http"]
		}
	}

	http: {
		description: """
				The HTTP configuration for the sink.
			"""
		type: object: options: {
			method: {
				required: false
				type: string: {
					default: "Post"
					examples: ["Post", "Put"]
				}
			}
			compression: {
				required: false
				default:  "none"
				type: string: {
					examples: ["none", "gzip"]
				}
			}
			encoding: {
				required: false
				type: string: {
					default: "Protobuf"
					examples: ["Protobuf"]
				}
			}
		}
	}

	grpc: {
		description: """
			The gRPC configuration for the sink.
			"""
		type: object: options: {
			compression: {
				required: false
				default:  "none"
				type: string: {
					examples: ["none", "gzip"]
				}
			}
			encoding: {
				required: false
				type: string: {
					default: "Protobuf"
					examples: ["Protobuf"]
				}
			}
		}
	}

	healthcheck: {
		description: "The healthcheck configuration for the sink."
		type: object: options: {
			path: {
				description: "The path to use for the healthcheck."
				required:    false
				type: string: {
					default: "/"
				}
			}
			skip: {
				description: "Whether to skip the healthcheck."
				required:    false
				type: bool: {
					default: false
				}
			}
		}
	}

	request: {
		headers: {
			description: "Additional HTTP headers to add to every HTTP request."
			required:    false
			type: object: {
				examples: [{
					Accept:               "text/plain"
					"X-My-Custom-Header": "A-Value"
				}]
				options: "*": {
					description: "An HTTP request header and it's value."
					required:    true
					type: string: {}
				}
			}
		}
	}
}
