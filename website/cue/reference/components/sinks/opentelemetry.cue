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
			enabled:  true
			uses_uri: false
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
