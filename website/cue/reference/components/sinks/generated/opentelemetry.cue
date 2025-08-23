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
		required: false
		type: object: options: {
			method: {
				description: "The HTTP method to use for requests."
				required:    false
				type: string: {
					default: "post"
					examples: ["post", "put"]
				}
			}
			compression: {
				description: "The compression algorithm to use for HTTP requests."
				required:    false
				type: string: {
					default: "none"
					examples: ["none", "gzip"]
				}
			}
		}
	}

	auth: {
		description: """
			Configuration of the authentication strategy for HTTP requests.

			HTTP authentication should be used with HTTPS only, as the authentication credentials are passed as an
			HTTP header without any additional encryption beyond what is provided by the transport itself.
			"""
		required: false
		type: object: options: {
			auth: {
				description:   "The AWS authentication configuration."
				relevant_when: "strategy = \"aws\""
				required:      true
				type: object: options: {
					access_key_id: {
						description: "The AWS access key ID."
						required:    true
						type: string: examples: ["AKIAIOSFODNN7EXAMPLE"]
					}
					assume_role: {
						description: """
																The ARN of an [IAM role][iam_role] to assume.

																[iam_role]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html
																"""
						required: true
						type: string: examples: ["arn:aws:iam::123456789098:role/my_role"]
					}
					credentials_file: {
						description: "Path to the credentials file."
						required:    true
						type: string: examples: ["/my/aws/credentials"]
					}
					external_id: {
						description: """
																The optional unique external ID in conjunction with role to assume.

																[external_id]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
																"""
						required: false
						type: string: examples: ["randomEXAMPLEidString"]
					}
					imds: {
						description: "Configuration for authenticating with AWS through IMDS."
						required:    false
						type: object: options: {
							connect_timeout_seconds: {
								description: "Connect timeout for IMDS."
								required:    false
								type: uint: {
									default: 1
									unit:    "seconds"
								}
							}
							max_attempts: {
								description: "Number of IMDS retries for fetching tokens and metadata."
								required:    false
								type: uint: default: 4
							}
							read_timeout_seconds: {
								description: "Read timeout for IMDS."
								required:    false
								type: uint: {
									default: 1
									unit:    "seconds"
								}
							}
						}
					}
					load_timeout_secs: {
						description: """
																Timeout for successfully loading any credentials, in seconds.

																Relevant when the default credentials chain or `assume_role` is used.
																"""
						required: false
						type: uint: {
							examples: [30]
							unit: "seconds"
						}
					}
					profile: {
						description: """
																The credentials profile to use.

																Used to select AWS credentials from a provided credentials file.
																"""
						required: false
						type: string: {
							default: "default"
							examples: ["develop"]
						}
					}
					region: {
						description: """
																The [AWS region][aws_region] to send STS requests to.

																If not set, this defaults to the configured region
																for the service itself.

																[aws_region]: https://docs.aws.amazon.com/general/latest/gr/rande.html#regional-endpoints
																"""
						required: false
						type: string: examples: ["us-west-2"]
					}
					secret_access_key: {
						description: "The AWS secret access key."
						required:    true
						type: string: examples: ["wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"]
					}
					session_name: {
						description: """
																The optional [RoleSessionName][role_session_name] is a unique session identifier for your assumed role.

																Should be unique per principal or reason.
																If not set, session name will be autogenerated like assume-role-provider-1736428351340

																[role_session_name]: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
																"""
						required: false
						type: string: examples: ["vector-indexer-role"]
					}
					session_token: {
						description: """
																The AWS session token.
																See [AWS temporary credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html)
																"""
						required: false
						type: string: examples: ["AQoDYXdz...AQoDYXdz..."]
					}
				}
			}
			password: {
				description:   "The basic authentication password."
				relevant_when: "strategy = \"basic\""
				required:      true
				type: string: examples: ["${PASSWORD}", "password"]
			}
			service: {
				description:   "The AWS service name to use for signing."
				relevant_when: "strategy = \"aws\""
				required:      true
				type: string: {}
			}
			strategy: {
				description: "The authentication strategy to use."
				required:    true
				type: string: enum: {
					aws: "AWS authentication."
					basic: """
						Basic authentication.

						The username and password are concatenated and encoded via [base64][base64].

						[base64]: https://en.wikipedia.org/wiki/Base64
						"""
					bearer: """
						Bearer authentication.

						The bearer token value (OAuth2, JWT, etc.) is passed as-is.
						"""
				}
			}
			token: {
				description:   "The bearer authentication token."
				relevant_when: "strategy = \"bearer\""
				required:      true
				type: string: {}
			}
			user: {
				description:   "The basic authentication username."
				relevant_when: "strategy = \"basic\""
				required:      true
				type: string: examples: ["${USERNAME}", "username"]
			}
		}
	}

	request: {
		description: "Outbound HTTP request settings."
		required:    false
		type: object: options: {
			adaptive_concurrency: {
				description: """
					Configuration of adaptive concurrency parameters.

					These parameters typically do not require changes from the default, and incorrect values can lead to meta-stable or
					unstable performance and sink behavior. Proceed with caution.
					"""
				required: false
				type: object: options: {
					decrease_ratio: {
						description: """
																The fraction of the current value to set the new concurrency limit when decreasing the limit.

																Valid values are greater than `0` and less than `1`. Smaller values cause the algorithm to scale back rapidly
																when latency increases.

																**Note**: The new limit is rounded down after applying this ratio.
																"""
						required: false
						type: float: default: 0.9
					}
					ewma_alpha: {
						description: """
																The weighting of new measurements compared to older measurements.

																Valid values are greater than `0` and less than `1`.

																ARC uses an exponentially weighted moving average (EWMA) of past RTT measurements as a reference to compare with
																the current RTT. Smaller values cause this reference to adjust more slowly, which may be useful if a service has
																unusually high response variability.
																"""
						required: false
						type: float: default: 0.4
					}
					initial_concurrency: {
						description: """
																The initial concurrency limit to use. If not specified, the initial limit is 1 (no concurrency).

																Datadog recommends setting this value to your service's average limit if you're seeing that it takes a
																long time to ramp up adaptive concurrency after a restart. You can find this value by looking at the
																`adaptive_concurrency_limit` metric.
																"""
						required: false
						type: uint: default: 1
					}
					max_concurrency_limit: {
						description: """
																The maximum concurrency limit.

																The adaptive request concurrency limit does not go above this bound. This is put in place as a safeguard.
																"""
						required: false
						type: uint: default: 200
					}
					rtt_deviation_scale: {
						description: """
																Scale of RTT deviations which are not considered anomalous.

																Valid values are greater than or equal to `0`, and we expect reasonable values to range from `1.0` to `3.0`.

																When calculating the past RTT average, we also compute a secondary “deviation” value that indicates how variable
																those values are. We use that deviation when comparing the past RTT average to the current measurements, so we
																can ignore increases in RTT that are within an expected range. This factor is used to scale up the deviation to
																an appropriate range.  Larger values cause the algorithm to ignore larger increases in the RTT.
																"""
						required: false
						type: float: default: 2.5
					}
				}
			}
			concurrency: {
				description: """
					Configuration for outbound request concurrency.

					This can be set either to one of the below enum values or to a positive integer, which denotes
					a fixed concurrency limit.
					"""
				required: false
				type: {
					string: {
						default: "adaptive"
						enum: {
							adaptive: """
															Concurrency is managed by Vector's [Adaptive Request Concurrency][arc] feature.

															[arc]: https://vector.dev/docs/about/under-the-hood/networking/arc/
															"""
							none: """
															A fixed concurrency of 1.

															Only one request can be outstanding at any given time.
															"""
						}
					}
					uint: {}
				}
			}
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
			rate_limit_duration_secs: {
				description: "The time window used for the `rate_limit_num` option."
				required:    false
				type: uint: {
					default: 1
					unit:    "seconds"
				}
			}
			rate_limit_num: {
				description: "The maximum number of requests allowed within the `rate_limit_duration_secs` time window."
				required:    false
				type: uint: {
					default: 9223372036854775807
					unit:    "requests"
				}
			}
			retry_attempts: {
				description: "The maximum number of retries to make for failed requests."
				required:    false
				type: uint: {
					default: 9223372036854775807
					unit:    "retries"
				}
			}
			retry_initial_backoff_secs: {
				description: """
					The amount of time to wait before attempting the first retry for a failed request.

					After the first retry has failed, the fibonacci sequence is used to select future backoffs.
					"""
				required: false
				type: uint: {
					default: 1
					unit:    "seconds"
				}
			}
			retry_jitter_mode: {
				description: "The jitter mode to use for retry backoff behavior."
				required:    false
				type: string: {
					default: "Full"
					enum: {
						Full: """
															Full jitter.

															The random delay is anywhere from 0 up to the maximum current delay calculated by the backoff
															strategy.

															Incorporating full jitter into your backoff strategy can greatly reduce the likelihood
															of creating accidental denial of service (DoS) conditions against your own systems when
															many clients are recovering from a failure state.
															"""
						None: "No jitter."
					}
				}
			}
			retry_max_duration_secs: {
				description: "The maximum amount of time to wait between retries."
				required:    false
				type: uint: {
					default: 30
					unit:    "seconds"
				}
			}
			timeout_secs: {
				description: """
					The time a request can take before being aborted.

					Datadog highly recommends that you do not lower this value below the service's internal timeout, as this could
					create orphaned requests, pile on retries, and result in duplicate data downstream.
					"""
				required: false
				type: uint: {
					default: 60
					unit:    "seconds"
				}
			}
		}
	}
}
