# HTTP Extension for {{product-name}} Flow

The HTTP extension provides the asynchronous sink `NYT::NFlow::TAsyncHttpSink`. It sends each input message to an HTTP or HTTPS endpoint in a POST request and waits for a successful response before marking the message as delivered.

The sink accepts one input stream with exactly one `string` column. A non-null value in the column named by `payload_column` becomes the request body byte-for-byte unchanged, whether it contains protobuf, YSON, JSON, or arbitrary `string` bytes. The sink does not parse, serialize, or otherwise transform the value. A null value is skipped without an HTTP request. Configure headers such as `Content-Type` in the `headers` section of the sink parameters in `Spec`.

You can find the extension code [here]({{source-root}}/yt/yt/flow/extensions/http).

## Configure the sink

Declare the sink under the computation that produces the payload stream. For example:

```yson
"computations" = {
    "<computation>" = {
        "sinks" = {
            "http" = {
                "sink_class_name" = "NYT::NFlow::TAsyncHttpSink";
                "input_stream_ids" = ["requests"];
                "parameters" = {
                    "url" = "https://receiver.example/api/events";
                    "payload_column" = "payload";
                    "headers" = {
                        "Content-Type" = "application/octet-stream";
                    };
                    "idempotency_header" = "Idempotency-Key";
                    "keep_alive" = %true;
                    "max_redirect_count" = 0;
                    "max_idle_connections" = 8;
                };
            };
        };
    };
};
```

Static header values are stored in the pipeline `Spec` in Cypress and are visible to principals with permission to read the `Spec`, including through spec dumps. There is no secret-safe header mechanism yet. Do not put OAuth tokens, API tokens, or other secrets in `headers`. Do not use this sink with an endpoint that requires secret headers unless access controls and an external mechanism make that safe.

The `requests` stream in this example must have this schema:

```yson
[
    {name = "payload"; type = "string"; required = %false;};
]
```

All parameters in the example are static. Stop the pipeline before changing them. Retry settings are dynamic. Set them in `DynamicSpec` at `computations/<computation>/sinks/<sink>/parameters`, where `<computation>` and `<sink>` are the names from `Spec`:

```yson
"computations" = {
    "<computation>" = {
        "sinks" = {
            "<sink>" = {
                "parameters" = {
                    "request_timeout" = "60s";
                    "attempt_timeout" = "10s";
                    "retry_initial_delay" = "1s";
                    "retry_minimum_delay" = "100ms";
                    "retry_multiplier" = 2.0;
                    "retry_maximum_delay" = "30s";
                    "retry_jitter_ratio" = 0.2;
                    "max_attempt_count" = 5;
                };
            };
        };
    };
};
```

The updated dynamic parameters are used by subsequent retry decisions, including a retry of a request that is already in progress.

### Static spec

The HTTP sink supports only at-least-once delivery. The shared sink schema contributes `at_most_once_strategy`, but enabling it makes spec loading fail.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TAsyncHttpSink.md) %}

### Dynamic spec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TAsyncHttpSink.md) %}

## Delivery and retry behavior

The sink accepts every numeric status from 200 through 299 as successful. It reads the complete response body before completing the attempt so that a keep-alive connection can be reused. A non-2xx status, transport error, attempt timeout, or response-body read error causes a retry while both `max_attempt_count` and `request_timeout` permit one.

Redirects are followed up to `max_redirect_count` times across all retry attempts for one delivery while preserving the POST method, body, and headers. A relative `Location` is resolved against the preceding request URL. The sink drains and counts every redirect response, and rejects HTTPS-to-HTTP redirects. The default is zero, so redirect following is opt-in. Configure a positive limit only when every possible destination is trusted to receive the configured headers and payload.

`request_timeout` covers the entire delivery operation, including attempts and delays. Each attempt is limited to the smaller of `attempt_timeout` and the time remaining before the total deadline. Retry delays grow from `retry_initial_delay` by `retry_multiplier`, are randomized according to `retry_jitter_ratio`, and remain between `retry_minimum_delay` and `retry_maximum_delay`.

The retry policy is finite. Exhausting the attempt count or deadline terminally fails that delivery for the current sink and job. The sink makes no further POST attempt for the message automatically during that job's lifetime. The failed message remains unacknowledged; source and input progress for its partition stops and lag grows, although ordinary epochs can continue committing eligible work.

Replay resumes only after a job or worker restart, or another sink recreation, reloads `max_persisted_message_id`. Flow does not promise an automatic restart after retry exhaustion. After fixing the cause, arrange a restart through the normal pipeline operations if the job is still running.

## Guarantees and replay

HTTP delivery is **at least once**. Flow records completion only after receiving and fully reading a 2xx response, so an unsuccessful delivery is not lost. However, the receiver can process a request and then lose the connection before Flow records success. A retry or worker restart then sends the same body again.

POST I/O is concurrent, but successful completions become visible to ordered persistence only as a contiguous prefix in issue order. A later 2xx response waits behind every earlier in-flight delivery. A terminal failure of the earliest unacknowledged message blocks acknowledgment of all later messages for the rest of that sink and job lifetime, so `max_persisted_message_id` cannot advance past it or move out of order. This acknowledgment barrier does not limit network concurrency.

A null value in `payload_column` is acknowledged and skipped without an HTTP request. It does not block later messages and is not replayed after its completion is persisted.

The receiver must make the operation idempotent or deduplicate requests. By default, the sink adds `Idempotency-Key` with the hexadecimal representation of the stable Flow message ID as its value. Retries and replay of the same message use the same value. Set `idempotency_header` to another name to rename the header or to an empty string to disable it. A static entry with the same name in `headers` is rejected.

This external HTTP side effect is not part of the epoch transaction and is not covered by Flow's [exactly-once guarantee](../../../flow/concepts/guarantees.md#side-effects). Internal state, source offsets, and output-message persistence keep their normal Flow semantics.

## Connection reuse

`keep_alive` is enabled by default. Every configured HTTP sink in every active partition job owns a distinct sink instance, long-lived HTTP client, long-lived HTTPS client, and one idle-connection pool per client. `max_idle_connections` sets the maximum number retained by each pool and defaults to eight, so a sink that follows redirects across both schemes may retain up to twice that number. This is also true for `TTransformOrderedSourceComputation` and `TSwiftOrderedSourceComputation`: an ordered-source partition corresponds to one source key, so there is no additional per-key multiplier inside the job. Set `keep_alive` to `%false` if the endpoint or an intermediary does not support persistent connections; neither client then keeps idle connections regardless of `max_idle_connections`. These settings control idle connection reuse, not the retry count.

Each sink instance issues requests for all messages in its epoch concurrently. The idle-connection count bounds connection reuse, not the number of requests in flight: the sink has no rate-limit or concurrency setting. If several configured sinks target the same endpoint, size its concurrency and connection capacity as the sum across all of those sinks and active partition jobs. For each sink, its contribution is its per-partition request rate multiplied by the number of active partition jobs that own it. Limit concurrency through input throughput or at the receiver.

## Metrics

Sensors are published under `/sink/async_http_sink/` with a `sink_id` tag that carries the sink name from `Spec`:

- `/sink/async_http_sink/responses{status_code="<code>"}` counts every HTTP response once, including successful and retried status codes. The `status_code` tag retains the numeric value returned by the endpoint.
- `/sink/async_http_sink/attempt_failures` counts transport and timeout failures before a response is received, as well as failures while reading a received response body.

First inspect the aggregated pipeline status at `/sinks/<sink>/async_http_sink`: it reports the error of the oldest delivery that is currently failing and clears only when no delivery remains failed. The corresponding `/status_profiler/broken{path="/sinks/<sink>/async_http_sink"}` gauge is `1` while the sink is broken. Then use the counters to classify the failure.

A null value in `payload_column` is skipped and produces no HTTP request, so it increments neither counter.

A growing non-2xx response counter indicates application-level rejection. Growth in `attempt_failures` without a matching response counter points to transport or timeout failures before a response was received. A response-body read failure increments both `attempt_failures` and the response counter for the received status, even when that status is 2xx. All of these failures can lead to retries and eventual replay.

## Troubleshooting

#|
|| Error text | Cause and fix ||
|| `URL must use http or https and contain a host` | `url` has another scheme or no host. Set a complete `http://` or `https://` URL with a host. ||
|| `header names must be valid HTTP tokens` | `headers` contains an empty or invalid key. Use a valid HTTP header name. ||
|| `idempotency_header must be empty or a valid HTTP header name` | `idempotency_header` contains invalid characters. Use a valid header name or an empty string to disable the header. ||
|| `idempotency_header must not be hop-by-hop or transport-managed` | `idempotency_header` uses a framing or hop-by-hop name such as `Content-Length` or `Connection`. Use an application-level header name. ||
|| `expects exactly one input stream` | The sink has zero or multiple `input_stream_ids`. Connect exactly one input stream. ||
|| `expects exactly one payload column` | The input stream schema has zero or multiple columns. Give it exactly one payload column. ||
|| `must be the only String column` | `payload_column` does not name the only column, or that column is not `string`. Match the configured name to the only input column and make its type `string`. ||
|| `Async HTTP POST retry policy exhausted` or `Async HTTP POST retry deadline exhausted` | The finite retry budget ended. Fix the endpoint, transport, or timeout configuration, then restart the job or worker, or otherwise recreate the sink, to resume replay. No further POST occurs automatically in the current job. ||
|| `No sink class "NYT::NFlow::TAsyncHttpSink" is registered` | A candidate worker predates the shipped extension. Upgrade every candidate worker to a version whose standard `flow_server` includes the extension before stop/edit/start. ||
|#

## See also

- [List of Extensions](../../../flow/extensions/about.md)
- [HTTP extension guarantees](../../../flow/concepts/guarantees.md#http-guarantees)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)
