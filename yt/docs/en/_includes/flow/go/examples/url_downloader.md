# URL Downloader in {{product-name}} Flow (Go)

An example of a [pipeline](../../../../flow/concepts/glossary.md#pipeline) that groups incoming URLs by host and processes them in batches by a [timer](../../../../flow/concepts/glossary.md#timer). It shows the typical pattern "accumulate in the [state](../../../../flow/concepts/glossary.md#state) → process by a timer → clear the state".

[Source code]({{source-root}}/yt/yt/flow/examples/go/url_downloader)

## Structure {#structure}

The pipeline consists of two [computations](../../../../flow/concepts/glossary.md#stream-and-computation):

- `url_reader` — a native source (`TSwiftPassthroughOrderedSourceComputation`) declared directly in the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec): it reads the queue and publishes messages with the `host` and `url` fields to the `urls` [stream](../../../../flow/concepts/glossary.md#stream-and-computation). It has no Go code.
- `url_downloader` (`urlDownloadFunction`) — a transform computation served by the companion.

The `url_downloader` computation works as follows:

1. It takes messages from the `urls` stream.
2. It appends the URL to the batch of its host in the internal YSON state `host-state`.
3. It sets a timer `flushDelay` (5 seconds) ahead on every new URL.
4. When the timer fires, it processes the whole accumulated batch, publishes the results to the `processed_urls` stream, and clears the state.

The messages are grouped by host (`group_by_schema` with `farm_hash(host)` and `host`), so the state of the key being processed is the batch of exactly one host.

## `main.go` {#main-go}

The entry point: creating the pipeline, registering the only computation, and starting it.

{% code '/yt/yt/flow/examples/go/url_downloader/main.go' lang='go' %}

## `url_download_function.go` {#url-download-function-go}

The state value is an ordinary Go structure with YSON tags: it holds the host name and the list of URLs that haven’t been processed yet.

{% code '/yt/yt/flow/examples/go/url_downloader/url_download_function.go' lang='go' lines='[BEGIN host_state]-[END host_state]' %}

`flow.RowFunction` implements both handlers: `OnMessage` accumulates the URLs in the state and sets the timer, `OnTimer` processes the batch as a whole.

{% code '/yt/yt/flow/examples/go/url_downloader/url_download_function.go' lang='go' lines='[BEGIN url_download_function]-[END url_download_function]' %}

## Key patterns {#key-patterns}

- Batch processing by a timer: `out.AddTimer(flow.TimerRequest{TriggerTimestamp: ...})` in `OnMessage` and the whole processing in `OnTimer` — the standard way to collect events into a time window. The worker keeps one timer per key, so a burst of messages collapses into a single firing rather than a firing per message.
- Internal YSON state through `flow.OpenYSONState[hostState](rt, hostStateName, msg)`: `Value()` returns the mutable batch, and `Clear()` empties it after processing. The state name (`host-state`) matches the name from `parameters.internal_states` of the computation in the spec.
- The state key by host is set by `group_by_schema` from the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec), so every partition of the computation processes the URLs of one host in isolation.
- Every result is created as a `processedURLMessage` and converted into an output message once, through `flow.ConvertFrom`.
- Clearing the state on all paths: `OnTimer` calls `state.Clear()` both when the batch is empty and when it has been processed — a timer that fires always leaves the key clean, and the URLs that arrive after that form a new batch.
