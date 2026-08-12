# URL Downloader in {{product-name}} Flow (C++)

The [pipeline](../../../../flow/concepts/glossary.md#pipeline) shows how you run external calls (similar to HTTP requests) from [computations](../../../../flow/concepts/glossary.md#stream-and-computation). You control the download speed (throttling), shard by host, and manage the [state](../../../../flow/concepts/glossary.md#state).

[Source code]({{source-root}}/yt/yt/flow/examples/cpp/url_downloader)

## Pipeline components

### TUrlDownloader

This helper class (a descendant of `TRefCounted`) manages download queues for each host. It lets you:

- **Register hosts** (`RegisterHost`) — for each host, it creates an asynchronous executor that processes URLs from the queue one after another with an artificial delay (to emulate throttling).
- **Add URLs** (`RegisterUrl`) — it adds a URL to the corresponding host’s queue.
- **Extract results** (`ExtractProcessedUrls`) — it returns processed URLs with their results.
- **Unregister a host** (`UnregisterHost`) — it stops processing and clears the queue.

The key pattern is using `AsyncVia(GetCurrentInvoker())` to run background processing within a serialized invoker. This lets you safely work with shared state without locks.

### TLimitedUrlDownloadComputation

This is the main computation, which inherits from `TTransformComputation`. It coordinates URL downloads using `TUrlDownloader`.

When you process an input message (`DoProcessMessage`):

1. You read a `TUrlMessage` with the `Host` and `Url` fields.
2. You save the URL in the host’s state.
3. You register the host and URL in `TUrlDownloader`.
4. You set a [timer](../../../../flow/concepts/glossary.md#timer) to periodically check results via `GetNextHostCheck`.
5. You apply a limit to the state size using `EnforceLimit`.

When the timer fires (`DoProcessTimer`):

1. You restore the host from the state.
2. You extract processed URLs from `TUrlDownloader`.
3. You remove processed URLs from the state.
4. You generate a `TProcessedUrlMessage` for each processed URL.
5. If the queue is empty, you unregister the host and reset the state; otherwise, you set the next timer.

## Message types

- **TUrlMessage** — a descendant of `TYsonMessage`. It contains the `Host` and `Url` fields.
- **TProcessedUrlMessage** — a descendant of `TYsonMessage`. It contains the `Host`, `Url`, and `Data` fields (the processing result).

## Key patterns

### Internal YsonState

You use `TKeyStateClient<TLimitedHostState>` to store the URL queue for each host. The `TLimitedHostState` state contains:

- `Host` — the host name.
- `Urls` — the URL queue (`std::deque<std::string>`) waiting to be processed.

### Timers for periodic checks

The `GetNextHostCheck` method calculates the time for the next host check. The time is calculated based on:

- `CheckHostPeriod` — the check period (default is 5 seconds).
- The host name hash — to evenly distribute checks for different hosts over time.

### Dynamic parameters

`TDynamicLimitedUrlDownloadParameters` lets you change parameters without restarting the pipeline:

- `CheckHostPeriod` — the host check period (default is 5 seconds, minimum is 1 second).
- `PersistLimit` — the maximum number of URLs stored in the state for a single host (default is 1000).

### PersistLimit

`EnforceLimit` limits the size of the URL queue in the state. If the number of URLs exceeds `PersistLimit`, the older URLs are removed. This keeps the state within the row size limits for a dynamic table.

{% note warning %}

During [partition](../../../../flow/concepts/glossary.md#partition) rebalancing, URLs that don’t fit within the `PersistLimit` will be lost. Choose the limit value considering the acceptable loss.

{% endnote %}

## Pipeline structure

1. **Input queue** → the `urls` stream (`TUrlMessage`).
2. The `urls` stream → **TLimitedUrlDownloadComputation** (with timers and state) → the `processed_urls` stream (`TProcessedUrlMessage`).

## main function

In `main`, you register two streams:

- `RegisterStream<TUrlMessage>("urls")` — input URLs.
- `RegisterStream<TProcessedUrlMessage>("processed_urls")` — processed URLs.

## Source code

### TUrlDownloader

{% code '/yt/yt/flow/examples/cpp/url_downloader/lib/url_downloader_functions.cpp' lang='cpp' lines='[BEGIN url_downloader]-[END url_downloader]' keep-indents %}

### TLimitedUrlDownloadComputation

{% code '/yt/yt/flow/examples/cpp/url_downloader/lib/url_downloader_functions.cpp' lang='cpp' lines='[BEGIN limited_url_download]-[END limited_url_download]' keep-indents %}

