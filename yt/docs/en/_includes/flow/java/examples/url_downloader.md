# URL Downloader in {{product-name}} Flow (Java)

The [pipeline](../../../../flow/concepts/glossary.md#pipeline) groups incoming URLs by host, accumulates them in the internal [state](../../../../flow/concepts/glossary.md#state), and processes them in batches based on a timer. The timer triggers 5 seconds after a URL is added to the queue and emits the results to the output [stream](../../../../flow/concepts/glossary.md#stream-and-computation).

[Source code (Java)]({{source-root}}/yt/yt/flow/examples/java/url_downloader)

[Source code (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/url_downloader)

## Components

### UrlDownloadFunction

This is the main process function that implements the logic for accumulating and processing URLs. The `onMessage` method adds a URL to the host’s internal state and sets a timer. The `onTimer` method processes the accumulated URLs and emits the results.

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/UrlDownloadFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/UrlDownloadFunction.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/UrlDownloadFunction.java' lang='java' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/UrlDownloadFunction.kt' lang='kotlin' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

{% endlist %}

### HostState

This is the internal state model that is serialized to YSON. It stores the host name and the list of URLs that are waiting to be processed.

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/model/HostState.java' lang='java' lines='[BEGIN host_state]-[END host_state]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/model/HostState.kt' lang='kotlin' lines='[BEGIN host_state]-[END host_state]' keep-indents %}

{% endlist %}

### Registering the computation and streams

You register the `url_downloader` computation with the `@FlowComputation` annotation on the process function class.

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/UrlDownloadFunction.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/UrlDownloadFunction.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

{% endlist %}

You declare typed streams via `ComputationProvider` (the `getStreams()` method).

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/UrlDownloaderComputationContext.java' lang='java' lines='[BEGIN stream_context]-[END stream_context]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/UrlDownloaderComputationContext.kt' lang='kotlin' lines='[BEGIN stream_context]-[END stream_context]' keep-indents %}

{% endlist %}

### NodeCompanionMain

This is the entry point for the companion based on Spring Boot.

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/NodeCompanionMain.java' lang='java' lines='[BEGIN main]-[END main]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/NodeCompanionMain.kt' lang='kotlin' lines='[BEGIN main]-[END main]' keep-indents %}

{% endlist %}

## Key patterns

- **Grouping by key**: you create a separate state for each host. Flow automatically routes messages with the same key to a single computation instance.
- **Wall-clock time-based timer**: `output.addTimer(System.currentTimeMillis() / 1000 + 5, 0L)` starts processing 5 seconds after a URL is added to the queue. Multiple `addTimer` calls with the same `triggerTimestamp` are deduplicated.
- **Batch processing in `onTimer`**: all accumulated URLs are processed at once when the timer triggers. This reduces the number of calls to downstream services.
- **State cleanup**: after processing, you delete the state via `accessor.clear()` to prevent memory leaks.
- **YsonStateAccessor**: the internal state is serialized to YSON and stored on the C++ worker side. You get the Java object via `getOrDefault`.

