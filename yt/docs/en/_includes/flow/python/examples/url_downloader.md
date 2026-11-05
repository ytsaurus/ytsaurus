# URL Downloader in {{product-name}} Flow (Python)

This example shows a [pipeline](../../../../flow/concepts/glossary.md#pipeline) that groups incoming URLs by host and processes them in batches using [timers](../../../../flow/concepts/glossary.md#timer). It demonstrates the typical pattern: “accumulate in state → process by timer → clear state.”

[Source code]({{source-root}}/yt/yt/flow/examples/python/url_downloader)

## Structure

The pipeline includes a single transform-[computation](../../../../flow/concepts/glossary.md#stream-and-computation) called `url_downloader`, which:

- Accepts messages with `host` and `url` fields from the input [stream](../../../../flow/concepts/glossary.md#stream-and-computation).
- Accumulates URLs for each host in an internal YSON [state](../../../../flow/concepts/glossary.md#state).
- Sets a timer for 5 seconds ahead each time a new URL arrives.
- When the timer fires, it processes all accumulated URLs and publishes the results to the output stream `processed_urls`.

## `url_download_function.py`

This file contains the core processing logic. The function implements both `RowFunction` methods: `on_message` to accumulate URLs in the state and `on_timer` to process them in batches.

{% code '/yt/yt/flow/examples/python/url_downloader/url_download_function.py' lang='python' lines='[BEGIN url_download_function]-[END url_download_function]' %}

## `__main__.py`

This is the entry point: it creates the pipeline and registers the single `url_downloader` computation.

{% code '/yt/yt/flow/examples/python/url_downloader/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

## Key patterns

- **Timer-driven batch processing**: `output.add_timer(int(time.time()) + 5)` in `on_message` and the processing logic in `on_timer` provide a standard way to batch events by a time window.
- **Internal YSON state** via `ctx.state("host-state", message)`: the `get_or_default` / `set` / `clear` pattern helps you accumulate and clear data.
- **State key by host**: you group URLs by host using `group_by_schema` in the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec), so each computation instance processes URLs for a single host in isolation.
- **MessageBuilder**: `ctx.message_builder("processed_urls")` builds output messages with an explicit stream schema.
- **Safety check in `on_timer`**: `if not data or not data.get("pending_urls")` prevents reprocessing data after the state is cleared.

