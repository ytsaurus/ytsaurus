# Export to static tables

`TProcessFunctionStaticTableExportSink` exports a {{product-name}} queue into immutable static tables, one table per time window. Every queue row is parsed by the per-message process function named in `processing_function`: it takes the original row and emits zero or more table rows in the columns of `schema`. Without `processing_function`, the columns of `schema` are taken from the queue row by name. Every published table keeps a contiguous offset (`$row_index`) range of each queue partition in `@_queue_metadata`, and every row is exported exactly once.

## How it works {#how-it-works}

- A row goes to the window of its queue write time (`$timestamp`). The windows of a partition never go back: if the write time decreases or the window is already published, the row goes to the last window of the partition or to the first unpublished one.
- The process function runs on the source job pool inside `TStaticTableExportWriterResource`, not in the computation, so all rows of one offset go to one window. The function cannot use state, timers, watermarks or throttlers, and `GetCurrentTimestamp` returns the wall-clock time; a row it emits nothing for still counts as exported. A function error fails the partition job, which restarts.
- The worker `TStaticTableExportWriterResource` appends the rows of all partitions to one shared staging table of the window. The window transaction appends the rows and advances the partition cursors, and the source is acknowledged only after it commits.
- A window is closed once the persisted watermark of the output stream reaches `window_end + closure_delta`; idle partitions do not hold it. The controller publishes closed windows one by one, empty windows included.
- Writers append only to the band of `max_unpublished_window_count` windows after the last published one, so a stalled publication bounds the unpublished data.
- A drain completes after the windows every partition wrote to are published; a partition of a finite source completes only after its windows are published.

## Configuration {#configuration}

Link `yt/yt/flow/extensions/static_table_export/process_function` into the pipeline binary. Start from the [example]({{source-root}}/yt/yt/flow/examples/cpp/queue_static_table_export/pipeline.yson).

The `TSwiftPassthroughOrderedSourceComputation` computation has one `TQueueSource`, one output stream and one `TProcessFunctionStaticTableExportSink`. The source needs `write_time_column`, `try_parse_flow_queue_meta = %false`, a cluster in `queue_path` and no `partition_filter`; skip expressions are forbidden. The stream schema contains `$row_index` of type `int64` and the `write_time_column` column of type `uint64`; without `processing_function`, it also contains every column of the sink `schema` with the same types.

The watermark strategy is validated; `column` matches the source `write_time_column`:

```yson
watermark_strategy = {
    event_timestamp_assigner = {column = write_time_ms; format = milli_seconds; limit_by_system_timestamp = %false;};
    watermark_generator = {
        use_source_watermark = %false;
        out_of_orderness_bound = 0;
        idle_partitions = {duration = "10s"; max_ratio = 1.0;};
    };
    watermark_alignment = {group_name = queue-export; drift_bound = "6m";};
};
```

`drift_bound` must exceed `window_period + closure_delta`, and the band must fit the alignment: `max_unpublished_window_count * window_period > drift_bound + closure_delta + 3 * window_period`.

Sink parameters:

- `output_directory` is the table directory; the export state is kept in its `_export_state` child.
- `schema` is an unsorted table schema without system columns.
- `processing_function` and `processing_function_parameters` are the name of a process function with a `ProcessMessage` method and its static parameters. The function builds rows with `context->MakeOutputMessageBuilder()`.
- `window_period` (5 minutes by default, a whole number of seconds), `closure_delta` (30 seconds by default), `max_unpublished_window_count` (16 by default).
- `table_name_utc_offset` (`+03:00` by default) is the UTC offset in which tables are named by the window start.
- `writer_resource_id` is the writer resource, required on workers only.
- Dynamic: `publication_period`, `retry_backoff`, `table_ttl`.

The `@_queue_metadata` attribute of a published table:

```yson
{
    cluster = primary;
    queue_path = "//path/to/queue";
    partitions = [{partition = 0; first_offset = 0; next_offset = 50;}; {partition = 1; first_offset = 0; next_offset = 50;}];
}
```

The state is bound to the pipeline, the computation, the queue, the period, the band size and the table name offset; changing any of them requires a new output directory. The queue partition count is fixed when the export starts. A compatible `schema` change, such as a new optional column, is applied to unpublished windows with `alter_table`.
