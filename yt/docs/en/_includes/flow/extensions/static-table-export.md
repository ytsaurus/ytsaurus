# Export to static tables

`TProcessFunctionStaticTableExportSink` exports an ordered source into immutable static tables, one table per time window. The source can be, for example, a {{product-name}} queue (`TQueueSource`).

Every source message is parsed by the per-message process function named in `processing_function`: it takes the original message and emits zero or more table rows in the columns of `schema`. Without `processing_function`, the columns of `schema` are taken from the message by name. Every published table keeps a contiguous offset range of each source partition in its metadata attribute, and every message is exported exactly once.

## How it works {#how-it-works}

- A message goes to the window of its source write time, not of the event time inside the data. The windows of a partition never go back: if the write time decreases or the window is already published, the message goes to the last window of the partition or to the first unpublished one.
- The source job does not parse messages. The sink hands them to the `TStaticTableExportWriterResource` of its worker, which runs the process function on the source job pool, so the CPU is accounted to that partition and all rows of one offset go to one window. The function cannot use state, timers, watermarks or throttlers, and `GetCurrentTimestamp` returns the wall-clock time; a message it emits nothing for still counts as exported. A function error fails the partition job, which restarts.
- All writers append rows to one shared staging table of the window. The window transaction appends the rows and advances the partition cursors, and the source is acknowledged only after it commits. A replayed prefix whose offsets are already persisted is acknowledged without parsing.
- A window is closed once the persisted watermark of the output stream reaches `window_end + closure_delta`; idle partitions do not hold it. The controller publishes closed windows one by one, empty windows included.
- The export starts at the earliest write time of the first messages of all partitions. For a partition that received no message during the first 30 seconds of a non-draining job, the start of these 30 seconds is taken instead: a draining job does not read, so its silence does not mean a lag. So the export of an empty source may start with empty windows.
- Writers append only to the band of `max_unpublished_window_count` windows after the last published one, so a stalled publication bounds the unpublished data.
- On a graceful job move, the writer commits the windows of the moving partition right away, so the successor claims the partition without waiting.
- A drain completes after the windows every partition wrote to are published; a partition of a finite source completes only after its windows are published.

## Configuration {#configuration}

Link `yt/yt/flow/extensions/static_table_export/process_function` and the library with the process function into the pipeline binary. Start from the [example]({{source-root}}/yt/yt/flow/examples/cpp/queue_static_table_export).

The `TSwiftPassthroughOrderedSourceComputation` computation has one source, one output stream and one `TProcessFunctionStaticTableExportSink`. The source needs `write_time_column` (the write time in milliseconds), `try_parse_flow_queue_meta = %false` and no `partition_filter`; skip expressions are forbidden. The stream schema contains the `write_time_column` column of type `uint64`; without `processing_function`, it also contains every column of the sink `schema` with the same types.

### {{product-name}} queue {#queue-source}

`queue_path` contains the cluster, and the stream schema contains the `$row_index` column of type `int64`, which is the row offset. `write_time_column` is filled from the row `$timestamp`.

### Watermarks {#watermarks}

The watermark strategy is validated; `column` matches the source `write_time_column`:

```yson
watermark_strategy = {
    event_timestamp_assigner = {column = write_time_ms; format = milli_seconds; limit_by_system_timestamp = %false;};
    watermark_generator = {
        use_source_watermark = %false;
        out_of_orderness_bound = 0;
        idle_partitions = {duration = "1m"; max_ratio = 1.0;};
    };
    watermark_alignment = {group_name = static-export; drift_bound = "40m";};
};
```

`max_ratio = 1.0` lets a quiet source publish its last windows. `drift_bound` must exceed `window_period + closure_delta`, and the band must fit the alignment: `max_unpublished_window_count * window_period > drift_bound + closure_delta + 3 * window_period`. While the export catches up, partitions drift apart by the whole `drift_bound`, so unpublished tables hold about `drift_bound` of the input: choose it so that this volume with replication factor 3 fits the disk quota.

### Sink {#sink}

```yson
export = {
    sink_class_name = "NYT::NFlow::TProcessFunctionStaticTableExportSink";
    input_stream_ids = [rows];
    parameters = {
        output_directory = "//home/service/export/5min";
        writer_resource_id = "ExportWriter";
        processing_function = "NYT::NFlow::NService::TParseRecord";
        schema = [{name = key; type = string;}; {name = value; type = int64;}];
    };
};
```

The function builds rows with `context->MakeOutputMessageBuilder()` and adds them to `output`.

- `output_directory` is the table directory; the export state is kept in its `_export_state` child.
- `schema` is an unsorted table schema without system columns.
- `processing_function` and `processing_function_parameters` are the name of a process function with a `ProcessMessage` method and its static parameters.
- `window_period` (5 minutes by default, a whole number of seconds), `closure_delta` (30 seconds by default), `max_unpublished_window_count` (16 by default).
- `table_name_utc_offset` (`+03:00` by default) is the UTC offset in which tables are named by the window start; region time zones are not accepted.
- `writer_resource_id` is the writer resource.
- Dynamic: `publication_period`, `retry_backoff`, `table_ttl`.

### Writer resource {#writer}

The resource is declared in the pipeline spec and required by the computation on workers only:

```yson
resources = {
    ExportWriter = {resource_class_name = "NYT::NFlow::TStaticTableExportWriterResource"; parameters = {};};
};
```

```yson
required_resource_ids = {
    ExportWriter = {worker = %true; controller = %false;};
};
```

Parameters: `max_raw_data_weight` bounds delivered but unacknowledged messages, `max_parsed_data_weight` bounds parsed but unwritten rows, and `max_unwritten_data_weight` bounds messages admitted to windows but not written yet. `table_writer_count` (4 by default) is the maximum number of table writers appending concurrently in the transaction of one window: a writer is added only while admission is blocked by `max_unwritten_data_weight`. `idle_flush_period` and `max_stream_age` close window streams, and `max_stream_age + commit_timeout` is the hard transaction deadline. Tables are written with replication factor 3, at least two replicas, without chunk merging.

## Table metadata {#metadata}

For a queue, the `@_queue_metadata` attribute:

```yson
{
    cluster = primary;
    queue_path = "//path/to/queue";
    partitions = [{partition = 0; first_offset = 0; next_offset = 50;}; {partition = 1; first_offset = 0; next_offset = 50;}];
}
```

The ranges of a partition start at its first exported offset.

## Operations {#operations}

The master load is constant per writer and window, plus one key lock per partition in each commit.

Sensors: `raw_bytes`, `open_windows`, `durable_bytes`, `rejected_records`, `stream_retries`, `fenced_partitions`, `resets` and `commit_duration` on the writer; `closed_through`, `publication_duration` and `publication_retries` on the sink controller. A stalled publication reports the window in the `publish` error.

The state is bound to the pipeline, the computation, the source, the period, the band size and the table name offset; changing any of them requires a new output directory. The export picks up added source partitions by itself: their ranges start at their first exported offset and are empty from zero before that. A decreased partition count stops publication with a `publish` error. When `schema` changes, the controller alters unpublished windows to the new schema with `alter_table`; {{product-name}} allows only compatible changes such as a new optional column, and an incompatible one requires a new output directory.
