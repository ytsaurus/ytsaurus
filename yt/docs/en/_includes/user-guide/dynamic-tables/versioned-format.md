# Versioned format for dynamic table interaction

Sorted dynamic tables store a timestamp for each written column value. These timestamps can be useful for implementing custom logic or data migrations where you need to preserve original timestamps for correct TTL-based data cleanup operations.

The versioned interaction format incurs virtually no overhead costs, except when the column itself isn't requested but its timestamp is. In this case, the column data will be read.

For all subsequent interaction types, it is assumed that the timestamp of the `value` column is denoted as `$timestamp:value`.

## Versioned lookup and select

C++ and Python APIs support versioned lookup and select. To add columns with timestamps to the output, specify `versioned_read_options = {read_mode = latest_timestamp}`.
In select queries, additionally escape column timestamps with square brackets: `[$timestamp:value]`.

In the Python API, you can specify the `with_timestamps` option instead of `versioned_read_options`.

### Examples

```python
yt.lookup_rows("//path/to/table", [{"key": 123}], with_timestamps=True)

# Read all columns and their timestamps.
yt.select_rows("* from [//path/to/table]", with_timestamps=True)

# Read select columns and timestamps.
yt.select_rows("col_a, [$timestamp:col_a] as ts_a, [$timestamp:col_b] as ts_b from [//path/to/table]", with_timestamps=True)
```

## Versioned map-reduce

### Reading

To read timestamp columns in map-reduce operations, add the following attribute to [rich YPath](../../../user-guide/storage/ypath#rich_ypath) of the input table: `<versioned_read_options = {read_mode = latest_timestamp}>`.

### Writing

To write data in map-reduce operations in versioned format, add the following attribute to [rich YPath](../../../user-guide/storage/ypath#rich_ypath) of the output table: `<versioned_write_options = {read_mode = latest_timestamp}>`. Timestamp will be written for all columns where it is specified. For columns without a specified timestamp, the commit timestamp of the bulk insert transaction will be written instead.

{% note warning "Attention" %}

The versioned writing format has a few limitations:
- Sort operations don't support writing.
- During the write process, there is no check for potential duplicate timestamps across different records in the same column for the same key.

{% endnote %}
