# Job output I/O benchmark

Measures parsing and chunk writing for Arrow IPC, binary/text YSON, and JSON:

```text
prebuilt bytes -> TTableOutput -> TWritingValueConsumer
  -> schemaless chunk writer -> TMemoryWriter
```

Format writers prepare the complete input stream before measurement.
Manual wall time excludes preparation, setup, validation, and teardown. The first
iteration validates every output row against the source, outside timing.
The benchmark preserves dataset values. JSON uses type annotations to preserve
unsigned integers inside composite values. JSON parsing reconstructs unordered
maps, while Arrow parsing preserves IPC field order. Both use the benchmark's
schema-ordering adapter before chunk writing, standing in for the multi-chunk
writer's row normalization. The adapter orders values by schema, fills missing
columns with nulls, and validates values. YSON writes directly to the chunk writer.

## Synthetic data

Run from `yt/yt`:

```sh
ya test -r benchmarks/row_io
ya test -r benchmarks/row_io -F 'row_io::*Arrow*' \
    --test-binary-args=--benchmark_min_time=10x
```

## Real data

Build the standalone importer and export a rich-path selection

```sh
ya make -r benchmarks/row_io/import_table
benchmarks/row_io/import_table/import_table \
    --proxy CLUSTER '//path/to/table{key,payload}[#0:#100000]' /tmp/row_io_data

ya make -r benchmarks/row_io
ROW_IO_DATASET=/tmp/row_io_data benchmarks/row_io/row_io \
    --benchmark_filter='^Arrow/' --benchmark_min_time=10x
```

Add `--clamp-integers` to the importer to clamp int64 to [-2^53, 2^53] and uint64
to [0, 2^53], recursively including nested values and attributes. This is opt-in;
without it the importer preserves all values and integer types.

Arrow preparation uses the existing format writer over the source chunk's columnar
batches. It emits dense arrays and dictionaries over dense values; storage RLE is
expanded by the ordinary conversions. Logical list/struct types remain supported.
`arrow_dense_batches`, `arrow_dictionary_batches`, and `arrow_record_batches`
come directly from the writer during untimed input preparation. Column-batch
counts include reused dictionaries; they do not count dictionary IPC messages.
