# Job output I/O benchmark

Measures parsing and chunk writing for Arrow IPC, binary/text YSON, and JSON:

```text
prebuilt bytes -> TTableOutput -> TWritingValueConsumer
  -> schemaless multi-chunk writer -> memory-backed chunk sinks
```

Format writers prepare the complete input stream before measurement.
Manual wall time excludes preparation, setup, readback verification, and teardown. The first
iteration validates every output row against the source, outside timing.
The benchmark preserves dataset values. JSON uses type annotations to preserve
unsigned integers inside composite values. JSON parsing reconstructs unordered
maps, while Arrow parsing preserves IPC field order. All output formats use the
production multi-writer's row normalization and validation. The output-only
schema-ordering wrapper is no longer needed; dataset preparation still uses it
because its source chunk is written directly by a single-chunk writer.

## Offline multi-chunk helper

`dataset/multi_chunk_writer.h` provides the shared output stack for every format:
the production schemaless multi-chunk writer with memory-backed physical sinks.
After waiting for close, the pipeline selects only emitted chunk specs, excluding
unused trailing sessions. The sequence overload of `ValidateOutput` checks checksums
and keeps one expected-row cursor across chunks.

The helper uses scan encoding, 256 KiB blocks, one compression worker, no compression
or erasure, and disables computed columns, Skynet sharing, and digests. Its default
desired chunk size is 256 MiB (the production non-erasure 2x size rule still applies).
Other production thresholds remain active, including the 100 GiB desired chunk weight
and 30 MiB maximum metadata size.

Initial session construction remains outside manual timing. Parsing, validation,
encoding, consumer flush, mid-stream rotations, and final close
are timed. Chunk collection, byte/block inspection, readback, and destruction are
outside timing. Counters report output chunks, blocks, bytes, and the common desired
chunk size; input preparation counters remain separate.

Performance comparisons must use fresh baselines. Historical single-chunk results
bypassed the multi-writer's validation.

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
