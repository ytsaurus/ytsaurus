# Compression

This section describes the compression algorithms supported by {{product-name}}.

By default, user data in {{product-name}} is stored and transmitted in compressed form.

The system performs compression:
1. When writing to a table or file, data is split into parts — [chunks](../../../user-guide/storage/chunks.md) — of a fixed size. The default chunk size is 16 MB.
2. Each chunk is compressed using the algorithm specified for that file or table and written to the disk.
3. For network transmission, data is read from the disk already in compressed form.

## Viewing a compression algorithm { #get_compression }

Chunks, files, and tables can be compressed in the {{product-name}} system.

The compression algorithm is specified in the `compression_codec` attribute:

- For chunks, the `compression_codec` attribute specifies the compression algorithm for all chunk blocks. Different chunks within a file or table can be compressed using different algorithms.
- For tables and files, `compression_codec` defines the default compression algorithm. It will be used if no algorithm is specified for individual file or table chunks. The default `compression_codec` value: for tables — `lz4`, for files — `none`.

The degree of data compression is specified in the `compressed_data_size` and `uncompressed_data_size` attributes of a chunk, table, or file:

- For chunks, the `uncompressed_data_size` attribute shows the size of the chunk before compression and the `compressed_data_size` attribute shows the size of the chunk after compression.
- For tables and files, the `uncompressed_data_size` attribute shows the total uncompressed data size of all object chunks and the `compressed_data_size` shows the compressed data size.

Statistics on the compression algorithms used in the table chunks are contained in the `compression_statistics` attribute of the table.

## Changing a compression algorithm { #set_compression }

To change the compression algorithm of an existing static table, set a new value of the `compression_codec` attribute, then run the `merge` command to recompress the chunks.

CLI

```bash
yt set //path/to/table/@compression_codec zstd_3
yt merge --src //path/to/table --dst //path/to/table --spec '{force_transform = %true}'
```

To change the compression algorithm of a [dynamic table](../../../user-guide/dynamic-tables/overview.md), set a new value for the `compression_codec` attribute. Then run the `remount-table` command.
The old chunks will eventually be recompressed in the process of compaction. The compression rate depends on the table size, the write speed, and the background compaction settings.

CLI

```bash
yt set //path/to/table/@compression_codec zstd_3
yt remount-table //path/to/table
```

If the table is permanently written to, there is usually no need for [forced compression](../../../user-guide/dynamic-tables/compaction.md#forced_compaction). If you need to force compression, use `forced compaction`.

## Supported compression algorithms { #compression_codecs }

| Compression algorithms | Description | Compression/decompression rate | Compression degree |
| ---------------------- | ---------------- | --------------------------- | --------------- |
| `none` | Without compression. | - | - |
| `snappy` | Learn more about the [snappy](https://google.github.io/snappy/) algorithm. | +++ | +- |
| `zlib_[1..9]` | The {{product-name}} system supports all 9 levels. The higher the level is, the more substantially and slowly data is compressed. Learn more about the [zlib](https://zlib.net) algorithm. | ++ | ++ |
| `lz4` | The default compression algorithm for tables. Learn more about the [lz4](https://lz4.github.io/lz4/) algorithm. | +++ | + |
| `lz4_high_compression` | The `lz4` algorithm with the enabled `high_compression` option. Compresses more efficiently, but is significantly slower. Its compression rate is inferior to `zlib`. | ++ | ++- |
| `zstd_[1..21]` | Learn more about the [zstd](https://github.com/facebook/zstd) algorithm. | ++ | ++ |
| `brotli_[1..11]` | We recommend using this algorithm for data that is not temporary. We recommend using levels 3, 5, and 8, because they have the best volume-to-speed ratio. Learn more about the [brotli](https://github.com/google/brotli) algorithm. | ++ | +++ |
| `lzma_[0..9]` | Learn more about the [lzma](https://www.7-zip.org) algorithm. | + | +++ |
| `bzip2_[1..9]` | Learn more about the [bzip2](http://www.bzip.org) algorithm. | ++ | ++ |

## Best practices { #best_practice }

- For files that are used in the operation as [symlinks](../../../user-guide/storage/links.md) to chunks, the `none` algorithm is used most often.
- `lz4` is often used for actual data. The algorithm provides high compression and decompression rates at an acceptable compression ratio.
- When you need maximum compression and long runtime is acceptable, `brotli_8` is often used.
- For operations consisting of a small number of jobs, for example, `final sort` or `sorted merge`, we recommend adding a separate processing stage — data compression in a merge operation with a large number of jobs.

{% note warning "Attention!" %}

Although algorithms with substantial compression — `zlib`, `zstd`, and `brotli` — can save disk space, their compression rate is much lower than the compression rate of the default `lz4` algorithm.
Using algorithms with substantial compression can lead to a significant increase in operation execution time. We recommend using them only for tables that take up a lot of space but rarely change.

{% endnote %}

## Deprecated compression algorithms { #deprecated }
INTERNAL DOCUMENTATION

In version 0.18.2 and higher, some compression algorithms are deprecated:

- `brotli3`, `brotli5`, `brotli8` (>= 18.0): Algorithms corresponding to compression levels 3, 5, and 8 of the [brotli](https://github.com/google/brotli) algorithm. They compress efficiently but slowly (150 mb/s, 50 mb/s, and 20 mb/s, respectively). Use `brotli_3`, `brotli_5`, and `brotli_8`.
- `zlib6` (`gzip_normal` up to 0.17.3): We recommend using `zlib_6`.
- `zlib9` (`gzip_best_compression` up to 0.17.3): We recommend using `zlib_9`.
- `zstd`: We recommend using `zstd_[1..21]` with the required compression level.

## Comparing compression algorithms { #benchmarks }

This method works only for static tables. To determine which algorithm is best for a particular table, run ```yt run-compression-benchmarks TABLE```.
A sample that is 1 GB by default will be taken from the table. It will be compressed by all algorithms.
After the operation is complete, you will see ```codec/cpu/encode```, ```codec/cpu/decode```, and ```compression_ratio``` for each algorithm.

For algorithms with multiple compression levels, the minimum, medium, and maximum levels are used by default.
To obtain results for all levels, use the ```--all-codecs``` option.

CLI
```bash
yt run-compression-benchmarks //home/dev/tutorial/compression_benchmark_data
```
```bash
[
    {
        "codec": "brotli_1",
        "codec/cpu/decode": 2103,
        "codec/cpu/encode": 2123,
        "compression_ratio": 0.10099302059669339
    },
    ...
    {
        "codec": "brotli_11",
        "codec/cpu/decode": "Not launched",
        "codec/cpu/encode": "Timed out", # compression did not finish within a standard --time-limit-sec (200)
        "compression_ratio": "Timed out"
    },
    ...
    {
        "codec": "none",
        "codec/cpu/decode": 0,
        "codec/cpu/encode": 247,
        "compression_ratio": 1.0
    },
    ...
    {
        "codec": "zstd_11",
        "codec/cpu/decode": 713,
        "codec/cpu/encode": 15283,
        "compression_ratio": 0.07451278201257201
    },
    ...
]
```
