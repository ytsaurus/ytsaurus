# Slow table and file reads

Pattern for moving data for reading:
![](../../../../images/read.svg)

## How reading is performed.

Table chunks are hosted on node disks in a native format compressed by `compression_codec`.
From there, they are moved to data proxies where they are unpacked, recoded as requested by the user (as YSON, Protobuf, or Skiff), compressed by the specified network codec as part of the HTTP response, and find their way to the user machine.

## Factors affecting read performance

1. [**Medium**](../../../user-guide/storage/media.md). This is the type of disk table chunks are stored on. Held in the `@primary_medium` attribute. SSD speed is higher than HDD but their quota is more limited. To change the medium, you first need to make a new entry in the table's `@media` attribute, such as, `yt set //path/to/table/@media/ssd_blobs '{replication_factor=3}'`, and then move the table to the new medium: `yt set //path/to/table/@primary_medium ssd_blobs`
1. [**Compression algorithms**](../../../user-guide/storage/compression.md) are a compromise between performance and compression quality. The fastest is `lz4`, the slower and the more compact ones range from `brotli_1` to `brotli_11` (the larger the number, the better the compression). You have to remember that there are two algorithms:
   - The one used to store the table (`yt get //path/to/table/@compression_codec`). To change this one, you need to set the `@compression_codec` attribute and re-compress all the table chunks (see [commands](../../../user-guide/storage/compression.md#set_compression)).
   - The one used for network transmission. It depends on the client used.
      * In C++ API, you need to set the `TConfig::Get()->AcceptEncoding` global configuration option. The algorithm names from the [section](../../../user-guide/storage/compression.md#compression_codecs) must be prefixed with `"z-"`, such as `"z-snappy"`.
      * In Python API, you need to set the `yt.wrapper.config["proxy"]["accept_encoding"]` option. The supported algorithms are `gzip`, `br` (brotli), and `identity`.
1. **Parallelism**. If you have reached the limit of disk read performance or node format/compression, parallel reads may help.
   - In Python API, use the `yt.wrapper.config["read_parallel"]` config section that includes `"enable"`, `"data_size_per_thread"`, and `"max_thread_count"`.
   - In C++ API, you need to use the `CreateParallelTableReader` function from a special library.

1. **Network**. If data is hosted in one data center, and data are read from a different one, you might discover that bandwidth is the bottleneck.
1. [**Format**](../../../user-guide/storage/formats.md) The fastest is skiff, followed by Protobuf and YSON.

1. **Data proxy load**. The proxies are divided into groups (roles), and by default, everyone uses the `data` role; that is why, such proxies carry a higher load.
1. **Work on the cluster**. When cluster nodes are updating, for instance, disks may carry a higher load.

## Where do I start?
1. Increase parallelism (see above). This may solve most of your reading problems. If this does not help or does not help enough, you need to do further troubleshooting.
1. Look CPU load up in the [profiler](https://en.wikipedia.org/wiki/Perf_(Linux)) or [top-e](https://en.wikipedia.org/wiki/Top). If you can see activities related to reading, the possible reasons are:
   - HTTP codec. Python API, for instance, uses gzip by default, and that may prove to be the bottleneck because of compression speed. In the latter case, top-e may show high CPU load from the python process. You can try replacing it with `identity`.
   - Format. Such as parsing YSON and distributing it to objects in memory.
1. As an experiment, try moving some of the data to an SSD (see above) and reading from there. This will help you ascertain whether node disk limitations are your problem.
1. If the above steps failed to increase performance, please contact the system administrator. If the problem is associated with system capability limitations, write to the  {{product-name}}  mailbox describing what you have done and the outcomes that you obtained and would like to obtain.
