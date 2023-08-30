# Job stats

This section provides a list and descriptions of statistics collected by the {{product-name}} system from operations and individual jobs. There are examples of issue troubleshooting based on the statistics collected.

## Overview

While operations are running, {{product-name}} collects various system and user statistics from jobs.

Individual job statistics are entered into the scheduler log while aggregated statistics for an operation are available through the web interface by going to the **Statistics** tab on the operation page. The figure shows an example of a page with operation job statistics:

![](../../../../images/jobstatistics.png)

{% note info "Note" %}

For a better understanding of job stats, you should be familiar with the notions of `job proxy` and `sandbox`, and have an idea of the way the {{product-name}} system delivers user files to jobs. You can find out about these things in the Jobs section.

{% endnote %}

## Statistic descriptions

### System stats { #system_stats }

| Statistic | Description |
| --- | --- |
| `time/total` | Job running time from the moment it is created by the scheduler until the moment the scheduler is notified that the job is completed (successfully or otherwise). Milliseconds. |
| `time/prepare` | Job set up time until a `job proxy` is started (files are uploaded into the chunk cache as required, a `sandbox` directory is created, files are created and copied to `tmpfs`, and requisite `cgroups` are set up). Milliseconds. |
| `time/artifact_download` | Time to upload files to the chunk cache (as required). Included in `time/prepare`. Milliseconds. |
| `time/prepare_root_fs` | Time to prepare the file system in the porto container (as required). Included in `time/prepare`. Milliseconds. |
| `time/gpu_check` | Time to run for the utility pre-checking node GPU functionality (relevant for some GPU operations). Included in `time/prepare`. Milliseconds. |
| `time/exec` | Job running time from start to `job proxy` process exit. Milliseconds. |
| `data/input/chunk_count` | Total number of data slices read by the job. A data slice is a continuous segment of a single chunk (for static tables) or a continuous range of rows between two keys (for dynamic tables). Pieces. |
| `data/input/row_count` | Total number of table rows read by a job. Pieces. |
| `data/input/compressed_data_size` | Total compressed block data read by a job (table data fed to operation input). Bytes. |
| `data/input/uncompressed_data_size` | Total uncompressed block data read by a job. Bytes. |
| `data/input/data_weight` | "Logical" amount of uncompressed data read by a job. Depends only on the values in the table cells and the number of rows. Not dependent on whether a table has a schema or the values of `optimize_for`, `compression_codec`, or `erasure_codec`. Calculated as `row_count + sum(data_weight(value))` for all values in the table cells. Bytes. |
| `data/input/not_fully_consumed` | 1 if a job has not read the entire input, 0 otherwise. |
| `data/output/K/chunk_count` | Number of chunks written to output table with index K. Pieces. |
| `data/output/K/row_count` | Number of rows written to output table with index K. In units. |
| `data/output/K/compressed_data_size` | Compressed block data written to output table K. In bytes. |
| `data/output/K/uncompressed_data_size` | Uncompressed block data written to output table K. In bytes. |
| `data/output/K/data_weight` | "Logical" uncompressed data amount written to output table K. Dependent only on the values in the table cells and the number of rows. Not dependent on whether a table has a schema or the values of `optimize_for`, `compression_codec`, or `erasure_codec`. Calculated as `row_count + sum(data_weight(value))` for all values in the table cells. Bytes. |
| `data/output/K/regular_disk_space` | Only populated for tables with `erasure_codec == none` and equal to `compressed_data_size` + metadata chunk size. Bytes. |
| `data/output/K/erasure_disk_space` | Populated only for tables with `erasure_codec != none`. In addition to `compressed_data_size`, includes the amount of data in the [parity](*parity) blocks. For more information about erasure codecs, see the [section](../../../user-guide/storage/replication.md). Bytes. |
| `job_proxy/cpu/user` | User mode CPU time of the `job proxy` process. Computed based on the `cpu_usage` value of the porto container. Milliseconds. |
| `job_proxy/cpu/system` | Kernel mode CPU time of the `job proxy` process. Computed based on the `cpu_usage_system` value of the porto container. Milliseconds. |
| `job_proxy/cpu/wait` | Wait CPU time of the `job proxy` process. Computed based on the `cpu_wait` value of the porto container. Milliseconds. |
| `job_proxy/cpu/throttled` | Throttled CPU time of the `job proxy` process. Computed based on the `cpu_throttled` value of the porto container. Milliseconds. |
| `job_proxy/block_io/bytes_written` | Number of bytes written by the `job proxy` process to the local block device. The value is derived from the `Write` section of `blkio.io_serviced_bytes` for the relevant cgroup. Normally, a small value because `job proxy` only writes logs to the local disk. Chunks are written via (local and/or remote) nodes and do not count towards this statistic. |
| `job_proxy/block_io/bytes_read` | Number of bytes read by the `job proxy` process from the local block device. The value is derived from the `Read` section of `blkio.io_serviced_bytes` for the relevant cgroup. In a typical case approaches zero because `job proxy` only reads its own configuration from disk. |
| `job_proxy/block_io/io_read` | Number of reads from the local block device by the `job proxy` process. The value is derived from the `Read` section of `blkio.io_serviced` for the relevant cgroup. Pieces. |
| `job_proxy/block_io/io_write` | Number of writes to the local block device by the `job proxy` process. The value is derived from the `Write` section of `blkio.io_serviced` for the relevant cgroup. Pieces. |
| `job_proxy/block_io/io_total` | Total number of input/output transactions on the local block device by the `job proxy` process. Pieces. |
| `job_proxy/max_memory` | Maximum amount of RAM used by the `job proxy` process while a job is running. Bytes. |
| `job_proxy/memory_reserve` | Amount of RAM guaranteed to the `job proxy` at launch. If actual usage exceeds `memory_reserve`, the job may be aborted with a `resource_overdraft` message. Bytes. |
| `job_proxy/traffic` | Amount of data transmitted over the network to/from the `job proxy`. Normally, matches the amount of job input/output data if it only reads and writes tables/files as a job usually does and does not generate any other traffic on its own. Contains fields looking like `A_to_B` where `A` and `B` are data center short names. The value in this field is equal to the amount of data transmitted in this direction. You will find the time it took to transmit the data in the `duration_ms` field. Normally matches the time the `job proxy` has been in existence. In addition, incoming/outgoing traffic is also tracked (`inbound` and `outbound` fields, respectively), itemized by data center as well. |
| `exec_agent/traffic` | Same as `job_proxy/traffic` but for an exec agent. Normally displays the traffic generated during job artifact setup. |
| `user_job/cpu/*` | Similar to `job_proxy/cpu/*` but applies to the user job processes. |
| `user_job/block_io/bytes_read`, `user_job/block_io/bytes_written`, `user_job/block_io/io_read`, `user_job/block_io/io_write`, `user_job/block_io/io_total` | Block IO statistics for a user job. Similar to `job_proxy/block_io/*`. |
| `user_job/cumulative_memory_mb_sec` | Integral of memory used, MB*s |
| `user_job/current_memory/major_page_faults` | Number of [major page faults](https://en.wikipedia.org/wiki/Page_fault#Major) in a user process. Value |
| Derived from the `pgmajfault` section of `memory.stat` from cgroup `memory` May assist in investigating the elevated `user_job/block_io/read` statistic. Pieces. |
| `user_job/current_memory/rss` | RSS size at job end. The value is derived from the `rss` section of `memory.stat` for the relevant cgroup. Bytes. |
| `user_job/current_memory/mapped_file` | Memory mapped files size at job end. The value is derived from the `mapped_file` section of `memory.stat` for the relevant cgroup. Bytes. |
| `user_job/tmpfs_size` | `tmpfs` currently being **used** by the running job. Computed as the difference between the values of `total` and `free` returned by a call to `statfs` on tmpfs mount point. Bytes. |
| `user_job/max_tmpfs_size` | Maximum `tmpfs` amount **used** throughout the job running time. Bytes. |
| `user_job/tmpfs_volumes/K/size`, `user_job/tmpfs_volumes/K/max_size` | Same as `user_job/tmpfs_size` and `user_job/max_tmpfs_size` for each requested tmpfs volume individually. |
| `user_job/disk/usage` | Disk space being taken up by a job sandbox; computed through a recursive sandbox walk by adding up all the file sizes. Bytes. |
| `user_job/disk/limit` | Requested limit on job sandbox size. Bytes. |
| `user_job/max_memory` | Maximum amount of RAM taken up by a user job while running, **net** of `tmpfs`. Bytes. |
| `user_job/memory_limit` | Limitation on RAM amount set in the operation spec at operation start. Duplicated in the stats for housekeeping reasons. Bytes. |
| `user_job/memory_reserve` | Amount of RAM guaranteed for a user job at time of launch. When actual usage becomes greater (but less than `memory_limit`), a job may be aborted with a `resource_overdraft` message if a cluster node is short on memory. You can control this value using an [option](../../../user-guide/data-processing/operations/operations-options.md) called `memory_reserve_factor` in the operation spec. Bytes. |
| `user_job/pipes/input/bytes` | Number of bytes transmitted via the user process stdin (that is, the amount of input data converted to the specified input format with all the control modifiers like `table_index`, `row_index`, and so on). |
| `user_job/pipes/input/idle_time` | Amount of time during which the `job proxy` transmitted no data to a user job via stdin because it was reading data. For instance, a data cluster node disk was busy, and the data were unavailable, or the `compression_codec` used was slow to uncompress. Milliseconds. |
| `user_job/pipes/input/busy_time` | The amount of time during which the `job proxy` process was writing data to a user job stdin. The value can be large if the user code implements computationally intensive processing and does not have time to read data. In addition, slow reads could be the reason when user code hangs up on a write not reading new data as a consequence. Milliseconds. |
| `user_job/pipes/output/K/bytes` | Number of bytes written by a user process to the descriptor corresponding to the Kth output table. For information on descriptors and their numbering, please see the dedicated section. |
| `user_job/pipes/output/K/idle_time` | Time during which the `job proxy` process did not read the stream corresponding to the Kth output table because it was writing data already read from that stream. For example, the cluster node being written to was slow to respond, or a very slow compression algorithm is being used. Milliseconds. |
| `user_job/pipes/output/K/busy_time` | Time during which the `job proxy` process was reading from the stream corresponding to the Kth output table. If this time value is large, the user code has not written anything to the stream for a long time. For instance, because it took a long time to process the input, or because the input was unavailable. Milliseconds. |
| `user_job/gpu/cumulative_utilization_gpu` | Net amount of time during which there were running GPU computations. Added up across all the GPUs used by a job. Milliseconds. |
| `user_job/gpu/cumulative_utilization_memory` | Net time spent accessing GPU memory. Added up across all the GPUs used by a job. Milliseconds. |
| `user_job/gpu/cumulative_utilization_clocks_sm` | An integral of the board frequency over time with respect to the maximum frequency. Added up across all the GPUs used by a job. Milliseconds * share. |
| `user_job/gpu/cumulative_utilization_power` | Integral of effective GPU board power over time with respect to the maximum power. Added up across all the GPUs used by a job. Milliseconds * share. |
| `user_job/gpu/cumulative_load` | Time during which GPU was at non-zero load. Added up across all the GPUs used by a job. Milliseconds. |
| `user_job/gpu/cumulative_memory` | Integral of GPU memory utilization. Added up across all the GPUs used by a job. Milliseconds * bytes. |
| `user_job/gpu/cumulative_power` | Integral of utilized GPU power. Added up across all the GPUs used by a job. Milliseconds * power. |
| `user_job/gpu/cumulative_clocks_sm` | Integral of utilized GPU frequency. Added up across all the GPUs used by a job. Milliseconds * frequency. |
| `user_job/gpu/max_memory_used` | Maximum recorded GPU memory utilization. Added up across all the GPUs used by a job. Bytes. |
| `user_job/gpu/memory_total` | Total available GPU memory. Added up across all the GPUs used by a job. Bytes. |
| `codec/cpu/decode` | Wall time spent uncompressing data. Milliseconds. |
| `codec/cpu/encode` | Wall time spent compressing data. Milliseconds. |
| `job_proxy/memory_reserve_factor_x10000`, `user_job/memory_reserve_factor_x10000` | Housekeeping parameters used to analyze the operation of the `memory_reserve` computational algorithm. |
| `job_proxy/aggregated_max_cpu_usage_x100`, `job_proxy/aggregated_smoothed_cpu_usage_x100`, `job_proxy/aggregated_preemptible_cpu_x100`, `job_proxy/aggregated_preempted_cpu_x100`, `job_proxy/preemptible_cpu_x100`, `job_proxy/smoothed_cpu_usage_x100` | Job CPU monitor housekeeping statistics. |
| `chunk_reader_statistics/data_bytes_read_from_disk` | Amount of data retrieved from disk during chunk reads. Bytes. |
| `chunk_reader_statistics/data_bytes_transmitted` | Amount of data transmitted over the network during chunk reads. Bytes. |
| `chunk_reader_statistics/data_bytes_read_from_cache` | Amount of data retrieved from cache while reading chunks. Bytes. |
| `chunk_reader_statistics/meta_bytes_read_from_disk` | Amount of metadata retrieved from disk during chunk reads. Bytes. |
| `chunk_reader_statistics/wait_time` | Time spent waiting for data during chunk reads. Milliseconds. |
| `chunk_reader_statistics/read_time` | Time spent actively reading chunks, such as parsing strings from read blocks. Milliseconds. |
| `chunk_reader_statistics/idle_time` | Time during which chunk reads were aborted for the processing of previously read data. Milliseconds. |

### Python API

| Statistic | Description |
| --- | --- |
| `custom/python_job_preparation_time` | Time between program main entry and the start of input row processing. |

## Troubleshooting examples

### Slow jobs

The most common use of statistics is to troubleshoot slow jobs.

{% note info "Note" %}

Job statistics are to be used alongside other troubleshooting methods available in the {{product-name}} system.

{% endnote %}

#### Slow user code

Simple case: if `user_job/cpu/user ~= time/exec`, user code is causing the job to run slowly. You should connect to the job via the [job shell](../../../user-guide/problems/jobshell-and-slowjobs.md) and look for the bottleneck using perf and gdb.

#### Multithreading

A more complex case: a user has analyzed the stats and retrieved the total utilized CPU time:
```
user_job/cpu/user + user_job/cpu/system+job_proxy/cpu/user+job_proxy/cpu/system = 303162 milliseconds
time/exec = 309955 milliseconds
```

The user is expecting the numbers to match.

The equality above is not totally proper since `time/exec` is wall time while `user_job/cpu/*` and `job_proxy/cpu/*` are CPU times spent computing. At the same time, both the `job proxy` and (potentially) the user process may use more than a single core because of multithreading, and on top of that, both the processes are running concurrently.

If the user process is single-threaded, the following should be approximately true:
```
time/exec ~= user_job/cpu/* + user_job/pipes/input/idle_time + max(user_job/pipes/output/N/idle_time)
```
The above computation does not include the time to "close" output chunks: when the output stream is already closed, all its data read but the `job proxy` is still finalizing chunks.

#### Long compression

If an operation is taking much longer to run than user code running time, and an execution time measurement showed that the {{product-name}} system is spending most of its time processing a `yield`, the problem is that inside `yield` the python wrapper is writing data to the output stream. If the `job proxy` write pipeline is overloaded, the `job proxy` process stops reading data and a `yield` becomes blocked. `user_job/pipes/output/N/idle_time` statistics showed "lost" minutes. There are two possible reasons why a `job proxy` process failed to read data: the `job proxy` took a long time processing data on its side or there was a cluster write. The `job_proxy/cpu/user` stats make it clear that 70% of the total job time, the `job proxy` process was keeping the CPU busy, which means that processing input data took a long time. The heaviest part of `job proxy` processing is compression, which is what the profiler indicated in the end.

{% note info "Note" %}

General rule for slow job troubleshooting: when analyzing running time, you need to find dominant metrics, then look for a cause of the problem in the identified component.

{% endnote %}

### Exceeding memory guarantees

Operations exceed memory limitations. Steps to resolve this issue:

1. When requesting tmpfs, you need to make sure that you are not ordering too much. To make sure, you need to look at the `user_job/tmpfs_size` statistics.

2. When selecting `memory_limit`, you need to proceed from the `user_job/max_memory` metric; if you are using `tmpfs`, you must add the required `tmpfs` amount to `user_job/max_memory`. If you use automatic `tmpfs` ordering via the C++ and the Python APIs, the APIs will make the addition on their own.

3. Most jobs do not use a lot of memory but certain jobs require significantly more memory, and you should set `memory_limit` to the maximum job memory requirement but set `memory_reserve_factor` lower (at the same time, you should be ready that jobs will be aborted for `resource_overdraft`). You can make the relevant decision by comparing the average and the maximum values of the `user_job/max_memory` statistic (the web interface includes appropriate switches): if average and maximum memory usage in a job differs by several dozen percent or by orders of magnitude, this is an indication that there are jobs whose usage is inordinately high.

4. Reverse situation: if too many jobs are being aborted for `resource_overdraft`, you should increase `memory_reserve_factor`.

### Operation `excessive io` message { #excessive_io }

You must review the `user_job/block_io/io_read` statistic. If a job makes tens of thousands of reads, it is harming both itself and other users.
You should check the `major page faults` counter. A large value is an indication of possible memory mapping use.

{% note warning "Attention!" %}

The use of **memory mapping** in jobs is **strongly discouraged** while jobs doing this create heavy disk load with random reads and writes. Please remember that a conventional hard drive is capable of no more than 100 random access operations per second while there are several dozen jobs running concurrently on a cluster node.

{% endnote %}

Solution for this issue: reducing executable size (by removing the debugging symbols, for instance), ordering tmpfs for the job, and uploading large files into memory at job launch (`copy_files` option). For more information on these options, please see [Operation settings](../../../user-guide/data-processing/operations/operations-options.md).

## Supplemental

{{product-name}} is unable to measure phase time inside a user process. For example, if user code uploads a large dictionary into memory at launch, you will not be able to see the time it took to load this dictionary in the system metrics. If this happens, you should take advantage of user statistics.

You can use user statistics to compute simple aggregates even in a map operation. The number of user statistics is limited to 128 per job.

You can build different plots from job metrics (in particular, visualize operation phases) to simplify troubleshooting.

[*parity]: Additional blocks for redundancy and original block recovery in the event of a loss of any part. Computed using a logical XOR.