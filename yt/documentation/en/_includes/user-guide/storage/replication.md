# Replication and erasure coding

This section provides information on replication and erasure coding as data storage redundancy mechanisms. These mechanisms protect distributed systems from data loss in the event of a hardware fault.

[Replication](https://en.wikipedia.org/wiki/Replication_(computing)) requires a lot of disk space but not a lot of computational power (CPU).

[Erasure coding](https://en.wikipedia.org/wiki/Erasure_code) requires a lot of computational power and less disk space.

The `replication_factor` attribute defines the replication factor while the `erasure_codec` attribute controls the type of erasure codec used.

## Replication { #replication }

For replication, the `erasure_codec` value is equal to `none` while the `replication_factor` determines the number of chunk replicas. To provide **rack awareness**, each replica occupies a separate **rack**.

Chunks remain available for reading even if all replicas but one are lost. In a typical case, the recommended `replication_factor` value is equal to 3 since data loss will be frequent if it is less.

To change the replication factor, set the `replication_factor` attribute. The system will automatically create the required additional number of chunks or delete those no longer needed.

CLI
```bash
yt set //home/your_new_file/@replication_factor 6
```

## Erasure coding { #erasure }

Erasure coding is a data storage method that saves space as compared to replication. Erasure coding also helps store data more reliably since it can survive the loss of any three cluster nodes whereas triple replication can only survive the loss of any two.

{% note warning "Attention!" %}

- We strongly advise against using erasure coding to store foreign tables for `joint reduce` and `reduce with foreign tables` operations if these tables are much smaller than the primary ones. While these operations are running, jobs will overload cluster nodes hosting the foreign table chunks with requests. This is less of a problem with simple `reduce` that has many more running jobs than input table chunks.
- For latency-sensitive processes, it may not be possible to use erasure coding.
- There is no point in using erasure coding for small amounts of data since there will be no significant gain.

{% endnote %}

The `replication_factor` attribute does not affect erasure chunks. A single copy of the data is stored.
There might be situations when a table stores both erasure and regular chunks. These tables may result from `merge` operations.

The `erasure_codec` shows the type of erasure codec used. The types listed in the table are supported.
If you edit the attribute, the method of writing new data will change. At the same time, old data will remain unchanged.


| Codec | Algorithm | Description | Size on disk as compared to compressed data. | Recovery cost. |
|-------|----------|----------|------------------------|--------------------------|
| `reed_solomon_6_3` | Uses the [Reed-Solomon codes](https://en.wikipedia.org/wiki/Reed-Solomon_error_correction). | When writing an erasure chunk, data are split into 6 data parts. Three control parts are generated for recovery. | 1.5x | The recovery cost is high in CPU resources and time. |
| `lrc_12_2_2` (recommended) INTERNAL DOCUMENTATION | Uses [Local Reconstruction Codes (LRC)](https://www.microsoft.com/en-us/research/publication/erasure-coding-in-windows-azure-storage/?from=http%3A%2F%2Fresearch.microsoft.com%2Fpubs%2F179583%2Flrc12-cheng%2520webpage.pdf) that are a variation on the Reed-Solomon codes. | When writing an erasure chunk, data are split into 12 data parts. Two xor parts are computed: for the first 6 parts containing data, and for the other 6 parts. Two further control parts are computed for recovery using an algorithm similar to Reed-Solomon (erasure parts). | 1.33x | The recovery cost is moderate in CPU resources and time. |

In both cases, the system is able to survive the loss of any three cluster nodes.
The system attempts to write each chunk part to a separate **rack** to provide **rack awareness**.

Erasure coding has the following weaknesses:

- Even after the loss of a single cluster node, the system will have to start background recovery to maintain failure tolerance. A different number of parts are required for recovery. Thus, `reed_solomon_6_3` requires 6 separate parts. The recovery process is automatic but needs CPU and takes time: between 10 and 60 minutes.
- Reading may be slow since the data has one physical replica. In fact, a read has nothing to choose from, and if the only cluster node hosting the data part being read is very busy for whatever reason, then reading will be slow. If a part cannot be read for a long time, the system will recover this part on the client (**read recovery**), which will require extra time and computational power. When using replication, the system selects the least busy replica for reading.
- A data write is an operation that has a high cost in terms of RAM usage. For reference, by default, a table write that uses replication requires 100 MB of RAM while an erasure coding table write needs 1500 MB.

### Using erasure coding { #erasure_usage }

To begin taking advantage of erasure coding, use the `transform` command.
This command is a client-side wrapper for `merge`. The command automatically selects `data_size_per_job` as well as other parameters and starts a `merge`.

{% list tabs %}

- CLI
   ```bash
   yt transform --src //path/to/table --dst //path/to/table --erasure-codec lrc_12_2_2 --compression-codec brotli_6
   ```

- Python
   ```python
   import yt.wrapper as yt
   yt.transform(src, dst, erasure_codec=erasure_codec, ...)
   ```

{% endlist %}

To force the operation, run `merge` manually with `force_transform=%true` after configuring the required codes for the output table.
`N` is specified in bytes. You must select its size to have the compressed chunk take up over 500 MB.

CLI
```bash
yt set //path/to/table/@erasure_codec lrc_12_2_2
yt set //path/to/table/@compression_codec brotli_6
yt merge --src //path/to/table --dst //path/to/table --spec '{force_transform = %true;data_size_per_job=N}
```

Data encoding settings are contained in the table attributes. To view them, run the command below:

CLI
```bash
yt get //path/to/table/@erasure_codec
yt get //path/to/table/@compression_codec
```

To find out the table's data storage format, run the commands below:

CLI
```bash
yt get //path/to/table/@erasure_statistics
yt get //path/to/table/@compression_statistics
```
