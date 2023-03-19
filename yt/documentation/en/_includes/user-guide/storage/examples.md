# Examples of Cypress object processing

This section provides examples of Cypress object processing.

## Static tables { #static_tables }

### Creating { #create_table }

Use the `create` command to create a table.

```bash
yt create table //home/dev/test_table
1282c-1ed72c-3fe0191-443cf2ee
```

### Deleting { #remove_table }

Use the `remove` command to delete a table.

```bash
yt remove //home/dev/test_table
```

### Reading { #read_table }

To read an existing table named `<table>`, use the `read-table` command.

```bash
yt read-table [--format FORMAT]
                [--table-reader TABLE_READER]
                <table>
```

The `--format` option defines the output data format. The supported formats are `json`, `yson`, `dsv`, and `schemaful_dsv`. For more information, see [Formats](../../../user-guide/storage/formats.md).

```bash
yt read-table --format dsv //home/dev/test_table
day=monday	time=10
day=wednesday	time=20
day=friday	time=30
```

The `--table-reader` option modifies the table read settings.

#### Sampling { #sampling_rate }

The `sampling_rate` attribute sets the percentage of data that must be read from an input table.

```bash
yt read-table --format dsv --table-reader '{"sampling_rate"=0.3}' //home/dev/test_table
day=monday	time=10
day=friday	time=30
```

In this example, `read-table` will return 30% of all the rows in the input table.

The `sampling_seed` controls a random number generator that selects the rows. It guarantees the same output for the same `sampling_speed` and collection of input chunks. If unspecified, the `sampling_seed` attribute will be random.

```bash
yt read-table --format dsv --table-reader '{"sampling_seed"=42;"sampling_rate"=0.3}' //home/dev/test_table
```

To place sampling output in a different table, please run `map`:

```bash
yt map cat --src //home/dev/input --dst //home/dev/output --spec '{job_io = {table_reader = {sampling_rate = 0.001}}}' --format yson
```

{% note info "Note" %}

The specified `sampling_rate` attribute notwithstanding, sampling reads all data from disk.

{% endnote %}


### Overwriting { #write_table }

The `write-table` command overwrites an existing table called `<table>` with the data transmitted.

```bash
yt write-table [--format FORMAT]
                 [--table-writer TABLE_WRITER]
                 <table>
```

The `--format` option defines the input data format. The supported formats are `json`, `yson`, `dsv`, and `schemaful_dsv`. For more information, see [Formats](../../../user-guide/storage/formats.md).

```bash
yt write-table --format dsv //home/dev/test_table
time=10	day=monday
time=20	day=wednesday
time=30 day=friday
^D
```

To add records to a table, use the `<append=true>` option before the table path.

```bash
cat test_table.json
{"time":"10","day":"monday"}
{"time":"20","day":"wednesday"}
{"time":"30","day":"friday"}
cat test_table.json | yt write-table --format json "<append=true>"//home/dev/test_table
```

The `--table-writer` option modifies the table write settings:

* [Limit on table row size.](#max_row_weight)
* [Chunk size.](#desired_chunk_size)
* [Replication factor.](#replication_factor)

##### Limit on table row size { #max_row_weight }

When writing table data, the {{product-name}} system checks its size. A write will return an error if the size is greater than the maximum legal value.
By default, maximum row size is 16 MB. To modify this value, use the `--table-writer` option's `max_row_weight` parameter. Enter a value in bytes.

```bash
cat test_table.json | yt write-table --format json --table-writer {"max_row_weight"=33554432} //home/dev/test_table
```

{% note info "Note" %}

`max_row_weight` cannot be greater than 128 MB.

{% endnote %}

#### Chunk size { #desired_chunk_size }

The `desired_chunk_size` defines chunk size in bytes.

```bash
cat test_table.json | yt write-table --format json --table-writer {"desired_chunk_size"=1024} //home/dev/test_table
```

#### Replication factor { #replication_factor }

You can control the replication factor for new table chunks through the `min_upload_replication_factor` and the `upload_replication_factor` attributes.
`upload_replication_factor` sets the number of synchronous replicas created when writing new data to a table.
`min_upload_replication_factor` sets the minimum number of successfully written chunks. Both attributes have a default value of 2 and a maximum value of 10.

```bash
cat test_table.json | yt write-table --format json --table-writer '{"upload_replication_factor"=5;"min_upload_replication_factor"=3}' //home/dev/test_table
```

To increase the number of replicas of an existing table, use one of the methods below:
- Start a Merge operation:

```bash
yt merge --mode auto --spec '{"force_transform"=true; "job_io"={"table_writer"={"upload_replication_factor"=5}}}' --src <> --dst <>
```
- Increase the table's `replication_factor` attribute. This will perform the conversion in the background at a later point in time. New records added to the table will have a replication factor set by the `upload_replication_factor` attribute. Subsequently, a background process will asynchronously replicate chunks to achieve the replication factor defined for the specific table.

```bash
yt set //home/dev/test_table/@replication_factor 5
```

### Medium { #medium }

To move a table to a different medium, change the value of the `primary_medium` attribute.
New data written to the table will be delivered directly to the new medium while old data will move in the background.
To force-move data to a new medium, start a Merge:

```bash
yt set //home/dev/test_table/@primary_medium ssd_blobs
yt merge --mode auto --spec '{"force_transform"=true;}' --src //home/dev/test_table --dst //home/dev/test_table
```

To check whether a table's medium has changed, run the command below:

```bash
yt get //home/dev/test_table/@resource_usage
```
```
{
    "tablet_count" = 0;
    "disk_space_per_medium" = {
        "ssd_blobs" = 930;
    };
    "tablet_static_memory" = 0;
    "disk_space" = 930;
    "node_count" = 1;
    "chunk_count" = 1;
}
```
The amount is shown in bytes.

