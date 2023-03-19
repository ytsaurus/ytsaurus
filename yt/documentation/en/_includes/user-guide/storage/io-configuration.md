# Input/output settings

This section lists operation input/output configuration options as well as table read and write command options.

{% note info "Note" %}

This section considers only important options that might help a user. In actuality, the options are much more numerous and provide fairly fine control of the various data read/write parameters. More often that not, this type of configuration is not required, and using non-default values might actually do harm.

{% endnote %}

You can control reading and writing via [table data formats](../../../user-guide/storage/formats.md), `TableReader/TableWriter`, and `ControlAttributes`.

You control data reads via settings in two sections: `table_reader` and `control_attributes`.
You can control data writes in the `table_writer` section.

## TableReader section { #table_reader }

These settings can be specified when reading tables using the `table_reader` option or in the `table_reader` section under `job_io` in the operation specification.

### Sampling { #sampling }

Under `table_reader`, you can specify sampling settings `sampling_seed` and `sampling_rate`:

```python
...
spec = {"job_io": {"table_reader": {"sampling_seed": 42, "sampling_rate": 0.3}}}
yt.run_map(map_function, input, output, spec=spec)
...
yt.read_table(table_path, table_reader={"sampling_seed": 42, "sampling_rate": 0.3})
...
```

The value of `"sampling_rate": 0.3` will result in the operation (`map_function`) receiving 30% of all the input table rows as input.
The `sampling_seed` option controls a random number generator used to sample rows. It guarantees the same output for the same `sampling_seed`, a pure `map_function`, and the same collection of input chunks. If `sampling_seed` is not included in the specification, it will be random.

When sampling is used all the data are read from disk, the specified probability notwithstanding.
Nonetheless, this could be less costly than independent data sampling since the sampling itself will occur system-side immediately following the disk read.
To obtain maximum sampling read performance in an operation at a reduced sampling quality, use the [`sampling`](#sampling) option in the operation specification.

### Buffer size for data fed to a job { #window_size }

You control the job data feed buffer via the `window_size` parameter expressed in bytes. The default value is 20 MB.

Configuring the job feed data buffer:

```python
...
spec = {"job_io": {"table_reader": {"window_size": 20971520}}}
yt.run_map(map_function, input, output, spec=spec)
...
yt.read_table(table_path, table_reader={"window_size": 20971520})
...
```

## TableWriter section { #table_writer }

These settings can be included in the `table_writer` section when writing table data or in the `table_writer` section under operation `job_io` settings.

### Limit on table row size { #max_row_weight }

When writing table data, the {{product-name}} system checks their size. If this size is over a certain limit, writing stops and the relevant job or `write` command fails.

The default string size cap is 16 MB; however, it can be configured under `table_writer`.

Updating the table row size constraint:

```python
...
spec = {"job_io": {"table_writer": {"max_row_weight": 32 * 1024 * 1024}}}
yt.run_map(map_function, input, output, spec=spec)
...
yt.write_table(table_path, table_writer={"max_row_weight": 32 * 1024 * 1024})
...
```

`max_row_weight` is specified in bytes. It cannot exceed 128 MB.

`table_writer` controls the various storage settings at table creation time or when calling `merge`.

### Chunk size { #desired_chunk_size }

The `desired_chunk_size` specification setting defines the desired chunk size in bytes.
To modify a table to achieve a chunk size close to 1 KB, you can call the merge command.

Modifying chunk size:

```bash
yt merge --mode auto --spec '{"force_transform"=true; "job_io"={"table_writer"={"desired_chunk_size"=1024}}}' --src <> --dst <>
```

Data fed to a job cannot be smaller than block size. Block size is defined by `block_size` in bytes. The minimum value is 1 KB, and the default is 16 MB.

### Replication factor { #upload_replication_factor }

The `upload_replication_factor` specification setting defines the number of table or file chunk replicas. The default is 2 and the maximum is 10.
To modify the number of replicas, simply call the merge command.

Modifying the replication factor:

```bash
yt merge --mode auto --spec '{"force_transform"=true; "job_io"={"table_writer"={"upload_replication_factor"=10}}}' --src <> --dst <>
```

For the modification to take place in the background at a later time, call `set` on the table `replication_factor`. The same is true for a table write with a pre-defined parameter.

### Sampling rate { #sample_rate }

The `sample_rate` specification setting defines the percentage of the rows to be selected and placed in chunk metadata.
This row selection affects the construction of buckets for the sort's `partition` stage. Increasing the `sample_rate` value helps the system compute a key distribution more precisely but increases system load by increasing the amount of metadata and reducing read and write performance.
To avoid skewed bucket sizes when putting values in a single partition for a column with few values, increase `sample_rate` prior to the `sort` or the `reduce stage`.
The default value is 1/10,000.

If a table has, say, 100,000 rows, the sample will include 10. Consequently, the sort will have no more than 10 partitions.

To obtain a sample of 0.1% of the table (maximum sample size), run a merge.

Modifying the sampling rate:

```bash
yt merge --mode auto --spec '{"force_transform"=true; "job_io"={"table_writer"={"sample_rate"=0.001}}}' --src <> --dst <>
```

## ControlAttributes section { #control_attributes }

When reading several tables or several ranges of the same table, it is useful to have housekeeping information to provide insight which table or read range data is coming from.
To support this information in jobs or when reading tables, we introduced the notion of system management (control) entries containing such information.
This concept is internal, and its representation in a job's input data stream depends on the requested format.

Let's look at control attributes using the YSON format. For this format, the stream might include special records of the Yson Entity type rather than regular data records that are of the Yson Map type. Their attributes contain housekeeping information.
For instance, if record `<table_index=2>#` comes in, it would mean that all subsequent records in the stream will be from the second input table until a new control record is received with a different `table_index`.

The `control_attributes` section supports the following options:

The values are specified in parentheses by default.
- **`enable_table_index`** (`false`): send control records with an input table index (`table_index` field of type `int64` in the control record attributes).
- **`enable_row_index`** (`false`): send control records with an input table record index (`row_index` field of type `int64` in the control record attributes).
- **`enable_range_index`** (`false`): send control records with a range number from among the requested input table ranges. Please see ranges (`range_index` field of type `int64` in the control record attributes).
- **`enable_key_switch`** (`false`): send records on input data key switch (`key_switch` field of type `bool` in the control record attributes). The setting is only valid for the `reduce` phase of the Map-Reduce and Reduce operations.

{% note info "Note" %}

The processing of control attributes through the Python API and the C++ API is transparent for the user. That is to say that their analysis and conversion to a more convenient representation are handled by the API.

{% endnote %}

Options that control the transmission of table indexes in the input stream are available at different system layers.
The `enable_input_table_index` option is found in the operation spec section describing the user process.
The value of this option overwrites the value in `enable_table_index` under `control_attributes`.

The `enable_table_index` option enables and disables `table index` at the format level.
Many formats have these indexes disabled by default; therefore, they must be enabled. Many formats have options responsible for representing `table_index` in this one. For more information on `table_index` in various formats, see Table switching. todo

The `row_index`, `range_index`, and `key_switch`representations are only supported by the YSON and the JSON formats.

Input data stream showing all the control attributes in a YSON format reduce job:

```json
<"table_index"=0;>#;
<"range_index"=0;>#;
<"row_index"=2;>#;
{"a"="2";};
<"key_switch"=%true;>#;
{"a"="3";};
<"key_switch"=%true;>#;
<"row_index"=0;>#;
{"a"="1";};
```

Enabling control attributes for reading in an operation:

{% list tabs %}

- CLI

   ```bash
   yt read --control-attributes="{enable_row_index=true;}" "//path/to/table[#10:#20]"
   {"$value": null, "$attributes": {"row_index": 10}}
   ...
   ```

- Python

   ```python
   ...
   spec = {"job_io": {"control_attributes": {"enable_row_index": True}}}
   yt.run_map(map_function, input, output, spec=spec) # will place "@row_index" key to input records
   ...
   yt.read_table(path, control_attributes={"enable_row_index": True}, format=yt.YsonFormat())
   ...
   ```

{% endlist %}