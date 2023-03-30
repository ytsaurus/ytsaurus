# Erase

The Erase operation deletes the specified row range from the table. If the range is not specified for the input table, all data is deleted from the table (the table itself remains).

General parameters for all operation types are described in [Operation options](../../../../user-guide/data-processing/operations/operations-options.md).

The Erase operation supports the following options (if set, default values are specified in brackets):

* `table_path` — path to the input table. Enables specifying row ranges on the input table, such as `//path/table[#10:#100]`. You can delete only a range of rows as opposed to all the data in the table.
* `combine_chunks` (false) — combine the remaining chunks in the table so that they are large enough.
* `schema_inference_mode` (auto) — schema definition mode. Possible values: auto, from_input, from_output. To learn more, see [Data schema](../../../../user-guide/storage/static-schema.md#schema_inference).

## Example specification

```yaml
{
  table_path = "//tmp/input_table[#10:#100]";
  combine_chunks = %true;
}
```

