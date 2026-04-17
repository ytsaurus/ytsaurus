# RemoteCopy

The RemoteCopy operation copies tables and files from one cluster to another. The RemoteCopy operation's jobs are run on the destination cluster and copy chunks from the source cluster ("pull"-schema). The operation copies the chunks in their original form, without uncompressing them or changing the `erasure` schema. If the input is a table and it is sorted, the output table will also be sorted.

General parameters for all operation types are described in [Operation options](../../../../user-guide/data-processing/operations/operations-options.md).

The RemoteCopy operation supports the following additional options (default values, if set, are specified in brackets):

* `cluster_name` — name of the cluster from which the data is copied. A description of the available clusters is available on the `//sys/clusters` node.
* `network_name` — name of the network ({{product-name}} terms) used for copying. The specified network must be configured on all nodes of the remote cluster. For example: `fastbone` or `backbone`.
* `networks` — set of networks used to copy the data (in order of priority). **Important:** Do not enable this and the previous option without first consulting with the system administrator.
* `input_table_paths` — list consisting of a single path to file or table to be copied.
* `output_table_path` — path to the table or file where the data is copied to. If this Cypress node exists, it must have the same type as the input node. It is not possible to copy table to file and vice-versa.
* `schema_inference_mode` (auto) — schema definition mode. Possible values: auto, from_input, from_output. To learn more, see [Data schema](../../../../user-guide/storage/static-schema.md#schema_inference).
* `cluster_connection` — configuration for connecting to the source cluster. Configured by the {{product-name}} system administrator when configuring the cluster.
* `copy_attributes` (false) — copy the attributes of the input table. Available only if there is one input table. Only user attributes are copied (not system attributes).
* `attribute_keys` — when this option is enabled along with `copy_attributes`, only attributes from the given list are copied.

**Important:** There is a special restriction for production clusters: remote copy operations can only be run in pools that have a limit on the number of simultaneously running jobs in one of the parents (that is, the `user_slots` value of the `resource_limits` attribute is below a certain threshold).

## Example specification

```yaml
{
  pool = "my_cool_pool";
  network_name = "fastbone";
  input_table_paths = [ "//tmp/input_table" ];
  output_table_paths = "//tmp/output_table";
  copy_attributes = %true;
  cluster_name = "my_cool_remote_cluster";
}
```

## Dynamic tables

The RemoteCopy (`remote_copy`) operation is supported only for [sorted tables](../../../../user-guide/dynamic-tables/sorted-dynamic-tables). To copy an ordered table, you need to convert it to a static one. It is impossible to copy an ordered table to another cluster with preserving tablets.

{% if audience == "internal" %}

{% note info "Note" %}

For `remote_copy` of dynamic tables, it is recommended to use the script {{dynamic-tables.remote-copy.links.dyntable-remote-copy-script}}.

{% endnote %}

{% endif %}

For dynamic tables, you need to prepare the table into which you plan to copy the data before copying. It is not possible to specify a non-existent path in `output_table_path`.

The table must be created with the same schema as the input table, all user attributes and system settings from the input table must be copied and reshard by keys in advance if necessary.

<!--During operation, system objects called chunk views may appear in the structure of a dynamic table. The `remote_copy` operation does not support them, so you need to delete them before running the operation by setting the `forced_chunk_view_compaction_revision` attribute to `1`, and then execute `remount-table`. For more details, see the section [Forced compaction](../../user-guide/dynamic-tables/compaction#forced_compaction).-->

While working with dynamic tables, you may encounter system objects known as chunk views within the table structure. Since the remote copy operation doesn't support these objects, you must remove them before starting the operation. To do this, set the `forced_chunk_view_compaction_revision` attribute to 1 and then run `remount-table`. For more details, see [Forced compaction](....user-guidedynamic-tablescompaction#forced_compaction).
