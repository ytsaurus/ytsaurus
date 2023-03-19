# RemoteCopy

The RemoteCopy operation copies tables from one cluster to another. The RemoteCopy operation's jobs are run on the destination cluster and copy chunks from the source cluster ("pull"). The operation copies the chunks in their original form, without uncompressing them or changing the `erasure` schema. If there is one input table and it is sorted, the output table will also be sorted.

General parameters for all operation types are described in [Operation options](operations-options.md).

The RemoteCopy operation supports the following additional options (default values, if set, are specified in brackets):

* `cluster_name` — name of the cluster from which the data is copied. A description of the available clusters is available on the `//sys/clusters` node.
* `network_name` — name of the network ({{product-name}} terms) used for copying. The specified network must be configured on all nodes of the remote cluster. For example: `fastbone` or `backbone`.
* `networks` — set of networks used to copy the data (in order of priority). **Important:** Do not enable this and the previous option without first consulting with the system administrator.
* `input_table_paths` — list of tables to be copied.
* `output_table_path` — name of the table where the data is copied to.
* `schema_inference_mode` (auto) — schema definition mode. Possible values: auto, from_input, from_output. To learn more, see [Data schema](../../../user-guide/storage/static-schema.md#schema_inference).
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
}
```
