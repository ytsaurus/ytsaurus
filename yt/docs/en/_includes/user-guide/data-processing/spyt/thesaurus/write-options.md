
# Write options

## sorted_by

A sort using a column prefix:

```python
df.write.sorted_by("uuid").yt("//sys/spark/examples/test_data")
```

## unique_keys

Uniqueness of a key in a table:

```python
df.write.sorted_by("uuid").unique_keys.yt("//sys/spark/examples/test_data")
```

## optimize_for

A table may be stored in row (lookup) or column (scan) format. The preferred format is selected based on the task:

```python
spark.write.optimize_for("scan").yt("//sys/spark/examples/test_data")
spark.write.optimize_for("lookup").yt("//sys/spark/examples/test_data")
```

## Schema v3

Write tables with schema in [type_v3](../../../../../user-guide/storage/data-types.md) instead of type_v1. It can be enabled via [Spark configuration](../../../../../user-guide/data-processing/spyt/cluster/configuration.md) or write option.

Python example:
```python
df.write.option("write_type_v3", "true")
```

## security_tags

For batch writes to static tables, SPYT automatically propagates the union of `security_tags` from the query's static input tables in {{product-name}}. This applies to DataFrame and Spark SQL writes, including cached DataFrames, temporary views, joins, and aggregations. The standard SPYT Spark extensions must be enabled.

In `overwrite` mode, the output receives the inferred tags. In `append` mode, these tags are added to the existing output tags. Both ordinary and distributed writes support this behavior.

To override the inferred tags, pass a string containing a YSON list:

```python
df.write.option("security_tags", '["sensitive";"userdata";]').yt("//tmp/output")
```

The `attr_security_tags` option is an alias. If both options are specified, `security_tags` takes precedence. An explicit empty list, `"[]"`, disables inheritance for that write. On append, an override does not remove existing output tags.

Tags are retained with input metadata, including when a DataFrame is cached. SPYT cannot infer tags for data read inside a UDF, data reconstructed after `collect()`, or data whose table provenance was lost through RDD transformations. Supply tags explicitly for these cases. Automatic inheritance is not supported for dynamic tables or streaming queries.

## Dynamic tables

For dynamic tables you should explicitly specify an additional option `inconsistent_dynamic_write` with `true` value so that you do agree that there is no support for transactional writes to dynamic tables.

Python example:
```python
df.write.option("inconsistent_dynamic_write", "true")
```
