
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

Write tables with schema in [type_v3](../../../../../user-guide/storage/data-types.md) instead of type_v1.
Setup in [Spark configuration](../cluster/configuration.md) or write option.

Python example:
```python
df.write.option("write_type_v3", "true")
```

## Dynamic tables

For dynamic tables you should explicitly specify an additional option inconsistent_dynamic_write with true value so that you do agree that there is no support (yet) for transactional writes to dynamic tables

Python example:
```python
df.write.option("inconsistent_dynamic_write", "true")
```