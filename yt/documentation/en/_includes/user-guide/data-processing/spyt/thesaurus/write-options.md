
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

