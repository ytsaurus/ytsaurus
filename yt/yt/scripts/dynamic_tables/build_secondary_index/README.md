### Build secondary index

Given a *table* and an *index table* this script creates a secondary index link between them and runs one or more map-sort-reduce operation to fill the *index table* with data from the *table*.

```
./build_secondary_index --proxy CLUSTER --table //path/to/table --index-table //path/to/index/table --kind full_sync --build-strictly --pool POOL_NAME
```

Pool for map-sort-reduce operations can be specified via `--pool`.

This scripts also supports replicated tables, but in this case `--pools` options must be specified and both tables must already be a part of the same collocation:

```
./build_secondary_index --proxy CLUSTER_0 --table //path/to/replicated/table --index-table //path/to/replicated/index/table --kind full_sync --build-strictly --pools '{CLUSTER_0: POOL_0; CLUSTER_1: POOL_1; CLUSTER_2: POOL_2}'
```

Options `--build-strictly` and `--build-online`: the latter results in less downtime for *table*, but allows a race between background compaction and map-sort-reduce operations, which may result in extra rows in the *index table*. This does not inhibit the primary function of the index, but makes *covering index* optimization unfeasible. It also potentially makes select queries using the index less efficient - due to presence of the extra rows, we must join on all shared columns and not just key columns.
