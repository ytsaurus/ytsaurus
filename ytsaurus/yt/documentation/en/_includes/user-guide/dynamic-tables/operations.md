# Operations with dynamic tables

## Mounting { #mount_table }

When reading or writing to a table, the client uses the metadata cache to understand which tablets are affected. In order to be able to execute queries, the relevant tablets must be mounted.

Once created, the tablet is not mounted and is in the `unmounted` state. To mount the tablet, use the `mount-table` command. The state of the tablets engaged will change from `unmounted` to `mounting` and then to `mounted`.

When mounting a tablet, {{product-name}} selects the tablet cell that will serve this tablet. You can specify such a cell in the arguments of the `mount-table` command or allow the system to do it. In the current implementation, the system selects a random functional cell, but future versions may improve the algorithm and balance it more accurately taking the load into account.

You can unmount a tablet using the `unmount-table` command. The tablet will change its state to `unmounting` and then to `unmounted`. An unmounted tablet cannot handle write or read queries. To reshard tablets, you must first unmount them.

To unmount a tablet:

- Wait until all write transactions are complete and do not allow any new ones.
- Wait for all data from dynamic_store memory to be written to chunk_store chunks.

Unmounting may take some time.

You can perform forced unmounting (the `force` option), which works instantly but may result in data loss and lags of some two-phase commits.

## Freezing { #freeze }

In addition to the `mount_table` and `unmount_table` commands, {{product-name}} has close analogs: the `freeze_table` and `unfreeze_table` commands.

The `freeze_table` command converts the table tablets — all or part of them — into a special `frozen` state. You can read data from these tablets, but you cannot write to them.

The `unfreeze_table` command returns "frozen" tablets to their normal `mounted` state.

Transition from `mounted` to `frozen`, in addition to locking writes, also causes all data accumulated in memory to be written as chunks to the disk. This process is not instantaneous and may take minutes. While the data is being written to the disk, the state of the tablet will be `freezing`.

Freezing a dynamic table is useful for making a copy of it, while the table remains readable.

## Copying dynamic tables { #copy }

If all tablets of the dynamic table are unmounted or frozen, the table can be copied as an ordinary static table using the `copy` command. The copy of the table will use the same set of chunks as the original one. As the state of these tables starts changing, the number of common chunks will start decreasing in the process of compaction. The disk space consumed by these tables will be equal to the sum of their volumes.

