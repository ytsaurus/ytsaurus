A stderr table has the following columns:

1. `job_id`: Job ID.
2. `part_index`: If a job has stderr that is too large, it is split into parts. The `part_index` value indicates the index of a specific stderr part.
3. `data`: Stderr data itself.

The table is sorted by `job_id`, `part_index`.

You can read such a table using the [read_blob_table](../../../../user-guide/storage/blobtables.md#read_blob_table) command.
