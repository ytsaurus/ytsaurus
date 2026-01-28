# FAQ

#### **Q: When working with dynamic tables from the Python API or C++ API, I get the "Sticky transaction 1935-3cb03-4040001-e8723e5a is not found" error. What should I do?** {#sticky-transaction}

**A:** The answer depends on how the transactions are used. If a master transaction is used, then this is a pointless action and you must set the query outside the transaction. To do this, you can either create a separate client or explicitly specify that the query must be run under a null transaction (`with client.Transaction(transaction_id="0-0-0-0"): ...`).
Full use of tablet transactions in Python API is only possible via RPC-proxy (`yt.config['backend'] = 'rpc'`). Use of tablet transactions over HTTP is currently not supported.{% if audience == "internal" %} You might want to send an email to "yt@" and describe your specific scenario.{% endif %}

------
#### **Q: When writing to a dynamic table, the "Node is out of tablet memory; all writes disabled" or "Active store is overflown, all writes disabled" error occurs. What does it mean and how should I deal with it?** {#tablet-memory}

**A:**The error occurs when the cluster node runs out of memory to store data not written to the disk. The input data stream is too large and the cluster node has no time to compress and write data to the disk. Queries with such errors must be repeated, possibly with an increasing delay. If this is a recurring error, this may mean (except in off-nominal situations) that individual tablets are overloaded with writes or that the cluster's capacity is not sufficient to cope with this load. Increasing the number of tablets can also help (the `reshard-table` command).

------
#### **Q: What does "Too many overlapping stores" mean? What should I do?** {#overlapping-stores}

**A:** This error is an indication that tablet structure is such that the dynamic store coverage of the key range being serviced by the tablet is too dense. Dense coverage with stores leads to degradation of read performance, so in this situation a protection mechanism is activated that prevents new data from being written. The background compaction and partitioning processes should gradually normalize the tablet structure. If this does not happen, the cluster may be failing to cope with the load.

------
#### **Q: When querying a dynamic table, I get the "Maximum block size limit violated" error.** {#block-size-limit}

**A:** The query involves a dynamic table once converted from a static table. The `block_size` parameter was not specified. If you receive an error like this, make sure you follow all the instructions from the [section](../../user-guide/dynamic-tables/mapreduce.md) about converting a static table into a dynamic table. If block size is large, you need to increase `max_unversioned_block_size` to 32 MB and re-mount the table. This can happen, if the table's cells store large binary data that are stored in a single block in their entirety.

------
#### **Q: When querying a dynamic table, I get the "Too many overlapping stores in tablet" error.** {#overlapping-stores-tablet}

**A:** Most likely, the tablet can't cope with the write flow and new chunks don't have time to compact. Check that the table was sharded for a sufficient number of tablets. When writing data to an empty table, disable auto-sharding, because small tablets will be combined into one.

------
#### **Q: When querying a dynamic table, I get the "Active store is overflown, all writes disabled" error.** {#active-store-overflown}

**A:** The tablet can't cope with the write flow — it doesn't have time to dump the data to the disk or it can't do it for some reason. Check for errors in the `@tablet_errors` table attribute and if there are none, check sharding as above.

------
#### **Q: When querying a dynamic table, I get the "Too many stores in tablet, all writes disabled" error.** {#too-many-stores}

**A:** The tablet is too large. Get the table to have more tablets. Note that [auto-sharding](../../user-guide/dynamic-tables/tablet-balancing.md) limits the number of tablets to the number of cells multiplied by the value of the `tablet_balancer_config/tablet_to_cell_ratio` parameter.

------
#### **Q: When querying a dynamic table, I get the error "Tablet ... is not known".** {#tablet-not-known}

**A:** The client sent a query to a cluster node no longer serving the tablet. This usually occurs as a result of automatic balancing of tablets or restarting of cluster nodes. You need to resend the query to make the error disappear after a cache update or to disable balancing.

------
#### **Q: When querying a dynamic table, I get the "Service is not known" error.** {#service-not-known}

**A:** The client sent a query to a cluster node no longer serving the cell tablet. This usually happens when cells are rebalanced. You need to resend the query to make the error disappear after a cache update.

------
#### **Q: When querying a dynamic table, I get the error "Invalid mount revision of tablet"** {#invalid-mount-revision}

**A:** The client sent a query to a cluster node that has an updated tablet revision. This can happen if the table was explicitly remounted or if the tablet was moved as a result of [automatic balancing](../user-guide/dynamic-tables/tablet-balancing) (the tablet may have remained on the same node but changed its tablet cell). Send another query — the error will disappear once the cache is updated. Alternatively, you can reconfigure the balancing settings.

------
#### **Q: When querying a dynamic table, I get the "Chunk data is not preloaded yet" error.** {#chunk-data-not-preloaded}

**A:** The message is specific to a table with the `in_memory_mode` parameter at other than `none`. Such a table is always in memory in a mounted state. In order to read from such a table, all data must be loaded into memory. If the table was recently mounted, the tablet was moved to a different cell, or the {{product-name}} process restarted, the data is no longer in memory, which will generate this type of error. You need to wait for the background process to load data into memory.

Tablets can move to other cells as a result of [automatic sharding](../../user-guide/dynamic-tables/tablet-balancing.md). You can see these moves on the "Tablet balancer moves" graph on the bundle's page. If you want, you can set a different schedule for automatic sharding. Your new schedule must allow the table to continue sustaining your load.

------
#### **Q: In tablet_errors, I see the "Too many write timestamps in a versioned row" or "Too many delete timestamps in a versioned row" error.** {#too-many-timestamps}

**A:** Sorted dynamic tables store many versions of the same value at the same time. In lookup format, each key can have no more than 2^16 versions. A simple solution is to use a column format (`@optimize_for = scan`). In reality, such a large number of versions is not necessary and they occur as a result of misconfiguration or a programming error. For example, when specifying `atomicity=none`, you can update the same table key with great frequency (in this mode, there is no row locking and transactions with overlapping time ranges can update the same key). This is not recommended. If writing a large number of versions results from a product need, such as frequent delta writes in aggregation columns, set the `@merge_rows_on_flush=%true` table attribute and correctly [configure TTL deletion](../../user-guide/dynamic-tables/sorted-dynamic-tables.md#remove_old_data) so that only a small number of actually needed versions are written to the chunk in a flush.

------
#### **Q: When querying Select Rows, I get the "Query terminated prematurely due to excessive input; consider rewriting your query or changing input limit" error.** {#excessive-input}

**A:** This error occurs if too much data is read during query execution.

Check the following:
1. The primary key prefix of the table must be specified in the `WHERE` query section. If this hasn't been done, [input data cutting off](../../user-guide/dynamic-tables/dyn-query-language.md#input_data_cut) can't be performed, and the query can't be executed efficiently (it will run as a full scan).
2. If the query contains `JOIN`, make sure it's executed [using an index](../../user-guide/dynamic-tables/dyn-query-language.md#joins).

You need to manually rewrite the query to make sure it runs efficiently. To debug the query, you can use the [explain](../../user-guide/dynamic-tables/dyn-query-language.md#explain_query) command.
You can temporarily fix the error by increasing `input_row_limit`, but this won't fully resolve your issue.

------
#### **Q: When querying Select Rows, I get the "Maximum expression depth exceeded" error.** {#maximum-expression-depth}

**A:** This error occurs if the expression tree depth is too large. This usually happens when writing expressions like
```FROM [...] WHERE (id1="a" AND id2="b") OR (id1="c" AND id2="d") OR ... <a few thousand conditions>```
Instead, you need to set them in the form ```FROM [...] WHERE (id1, id2) IN (("a", "b"), ("c", "b"), ...)```
There are a few problems with the first option. The problem is that queries are compiled into machine code. Machine code for queries is cached so that query structure not including constants serves as the key.
1. In the first case, as the number of conditions increases, query structure will constantly change. There will be code generation for each query.
2. The first query will generate a very large amount of code.
3. Compiling the query will be very slow.
4. The first case will work simply by checking all the conditions. No transformation into a more complex algorithm is envisaged.
5. Besides that, if the columns are key, read ranges are displayed for them so that only the relevant data can be read. The read range output algorithm will work more optimally for the IN variant.
6. In case of IN, checking the condition will perform search in the hash table.

------
#### **Q: When working with a dynamic table, I get the "Value is too long" error.** {#value-too-long}

**A:** There are quite strict limits on the size of values in dynamic tables. Single value (table cell) size must not exceed 16 MB, while the length of an entire row should stay under 128 and 512 MB taking into account all versions. There can be a maximum of 1024 values in a row, taking all versions into account. There is also a limit on the number of rows per query, which defaults to 100,000 rows per transaction when inserting, 1 million rows for selects, and 5 million for a lookup. Please note that operation may become unstable in the vicinity of the threshold values. If you are over limit, you need to change data storage in a dynamic table and not store such long rows.

{% if audience == "internal" %}

------
#### **Q: I get the "No healthy tablet cells in bundle" error after creating a bundle.** {#no-healthy-tablet-cells}
**A:** If this error occurs after you create a bundle, first check whether you [allocated](../../user-guide/dynamic-tables/dynamic-tables-resources#upravlenie-instansami) instances within the bundle. You can find them in the "Instances" tab on the bundle's page.
{% endif %}

------
#### **Q: How do I clear a replicated table using a CLI?** {#clear-replicated-table-cli}

**A:** You can't clear a replicated table atomically, but you can clear individual replicas using the [Erase](../../user-guide/data-processing/operations/erase.md) operation. To do this, you need to [enable](../../user-guide/dynamic-tables/bulk-insert.md) bulk insert for the entire cluster by setting:

- `//sys/@config/tablet_manager/enable_bulk_insert` to `%true`
- `//sys/controller_agents/config/enable_bulk_insert_for_everyone` to `%true` (note the missing `@` symbol)

{% if audience == "public" %}
If the last specified node doesn't exist, create it using the following command:

```bash
$ yt create document //sys/controller_agents/config --attributes '{value={}}'
```
{% endif %}

------
#### **Q: When reading from a dynamic table, I get the "Timed out waiting on blocked row" error.** {#timed-out-blocked-row}

**A:** This error occurs due to read-write conflicts – you attempt to read by a key used by a concurrent transaction. The system must wait for the result of that transaction to ensure the required level of isolation between transactions.

If you don't require strict consistency, you can [weaken](../../user-guide/dynamic-tables/sorted-dynamic-tables#chtenie-stroki) the isolation level for the reading transaction.

------
#### **Q: When writing to a dynamic table, I get the "Row lock conflict due to concurrent ... write" error.** {#row-lock-conflict}

**A:** This error occurs due to write conflicts – you're trying to write to a row that is locked by a concurrent transaction (this can be a write operation or an explicit lock). In some cases, the system may indicate the conflicting transaction. For example, for write-write conflicts, the error attributes include the transaction ID.

In general, to eliminate conflicts caused by writing to the same key from multiple sources, you need to revise your logic for writing to dynamic tables.

In some scenarios, weakening [guarantees](../../user-guide/dynamic-tables/transactions#conflicts) may help. For example, you can use `atomicity=none` mode or shared write lock mode. Before you weaken the guarantees, be sure to read the documentation to find out whether the side effects can impact you.

------
#### **Q: When reading from a dynamic table, I get the "No working in-sync replicas found for table" error.** {#no-working-replicas}

**A:** This error occurs when reading from a replicated or chaotic table if there is no replica to read from. The replica must meet the following criteria:
1. It must contain fairly recent data. If a timestamp is specified when reading, the replica must contain data up to that timestamp. If a timestamp is not specified, reads must be made from an *actually synchronous* replica: a one that has `mode=sync` and definitely contains older data. For more information, see [guarantees of replicated dynamic tables](../../user-guide/dynamic-tables/replicated-dynamic-tables#garantii).
2. It must be a working replica. If the client has recently got an error from a replica, next time it will try to access a different replica. The replicas you cannot read data from are listed in the `banned_replicas` attribute of the error in question.

---
#### **Q: When reading from a dynamic table, I get the error 'Timestamp ... is less than tablet ... retained timestamp'** {#retained-timestamp}

**A:** The error occurs when requesting data that may have been deleted by compaction. Check that the `min_data_ttl` of the table is bigger than the expected lifespan of user transactions.

For more information about compaction settings, see the section [Deleting old data](../../user-guide/dynamic-tables/sorted-dynamic-tables#remove_old_data).

---

---
#### **Q: When attempting to execute RemoteCopy, I get the "Chunk view cannot be copied to remote cluster" error.** {#chunk-view-cannot-be-remote-copied}

**A:** To ensure cost efficiency, some operations on a dynamic table, such as resharding, may create chunk views — wrappers for chunks. Currently, RemoteCopy can only copy regular chunks, not chunk views.
To remove chunk views from a table, perform a special forced compaction by setting the `forced_chunk_view_compaction_revision` attribute to 1, and then execute `remount-table`. For more information, see [Forced compaction](../../user-guide/dynamic-tables/compaction#forced_compaction).
{% if audience == "internal" %}

{% note info "Note" %}

To execute RemoteCopy for dynamic tables, we recommend using the {{dynamic-tables.remote-copy.links.dyntable-remote-copy-script}} script.

{% endnote %}

{% endif %}
