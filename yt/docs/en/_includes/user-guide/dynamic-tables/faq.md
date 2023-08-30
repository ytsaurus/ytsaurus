#### **Q: When working with dynamic tables from Python API or C++ API, I get the "Sticky transaction 1935-3cb03-4040001-e8723e5a is not found" error. What should I do?**

**A:** The answer depends on how the transactions are used. If a [master transaction](../../../user-guide/dynamic-tables/transactions.md) is used, then this is a pointless action and you must set the query outside the transaction. To do this, you can either create a separate client or explicitly specify that the query must be run under a null transaction (`with client.Transaction(transaction_id="0-0-0-0"): ...`).
Full use of tablet transactions in Python API is only possible via RPC-proxy (`yt.config['backend'] = 'rpc'`). Use of tablet transactions via HTTP is impossible in the current implementation.

------
#### **Q: When writing to a dynamic table, the "Node is out of tablet memory; all writes disabled" or "Active store is overflown, all writes disabled" error occurs. What does it mean and how should I deal with it?**

**A:**The error occurs when the cluster node runs out of memory to store data not written to the disk. The input data stream is too large and the cluster node has no time to compress and write data to the disk. Queries with such errors must be repeated, possibly with an increasing delay. If the error occurs continuously, this may mean (apart from abnormal operation) that either individual tablets are overloaded by the write load or the cluster as a whole is not powerful enough to cope with the load. Increasing the number of tablets can also help (the `reshard-table` command).

------
#### **Q: What does the "Too many overlapping stores" alert mean? What should I do?**

**A:** This error message means that the tablet structure is such that the tablets' coverage of the key interval served by the tablet is too dense. Dense coverage with stores leads to degradation of read performance, so in this situation a protection mechanism is activated that prevents new data from being written. The background compaction and partitioning processes should gradually normalize the tablet structure. If this does not happen, the cluster may be failing to cope with the load.

------
#### **Q: When querying a dynamic table, I get the "Maximum block size limit violated" error**

**A:** The query involves a dynamic table once converted from a static table. The `block_size` parameter was not specified. If you receive an error like this, please make sure you follow all the instructions from the [section](../../../user-guide/dynamic-tables/mapreduce.md) about converting a static table into a dynamic table. If the block size is large (this can be the case if more blobs are stored in the table cells, and they all get into one block), you should increase `max_unversioned_block_size` to 32 megabytes and remount the table.

------
#### **Q: When querying a dynamic table, I get the "Too many overlapping stores in tablet" error**

**A:** Most likely, the tablet can't cope with the write flow and new chunks don't have time to compact. Check that the table was sharded for a sufficient number of tablets. When writing data to an empty table, disable auto-sharding, because small tablets will be combined into one.

------
#### **Q: When querying a dynamic table, I get the "Active store is overflown, all writes disabled" error**

**A:** The tablet can't cope with the write flow — it doesn't have time to dump the data to the disk or it can't do it for some reason. Check for errors in the `@tablet_errors` table attribute, and if there are no errors, check sharding as above.

------
#### **Q: When querying a dynamic table, I get the "Too many stores in tablet, all writes disabled" error**

**A:** The tablet is too large. Get the table to have more tablets. Note that [auto-sharding](../../../user-guide/dynamic-tables/resharding.md#auto) limits the number of tablets as the number of cells multiplied by the value of the `tablet_balancer_config/tablet_to_cell_ratio` parameter.

------
#### **Q: When querying a dynamic table, I get the error "Tablet ... is not known'**

**A:** The client sent a query to the cluster node on which the tablet is no longer being served. This usually occurs as a result of automatic balancing of tablets or restarting of cluster nodes. Resend the query and the error will disappear after the cache is updated or disable balancing.

------
#### **Q: When querying a dynamic table, I get the "Service is not known" error**

**A:** The client sent a query to the cluster node on which the tablet cell is no longer being served. This usually happens when cells are rebalanced. Resend the query and the error will disappear after the cache is updated.

------
#### **Q: When querying a dynamic table, I get the "Chunk data is not preloaded yet" error**

**A:** The message is specific to a table with the `in_memory_mode` parameter at other than `none`. Such a table is always in memory when it is mounted. In order to read from such a table, all data must be uploaded into memory. If the table has just been mounted, or the tablet has moved to another cell, or the {{product-name}} process has been restarted, there is no data in memory and this error occurs. Wait for the background process to upload the data into memory.

------
#### **Q: In tablet_errors, I see the "Too many write timestamps in a versioned row" or "Too many delete timestamps in a versioned row" error**

**A:** Sorted dynamic tables store many versions of the same value at the same time. In lookup format, each key can have no more than 2^16 versions. A simple solution is to use a column format (`@optimize_for = scan`). In practice, such a large number of versions is not necessary and they occur as a result of misconfiguration or a programming error. For example, when specifying `atomicity=none`, you can update the same table key with great frequency (in this mode, there is no row locking and transactions with overlapping time ranges can update the same key). It's not a good idea to do this.  If writing a large number of versions is due to a product need, such as frequent delta writes in aggregation columns, set the `@merge_rows_on_flush=%true` table attribute and correctly [ configure the TTL deletion](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#remove_old_data) so that only a small number of actually needed versions are written to the chunk in case of flush.

------
#### **Q: When querying Select Rows, I get the "Maximum expression depth exceeded" error**

**A:** This error occurs if the expression tree depth is too large. This usually happens when writing expressions like
```FROM [...] WHERE (id1="a" AND id2="b") OR (id1="c" AND id2="d") OR ... <a few thousand conditions>```
Instead, you need to set them in the form ```FROM [...] WHERE (id1, id2) IN (("a", "b"), ("c", "b"), ...)```
There are a few problems with the first option. The problem is that queries are compiled into machine code. Machine code for queries is cached so that the key is the query structure (not including constants).
1. In the first case, as the number of conditions increases, the query structure will constantly change. There will be code generation for each query.
2. The first query will generate a very large amount of code.
3. Compiling the query will be very slow.
4. The first case will work simply by checking all the conditions. No transformation into a trickier algorithm is anticipated.
5. Besides that, if the columns are key, read ranges are displayed for them so that only the relevant data can be read. The read range output algorithm will work more optimally for the IN variant.
6. In case of IN, checking the condition will perform search in the hash table.

------
#### **Q: When working with a dynamic table, I get the "Value is too long" error**

**A:** There are quite strict limits on the size of values in dynamic tables. A single value (table cell) should now be no larger than 16 megabytes and the length of an entire row should be no more than 128 megabytes and 512 megabytes, taking all versions into account. There can be a maximum of 1024 values in a row, taking all versions into account. There is also a limit on the number of rows per query, which defaults to 100,000 rows per transaction when inserting, one million rows in case of select, and 5 million in case of lookup. Note that you must not get close to the thresholds. Some of them are hardcoded and we won't be able to help you easily if you go beyond the limits.

