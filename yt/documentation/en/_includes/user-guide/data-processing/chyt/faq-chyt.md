# FAQ

#### **Q: Why does CHYT have cliques, while regular ClickHouse has nothing analogous? What is a clique anyway?**

**A:** There is a dedicated [Concepts](../../../../user-guide/data-processing/chyt/general.md) article about this.

------

#### **Q: I get one of the `DB::NetException: Connection refused`, `DB::Exception: Attempt to read after eof: while receiving packet from` errors. What does it mean?**

**A:** This usually means that an instance has left for one reason or another. You can view the counters of the number of aborted/failed jobs in the [operation UI](../../../../user-guide/data-processing/chyt/cliques/ui.md#jobs). If there are (recent) jobs aborted due to preemption, it means that the clique does not have enough resources. If there are (recent) failed jobs, contact the administrator.

------

#### **Q: What does the `Subquery exceeds data weight limit: XXX > YYY` error mean?**

**A:** See the `max_data_weight_per_subquery` option in the article about [configuration](../../../../user-guide/data-processing/chyt/reference/configuration.md#configuration_example).

------

#### **Q: How do I save data to a table?**

**A:** There are **INSERT INTO** and **CREATE TABLE** functions. Learn more about them in the [Working with {{product-name}} tables](../../../../user-guide/data-processing/chyt/yt-tables.md#save) section.

------

#### **Q: How do I load geo-dicts in my own clique?**

**A:** When starting any clique, you can specify the `--cypress-geodata-path` option that enables you to specify the path to geo-dicts in Cypress. For more information about this option, see the [How to try](../../../../user-guide/data-processing/chyt/try-chyt.md) article.

------

#### **Q: Can CHYT process dates in `TzDatetime` format?**

**A:** CHYT can process dates in `TzDatetime` format just as much as regular ClickHouse (all the same functions are available). You will have to store the data as strings or numbers and convert when reading and writing. Date extraction example:

```sql
toDate(reinterpretAsInt64(reverse(unhex(substring(hex(payment_dt), 1, 8)))))
```
------

#### **Q: How do I move a table to an SSD?**

**A:** First, make sure that your {{product-name}} account has a quota for the **ssd_blobs** [medium](../../../../user-guide/storage/media.md). To do this, go to the **Account** page, switch the medium type to **ssd_blobs**, and enter your account name. If there is no quota in the `ssd_blobs` medium, request it from the administrator.

After obtaining the quota, you need to change the value of the `primary_medium` attribute on the **ssd_blobs** medium, the data will be moved to the corresponding medium in the background.

For static tables, you can force a move using the [Merge](../../../../user-guide/data-processing/operations/merge.md) operation.

```bash
yt set //home/dev/test_table/@primary_medium ssd_blobs
yt merge --mode auto --spec '{"force_transform"=true;}' --src //home/dev/test_table --dst //home/dev/test_table
```

If the table is dynamic, to change the medium, you must first unmount the table,
set the attribute, and then mount it back:

```bash
yt unmount-table //home/dev/test_table --sync
yt set //home/dev/test_table/@primary_medium ssd_blobs
yt mount-table //home/dev/test_table --sync
```

You can additionally speed up moving using [forced_compaction](../../../../user-guide/dynamic-tables/overview.md#attributes), but using this method creates a heavy load on the cluster and is strongly not recommended.


To check that the table really changed the medium, you can use the command:

```bash
yt get //home/dev/test_table/@resource_usage

{
    "tablet_count" = 0;
    "disk_space_per_medium" = {
        "ssd_blobs" = 930;
    };
    "tablet_static_memory" = 0;
    "disk_space" = 930;
    "node_count" = 1;
    "chunk_count" = 1;
}
```

------

#### **Q: Is the `SAMPLE` construction of the СlickHouse language supported?**

**A:** CHYT supports the `Sample` construction. The difference is that CHYT ignores the `OFFSET ...` command, so you cannot get a sample from another part of the selected data.

Example:

```SQL
SELECT count(*) FROM "//tmp/sample_table" SAMPLE 0.05;

SELECT count(*) FROM "//tmp/sample_table" SAMPLE 1/20;

SELECT count(*) FROM "//tmp/sample_table" SAMPLE 100500;
```

------

#### **Q: How do I get the table name in a query?**

**A:** You can use the `$table_name` and `$table_path` virtual columns. For more information about the virtual columns, see [Working with {{product-name}} tables](../../../../user-guide/data-processing/chyt/yt-tables.md#virtual_columns).




