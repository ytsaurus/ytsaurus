# FAQ

#### **Q: Why does CHYT have cliques, while regular ClickHouse has nothing analogous? What is a clique?**

**A:** There is a dedicated [article](../../../user-guide/data-processing/chyt/general.md) about this.

------

#### **Q: I am getting the "DB::NetException: Connection refused" or the "DB::Exception: Attempt to read after eof: while receiving packet" error. What does it mean?**

**A:** This normally means that the CHYT process inside the Vanilla transaction crashed. You can view the aborted/failed job [counters](../../../user-guide/data-processing/chyt/cliques/ui.md) in the operation UI. If there are recent jobs aborted due to preemption, it means that the clique is short on resources. If there are recent failed jobs, please contact your system administrator.

------

#### **Q: I am getting the "Subquery exceeds data weight limit: XXX > YYY" error. What does it mean?**

<!-- **A:** смотрите опцию `max_data_weight_per_subquery` в документации по [конфигурации](../../../user-guide/data-processing/chyt/reference/configuration.md#yt) клики. -->

------

#### **Q: How do I save to a table?**

**A:** There are **INSERT INTO** and **CREATE TABLE** functions. Learn more in the section on [Differences from ClickHouse.](../../../user-guide/data-processing/chyt/yt-tables.md#save)

------

#### **Q: How do I load geo-dicts in my own clique?**

**A:** When starting any clique, you can specify the `--cypress-geodata-path` option that enables you to specify the path to geo-dicts in [Cypress](../../../user-guide/storage/cypress.md).

<!-- For more information, see [Getting started.](../../../user-guide/data-processing/chyt/reference/start-clique.md) -->

------

#### **Q: Can CHYT handle dates in TzDatetime format?**

**A:**  CHYT can handle dates in TzDatetime format just as well as conventional ClickHouse. You will have to store data as strings or numbers and convert them for reading and writing. Example date extraction by **@gri201**:

```sql
toDate(reinterpretAsInt64(reverse(unhex(substring(hex(payment_dt), 1, 8)))))
```

------

#### **Q: How do I move a table to an SSD?**

**A:** First, make sure that your {{product-name}} account has a quota for the **ssd_blobs** medium. To do this, go to the account page, switch your medium type to **ssd_blobs**, and enter your account name. If you have no quota for the **ssd_blobs** medium, you can request it via a special form.

After obtaining the quota for the **ssd_blobs** medium, you will need to change the value of the `primary_medium` attribute, and the data will be moved to the corresponding medium in the background. Learn more in the section on [storage](../../../faq/faq.md).

For static tables, you can force a move using the [Merge](../../../user-guide/data-processing/operations/merge.md) operation:

```bash
yt set //home/dev/test_table/@primary_medium ssd_blobs
yt merge --mode auto --spec '{"force_transform"=true;}' --src //home/dev/test_table --dst //home/dev/test_table
```

If the table is dynamic, to change the medium, you must first unmount the table,
set the attribute, and then re-mount it:

```bash
yt unmount-table //home/dev/test_table --sync
yt set //home/dev/test_table/@primary_medium ssd_blobs
yt mount-table //home/dev/test_table --sync
```

You can speed up the move further with [forced_compaction](../../../user-guide/dynamic-tables/overview.md#attributes) but using this method creates a heavy load in the cluster and is strongly discouraged.

To verify that the table has in fact changed its medium, use the command below:

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

#### **Q: Is the SAMPLE construct of the ClickHouse language supported?**

**A:** CHYT supports the Sample construction. The difference is that CHYT ignores the `OFFSET ...` command, so you cannot get a sample from another part of the selected data.

Example:

```SQL
SELECT count(*) FROM "//tmp/sample_table" SAMPLE 0.05;

SELECT count(*) FROM "//tmp/sample_table" SAMPLE 1/20;

SELECT count(*) FROM "//tmp/sample_table" SAMPLE 100500;
```

------

#### **Q: How do I get the table name in a query?**

**A:** You can use the `$table_name` and `$table_path` virtual columns. For more information about the virtual columns, see [Working with {{product-name}} tables](../../../user-guide/data-processing/chyt/yt-tables.md##virtual_columns).


