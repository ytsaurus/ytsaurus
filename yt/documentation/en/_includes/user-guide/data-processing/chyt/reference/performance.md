# Performance and recommendations

When choosing between CH and CHYT, the question arises — which is faster and by how much?

We cannot give a general answer to this question, since the query execution speed depends on many factors, including the query itself, the number of resources, and the data storage schema.

The data in {{product-name}} is not originally designed to execute OLAP queries. To get normal speed, you need to prepare them.
You are unlikely to achieve the speed of CH, but you can get closer, choosing the proper schema for storing data and queries to it.
But if you store data improperly, you can easily achieve query speeds hundreds of times lower than in CH.


## Where the time goes { #where_my_time }

Below are the query execution stages that can be a bottleneck.

1. Reading table and chunk metainformation, distributing a query to instances.
2. Reading data from the disk.
3. Transmitting data over the network.
4. Data uncompressing and decoding.
5. Version merging (for [dynamic](../../../dynamic-tables/overview.md) tables only).
6. Converting data from {{product-name}} format into CH format.
7. Data processing by the ClickHouse engine.
8. Converting data from CH format into {{product-name}} format (for write operations only).
9. Writing data in {{product-name}} (for write operations only).

Depending on how the data is stored and what queries are processed, different stages can be a bottleneck.

## Reading data { #read }

Below are the factors that affect the read query speed.

### Processed data size { #data_weight }

CHYT is primarily designed for fast (a few seconds) processing of small (about 1 GB) and medium (about tens of GB) data. You cannot use CHYT effectively to process TB of data. You can reduce the amount of processed data by using a data storage [schema](../../../../../user-guide/storage/static-schema.md) (using column selection and sorting).

To build dashboards on top of a large amount of data, we recommend making separate tables with the necessary data and pre-aggregated values. For example, if the data is specified in milliseconds and you need to build analytics by days, you need to pre-aggregate the data to a day.

How to reduce the amount of computations on the fly:
* For example, you can compute the date by the `datetime` field, but filtering by this field will be slower.
* Avoiding unreasonably heavy queries. For example, sorting 100 GB, `SELECT DISTINCT` or `GROUP BY` by the column that has millions of different values, or using the `JOIN` operator for large tables. Such queries will always be processed slowly.

Reading large tables can bump into any of the query execution points from the list above.

### Erasure coding { #erasure }

Erasure coding is intended for "cold" tables. In erasure coding, data takes up less disk space, but this has a negative impact on the read speed of such data:
1. Each chunk is divided into 16 (or 9 depending on the coding algorithm) parts, which increases the amount of metainformation (point 1).
2. Data is stored in one copy, so reading cannot be paralleled (point 2).
3. If the disk on the node is loaded, you cannot switch to read data from another node. And since the chunk is divided into 16 parts, the probability that reading at least one of them will freeze greatly increases.
4. If data is unavailable due to a cluster node failure, it must be restored. This is a rather long and resource-consuming process. (points 2, 4?).

As a result, it takes much longer to read erasure-coded data and reading latency becomes more likely and less predictable. **We strongly do not recommend** building dashboards or making other regular queries on top of erasure-coded data.

### Data storage format { #optimize_for }
Data in {{product-name}} tables can be stored both in row-by-row (`optimize_for=lookup`) and column-by-column (`optimize_for=scan`) formats.

{% note warning "Attention!" %}

ClickHouse is a column-oriented DBMS, so we highly recommend using column-by-column storage.

{% endnote %}

Column-by-column data storage has the following advantages:
- Converting data into CH format from {{product-name}} column-by-column format is many times (if not dozens of times) more efficient than from row-by-row format (point 6).
- When storing data on a column-by-column basis, only the requested columns will be read during the query. (points 2, 3, and 4).
- Data stored on a column-by-column basis is better compressed, so it takes up less space (points 2, 3).

{% note info %}

The ordinary change of the `optimize_for/erasure_codec` attributes does not convert old data into the new format. To change the format, run the `merge` operation with the `force_transform=%true` option.

{% endnote %}


### Key columns { #key_columns }

ClickHouse has indexes and sharding keys for efficient reading. In {{product-name}}, the data storage model is a bit different, there are no usual indexes, but there are sorted tables. Queries on top of sorted tables use the sort key as a primary index (primary key) and the sharding key.

This enables you to:
- Effectively filter and not read unnecessary data from the disk (point 2).
- Make a query, in particular `Sorted Join`, more effectively.
- Better compress sorted tables so that they take up less space (point 2).

The sort key must be selected depending on the queries:

- For example, if the queries look like
   ``` ... where date between '2021-01-01' and '2021-02-01'```

A good key will be `date`.

- For queries of type
   ``` ... where user_id = 1234```
   A good key will be `user_id`.

In some cases, if you sort the table, there will be no effective filtering. The column is string and the `DateTime` <-> `String` transformation is not monotonic and unambiguous. The `Int` <-> `DateTime` transformation is monotonic, but such optimization cannot be generally applied with string representation. For example, `2020-01-01 00:00:00` and `2020-01-01T00:00:00` are the correct representation of the same time moment in ClickHouse, but when sorting with string representation, the `2020-01-01 00:00:01` value may appear between them, so the `String` -> `DateTime` transformation is not monotonic and you cannot use this optimization.

### Number of chunks { #chunk_count }

The number of chunks affects several query execution stages.

The more chunks a table has, the more metainformation must be read at step 1. If a table has thousands of chunks, it may take several seconds to read metainformation. If there are many chunks in a very small table, reading each chunk becomes a random read and can be slow (step 2).
On the other hand, if there are too few chunks (for example, 1 chunk per 10 GB), all data is stored on the same disks, which will limit the concurrency of reads at step 2.

Use the `Merge` operation to enlarge the chunks.

### Number of input tables { #table_count }

Similar to the number of chunks, but metainformation for a table is much heavier than metainformation for a chunk. Besides that, access permissions must be checked for each table separately. We recommend making no more than hundreds of tables. You better combine many tables into a single large one.

### Number of columns { #column_count }

The number of columns in the table also affects the speed of data even if is is stored in column-by-column format. The block size is selected so that the reader can read a block for each column and output the result. If you want the data to fit into memory in this case, the block size must decrease as the number of columns increases. When blocks are very small, reading becomes a random read (step 2). We recommend having dozens or a maximum of 100 columns. Thousands of columns are guaranteed to perform poorly.
Besides that, the large number of columns increases the amount of metainformation at step 1.

### Dynamic vs static tables { #dyn-vs-stat }

Data in dynamic tables is stored in a different way and is not intended for full-scan reading. Additional expenses are required to read them:

1. First, you have to read its own version for each value.
2. Second, you must read all the key columns for each row even if they are not used in the query.
3. Next, you need to merge all the rows with the same key, leaving only 1 most recent version. It's a very hard process with a lot of comparisons. In addition, the process is purely row-by-row, so even if the data is stored in column-by-column format, it will have to be converted into row-by-row format.
4. After that, you need to convert the data back into column-by-column format.

`Merge` can add x10 to the data read time.

We strongly do not recommend building dashboards or making other regular queries on top of large dynamic tables due to the inefficiency of reading.

### SSD vs HDD { #ssd_vs_hdd }

SSDs have the obvious advantage of higher read speed and lower latency at step 2.

### Optional columns { #optional_columns }

Most columns in {{product-name}} are optional (`required: false`), `Null` can be stored instead of a value. In CH, this is not the case, most columns are not `Nullable`. Using optional speakers slows down almost all stages, from reading and converting data to processing in CH.

We recommend applying a more rigid schema and getting rid of optional columns when they are not required.

### Data caches { #data_caches }

CHYT has a large number of caches to speed up some query execution stages.
The main caches are `compressed_block_cache` and `uncompressed_block_cache`. Using the second one eliminates stages 2 and 3. Using the first one eliminates stages 2, 3, and 4 (uncompressing).
Small tables will probably be always read from the cache.
