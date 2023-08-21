# Query settings

This section contains information about CHYT-specific settings. ClickHouse has a large number of different settings which can be specified in different ways: via the GET parameter of a query, directly in the query via the SETTINGS clause, when using YQL via pragma, or in other ways. For a list of the original ClickHouse settings, see the [documentation](https://clickhouse.com/docs/ru/operations/settings/settings/).

The settings related to CHYT start with the `chyt.` prefix. Some settings are placed into separate subsections: in this case, the name of the setting will consist of several tokens separated by dots, for example, `chyt.composite.enable_conversion`.

As usual in ClickHouse, the `1` and `0` values are used as `True` and `False` for logical settings.

As usual, many of the settings include certain optimizations that are enabled by default and are listed only to emphasize the possibility of disabling them in case of incorrect optimization.

## Available settings

- `enable_columnar_read` [`%true`]: Enables the use of the fast column-by-column reading interface for tables in scan format.

- `enable_computed_column_deduction` [`%true`]: Enables the output of values for the computed key columns using the predicate in WHERE. For example, if the `key_hash` key column is specified in the schema as a result of the `farm_hash(key)` expression, then if the query condition contains the `key = 'xyz'` expression, the `key_hash = 16518849956333482075` consequence will be automatically added to the predicate. Only conditions of the `column = constant expression`, `column tuple = tuple of constant expressions`, `column IN tuple of constant expressions`, and `column tuple IN tuple of constant expression tuples` type are supported.

- `infer_dynamic_table_ranges_from_pivot_keys` [`%true`]: Enables the additional step of range output when querying via a dynamic table using [tablet pivot keys](../../../../../user-guide/dynamic-tables/overview.md).

- `composite`: A section with settings related to composite `type_v3` and `YSON any/composite` types:

   - `default_yson_format` [`binary`]: The default format for representing YSON strings. Possible values are `binary, text, and pretty`. Note that options other than `binary` are less efficient, because they require an explicit conversion from binary format. [Functions for working with YSON](../../../../../user-guide/data-processing/chyt/reference/functions.md) work with any of the possible YSON formats.

- `dynamic_table`: A section with settings related to working with dynamic tables.

   - `enable_dynamic_store_read` [`%true`]: Work with dynamic tables by reading data from dynamic stores. If you enable this option, you cannot read a table that was not mounted with the `enable_dynamic_store_read = %true` attribute. Attempting to read will cause an error. If this option is disabled, any query will forcibly read only data from chunk stores, i.e. not including the most recent data from memory store (regardless of whether the table is mounted with `enable_dynamic_store_read = %true` or not).

- `execution`: A section with settings that affect the process of scheduling and executing a query:

   - `query_depth_limit` [0]: The limit on the maximum depth of a distributed query. When reaching the specified depth, the query ends with an error. The 0 value means there is no limit.

   - `join_policy` [`distribute_initial`]: JOIN query mode on top of a {{product-name}} table. Possible options:
      - `local`: JOIN is executed locally on the query coordinator. The left- and right-hand side tables (or subquery) are read according to `select_policy` mode.
      - `distribute_initial` (default): `JOIN` in the initial query is executed in a distributed manner, while JOIN in the secondary queries are executed locally.
      - `distribute`: `JOIN` in all the queries (initial and secondary) is executed in a distributed manner. We do not recommend using  this mode, because it can lead to an exponential number of secondary queries in case of multiple `JOIN`.

   - `select_policy` [`distribute_initial`]
      - `local`: The `SELECT` query is read and processed completely only on one instance (coordinator).
      - `distribute_initial` (default): `SELECT` is executed in a distributed manner on clique instances in the initial query. In secondary queries, the execution is local.
      - `distribute`: `SELECT` in all the queries (initial and secondary) is executed in a distributed manner. We do not recommend using  this mode, because it can lead to an exponential number of secondary queries.

   - `join_node_limit` [0]: The maximum number of clique nodes on which the distributed `JOIN` query is allowed. The 0 value means there is no limit (default).

   - `select_node_limit` [0]: The maximum number of clique nodes on which the distributed `SELECT` query is allowed. The 0 value means there is no limit (default).

   - `distribution_seed` [42]: The seed used for deterministic distribution of the query to the clique nodes.

   - `input_streams_per_secondary_query` [0]: The limit on the number of concurrent table read threads on each clique node during query execution. In case of the 0 value, the value of the `max_threads` clickhouse setting (default) is used as a limit.

   - `filter_joined_subquery_by_sort_key` [`%true`]: Enables optimization that pre-filters the right-hand side JOIN table (or subquery) by the sort key of the left-hand side table.

   - `keep_nulls_in_right_or_full_join` [`%true`]: Adjusts the behavior of `RIGHT` and `FULL JOIN` when the right-hand side table (subquery) has the `Null` values in the key columns. If `%true` is set, the query is executed as usual. The query result will have all rows with the `Null` value in the key columns (default). If `%false` is set, all or some rows from the right-hand side table with the `Null` value in the key columns may be missing and the query can be executed more efficiently. If there are no `Null` values in the key columns of the right-hand side table (subquery), the query execution result does not depend on the values of this option.

   - `distributed_insert_stage` [`with_mergeable_state` (may be changed in the future)] is the minimum query execution stage after which distributed writing to the table is allowed when the `parallel_distributed_insert_select` ClickHouse setting is enabled. Possible values:

      - `none`: Never use distributed writing.

      - `with_mergeable_state` (default, may be changed in the future): Use distributed writing if the query can be executed in a distributed manner at least to the `with_mergeable_state` stage. Data aggregation (`GROUP BY`, `DISTINCT`, `LIMIT BY`) and `ORDER BY` and `LIMIT` clauses can be executed independently on each clique node, so in case of distributed writing the query execution result may contain several rows with the same aggregation key, rows may not be sorted in the ORDER BY order, and the number of rows may exceed a specified LIMIT (but the total number of rows will not exceed the `number of instances in the clique` * `specified LIMIT`).

      - `after_aggregation`: Use distributed writing if the query can be executed in a distributed manner at least to the `after_aggregation` stage. Data aggregation (`GROUP BY`, `DISTINCT`, `LIMIT BY`) is guaranteed to be executed to the end and the `ORDER BY` and `LIMIT` clauses can be executed independently on each clique node (which can cause sorting to fail and exceeding the specified row limit, as described above). If the query cannot be executed in a distributed manner to the `after_aggregation` stage, data will be written locally from the query coordinator.

      - `complete`: Use distributed writing only if the query can be executed in a completely distributed manner. The result of execution of the query with distributed writing in this case is indistinguishable from an ordinary query, all the clauses (aggregation, `ORDER BY`, and `LIMIT`) will be executed to the end. If the query cannot be executed in a distributed manner to the `complete` stage, data will be written locally from the query coordinator.

- `caching`: A section with settings related to the different caches used in CHYT:

   - `table_attributes_invalidate_mode` [`sync`]: The data invalidation mechanism mode in the table attribute cache when the table is changed (the `CREATE TABLE/INSERT INTO/DROP TABLE` queries). Possible values:
      - `none`: Data invalidation in the cache does not occur. Reading tables immediately after a change may return the old data or cause an error.
      - `local`: Data invalidation occurs only in the local instance cache, without any rpc queries. Reading tables on other instances immediately after a change may return the old data or cause an error.
      - `async`: Data invalidation occurs synchronously in the local cache and asynchronously on all clique instances. The query completes without waiting for data invalidation on the other clique instances. Errors during data invalidation in the cache do not cause a query to fail. Reading tables on other instances immediately after a change may still return the old data or cause an error, but this time interval after a data change is much shorter than in the `none` and `local` cases. We recommend using an invalidation mode not lower than this one.
      - `sync` (default):  Data invalidation occurs synchronously on all clique instances. The query execution time may be slightly longer, because it is necessary to wait for confirmation of cache invalidation on all instances before completing. Errors during cache invalidation will cause a query to fail. After a successful modifying query, you can immediately read the modified tables.

   - `invalidate_request_timeout` [5000]: The timeout in milliseconds (ms) for rpc queries to clique instances at the cache invalidation stage. If this timeout is exceeded, an error will be shown.

- `concat_tables`: A section with settings related to the functions for merging tables of the `concatYtTables*` and `ytTables` range:

   - `missing_column_mode` [`read_as_null`]: The output mode of the general column schema if it is missing in one of the tables. Possible values:
      - `throw`: The query will end with an error.
      - `drop`: The column will be missing in the general schema. You cannot read such a column.
      - `read_as_null` (default): The column type will be set to `Optional(T)`. In the rows from tables where this column is missing, the value in the column will be `Null`.

   - `type_mismatch_mode` [`throw`]: The general column schema output mode if column types in different tables are different and cannot be reduced to a general form. Possible values:
      - `throw` (default): The query will end with an error.
      - `drop`: The column will be missing in the general schema. You cannot read such a column.
      - `read_as_any`: The column type will be set to `Any`. All values will be represented as YSON strings which can be handled using the functions of the `YSONExtract*` and `YPath*` range.

   - `allow_empty_schema_intersection` [`%false`]: Allows combining multiple tables if they have no common columns.

   - `max_tables` [250]: The maximum number of tables that can be combined for reading. If this limit is exceeded, the query will end with an error.
