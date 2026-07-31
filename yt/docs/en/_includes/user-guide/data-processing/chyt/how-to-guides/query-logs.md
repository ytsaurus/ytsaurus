# Getting Query Logs

To analyze query execution, identify performance issues, or track failed jobs, you can review the clique's query logs.

{% note warning %}

Query Logs functionality is available in CHYT starting from version `2.16`.

{% endnote %}

Logs of all queries are stored in tables. They are located in [Cypress](../../../../../overview/about.md#cypress)  at the following path: `//sys/strawberry/chyt/<alias>/artifacts/system_log_tables/query_log`.

The `query_log` folder contains:

- one or more tables named from `0` to `n`;
- a link to the latest table `latest` — for example, `//sys/strawberry/chyt/<alias>/artifacts/system_log_tables/query_log/n`.

New tables in `query_log` appear when the schema changes — for example, when new fields are added or data types are modified. This can happen when you upgrade or downgrade the CHYT version. For example, if the CHYT version in the clique is upgraded from 2.16 to 2.17:

- a new table is automatically created with the next sequential number (if there was table `0`, table `1` will appear);
- the `latest` link is automatically switched to the latest table version (in this case, to table `1`).

If you then downgrade the CHYT version back to 2.16, the system will add a new table numbered `2` and start writing logs to it.

## How to view query logs { #instruction }

1. Open the [web interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui) of the clique.
1. In the [Tabs panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs), select the **Query Logs** tab.
1. Switch to the tab that opens in your browser. It will display the **Strawberry operation** interface with the current log table version.
1. Analyze queries using standard SQL queries against this table.

## Analyzing the log table { #analysis }

To properly search for and analyze information in the log table, it’s important to understand that queries in CHYT are processed in a distributed manner: they are split into phases (primary and secondary queries). Query logs tables contain information about all queries — both primary and secondary — so during analysis you need to be able to tell them apart.

A **primary** user query arrives at the clique instance, which is called the **coordinator**. The coordinator performs the initial query preparation, after which subqueries — **secondary** queries — are generated and sent from the coordinator to the other instances.

### How to find the required queries in Query logs { #filter-queries }

To find information about primary queries only, use the following filter:

```sql
WHERE query_id = <my_query_id> -- where <my_query_id> is the ID of the query you’re interested in
```

Secondary queries have their own unique `query_id` values, so they won’t be included in the result set. Also, in a primary query, the flag column `is_initial_query` will be equal to 1.

To find secondary queries associated with a primary one, use:

```sql
WHERE initial_query_id = <my_query_id> -- filters both primary and secondary queries
AND NOT is_initial_query -- excludes primary queries, leaving only secondary ones
```

### Query parameters in the Query logs table { #params }

The columns in the log table partially match the columns in {{clickhouse}} log tables and include additional CHYT query parameters.

{% note info %}

To decode {{clickhouse}} log names, refer to the [ClickHouse documentation](https://clickhouse.com/docs/ru/operations/system-tables/query_log).

{% endnote %}

CHYT parameters are marked in the Query logs table with the `chyt_` prefix.

#|
|| **Option** | **Data type** | **Description** ||
|| `chyt_instance_cookie` | `Optional[Int64]` | The instance ID that the log row belongs to. Matches the `job_cookie` of the job within which the instance is running. Contains a value in the range `[0, n]`, where `n` is the clique size ||
|| `chyt_instance_id` | `Optional[String]` | Instance ID; contains the `job_id` of the job, which is unique within an operation. For example, after restarting any job, the new one may have the same cookie but a new `id` ||
|| `chyt_instance_fqdn` | `Optional[String]` | Contains the fully qualified domain name (FQDN) of the job/instance ||
|| `chyt_version` | `Optional[String]` | The CHYT version used to record this log line ||
|| `chyt_secondary_query_ids` | `Optional[List[String]]` | Relevant only for `type = 'QueryFinish'` and `is_initial_query`, i.e., for the final log line of a primary query. Contains a list of all `id`s of secondary queries launched within the primary query ||
|| `chyt_query_runtime_variables` | `Optional[Yson]` | Contains YSON with `key‑value` pairs for simple query execution analysis. For example, it’s used to determine whether optimization X occurred. Relevant only for `type = 'QueryFinish'`.

{% cut "YSON example" %}

    ```json
    {
        "try_optimize_distinct_read" = %false;
        "use_input_specs_pulling" = %false;
        "use_min_max_optimization" = %false;
        "use_read_range_inferring" = %false;
    }
    ```
{% endcut %}

||
|| `chyt_query_statistics` | `Optional[Yson]` | Contains YSON with quantitative measurements of various query metrics. Relevant only for `type = 'QueryFinish'` ||
|#

#### The `chyt_query_statistics` parameter

This parameter contains a large YSON and has a different structure for primary and secondary queries within a single CHYT query. The most important options are described below.

{% cut "Main options for primary queries" %}

- `input_fetcher` — contains quantitative information about the data that the coordinator received from the master and will distribute to the other clique instances for further execution. These are not the data themselves, but **descriptors** that help understand where and how much data needs to be read. This key may contain two additional ones:

  - `filtered_data_slices` — contains information about how much data the coordinator filtered out, i.e. removed from the selection. This happens according to the query filter;
  - `data_slices` — the opposite value, i.e. how much data the coordinator kept for reading.

  ```yson
  {
      "data_slices" = {
          "data_weight" = {
              "count" = 1;
              "last" = 936245229;
              "max" = 936245229;
              "min" = 936245229;
              "sum" = 936245229;
          };
          "row_count" = {
              "count" = 1;
              "last" = 9694599;
              "max" = 9694599;
              "min" = 9694599;
              "sum" = 9694599;
          };
      };
      "filtered_data_slices" = {
          "data_weight" = {
              "count" = 2;
              "last" = 243363;
              "max" = 20320269;
              "min" = 243363;
              "sum" = 20563632;
          };
          "row_count" = {
              "count" = 2;
              "last" = 2563;
              "max" = 88233;
              "min" = 2563;
              "sum" = 90796;
          };
      };
  }
  ```

  Each key contains a `row_count` counter that shows how many measurements were taken — `count` — and their aggregates.

- `phase_duration_us` — information about the execution duration of various query phases:

  - `Start` — the period from when the instance receives the query over the network until it actually starts executing;
  - `Preparation` — the phase when the coordinator collects metadata about the query and composes secondary queries;
  - `Execution` — the phase that starts when the coordinator sends secondary queries to the instances and ends when the coordinator sends the entire result to the user. It includes the time the coordinator spends receiving responses from secondary queries and processing them locally as needed.

  ```yson
  {
      "Execution" = {
          "count" = 1;
          "last" = 13367;
          "max" = 13367;
          "min" = 13367;
          "sum" = 13367;
      };
      "Preparation" = {
          "count" = 1;
          "last" = 112808;
          "max" = 112808;
          "min" = 112808;
          "sum" = 112808;
      };
      "Start" = {
          "count" = 1;
          "last" = 318883;
          "max" = 318883;
          "min" = 318883;
          "sum" = 318883;
      };
  }
  ```

{% endcut %}

{% cut "Main options for secondary queries" %}

- `phase_duration_us` — contains a breakdown by phases. It differs from `phase_duration_us` in the primary query only in the semantics of the preparation phase: during this phase, the secondary instance only parses the input from the coordinator and prepares for execution;
- `granule_min_max_filter` — when reading data in secondary queries, there is a step for skipping chunks using `min`/`max` statistics. This option contains information about how many chunks were reviewed and how many were skipped;

  ```yson
  {
      "can_skip" = {
          "count" = 92;
          "last" = 1;
          "max" = 1;
          "min" = 0;
          "sum" = 42;
      };
  }
  ```

- `secondary_query_source` — an option with information about reading data in secondary queries. It contains the following child options:

  - `block_bytes`, `block_rows`, `block_columns` — aggregates for the read data that eventually go to the {{clickhouse}} engine, in different metrics;
  - `steps` — CHYT can read and convert different columns in stages. This logic is similar to the `PREWHERE` predicate in {{clickhouse}}, where filtering columns are read first, and then, depending on their content, the rest. In this section, you can see how many steps were executed and how much data passed through them;
  - `columnar_conversion_cpu_time_us`, `columnar_conversion_wall_time_us` — CHYT has to convert the read data from the {{product-name}} format to the {{clickhouse}} format. Conversion sometimes takes a significant amount of time, and these options show exactly how much;
  - `chunk_reader` — the main section with information about reading data from {{product-name}}. The main sub‑options are `data_bytes_read_from_cache` and `data_bytes_read_from_disk`, which show how much data was taken from the cache and how much was read from disks;
  - `wait_ready_event_time_us` — this option is semantically related to `chunk_reader` and shows how long it took to wait for the next data portion to start converting it.

  ```yson
  {
      "block_bytes" = {
          "count" = 3661;
          "max" = 4080068;
          "min" = 0;
          "sum" = 7888434964;
      };
      "block_columns" = {
          "count" = 3661;
          "max" = 8;
          "min" = 0;
          "sum" = 29272;
      };
      "block_rows" = {
          "count" = 3661;
          "max" = 10000;
          "min" = 0;
          "sum" = 31148625;
      };
      "chunk_reader" = {
          "data_bytes_read_from_cache" = {
              "count" = 2;
              "max" = 297136447;
              "min" = 149780474;
              "sum" = 446916921;
          };
          "data_bytes_read_from_disk" = {
              "count" = 2;
              "max" = 0;
              "min" = 0;
              "sum" = 0;
          };
          "data_bytes_transmitted" = {
              "count" = 2;
              "max" = 0;
              "min" = 0;
              "sum" = 0;
          };
          "data_io_requests" = {
              "count" = 2;
              "max" = 0;
              "min" = 0;
              "sum" = 0;
          };
          "meta_bytes_read_from_disk" = {
              "count" = 2;
              "max" = 0;
              "min" = 0;
              "sum" = 0;
          };
      };
      "columnar_conversion_cpu_time_us" = {
          "count" = 3659;
          "max" = 14601;
          "min" = 36;
          "sum" = 4846058;
      };
      "conversion_sync_wait_time_us" = {
          "count" = 3659;
          "max" = 19267;
          "min" = 106;
          "sum" = 6489348;
      };
      "idle_time_us" = {
          "count" = 3663;
          "max" = 4150;
          "min" = 8;
          "sum" = 145923;
      };
      "processed_reader_count" = {
          "count" = 2;
          "max" = 2;
          "min" = 1;
          "sum" = 3;
      };
      "read_count" = {
          "count" = 2;
          "max" = 2441;
          "min" = 1220;
          "sum" = 3661;
      };
      "read_impl_us" = {
          "count" = 3661;
          "max" = 34904;
          "min" = 122;
          "sum" = 6904768;
      };
      "step_count" = {
          "count" = 2;
          "max" = 1;
          "min" = 1;
          "sum" = 2;
      };
      "steps" = {
          "0" = {
              "block_bytes" = {
                  "count" = 3659;
                  "max" = 4080068;
                  "min" = 6268;
                  "sum" = 7888434964;
              };
              "block_columns" = {
                  "count" = 3659;
                  "max" = 8;
                  "min" = 8;
                  "sum" = 29272;
              };
              "block_rows" = {
                  "count" = 3659;
                  "max" = 10000;
                  "min" = 30;
                  "sum" = 31148625;
              };
          };
      };
      "total_rows" = {
          "count" = 3661;
          "max" = 10000;
          "min" = 0;
          "sum" = 31148625;
      };
      "wait_ready_event_time_us" = {
          "count" = 36;
          "max" = 32988;
          "min" = 2;
          "sum" = 144348;
      };
  }
  ```

{% endcut %}
