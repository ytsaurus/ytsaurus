## See also

- [Quick start (C++)](../../../../flow/cpp/getting-started.md)
- [Computation (C++)](../../../../flow/cpp/computation.md)

- You must include the `value` column (and its hash) in `group_by_schema` to group by `value`.
- List all streams — `event_a`, `event_b`, `event_c`, and `event_d` — in `input_stream_ids` to read all resulting streams. From a business logic perspective, this isn’t the most meaningful action. However, the original goal of this pipeline was to test the `exactly-once` guarantees, even in the case of a `Swift` chain.
- Since `TReducer` inherits from `TTransformComputation`, `input_message_ids` and `output_messages` are always saved in {{product-name}}. However, `output_messages` is empty here. Essentially, this pipeline saves only the metadata within `reader` in {{product-name}}, the metadata (`message_id` and `key`) for each input message, and the `value => count` table within `reducer`. The intermediate `computation` components don’t interact with {{product-name}} at all.

### DynamicSpec

- Fill the `dynamic_spec/computations/<computation_id>/desired_partition_count` field for each `computation` except `reader`. The `test_shuffle.py` test changes the number of partitions.
- Specify the required number of threads for running all jobs in `dynamic_spec/job_tracker/job_threads`.

### Config for running

- Key settings for running: `cluster_url`, `proxy_role`, `path`, `rpc_proxy`, and `monitoring_port`.
- Set `controller/scheduler_period` to 200 for this specific test — in reality, the default value should be sufficient.
- `logging` contains the logging settings.

```yson
{
    "cluster_url" = "cluster_name";
    "path" = "//path/to/pipeline";
    "rpc_port" = 81;
    "monitoring_port" = 80;
    "controller" = {
        "scheduler_period" = 200;
    };
    "logging" = {
        "suppressed_messages" = [
        ];
        "rules" = [
            {
                "exclude_categories" = [
                    "Bus";
                    "Dns";
                    "Concurrency";
                    "QueryClient";
                    "Profiling";
                    "RpcClient";
                    "Monitoring";
                    "Net";
                    "Solomon";
                    "Jaeger";
                    "RpcProxyClient";
                    "RpcServer";
                    "Dns";
                    "BufferMetrics";
                ];
                "min_level" = "debug";
                "writers" = [
                    "Stderr";
                ];
            };
        ];
        "writers" = {
            "Stderr" = {
                "type" = "file";
                "file_name" = "/path/to/file.log";
            };
        };
    }
}
```