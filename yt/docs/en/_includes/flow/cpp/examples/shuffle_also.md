## See also

- [Quick start (C++)](../../../../flow/cpp/getting-started.md)
- [Computation (C++)](../../../../flow/cpp/computation.md)

- You must include the `value` column (and its hash) in `group_by_schema` to group by `value`.
- List all streams — `event_a`, `event_b`, `event_c`, and `event_d` — in `input_stream_ids` to read all resulting streams. From a business logic perspective, this isn’t the most meaningful action. However, the original goal of this pipeline was to test the `exactly-once` guarantees, even in the case of a `Swift` chain.
- `TReducer` implements `IProcessFunction` and is hosted by `TProcessFunctionComputation`. The adapter persists `input_message_ids` and `output_messages` in {{product-name}}, but this example produces no output messages. In effect, the pipeline saves the `reader` metadata, the (`message_id`, `key`) metadata for each `reducer` input message, and the `value => count` table. The intermediate passthrough computations don’t interact with {{product-name}}.

### DynamicSpec

- Fill the `dynamic_spec/computations/<computation_id>/parameters/desired_partition_count` field for each `computation` except `reader`. The `test_shuffle.py` test changes the number of partitions.
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
