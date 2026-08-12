# Worker Groups in {{product-name}} Flow

## Description {#desc}

Worker Groups are a mechanism for grouping [workers](../../../flow/concepts/glossary.md#worker) and binding [computations](../../../flow/concepts/glossary.md#stream-and-computation) to specific worker groups. This lets you:

- Isolate the execution of different computations on separate sets of workers.
- Configure balancing parameters separately for each group.
- Optimize resource usage in heterogeneous clusters.

## Configuring Worker Groups {#setup}

### Worker configuration {#worker-config}

You assign workers to groups via the `YT_FLOW_WORKER_GROUPS` environment variable. The value is a comma-separated list of group names.

Examples:

```bash
# The worker belongs to a single group
export YT_FLOW_WORKER_GROUPS="gpu"

# The worker belongs to multiple groups
export YT_FLOW_WORKER_GROUPS="cpu,memory-intensive"

# The worker has no groups (default)
# YT_FLOW_WORKER_GROUPS is unset or set to an empty string
```

{% note info %}

If the variable is unset or set to an empty string, the worker is considered part of the default group and can run computations without a specified group.

{% endnote %}

### Computation configuration {#computation-config}

You bind computations to a worker group using the `worker_group` parameter in the [spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec).

Example computation spec:

```yson
{
    "computations" = {
        "my_computation" = {
            "computation_class_name" = "MyComputation";
            "worker_group" = "gpu";  # This computation runs only on workers in the "gpu" group
            "input_stream_ids" = ["input_stream"];
            "output_stream_ids" = ["output_stream"];
        };
    };
}
```

Rules:

- A computation runs only on workers that list the corresponding group in `YT_FLOW_WORKER_GROUPS`.
- If `worker_group` isn’t specified, the computation can run only on workers that don’t list any group in `YT_FLOW_WORKER_GROUPS`.
- If no available workers exist for the specified group, the computation won’t run (see [Computation doesn’t run](#computation-does-not-run)).

## Balancer settings for groups {#balancer-config}

### Common balancer settings {#common-balancer-config}

By default, all worker groups use the common balancer settings defined in the `job_manager` section of the pipeline’s dynamic [spec](../../../flow/concepts/glossary.md#pipeline):

```yson
{
    "job_manager" = {
        "use_cpu_aware_balancer" = %true;
        "rebalance_delay_after_pipeline_sync" = 30000;  # 30s in milliseconds
        "rebalance_target_deviation" = 0.05;
        # ... other balancer parameters
    };
}
```

### Overriding settings for a group {#group-override}

You can override balancer settings for a specific worker group using the `worker_group_override` parameter in the `job_manager` section:

```yson
{
    "job_manager" = {
        # Common balancer settings
        "use_cpu_aware_balancer" = %true;
        "rebalance_delay_after_pipeline_sync" = 30000;  # 30s in milliseconds
        "rebalance_target_deviation" = 0.05;

        # Override settings for specific groups
        "worker_group_override" = {
            "gpu" = {
                # Use more aggressive balancing for GPU workers
                "rebalance_target_deviation" = 0.02;
                "rebalance_hot_mode_coeff" = 3.0;
                "rebalance_sync_period" = 5000;  # 5s in milliseconds
            };

            "memory-intensive" = {
                # Balance less frequently for memory-intensive workers
                "rebalance_delay_after_pipeline_sync" = 60000;  # 60s in milliseconds
                "rebalance_sync_period" = 20000;  # 20s in milliseconds
            };
        };
    };
}
```

`worker_group_override` format:

- Key: the worker group name (string).
- Value: balancer settings (all parameters from [`TDynamicJobBalancerSpec`](../../../flow/concepts/spec.md#jobmanager)).

For a full description of the parameters, see the [JobManager documentation](../../../flow/concepts/spec.md#jobmanager).

## Usage examples {#examples}

### Example 1: Separating CPU and GPU computations {#example-1}

```yson
# Static pipeline spec
{
    "computations" = {
        "preprocessing" = {
            "computation_class_name" = "PreprocessingComputation";
            # No worker_group — runs on any workers
        };

        "gpu_inference" = {
            "computation_class_name" = "GPUInferenceComputation";
            "worker_group" = "gpu";
        };

        "postprocessing" = {
            "computation_class_name" = "PostprocessingComputation";
            "worker_group" = "cpu";
        };
    };
}
# Dynamic pipeline spec
{
    "job_manager" = {
        "worker_group_override" = {
            "gpu" = {
                # GPU workers are expensive, so balance more carefully
                "rebalance_target_deviation" = 0.01;
                "rebalance_sync_period" = 15000;  # 15s in milliseconds
            };
        };
    };
}
```

Example worker configuration:

```bash
# GPU workers
export YT_FLOW_WORKER_GROUPS="gpu"

# CPU workers
export YT_FLOW_WORKER_GROUPS="cpu"
```

### Example 2: Isolating critical computations {#example-2}

```yson
# Static pipeline spec
{
    "computations" = {
        "critical_computation" = {
            "computation_class_name" = "CriticalComputation";
            "worker_group" = "critical";
        };

        "regular_computation" = {
            "computation_class_name" = "RegularComputation";
            # No worker_group
        };
    };
}
# Dynamic pipeline spec
{
    "job_manager" = {
        "minimum_worker_count" = 10;

        "worker_group_override" = {
            "critical" = {
                # Use stable balancing for critical computations
                "rebalance_delay_after_pipeline_sync" = 120000;  # 120s in milliseconds
                "async_balancing" = %false;
            };
        };
    };
}
```

### Example 3: A worker in multiple groups {#example-3}

```bash
# The worker can handle both CPU and memory-intensive tasks
export YT_FLOW_WORKER_GROUPS="cpu,memory-intensive"
```

```yson
# Static pipeline spec
{
    "computations" = {
        "cpu_computation" = {
            "worker_group" = "cpu";
        };

        "memory_computation" = {
            "worker_group" = "memory-intensive";
        };

        # Both computations can run on this worker
    };
}
```

## Monitoring and debugging {#monitoring}

### Checking worker groups {#checking-worker-groups}

You can view worker group information via the yt interface:

```bash
# List active workers and their groups
{{yt-cli}} --proxy=<proxy> flow get-flow-view <//home/path/to/pipeline> --view-path="/state/workers"
```

Example output:

```yson
{
    "[2a02:6b8:c42:cec3:7800:18:6687:0]:81" = {
        "worker_groups" = [];  # No groups assigned, default group
        #... other fields
    };
    "[2a02:6b8:c42:d6ca:7800:18:22a7:0]:81" = {
        "worker_groups" = [
            "GPU_1";  # A specific group is assigned
        ];
        #... other fields
    };
    #...
}
```

### Common issues {#troubleshooting}

#### Computation doesn’t run {#computation-does-not-run}

Symptoms:

- On the pipeline graph page, all its streams show `0 pcs/s 0 B/S`.
- The node with the computation shows `CPU Usage –` and `RAM Usage 0 B`.
- On the pipeline page `Computations/<Name of this computation>`, the table has rows, but the `Worker` column is empty for all of them.

Possible causes:

- A typo in the group name.
- No available workers with the specified group.

How to fix it:

1. Check that the group name is spelled correctly in the spec and the environment variable (they must match, including case).
2. Check for available workers with the needed group using [{{yt-cli}}](#checking-worker-groups).
3. Review the controller logs for balancing errors.
4. Review the logs of workers in that group for operational errors.

## See also

- [YT Flow core concepts](../../../flow/concepts/glossary.md)
- [Pipeline spec](../../../flow/concepts/spec.md)
- [Load balancing](../../../flow/concepts/spec.md#jobmanager)