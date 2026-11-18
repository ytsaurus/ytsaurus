# Jobs

A job is a unit of computation within a {{product-name}} operation. An operation distributes data processing across jobs, each running on a single cluster node. For example, jobs in [Map](map.md), [Reduce](reduce.md), and [MapReduce](mapreduce.md) operations run user code.

This article describes job environments, file and data transfer, resources, and custom statistics. For resource allocation between operations, see [Scheduler and pools](../scheduler/scheduler-and-pools.md).

<a id="anatomiya-dzhoba-v-yt"></a>

## Job structure {#job-anatomy}

A job consists of one or more processes. Every job has a system process called `JobProxy`. For jobs with user code, it starts a `UserJob` process. This process runs the mapper or reducer command and can create child processes.

`JobProxy` retrieves input data from cluster nodes and passes it to the user process through file descriptors. It also receives processing results through file descriptors. In addition, `JobProxy` tracks the user process's resource consumption and reports it to the node that runs the job.

![JobProxy transfers data between cluster nodes and the UserJob process](../../../../_images/job_anatomy_en.svg =800x)

## Job environment {#job-environment}

The user command runs in the `sandbox` working directory. Files passed in the operation specification are also available here. The process's home directory is `home`.

Process isolation and file system access depend on the cluster's execution environment. Supply required files through the specification instead of relying on files or programs on the host that submits the operation. Use an image or layers to configure the system environment.

For temporary files, you can request tmpfs, a file system that stores its contents in memory. Use `tmpfs_path` to set its path within `sandbox` and `tmpfs_size` to set its size. For multiple volumes, use `tmpfs_volumes`. Space used in tmpfs counts toward the job's memory consumption. For details, see [Operation options](operations-options.md#user_script_options).

### Root file system {#rootfs}

Use `docker_image` or `layer_paths` in the user script specification to set the container's root file system. The availability of these options depends on the cluster's execution environment configuration. For details, see [Root file system images](../layers/layer-paths.md).

## Files available in jobs {#job-files}

To pass a file to a job, add its Cypress path to `file_paths` in the user script specification. The file becomes available at the root of `sandbox` before the job starts. By default, it is a read-only symbolic link. Set `copy_files` to copy files into `sandbox`.

The file name is selected in this order:

1. The `file_name` attribute on the path passed in the specification.
1. The file's own `file_name` attribute in [Cypress](../../storage/cypress.md).
1. The last component of the file's Cypress path.

You can also pass a table through `file_paths`. For a table, you must set the `format` path attribute, which specifies the format for saving the table as a file.

To pass a directory hierarchy, package it as an archive, pass the archive to the job, and extract it in user code.

## Job resources {#job-resources}

Set `memory_limit`, `cpu_limit`, and `gpu_limit` in the [user script specification](operations-options.md#user_script_options). These options apply to a single job.

### Memory {#memory}

`memory_limit` limits a job's memory consumption in bytes. The default is 512 MiB. Exceeding the limit can terminate the job with a `Memory limit exceeded` error.

The scheduler uses a [memory reserve](../scheduler/memory-digest.md) to schedule jobs. If the reserve is below the limit, the job may need additional node memory while running. Include the user process's memory and files stored in tmpfs when setting the limit.

### CPU {#cpu}

`cpu_limit` specifies the number of CPU cores that the scheduler accounts for when scheduling a job. The default is 1. You can use fractional values, such as `0.5`, and run multithreaded code.

In a Porto environment, the container's CPU weight is proportional to `cpu_limit` by default. Use `set_container_cpu_limit` to also set the container's CPU limit. Actual CPU time allocation depends on the environment configuration and node load.

Increasing `cpu_limit` raises the resource request per job and may reduce the number of jobs that run concurrently in the operation.

### GPU {#gpu}

On clusters with GPUs, `gpu_limit` specifies the number of GPUs required per job. If GPU nodes belong to separate pool trees, select the appropriate tree in `pool_trees`. To list pool trees, run:

```shell
yt --proxy "<cluster_proxy>" list //sys/pool_trees
```

The tree selection depends on the cluster configuration. Ask your administrator which trees provide the GPUs that you need.

<a id="peremennye-okruzheniya-v-dzhobah"></a>

## Environment variables {#environment-variables}

The following variables are available to the user process:

- `YT_OPERATION_ID`: The operation ID.
- `YT_JOB_ID`: The job ID.
- `YT_JOB_INDEX`: The index of the started job. It can exceed `job_count` if some jobs failed and restarted.
- `YT_START_ROW_INDEX`: The start of the index range allocated to a job with tabular input. For jobs within the same processing stage, ranges spanning the number of input rows do not overlap. This is not the actual index of the first row in the input table. Input rows may be nonconsecutive, and a restarted job receives a new range.

Pass your own variables through the `environment` dictionary in the user script specification. For example, `environment = {MODE = "test"}` adds the `MODE` variable with the value `test`.

{% note warning %}

Do not pass tokens, passwords, or other secrets through `environment`. These values are part of the operation specification and may be visible to other users.

{% endnote %}

For secrets, use the `secure_vault` dictionary in the [operation specification](operations-options.md). For example, the entry `key = "value"` adds the `YT_SECURE_VAULT_key` environment variable with the value `value`. The `YT_SECURE_VAULT` variable contains the entire dictionary in YSON format. The contents of `secure_vault` are removed from the publicly visible specification and stored separately.

The cluster configuration may add other environment variables. If you have access to the relevant Cypress node, you can view the controller agent configuration with:

```shell
yt --proxy "<cluster_proxy>" get "//sys/controller_agents/instances/<instance>/orchid/controller_agent/config/environment"
```

## Job I/O {#job-io}

The `job_io` sections of the [operation specification](operations-options.md) contain I/O settings:

- `table_reader` and `table_writer`: [Table data reader and writer settings](../../storage/io-configuration.md).
- `error_file_writer`: Settings for writing the stderr file.
- `control_attributes`: Settings for [control attributes](../../storage/io-configuration.md#control_attributes), which carry metadata alongside the data.
- `buffer_row_count`: The maximum number of records that `JobProxy` can buffer when passing input data to the job.
- `pipe_io_pool_size`: The size of the data processing pool in `JobProxy`.

### File descriptors {#descriptors_in_job}

`UserJob` and `JobProxy` exchange data through these descriptors:

- `0` (`stdin`): Data from all input tables, subject to the operation's guarantees. Control attributes identify a record's source table.
- `1` (`stdout`), `4`, `7`, …, `3k + 1`: Data written to output tables. Here, `k` is the zero-based table index. You can also select the output table using [table switches](table-switch.md).
- `2` (`stderr`): The standard error stream.
- `5`: [Custom statistics](#user_stats).
- `8`: Profiling data.

## Job statistics {#job_stat}

{{product-name}} collects, aggregates, and displays job statistics. You can view them on the **Statistics** tab of the operation page. For a list of system metrics and diagnostic examples, see [Job statistics](../../problems/jobstatistics.md).

### Aggregation {#statistics-aggregation}

When a job finishes, it sends statistics as a YSON document. Each statistic is identified by the path to a numeric value in the document tree. Values of types `i64` and `ui64` are supported.

Statistics are grouped by job type, such as `map` or `reduce`, and state, such as `completed` or `failed`. The sum, minimum, maximum, and value count (`sum`, `min`, `max`, `count`) are calculated for each group. Aggregated statistics are available in the operation's `@progress/job_statistics` attribute in Cypress.

### Custom statistics {#user_stats}

User code can export custom metrics through file descriptor `5`. The stream must be a YSON list fragment: a sequence of dictionaries separated by semicolons. If a metric occurs multiple times, its values are aggregated within the job. Across a group of jobs, the sums are summed, the minima are minimized, and the maxima are maximized.

For example, a job can measure initialization, data processing, and finalization times in milliseconds and write a single dictionary:

```yson
{init_time = 10; run_time = 250; finalize_time = 5};
```

You can also send these metrics as separate records as the job runs:

```yson
{init_time = 10};
{run_time = 250};
{finalize_time = 5};
```

The metric tree can have multiple levels:

```yson
{time = {init = 10; run = 250}};
{time = {finalize = 5}};
```

#### Limits {#custom-statistics-limits}

- A custom statistic's full path, including the system prefix `custom`, must not exceed 512 bytes.
- The default limit is 128 custom statistics. Use `custom_statistics_count_limit` in the [user script specification](operations-options.md#user_script_options) to change it. The maximum is 1024.

### Python API {#python-statistics}

In Python jobs, use `yt.wrapper.write_statistics(dict)`. It writes a dictionary of metrics to file descriptor `5`.

For more information about the client, see [Python API](../../../api/python/start.md).

## Diagnostics and debugging {#job-debugging}

Use [Job Shell](../../problems/jobshell-and-slowjobs.md) to connect to a running job. Use [Job Tool](../../problems/jobtool.md) to reproduce a job locally.
