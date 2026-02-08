# Operation options

This section describes general properties of various operations, as well as user script parameters on paths, and file attributes.

## General options for all operation types { #common_options }

The parameters below can be indicated in any operation's specification root.
The default values, if set, are provided in parentheses:

- `pool` — name of the [compute pool](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) where the operation runs. If this parameter is not specified, the pool name defaults to the name of the {{product-name}} user who started the operation.
- `weight`(1.0) — weight of the started operation. It determines how much of the pool's compute quota is allocated to the operation. For more information, see [Scheduler and pools](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).
- `pool_trees` — list of [pool trees](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) where the operation's jobs are run. The default pool tree is configured by the cluster administrator.
- **DEPRECATED** `tentative_pool_trees` — list of test pool trees that will be used for running the operation if there is no performance degradation.
- **DEPRECATED** `use_default_tentative_pool_trees` — enable the use of all trees in the cluster that are `tentative` relative to the default pool tree.
- **DEPRECATED** `tentative_tree_eligibility` — configuration for running the operation's jobs in `tentative` trees:
  - `sample_job_count` (10) — number of jobs to be run as samples.
  - `max_tentative_job_duration_ratio` (10) — maximum allowed latency of job execution.
  - `min_job_duration` (30) — job duration threshold in `tentative` trees. If the average job duration in the `tentative` tree exceeds this threshold, new jobs of the operation are not run in that `tentative` tree.
  - `ignore_missing_pool_trees` (false) — allows ignoring `tentative` trees that are missing on the cluster.
- `resource_limits` — fair-share settings that will apply to all jobs of the operation in aggregate. For more information, see [Operation resources configuration](../../../../user-guide/data-processing/scheduler/pool-settings.md#operations).
- `time_limit` — general time limit on the operation (in milliseconds). If the operation does not complete within the specified time, it will be force-stopped by the scheduler.
- `acl` — Access Control List, which sets out access permissions for the operation. The `manage` permission is associated with mutating actions on the operation and its jobs: abort, suspend, job shell, and so on. In turn, the `read` permission relates to non-mutating actions like get_job_stderr and get_job_input_context. This `acl` will be supplemented with records on the user that ran the operation and {{product-name}} administrators.
- `max_stderr_count` (10) — limit on the amount of stored data from the standard error stream (stderr) of jobs, by default taken from cluster settings that usually have it equal to 10. The maximum value is 150.
- `max_failed_job_count` (10) — number of failed jobs past which the operation is considered failed. By default, this value is taken from the cluster settings, where it is usually set to 10.
- `unavailable_chunk_strategy` and `unavailable_chunk_tactics` — determine further action in case intermediate calculation results are unavailable. For more information, see [Operation types](../../../../user-guide/data-processing/operations/overview.md#chunk_strategy).
- `chunk_availability_policy` — system behavior if erasure chunks are unavailable. For more information, see [Operation types](../../../../user-guide/data-processing/operations/overview.md#chunk_erasure_strategy).
- `scheduling_tag_filter` — filter for selecting cluster nodes upon job launch. The filter is a [logical expression](../../../../admin-guide/node-tags.md) that includes `&` (logical AND), `|` (logical OR), and `!` (logical NOT) operators and parentheses. The possible values of cluster node tags act as variables in the expression. A tag is a marker placed on a cluster node. Markup is done by the {{product-name}} service when configuring a cluster. If the cluster node has a tag specified in the expression, the expression takes the "true" value, if not — the value is "false". If the expression as a whole returns "false", then that cluster node is not selected to run the job.
- `max_data_weight_per_job` (200 GB) — maximum allowed size of input data per job (in bytes). This option sets a hard cap on the job size. If the scheduler is not able to generate a job with a smaller size, the operation ends with an error.
- `data_weight_per_job` (256 MB) — recommended size of input data per job (in bytes). This option is useless for composite operations such as MapReduce. which instead call for the following options:
    - `data_weight_per_map_job`
    - `data_weight_per_sort_job`
    - `data_weight_per_partition_job`
    - `data_weight_per_sorted_merge_job`
    - `data_weight_per_reduce_job`
- `secure_vault` — values from this map end up in the environments of all the user jobs of a given operation and can't be viewed by unauthorized users, unlike the `environment` section in the user job specification. In particular, the entire passed-in map in the YSON format is written to the `YT_SECURE_VAULT` environment variable, and, to save time, "value" is written to the `YT_SECURE_VAULT_key` environment variable for each `key=value` pair from the map. This only happens for elementary type values, such as int64, uint64, double, boolean, or string.
- `stderr_table_path` — for specifying the path to an existing table, which must be created outside of transactions. If this option is enabled, the complete data from the standard error stream (stderr) of all jobs (except for the aborted ones) is written to the specified table. That table will have the following columns:
  - `job_id` — job ID.
  - `part_index` — for large error messages, the stderr message of a single job can be split into several segments that are stored in different rows. This column contains the numbers corresponding to these segments.
  - `data` — data from the standard error stream.
- `redirect_stdout_to_stderr` — option for redirecting a standard output stream (stdout) to a standard error stream (stderr).
- `core_table_path` — path to an existing table for writing core dump. The table format is identical to that described for `stderr_table_path`. {% if audience == "internal" %}For more information about using this option, see [Debugging MapReduce programs](../../../../user-guide/problems/mapreduce-debug.md).{% endif %}
- `input_query` — for the [Map](../../../../user-guide/data-processing/operations/map.md) and [Merge](../../../../user-guide/data-processing/operations/merge.md) operations, you can pre-filter each input row using the [query language](../../../../user-guide/dynamic-tables/dyn-query-language.md). Because the input table is already known, and it is assumed that each row will be individually filtered, the query syntax changes and gets simplified to `* | <select-expr-1> [AS <select-alias-1>]... [WHERE <predicate-expr>]`. Speeifically, you cannot use `GROUP BY` and `ORDER BY`. Queries need strictly schematized data, so input tables must have a strict schema. In the [Merge](../../../../user-guide/data-processing/operations/merge.md) operation, the schema of the final data in the query is incorporated when outputting the schema for the output table.

  {% note info "Note" %}

  If the `input_query` parameter is specified, the `enable_row_index` and `enable_range_index` attributes are excluded from the job.

  {% endnote %}

- `omit_inaccessible_columns` (false) — skips columns that cannot be accessed. By default, operations in this scenario end with an error at startup.
- `omit_inaccessible_rows` (false) — skips rows that cannot be accessed. By default, operations in this scenario end with an error at startup.
- `alias` — alias of the operation. Makes the operation available in search and through the commands (e.g. `get-operation`) on the specified row.
  The alias must start with the `*` symbol. Only one operation with this given alias can be run simultaneously. You can run an operation with another alias or rerun the operation with the same alias after it is completed. The `get-operation` returns the recent operation.
- `job_node_account` (tmp_jobs) — account under which chunks of files with the error log (stderr) and input context are stored.
- `suspend_operation_if_account_limit_exceeded` (false) — suspends (pauses) the operation in case of the "Account limit exceeded" error in a job.
- `enable_job_splitting` (true) — determines if the scheduler is allowed to adaptively split user jobs that take a long time.
- `fail_on_job_restart` (false) — force-interrupts the operation if a started job terminates in any state other than success (such as abort or fail). Useful for operations that do not tolerate job restarts.
- `suspend_on_job_failure` (false) — suspends (pauses) the operation if any job fails. When enabled, instead of failing the operation after reaching `max_failed_job_count`, the operation is suspended for investigation. The operation can be resumed after addressing the issue.
- `title` — operation title. The passed-in text is shown in the web interface.
- `started_by` — a map describing the client that started the operation. This field is not associated with a strict schema. The specified information is shown on the operation page in the web interface.
- `annotations` — a map where the user can write any structured information related to the operation. The `annotations` contents are saved to the operation archive as [YSON](../../../../user-guide/storage/formats.md#yson) and can then be used to look up and identify operations within the archive.

  {% note info "Note" %}

  Adding user information to the specification root is not recommended: This leads to user data appearing in Unrecognized Spec. In the future, this practice will be prohibited.

  {% endnote %}

- `description` — a map similar to `annotations` that also appears on the operation page in the web interface. It is best to add only small amounts of human-readable information to this section to avoid overcrowding the operation page.
- `use_columnar_statistics` (false) — determines whether to use columnar statistics to accurately estimate the size of jobs when starting operations on tables with column samples. This option improves automatic splitting into jobs, with columns taken from the input data forming a small fraction of the input table size (previously, column selection was ignored, and the resulting jobs were very small, which had to be rectified with the `data_weight_per_job` option).
- `sampling` — configuration of large-block sampling of input data (not available for Sort, RemoteCopy, and Vanilla operations). This option is obsolete: use [read sampling](../../../../user-guide/storage/io-configuration.md#sampling) instead. The `sampling` option supports only block-based sampling. It works to proportionally decrease the amount of data read from disk, which, under ideal conditions, should result in a proportional increase in operation speed. The trade-off is that the resulting process is not really sampling in its true mathematical sense, but only an approximation: the data is divided into fairly large blocks of successive rows, and then these blocks are sampled whole. The result may be adequate for applied calculations on sufficiently large tables. The parameter value is set by a map with the following keys:
  - `sampling_rate` (null) — share of sampled rows.
  - `io_block_size` (16 MB) — minimum sampling block size. We do not recommend changing the default value.
- `max_speculative_job_count_per_task` — limit on the number of speculative jobs (currently supported for Map, Reduce, MapReduce, Sort, and Merge operations).
- `job_speculation_timeout` — timeout period, in milliseconds, after which a speculative job is run for the currently executing one.
- `try_avoid_duplicating_jobs` (false) — if possible, prevents jobs with the same input from running, unless required. In particular, this disables speculative jobs. Still, it is impossible to guarantee that there won't be any jobs run with the same input. For example, if the node hosting the first instance of a job loses connectivity with the cluster, the job continues to run on that cluster and can potentially perform observable side actions. In this case, the cluster has to restart the job with the same input on another node.


You can configure [I/O parameters](../../../../user-guide/storage/io-configuration.md) for each type of job in the operation. If the operation has one job type, the specification section is called `job_io`. If the operation involves jobs of different types (this is applicable to MapReduce and Sort operations), then each type has its own section. The names of these sections are listed in the settings of the corresponding operations.

## User script options { #user_script_options }

The following parameters are supported for each user script (default values, if defined, are provided in parentheses):

- `command` (required parameter) — string to be executed via the `bash -c` call.
- `file_paths` — list of [Cypress](../../../../user-guide/storage/cypress.md) paths to files or tables. These files and tables are downloaded to the cluster node where the command is executed, and a special `sandbox` environment houses symbolic links to the downloaded files. The links are read-only. The filename defaults to the basename of the Cypress path, but you can change it by specifying the `file_name` attribute for the path. If the path points to a table, the `format` attribute must be specified, which determines the [format](../../../../user-guide/storage/formats.md) in which the table is saved to a file on disk. The size of table files is limited by a constant value that can be configured at the cluster level.
- `docker_image` — name of the Docker image for the job's root file system.
- `layer_paths` — list of paths to porto layers in Cypress. Layers are listed from top to bottom.
- `cuda_toolkit_version` — [CUDA](https://ru.wikipedia.org/wiki/CUDA) version required by the job. If the version is specified, then a standard delta layer with the respective CUDA toolkit is provided to the job.
- `format`, `input_format`, `output_format` (yson) — [format](../../../../user-guide/storage/formats.md) in which input table data is fed to the operation. The `input_format` and `output_format` options take priority over `format`.
- `environment`— a map of environment variables specified while the operation is running. This option helps avoid specifying environment variables in the `command` parameter so as not to hinder its readability.
- `cpu_limit` (1) — number of compute cores that the scheduler may allocate for the job being executed.
- `set_container_cpu_limit` (false) — sets the `cpu_limit` for the porto container to the requested value.  By default, only the weight proportional to the requested `cpu_limit` is set.
- `gpu_limit` (0) — number of GPUs to be allocated for the job.
- `memory_limit` (512 MB) — limit on job memory consumption (in bytes).
- `user_job_memory_digest_default_value` (0.5) — initial estimate for [memory reserve](../../../../user-guide/data-processing/scheduler/memory-digest.md) selection.
- `user_job_memory_digest_lower_bound` (0.05) — minimum threshold for the [memory reserve](../../../../user-guide/data-processing/scheduler/memory-digest.md).{% if audience == "internal" %} Do not specify this option without explicit approval from YT developers.{% endif %}
- `enable_input_table_index` — whether to write the input index of a row's originating table to the input stream. By default, the attributes are only written if there is more than one input table.
- `tmpfs_path` — path to the `sandbox` where tmpfs is mounted. By default, the tmpfs size matches the job's `memory_limit`, and the effective occupied space counts toward the job's memory consumption.
- `tmpfs_size` — optional parameter that sets the limit of the tmpfs size for a job. This parameter only matters when `tmpfs_path` is specified. If the parameter is not specified, it defaults to `memory_limit`, and `memory_reserve_factor` is automatically set to 1.
- `tmpfs_volumes` — this parameter allows specifying a list of tmpfs volumes for the job. Each list element is a map containing two fields: `size` and `path`. The paths to the requested volumes must not overlap.
- `copy_files` (false) — by default, user files go to the job's `sandbox` via a [symbolic link](../../../../user-guide/storage/links.md). If this option is enabled, they are copied to the `sandbox`. It can be useful in tandem with `tmpfs_path=.`.
- `check_input_fully_consumed` (false) — this option allows you to run system-wide checks to make sure the job has not exited prematurely and read all the data written to its buffer.
- `max_stderr_size` (5 MB) — limit on the size of stderr resulting from the job. Any data written to stderr in excess of this limit is ignored.
- `custom_statistics_count_limit` (128) — maximum number of user statistics that can be written by the job (up to 1024).
- `job_time_limit` — time limit (in milliseconds) on a job. Jobs exceeding this limit are deemed failed.
- `disk_request` — list of disks required by the job. By default, a job is assigned a local disk on the cluster node where it was initiated. Disk space usage is not limited, but the job can be interrupted if the disk space runs out. Each disk is characterized by the following parameters:
  - `disk_space` — maximum total file size for the job's `sandbox` on the disk.
  - `inode_count` — limit on the number of [inodes](https://ru.wikipedia.org/wiki/Inode) on the disk.
  - `account` — account for which the requested disk space is provided.
  - `medium_name` (default) — name of the [medium](../../../../user-guide/storage/media.md) that a disk is being requested from.
- `interruption_signal` — number of the signal sent to the user process if the job is preempted. There is a timeout period between sending the signal and the job interruption. The duration of this timeout is determined by the cluster settings. During this time, the user process may attempt to finish processing the data and successfully complete the job.
- `restart_exit_code` — exit code signaling the need to restart the job after it was interrupted. This option is important for vanilla jobs that represent services or handle data that is external to {{product-name}}.
{% if audience == "internal" %}
- `network_project` — name of the MTN project with the network where the job should be run. The project must be registered in Cypress, with the necessary access rights granted.{% endif %}
- `job_speculation_timeout` — timeout period after which a speculative job can be run for the current one. A speculative job is a mechanism for running an additional copy of a job with the same input data and in parallel with the original job in cases where the latter appears to have frozen. The scheduler identifies "suspicious" jobs by itself based on the statistics regarding the read (for input data) and write (for output data) speeds, job preparation time, attempts to divide up the job, and other heuristics.
- `waiting_job_timeout` — timeout period during which the job can remain queued for launch on the node. By default, it is taken from the settings of a particular cluster.
- `enable_rpc_proxy_in_job_proxy` (false) — if this option is specified, an RPC Proxy service will be started for each job within the Job Proxy. This provides a convenient way to communicate with the cluster from a job without the risk of overloading the cluster-wide RPC Proxies; the RPC Proxy is accessed via a local socket, the path to which is defined in the `YT_JOB_PROXY_SOCKET_PATH` environment variable.

{% note info "Note" %}

The options listed above characterize the script rather than the operation as a whole. They should be located not in the specification root, but in branches like `mapper` or `reducer`. The user script is a separate entity because the [MapReduce](../../../../user-guide/data-processing/operations/mapreduce.md) operation involves more than one such script (mapper, reducer, reduce_combiner).

{% endnote %}

### Memory reserve { #memory_reserve_factor }

By default, the operation controller looks at the jobs statistics and determines a memory reserve for newly started jobs based on that. The mechanism is transparent to users and does not impact the operation's success. However, it may result in aborted jobs due to `resource_overdraft`.

For a detailed description of this mechanism, see [Selecting memory for jobs](../../../../user-guide/data-processing/scheduler/memory-digest).

{% note warning "Attention" %}

When tmpfs is enabled (specifying `tmpfs_path` is equivalent to enabling tmpfs) without an explicit limit on its size (the `tmpfs_size` parameter), the memory reserve is automatically set to 1. This is because the Linux OS kernel does not allow overcommitting for the tmpfs size, and the total size exceeding the physical capabilities can therefore lead to the cluster node freezing.

{% endnote %}

### Multiple input table reads { #many_input_tables }

Data from input tables written according to the specified input format is sent to the user script, standard input (`stdin`). The user script has output table write descriptors open. The descriptor numbers `1, 4, 7, ..` are consistent with the order in which the output tables are passed to the operation.

### Disk requests for jobs { #disk_request }

If only `disk_space` is specified in the `disk_request` specification, the default disks for jobs are used. {% if audience == "internal" %}Namely, HDD in most cases or NVME when running on modern GPU hosts (v100 and newer). {% endif %}In this case, no book-keeping is done on used space — free disk space is assumed to be abundant, no problems are expected. Only the data that the user stores on the cluster counts toward that user's disk quota (account), while the disk space used for the job does not. The scheduler ensures that no job exceeds its limits and that the sum of the limits of all jobs on the cluster node does not exceed the available disk space on the cluster node.

In addition, specifying `disk_space` means that {{product-name}} starts limiting the space used by the job, but also guarantees it.

If you want to use other disks for your operations' jobs, such as an SSD in place of the default HDD, specify the desired `medium` and your `account` for quoting the requested space.

{% if audience == "internal" %}In the current implementation, {{mr-clusters}} clusters have SSDs for jobs in the `ssd_slots_physical` medium. In order to use them, you need to request the disk quota for your account in this medium. For more information about requesting a quota, see [Requesting and getting resources](../../../../user-guide/storage/quota-request.md#zapros-na-rasshirenie-kvoty-pod-sushestvuyushij-akkaunt).{% endif %}

## Path attributes { #path_attributes }

The following attributes are supported for operations' output tables (default values, if set, are specified in parentheses):

- `sorted_by` — set of columns by which to sort the table.
- `append` (false) — determines if data is appended to a given output table. If `append` is set to `false`, the data in the table is overwritten.
- `row_count_limit` — finishes the operation ahead of time as soon as the output table reaches at least `row_count_limit` rows. Can only be set for one table.
- `create` (false) — if the table at the specified path does not exist, the scheduler will create it automatically with default settings (it will execute the `create` command with the parameters `{ "path" = "//path/from/spec" ; "type" = "table" }`)

The following attributes are supported on operations' input tables:
- Standard [modifiers for selecting rows and columns](../../../../user-guide/storage/ypath.md#rich_ypath_suffix).
- `transaction_id` — specifies the transaction within which to access the input table.
- `timestamp` — allows using the state of a dynamic table [at a specific point in time](../../../../user-guide/dynamic-tables/mapreduce.md#dyntable_mr_timestamp).
- `rename_columns` — enables renaming certain columns before table processing starts as part of the operation. Only columns within the schema can be renamed. Post-rename, all restrictions on the table schema should be met, and there should be no column name overlaps within chunks when it comes to the non-strict schema. When the `sorted_by` column set is specified for the operation's output table, or when the output table schema has key columns, the following occurs:
  1. When generating chunks for the table, the operation's jobs check if records are sorted by the specified set of columns.
  2. The scheduler validates that the key ranges of the chunks generated for this output table do not overlap and proceeds to form a sorted table from this set of chunks.

The operation will end with an error if the validation fails at any of these steps.

### Files

The following attributes are supported on file paths:

- `file_name` — name of the file in the job's `sandbox`.
- `executable` — makes the file executable.
- `format` — format in which the file from the table should be generated if the file constitutes the path to the table.
- `bypass_artifact_cache` — a flag indicating that the file should be downloaded to the job's `sandbox`, bypassing the cache. This attribute can be useful if the files are already on the SSD or in memory, and you want to deliver them to the job without extra delays. Good for cases where the operation involves a small number of jobs, rendering caching ineffective.
