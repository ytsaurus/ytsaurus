# Operation options

This section describes general properties of various operations, as well as user script parameters on paths, and file attributes.

## General options for all operation types { #common_options }

The parameters below can be indicated in any operation's specification root.
The default values, if set, are provided in brackets:

- `pool` — name of the [compute pool](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) where the operation runs. If this parameter is not specified, the name of the pool is set as the name of the {{product-name}} user who started the operation.
- `weight`(1.0) — weight of the started operation. It determines how much of the pool's compute quota is allocated to the operation. For more information, see [Scheduler and pools](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).
- `pool_trees` ("physical") — list of [pool trees](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) where the operation's jobs are run.
- `tentative_pool_trees` — list of additional pool trees whose resources are added to those of the main trees. Jobs that run in these trees are assessed to see if their metrics are slower compared to the main trees. If they are, new jobs of a specific operation are not launched in such trees.
- `use_default_tentative_pool_trees` — enable the use of all the trees in the cluster that fit the definition of `tentative`.
- `tentative_tree_eligibility` — configuration for running the operation's jobs in `tentative` trees.
   - `sample_job_count` (10) — number of jobs to be run as samples.
   - `max_tentative_job_duration_ratio` (10) — maximum allowed latency of job execution.
   - `min_job_duration` (30) — job duration threshold in `tentative` trees. If the average job duration in the `tentative` tree exceeds this threshold, new jobs of the operation are not run in that `tentative` tree.
   - `ignore_missing_pool_trees` (false) — enabling this option results in missing tentative trees in clusters being ignored.
- `resource_limits`, `max_share_ratio` — fair-share settings that apply to all the operation's jobs in aggregate. For more information, see [Pool characteristics](../../../../user-guide/data-processing/scheduler/pool-settings.md#operations).
- `time_limit` — general time limit on the operation (in milliseconds). If the operation does not complete within the specified time, it will be force-stopped by the scheduler.
- `acl` — Access Control List, which sets out access rights for the operation. The `manage`
    right is associated with mutating actions on the operation and its jobs: abort,
   suspend, job shell, and so on. In turn, the `read` right is about non-mutating actions like
   get_job_stderr and get_job_input_context. This `acl` will be
    supplemented with records on the user that ran the operation and {{product-name}} administrators.
- `max_stderr_count` (10) — limit on the amount of stored data from the standard error stream (stderr) of jobs, by default taken from cluster settings that usually have it equal to 10. The maximum value is 150.
- `max_failed_job_count` (10) — number of failed jobs past which the operation is considered failed. By default, it is taken from the cluster settings, where this value is usually 10.
- `unavailable_chunk_strategy` and `unavailable_chunk_tactics` — determine further action in case intermediate calculation results are unavailable. To learn more, see Operations.
- `scheduling_tag_filter` — filter for selecting cluster nodes upon job launch. The filter is a Boolean expression with the `&` (Boolean AND), `|` (Boolean OR), and `!` (logical "not") operators and round brackets. The possible values of cluster node tags act as variables in the expression. A tag is a marker placed on a cluster node. Markup is done by the {{product-name}} service when configuring a cluster. If the cluster node has a tag specified in the expression, the expression takes the "true" value, if not — the value is "false". If the entire expression takes the "false" value, then that cluster node is not selected to run the job.
- `max_data_size_per_job` (200 GB) — maximum allowed amount of input data for one job. This option sets a hard cap on the job size. If the scheduler is not able to generate a job with a smaller size, the operation ends with an error.
- `data_size_per_job` (`data_weight_per_job`) — recommended amount of input data for one job. This option is useless for composite operations such as MapReduce, which instead call for the following options:
   - `data_size_per_map_job` (`data_weight_per_map_job`);
   - `data_size_per_sort_job` (`data_weight_per_sort_job`);
   - `data_size_per_partition_job` (`data_weight_per_partition_job`);
   - `data_size_per_sorted_merge_job` (`data_weight_per_sorted_merge_job`);
   - `data_size_per_reduce_job` (`data_weight_per_reduce_job`).
- `secure_vault` — values from this dictionary end up in the environments of all the user jobs of a given operation without being viewable by unauthorized users, unlike the environment section in the user job specification. In particular, the entire passed-in dictionary in the YSON format is written to the `YT_SECURE_VAULT` environment variable, and, to save time, "value" is written to the `YT_SECURE_VAULT_key` environment variable for each `key=value` pair from the dictionary. This only happens for primitive-type values, such as int64, uint64, double, Boolean, or string.
- `stderr_table_path` — for specifying the path to an existing table, which has to be created outside of transactions. If this option is enabled, the complete data from the standard error stream (stderr) of all jobs (except for the aborted ones) is written to the specified table. That table will have the following columns:
   - `job_id` — job identifier.
   - `part_index` — for large error messages, the stderr message of one job can be divided into several parts (within different lines), and this column contains the number of this part.
   - `data` — data from the standard error stream.
- `core_table_path` — path to an existing table for writing core dump. The table format is identical to that described for `stderr_table_path`.
- `input_query` — for the [Map](../../../../user-guide/data-processing/operations/map.md) and ordered/unordered [Merge](../../../../user-guide/data-processing/operations/merge.md) operations, you can pre-filter each input string using a [query language](../../../../user-guide/dynamic-tables/dyn-query-language.md). Because the input table is already known, and it is assumed that each row will be individually filtered, the query syntax changes and gets simplified to `* | <select-expr-1> [AS <select-alias-1>]... [WHERE <predicate-expr>]`. Specifically, you cannot use `GROUP BY` and `ORDER BY`. Queries need strictly schematized data, so input tables must have a strict schema. In the [Merge](../../../../user-guide/data-processing/operations/merge.md) operation, the schema of the final data in the query is incorporated when outputting the schema for the output table.
- `omit_inaccessible_columns` (false) – skips columns that cannot be accessed. By default, operations in this scenario end with an error at startup.
- `alias` – alias of the operation. Makes the operation available in search and through the ```get-operation``` command on the specified row.

{% note info "Note" %}

If the `input_query` parameter is specified, the `enable_row_index` and `enable_range_index` attributes do not make it into the job.

{% endnote %}

- `job_node_account` (tmp_jobs) — account within which chunks of files with the error log (stderr) and input context are stored.
- `suspend_operation_if_account_limit_exceeded` (false) — suspends (pauses) the operation in case of the "Account limit exceeded" error in a job.
- `enable_job_splitting` (true) — determines if the scheduler is allowed to adaptively do further splits to users' jobs that take a long time.
- `fail_on_job_restart` (false) — force-interrupts the operation if a started job terminates in any state other than success (like abort or fail). Useful for operations that do not tolerate job restarts.
- `title` — operation title. The passed-in text is shown in the web interface.
- `started_by` — dictionary describing the client that started the operation. This field is not associated with a strict schema. The information is shown in the web interface on the operation page.
- `annotations` — dictionary where the user can write any structured information related to the operation. The annotations contents go to the operation archive in the [YSON](../../../../user-guide/storage/formats.md#yson) text format and can then be used to search and identify operations in the archive.

{% note info "Note" %}

Adding user information to the specification root is not recommended: it leads to user data turning up in Unrecognized Spec. In the future, this practice will be prohibited.

{% endnote %}

- `description` — dictionary similar to annotations that is also shown in the web interface on the operation page. It is best to add only small amounts of human reader friendly information to this section so as not to overcrowd the operation page.
- `use_columnar_statistics` (false) — determines whether to use columnar statistics to accurately estimate the size of jobs when starting operations on tables with column samples. This option improves automatic splitting into jobs, with columns taken from the input data forming a small fraction of the input table size (previously, column selection was ignored and jobs turned out to be very small, which had to be rectified with the `data_weight_per_job` option).
- `sampling` — configuration of large-block sampling of input data (not available for Sort, RemoteCopy, and Vanilla operations). This type of sampling, unlike [read sampling](../../../../user-guide/storage/io-configuration.md#sampling), works to proportionally decrease the amount of data read from the disk, which, in ideal circumstances, should lead to a proportional increase in operation speed. The trade-off is that the resulting process is not really sampling in its true mathematical sense, but only an approximation: the data is divided into fairly large blocks of successive rows, and then these blocks are sampled whole. In applied calculations on rather large tables, the result may be satisfactory. The value of the parameter is set by the dictionary with the following keys:
   - `sampling_rate` (null) — proportion of rows left in;
   - `io_block_size` (16 MB) — minimum sampling block size. Changing the default value is not recommended.
- `max_speculative_job_count_per_task` — limit on the number of speculative jobs (currently supported for Map, Reduce, MapReduce, Sort, and Merge operations).
- `job_speculation_timeout` — timeout in milliseconds, after which a speculative job is launched for the executing one.
- `try_avoid_duplicating_jobs` — (false) if possible, do not run jobs with the same input, if this is not required. In particular, it disables speculative jobs. But it is impossible to guarantee that jobs with the same input will not be launched. For example, if the node on which the first instance of the job was launched has lost connectivity with the cluster, then the job on it will continue to run and potentially create observable side effects. But the cluster will have to restart the job with the same input on another node.


You can configure [I/O parameters](../../../../user-guide/storage/io-configuration.md) for each type of job in the operation. If the operation has one job type, the specification section is called `job_io`. If the operation features job multiple (applies to MapReduce and Sort operations), then each type has its own section. The names of these sections are listed in the settings of the corresponding operations.

## User script options { #user_script_options }

The following parameters are supported for each user script (default values, if defined, are provided in brackets):

- `command` (mandatory parameter) — string to be executed via the`bash -c` call.
- `file_paths` — list of [Cypress](../../../../user-guide/storage/cypress.md) paths to files or tables. These files and tables are downloaded to the cluster node where the command is executed, and a special `sandbox` environment houses symbolic links to the downloaded files. The links are read-only. The default filename is the basename of the Cypress path, but it can be changed by specifying the `file_name` path attribute. If the path points to a table, the `format` attribute must be specified, which determines the [format](../../../../user-guide/storage/formats.md) in which the table is saved to a file on disk. The size of tabular files is limited to 10 GB.
- `layer_paths` — list of paths to porto layers in Cypress. The layers are listed top to bottom.
   – `cuda_toolkit_version` — [CUDA](https://en.wikipedia.org/wiki/CUDA) version required by a job. If the version is specified, then a standard delta layer with the respective CUDA toolkit is provided to the job.
- `format`, `input_format`, `output_format` (yson) — [format](../../../../user-guide/storage/formats.md) in which input table data is fed to the operation. The `input_format` and `output_format` options have priority over `format`;
- `environment`— dictionary of environment variables specified during the operation. This option is needed for specifying environment variables in the `command` parameter without making it less readable.
- `cpu_limit` (1) — number of compute cores based on which the scheduler schedules the job being executed.
   – `set_container_cpu_limit` (false) – sets the `cpu_limit` for the porto container equal to the one requested.  By default, only the weight proportional to the requested `cpu_limit` is set.
- `gpu_limit` (0) — number of GPU cards provided to the job.
- `memory_limit` (512 MB) — limit on job memory consumption (in bytes).
- `user_job_memory_digest_default_value` (0.5) — initial estimate for [memory reserve](../../../../user-guide/data-processing/scheduler/memory-digest.md) selection.
- `enable_input_table_index` — determines whether to write in the input stream information about which table the row originates from (input table index). By default, attributes are only written if there is more than one input table.
- `tmpfs_path` — path to the `sandbox` where tmpfs is mounted. The default tmpfs size is equal to the job's `memory_limit`, and the actual occupied space factors into the job's memory consumption.
- `tmpfs_size` — optional parameter that sets the limit of the tmpfs size for a job. This parameter only matters when `tmpfs_path` is specified. If the parameter is not specified, it is taken to equal `memory_limit`, with `memory_reserve_factor` being automatically set to 1.
- `tmpfs_volumes` — this parameter allows you to specify the list of tmpfs volumes for a job. A dictionary with two fields — `size` and `path` — serves as the list element. The requested volumes' paths must not overlap.
- `copy_files` (false) — by default, user files go to the job's `sandbox` via a [symbolic link](../../../../user-guide/storage/links.md). If this option is enabled, they are copied to the `sandbox`. It can be useful in tandem with `tmpfs_path=.`.
- `check_input_fully_consumed` (false) — this option allows you to run system-wide checks to make sure the job has not exited prematurely and read all the data written to its buffer.
- `max_stderr_size` (5 MB) — limit on the size of stderr saved as a result of the job. Any data written to stderr in excess of this limit is ignored.
- `custom_statistics_count_limit` (128) — limit on the amount of user statistics that can be written from the job, upto 1024.
- `job_time_limit` — time limit (in milliseconds) on a job. Jobs exceeding the limit are deemed failed.
- `disk_request` — list of disks the job wants. By default, a job is assigned a local disk on the same cluster node where the job is run. Disk space usage is not limited, but the job can be interrupted if the disk space runs out. Each disk is characterized by the following parameters:
   - `disk_space` — limit on the total size of files in a job's `sandbox` on the disk.
   - `inode_count` — limit on the number of [inodes](https://en.wikipedia.org/wiki/Inode) on the disk.
   - `account` — account for which the requested space is provided.
   - `medium_name` (default) – name of the required disk [medium](../../../../user-guide/storage/media.md).
- `interruption_signal` — number of the signal sent to the user process upon job preemption. Between the signal being sent and the job being interrupted, there is some timeout that depends on the cluster settings. During this time, the user process can work to finish processing the data and exit.
- `restart_exit_code` — exit code signaling the need to restart the job after an interruption. This option is important for vanilla jobs that represent services or process data that is external to {{product-name}}.
- `network_project` — name of the MTN project in whose network the job is run. The project must be registered in Cypress, with the necessary access rights granted.
- `job_speculation_timeout` — timeout following which a speculative job can be launched for the current one. A speculative job is a mechanism wherein an additional copy of a job is run with the same input data and in parallel with the original job in cases where the latter seems to have frozen. The scheduler identifies "suspicious" jobs by itself based on the statistics regarding the read (for input data) and write (for output data) speeds, job preparation time, attempts to divide up the job, and other heuristics.
- `waiting_job_timeout` — timeout during which a job may be in a queue to be run on the node. By default, it is taken from the settings of a particular cluster.


{% note info "Note" %}

The options listed above characterize the script rather than the operation as a whole. They should be located not in the specification root, but in branches like `mapper` or `reducer`. The user script is presented as a separate entity, because there is more than one such script in the [MapReduce](../../../../user-guide/data-processing/operations/mapreduce.md) operation (mapper, reducer, combiner).

{% endnote %}

### Memory reserve { #memory_reserve_factor }

By default, the operation controller looks at the jobs statistics and determines a memory reserve for newly started jobs based on that. The mechanism is transparent as far as users are concerned and does not affect the success of the operation. However, it may result in interrupted jobs, with `resource_overdraft` being cited as the reason.

For more details on the mechanism, see the dedicated page.

{% note warning "Attention!" %}

When tmpfs is enabled (specifying `tmpfs_path` is equivalent to enabling tmpfs) and without there being an explicit limit on its size (the `tmpfs_size` parameter), the memory reserve is automatically set to 1, because the Linux OS kernel does not allow overcommitting for the tmpfs size. The total size exceeding the physical capabilities can lead to the cluster node freezing.

{% endnote %}

### Multiple input table reads { #many_input_tables }

Data from input tables written according to the specified input format is sent to the user script, standard input (`stdin`). The user script has output table write descriptors open. The descriptor numbers `1, 4, 7, ..` are consistent with the order in which the output tables are passed to the operation.

### Disk requests for jobs { #disk_request }

If only `disk_space` is specified in the `disk_request` specification, the default disks for jobs are used. Namely, HDD (the `default` medium, in most cases) or NVME (when running on modern GPU hosts — v100 and newer). In this case, no book-keeping is done on used space — free disk space is assumed to be abundant, no problems are expected. Only the data that the user stores on the cluster is quoted, while the disk space used for the job does not count toward the user's disk quota (account). The scheduler ensures that no job exceeds its limits and that the sum of the limits of all jobs on the cluster node does not exceed the available disk space on the cluster node.

In addition, specifying `disk_space` means the space used by the job becomes limited, but also guaranteed.

If you want to use other disks for your operations' jobs, such as an SSD in place of the default HDD, you need to specify a special `medium` and your `account` where the space requested for the jobs is quoted. The Arnold and Hahn clusters currently have disks for jobs in the `ssd_slots_physical` medium, so in order to use them, you need to request the space for your account in this medium.

## Path attributes { #path_attributes }

The following attributes are supported for operations' output tables (default values, if set, are specified in brackets):

- `sorted_by` — column set serving as the basis for sorting a table.
- `append` (false) — determines if data is appended to a given output table. If `append` is set as `false`, the data in the table is overwritten.
- `row_count_limit` — finishes the operation ahead of time as soon as the output table reaches at least `row_count_limit` rows. Can only be set for one table.

The following attributes are supported on operations' input tables:
- standard modifiers for selecting rows and columns.
- `transaction_id` — specifies within which transaction to access the input table.
- `rename_columns` — enables renaming certain columns before table processing starts as part of the operation. Only columns within the schema can be renamed. Post-rename, all restrictions on the table schema should be met, and there should be no column name overlaps within chunks when it comes to the non-strict schema. When the `sorted_by` column set is specified for the operation's output table or the output table schema has key columns, the following occurs:
   1. Operation jobs check if records are sorted based on the specified set of columns when forming chunks for the table.
   2. The scheduler validates that the key ranges of the chunks generated for this output table do not overlap and proceeds to form a sorted table from this set of chunks.

The operation will end with an error if the validation fails at any of these steps.

### Files

The following attributes are supported on file paths:

- `file_name` — name of a file in a job's `sandbox`.
- `executable` — makes a file executable.
- `format` — format in which the file from the table should be generated if the file constitutes the path to the table.
- `bypass_artifact_cache` — flag indicating that the file should be downloaded to the job's `sandbox`, bypassing the cache. This attribute can be useful if the files are already on the SSD or in memory, and you want to get them into the job without extra delays. Good for cases where the operation has few jobs, rendering caching ineffective.
