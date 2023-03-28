# Selecting memory for jobs

Memory is the RAM consumed by operation jobs.
For all operations, there is a default mechanism for selecting the actual memory reserve for the jobs to be run.

The operation controller collects memory consumption information for all successfully completed jobs. This information is saved as a special digest, based on which the actual memory reserve for the newly run jobs is selected.

The main aspects of the digest:
1. Collects actual values (typically in the range from 0.0 to 1.0) describing the share of `memory_limit` consumed by the job.
2. Only successfully completed jobs and jobs that were aborted because they exceeded the memory reserve are counted.
3. For jobs that were aborted because they exceeded the reserve, a point with the memory consumption share of that job multiplied by a certain constant slightly above one (the default value is 1.1) is added to the digest.
4. The `memory_limit * percentile(digest, P)` (by default, `P` is 0.95) is taken as a reserve for the newly run job.
5. The digest does not store all points internally, but uses a data structure that allows an approximate calculation of percentiles.

The memory reserve allocated to the job is used by the scheduler to consider consumption the lower limit. If the reserve value is exceeded and there is no free memory on the cluster node, the job may be aborted due to the `resource_overdraft` reason. If there is extra free memory on the cluster node, the job may exceed its allocated reserve and be completed successfully. If the memory limit specified by the user in the operation specification is exceeded, the job will be aborted with the `Memory limit exceeded` error.

Note that in addition to the user process, there is also a `job_proxy` process, which is a layer between the user process and {{product-name}}. Depending on different circumstances, job proxy can consume a significant amount of RAM for compression, columnar read/write mechanism, erasure, and other needs. The operation controller supports individual digests for the user process (`user_job`) and the `job_proxy` process. In the case of the `job_proxy` process, the controller makes an assumption about the expected memory usage based on the number of input/output tables and their settings. Based on the assumption , the digest selects a reserve (ranging from 0.5 to 2.0 by default).

Besides that, the memory allocated to the user process has a lower limit. By default, it is indicated in the specification and equals 0.05. However, if a job orders `tmpfs`, the memory reserve cannot be less than `tmpfs_size / memory_limit`, i.e. the system tends not to allow overcommit on `tmpfs`, because it can cause scheduler locking.

### Statistics

There are a number of values in the operation statistics that enable you to analyze the reserved memory:
* `user_job/memory_reserve` and `job_proxy/memory_reserve` (bytes): The memory reserve allocated to the appropriate job process.
* `user_job/max_memory` and `job_proxy/max_memory` (bytes): The memory consumed by the appropriate job process. Maximum during the operation of a particular job.
* `job_proxy/estimated_memory` (bytes) : The scheduler's evaluation of the `job_proxy` process memory of a given job.
* `user_job/memory_limit` (bytes) : The job memory limit specified by the user.
* `user_job/cumulative_memory_reserve`, `job_proxy/cumulative_memory_reserve`, `user_job/cumulative_max_memory`, `job_proxy/cumulative_max_memory`, `job_proxy/cumulative_estimated_memory` (bytes * seconds): The appropriate cumulative metrics taking into account the job duration.

### Digest settings

The following options are available to the user in the operation specification:
* `user_job_memory-digest_default_value`: Initial assumption for selecting the memory reserve (the default value is 0.5).
* `user_job_memory-digest_lower_bound`: The limit below which the reserve must not fall (the default value is 0.05). We do not recommend changing the default value.
* `memory_reserve_factor`: The alias for the `user_job_memory-digest_lower_bound` and `user_job_memory-digest_default_value` options concurrently. Using this option is not recommended.
