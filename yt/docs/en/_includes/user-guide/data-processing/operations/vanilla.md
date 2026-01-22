# Vanilla

Vanilla operations execute arbitrary code. They do not have input tables, but they do support output tables,

tables for saving data from a standard error stream (`stderr` tables), and tables with `coredump`.

### Key points to know

* Vanilla operations can be used to start a distributed process: you can describe several types of jobs and specify how many jobs of each type there should be.

  [The {{product-name}} scheduler](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) carries the function of keeping this distributed process running, working to have the required number of jobs of each type planned and completed, just like in any other {{product-name}} operation.

* A Vanilla job receives input data via the `file_paths` option in the user script specification or reads data directly over the network.

  {% note warning "Attention" %}

  If you upload data to the job directly over the network, first discuss the intended use method with a {{product-name}} system administrator and with a representative of the service you plan to collect data from.

  This is important to avoid excessive load on the network and the service (data source).

  {% endnote %}

  {% note info "Note" %}

  The user is responsible for all coordination and distribution of work between the running jobs, as well as for maintaining consistency in case of job restarts.

  {% endnote %}

* Jobs can write output data over the network (in which case network load assessment is in order) and output to tables. Each type of job is associated with its own output tables.

### Tasks and script

The main part of describing a Vanilla operation is the enumeration of various **tasks**.
In the {{product-name}} ecosystem, a task is a group of jobs of the same type running as part of an operation.

* Task description encompasses the user script description, the number of jobs to be executed within this task, and, sometimes, the description of the output tables from this task.

* A task description in a Vanilla operation is very similar to a user script description, but we should draw a distinction between these concepts, because they correspond to different things in the [Scheduler](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).

* A user script description serves specifically to indicate how a user-provided code is to be run on a machine. It does not contain any information about how much data should be given to each job.

  **For example:** the `data_weight_per_map_job` option in a MapReduce operation is not included in the user script specification, because it is not related to executing the user script (which may be absent) at the map stage.

* A task description is a specification of how the scheduler should manage jobs of this type. This concept is found only in Vanilla operations.

General parameters for all operation types are described in [Operation options](../../../../user-guide/data-processing/operations/operations-options.md).

## Operation options

Vanilla operations support the following additional options:

* `tasks`: A dictionary describing the operation's tasks. Each task must correspond to a key–value pair, where the key is the task's name, and the value is the task's specification. In {{product-name}}, the lowercase_with_underscores task naming convention is preferred, like `partition_map`. The name of the task is displayed on the operation's page in the web interface, and it is also available as keys in the operation progress represented in yson format, so it is recommended that you not make it too long.

  The task specification features the same options as the user script specification, plus some additional ones:

  * `job_count`: The number of jobs of a given type that should run as part of the task. If the `fail_on_job_restart` option is not set in the operation's specification, the task is considered completed when exactly `job_count` of jobs of this type finish successfully (that is, with code 0 and without interruptions for any reason).
  * `output_table_paths`: A list of output tables (can be empty). If the same table is the output for different tasks, all its occurrences must feature the same attributes on the paths.
  * `gang_options`: Options for gang operations (see [Gang operations](#gang-operations)).

{% note info "Note" %}

You can easily control job restarts using the `max_failed_job_count` and `fail_on_job_restart` options (common for all operations).

Vanilla operations additionally support one more option: `restart_completed_jobs`. It enables restart mode for completed jobs.

{% endnote %}

## Example specification

```yaml
{
  tasks = {
    master = {
      job_count = 1;
      command = "python run_master.py < duties.dat";
      file_paths = ["//tmp/run_master.py"; "<file_name=duties.dat>//tmp/duties_180124.dat"];
    };
    slave = {
      job_count = 100;
      command = "python run_slave.py";
      file_paths = ["//tmp/run_slave.py"];
    };
  };
  max_failed_job_count = 1;
  stderr_table_path = "//tmp/stderrs";
}
```

## Gang operations { #gang-operations }

Gang operations are a special mode of Vanilla operations that is intended for starting coordinated distributed processes.

In contrast to regular Vanilla operations, where each job runs and completes independently from the {{product-name}} controller agent's perspective, the controller agent ensures the restart of all jobs in a gang operation if any job in a **gang** task aborts or fails. This includes the restart of completed jobs as well (this restart type is called a gang restart).

A gang task is a task whose description contains the `gang_options` option. Gang operations must contain at least one gang task.

### Operation incarnations and job ranks

One of the most important concepts in the gang operation model is incarnation.

* An incarnation is a string ID that uniquely defines a gang operation's lifecycle period between two consecutive start, restart, or completion events.
* Jobs inherit their operation's incarnation, but their own incarnation never changes. If an operation's incarnation changes, jobs from the previous incarnation are aborted.
* Each job (except for [reserved jobs](#reserved-jobs)) also has a unique rank within a gang operation task.

**For example:** a typical use case for gang operations is launching distributed ML training.

  In this use case, jobs interact with each other over the network. Incarnations are primarily essential for user code to prevent communication between jobs from the new incarnation and from the old one.

### Restarting a gang operation

What happens when the controller agent decides to restart a gang operation: controller agent aborts all jobs and attempts to restart jobs in the new incarnation. Controller agent attempts to reuse the allocations for the new jobs due to optimization. Completed jobs are also restarted.

If the controller agent manages to reuse a job allocation from the previous incarnation when the operation transitions to a new incarnation, the following guarantees apply:

* `job_cookie` of jobs from the previous and new incarnations match.
* If a job from the previous incarnation was assigned a `gang_rank`, the job in the new incarnation is assigned the same `gang_rank`.
* If a job from the previous incarnation was assigned a `monitoring_descriptor`, the job in the new incarnation is assigned the same `monitoring_descriptor`.

### Reserved jobs { #reserved-jobs }

When a gang operation is restarted, there will be at least one allocation that is not reused — the allocation of the job whose termination prompted the controller agent to restart the operation.

Typically, starting jobs in reused allocations is significantly faster than scheduling a new allocation and starting a new job there.

{% note tip %}

To prevent idle time for jobs for which the controller agent managed to reuse an allocation, you can use the reserved jobs mechanism during the time it takes for a job to start in a newly created allocation.

{% endnote %}

Reserved jobs are jobs that are not part of a gang operation in the current incarnation (their termination does not restart the gang operation, nor do they have a `gang_rank`), but their allocations can be used to start jobs in the new incarnation.

* Although reserved jobs are not part of a gang operation, they still run user code.

  To learn how user code can determine if it is running in a backup job or a gang job, see [Environment variables](#environment-variables).

* When `gang_options/size` is less than `job_count`, the controller agent starts `job_count`-`gang_options/size` reserved jobs.

### Behavior when reviving an operation

When [reviving](../../../../user-guide/data-processing/reliability.md#revival) a gang operation, the system checks that all non-reserved jobs have been successfully restored. If this is not the case, the operation will be restarted without reusing allocations.

### Special monitoring mode for gang operations

Gang operations support a special mode for generating monitoring descriptors:
**The `use_operation_id_based_descriptors_for_gangs_jobs` option** enables a special mode for generating descriptors for gang operations: the descriptor is generated based on `operation_id`, `gang_rank`, and task index, which guarantees uniqueness.

All these descriptors use `operation_id` as a prefix.

{% note tip %}

This mode poses a potential risk of monitoring quota overflow, so you should use it with caution, only after consulting with the {{product-name}} team.

{% endnote %}

### Environment variables { #environment-variables }

In gang operations, jobs have the following additional environment variables:

* `YT_OPERATION_INCARNATION`: Unique operation incarnation ID.
* `YT_TASK_GANG_SIZE`: The total job group size across all operation tasks.
* `YT_GANG_RANK`: The current job rank on the task (from 0 to `YT_GANG_SIZE - 1`).
* `YT_TASK_JOB_COUNT`: The total number of jobs in a task.

### Restrictions and special features

* Gang operations are not compatible with `fail_on_job_restart`.
* Gang operations are not compatible with `restart_completed_jobs = true`.
* If any gang job fails or aborts, the entire group is restarted with a new incarnation.
* Changing `job_count` while a gang operation is running is prohibited.
* **When reviving an operation**: If not all ranked jobs can be restored, the entire group is restarted with a new incarnation.
* Output tables are not supported.

### Configuring gang operations

To enable gang mode, add the `gang_options` parameter to the task specification:

```yaml
{
  tasks = {
    worker = {
      job_count = 3;
      command = "python distributed_worker.py";
      gang_options = {};
    };
  };
}
```

### gang_options parameters

* `size`: The job group size. By default, it equals the task's `job_count`. It cannot exceed `job_count`. Setting `size` enables the backup job mechanism.
