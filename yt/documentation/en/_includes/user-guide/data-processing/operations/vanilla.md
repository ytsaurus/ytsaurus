# Vanilla

The Vanilla operation differs from other operations in that it does not have input tables, but it does support output tables, tables for saving data from a standard error stream (stderr tables), and coredump tables. The Vanilla operation can be used to start a distributed process where you describe several types of jobs and specify how many jobs of each type there should be. [The {{product-name}} scheduler](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) carries the function of keeping this distributed process running, working to have the required number of jobs of each type planned and completed, just like in any other {{product-name}} operation.

The Vanilla operation's job receives input data via the `file_paths` option in the user script specification, or by reading the data directly over the network.
{% note warning "Attention!" %}

If uploading data to the job directly over the network, first discuss the intended use method with a {{product-name}} system administrator and with the service from which you plan to collect data, so as to avoid excessive load on the network and the said service.

{% endnote %}

{% note info "Note" %}

The user is responsible for all coordination and distribution of work between the running jobs, as well as for maintaining consistency in case of job restarts.

{% endnote %}

Jobs can write output data over the network (in which case network load assessment is in order) and output to tables. Each type of job is associated with its own output tables.

The main part of the Vanilla operation's description is the enumeration of various **tasks**. In the {{product-name}} ecosystem, a task is a group of jobs of the same type running as part of an operation. The description of a task encompasses the user script description, the number of jobs to be executed within this task, and, sometimes, the description of the output tables from this task.

The task description in the Vanilla operation is very similar to the user script description, but we should draw a distinction between these concepts, because they correspond to different things in the [Scheduler](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).

The user script description serves specifically to indicate how a user-provided code is to be run on a machine. It does not contain any information about how much data should be given to each job. For example: The `data_weight_per_map_job` option in the MapReduce operation is not included in the user script specification, because it is not related to executing the user script — which may be absent — in the map phase.

The task description is a specification of how the scheduler should manage jobs of this type. This concept is found only in the Vanilla operation, where it is practically the same thing as the user script description.

General parameters for all operation types are described in [Operation options](../../../../user-guide/data-processing/operations/operations-options.md).

The Vanilla operation supports the following additional options:

* `tasks` — dictionary describing the operation's tasks. Each task must correspond to a key–value pair, where the key is the task's name, and the value is the task's specification. In {{product-name}}, the lowercase_with_underscores task naming convention is preferred, like `partition_map`. The name of the task is displayed on the operation's page in the web interface, and it is also available as keys in the operation progress represented in yson format, so it is recommended that you not make it too long. The task specification features the same options as the user script specification, plus some additional ones:
   * `job_count` — number of jobs of a given type that should run as part of the task. If the `fail_on_job_restart` option is not set in the operation's specification, the task is considered completed when exactly `job_count` of jobs of this type finish successfully (that is, with code 0 and without interruptions for any reason).
   * `output_table_paths` — list of output tables (can be empty). If the same table is the output of different tasks, all its occurrences must feature the same attributes on the paths.

{% note info "Note" %}

The options `max_failed_job_count` and `fail_on_job_restart`, which are common across all operations, are useful tools for controling job restarts.

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

