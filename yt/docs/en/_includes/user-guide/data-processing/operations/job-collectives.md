# Job collectives

Job collectives is a mechanism that enables you to process one data batch with multiple jobs. This is useful for computations that involve multiple processes running together — think distributed GPU computations or parallel data processing by multiple jobs (when you need more than one).

To learn how to set up job collectives in an operation, see [Configuration](#configuration).

## Basic concept { #concept }

**Operations with job collectives function similarly to regular operations, but with one key difference: each data batch is processed not by a single job, but by a collective of multiple jobs.**

This means that:
- Job slicing works exactly the same way as in regular operations (meaning you can configure it in the spec the same way as with regular operations).
- In terms of the operation's data stream, a collective functions as a single job, receiving one input data batch and generating one output data batch.

{% note info "Note" %}

Job collectives are not the same as [gang operations](../../../../user-guide/data-processing/operations/vanilla.md#gang-operations). A gang operation restarts **all** its jobs when any non-backup job is restarted, whereas job collectives restart jobs only in a **single** collective (the one processing a given data batch).

{% endnote %}

## Use cases { #use-cases }

### Distributed GPU computations

A typical use case is data processing using multiple GPUs. The master job reads the data and distributes it among slave jobs, each of which performs computations on its own GPU.

### Parallel processing with aggregation

Slave jobs perform parallel computations, while the master job aggregates their results and writes them to the output table.

## Gang operations vs. operations with job collectives { #when-to-use }

[Gang operations](../../../../user-guide/data-processing/operations/vanilla.md#gang-operations) are a type of Vanilla operations, so when you need to run an operation of a different type, operations with job collectives are the only option.
For scenarios requiring a Vanilla operation, you use either gang or Vanilla operations with job collectives. The difference will lie in some details:
- Gang operations provide more guarantees (which, in particular, make restarting a collective more optimal) (see [Restarting a gang operation](../../../../user-guide/data-processing/operations/vanilla.md#gang-operation-restart)).
- In contrast to gang operations, operations with job collectives do not support backup jobs (see [Backup jobs](../../../../user-guide/data-processing/operations/vanilla.md#reserved-jobs)).
- In contrast to gang operations, operations with job collectives can have output tables (see [Limitations and considerations](../../../../user-guide/data-processing/operations/vanilla.md#gang-operation-limitations)).

## Collective structure { #structure }

A collective comprises multiple jobs working together to process a single data batch. Each job in a collective is assigned a **rank** (an integer from 0 to `size - 1` where `size` is the collective size).

* **Master job** (rank 0): Reads input data and writes results to output tables. From the system's point of view, the master job is what represents the collective as it interacts with the data stream.
* **Slave jobs** (ranks 1, 2, ...): Auxiliary jobs that do not have access to the operation’s input/output data streams. They perform computations and exchange data with the master job over the network.

## Collective lifecycle { #lifecycle }

1. When the controller agent initiates the processing of a data batch, it creates a collective of multiple jobs instead of just one job.
2. The master job receives input data via `stdin` (for Map/Reduce/MapReduce operations) exactly the same way as a regular job.
3. All jobs in a collective can interact with each other over the network using `YT_COLLECTIVE_ID` for identification.
4. Only the master job can write output data.
5. A collective is considered completed when the master job has completed. This means that if slave jobs finished unsuccessfully after the master job has completed (according to the controller agent), the data is still considered processed.

## Failure handling { #failure-handling }

If any job in a collective finishes unsuccessfully (fails or aborts), and the master job has not completed yet, the following happens:

* All other jobs in the collective are aborted.
* The collective is restarted entirely, just like a regular job would.
* Other collectives in that operation continue running independently.

## Supported operations { #supported-operations }

Job collectives are supported in all operations with user code:

| Operation | Where to specify `collective_options` |
|----------|-----------------------------------|
| [Map](../../../../user-guide/data-processing/operations/map.md) | In the `mapper` specification |
| [Reduce](../../../../user-guide/data-processing/operations/reduce.md) | In the `reducer` specification |
| [MapReduce](../../../../user-guide/data-processing/operations/mapreduce.md) | In the `mapper`, `reducer`, or `reduce_combiner` specification |
| [Vanilla](../../../../user-guide/data-processing/operations/vanilla.md) | In the task specification |

## Environment variables { #environment-variables }

The following environment variables are present in the job collectives:

| Variable | Description |
|------------|----------|
| `YT_COLLECTIVE_MEMBER_RANK` | Job's rank in the collective (0 for the master job, 1, 2, ... for slave jobs) |
| `YT_COLLECTIVE_ID` | The collective's unique ID (currently matches the `job_id` of the master job, but this property is not guaranteed and may change in the future) |

Jobs use these variables to:
- Determine their role in the collective.
- Find each other for interaction over the network.
- Coordinate their processes.

## Configuration { #configuration }

To integrate job collectives, add the `collective_options` parameter to the job specification:

```yaml
{
  collective_options = {
    size = 2;  # Number of jobs in the collective
  };
}
```

### Collective_options parameters

| Parameter | Type | Description |
|----------|-----|----------|
| `size` | int | Number of jobs in the collective. Must be more than 1. |

## Specification examples { #examples }

### Vanilla operation with job collectives

In this example, a Vanilla operation runs with a collective of two jobs. The master job (rank 0) writes the result to the output table, while the slave job (rank 1) does nothing.

```yaml
{
  tasks = {
    worker = {
      job_count = 1;
      output_table_paths = ["//tmp/output"];
      format = "yson";
      command = "if [ \"$YT_COLLECTIVE_MEMBER_RANK\" == 0 ]; then echo '{result=42}'; fi";
      collective_options = {
        size = 2;
      };
      close_stdout_if_unused = %true;
    };
  };
}
```

### Map operation with job collectives

```yaml
{
  mapper = {
    command = "if [ \"$YT_COLLECTIVE_MEMBER_RANK\" == 0 ]; then cat; fi";
    collective_options = {
      size = 2;
    };
    close_stdout_if_unused = %true;
  };
}
```

### MapReduce operation with job collectives

In this MapReduce operation, job collectives are used at all of its stages: mapper, reduce_combiner, and reducer.

```yaml
{
  mapper = {
    command = "if [ \"$YT_COLLECTIVE_MEMBER_RANK\" == 0 ]; then cat; fi";
    collective_options = {
      size = 2;
    };
    close_stdout_if_unused = %true;
  };
  reduce_combiner = {
    command = "if [ \"$YT_COLLECTIVE_MEMBER_RANK\" == 0 ]; then cat; fi";
    collective_options = {
      size = 2;
    };
    close_stdout_if_unused = %true;
  };
  reducer = {
    command = "if [ \"$YT_COLLECTIVE_MEMBER_RANK\" == 0 ]; then cat; fi";
    collective_options = {
      size = 2;
    };
    close_stdout_if_unused = %true;
  };
  force_reduce_combiners = %true;
}
```

## Limitations { #limitations }

* **Incompatibility with gang_options**: You cannot use job collectives together with [gang operations](../../../../user-guide/data-processing/operations/vanilla.md#gang-operations). If you attempt to specify both parameters, the operation will fail with an error.

* **Data output from the master job only**: Only the master job (rank 0) can write data to output tables.

  {% note tip %}

  Use the `close_stdout_if_unused = %true` option in the job specification to explicitly close `stdout` for slave jobs and avoid accidental writes.

  {% endnote %}

* **Job interruption**: You can interrupt only the master job. Attempting to interrupt a slave job results in its abortion followed by a restart of the collective.

## See also { #see-also }

* [Gang operations](../../../../user-guide/data-processing/operations/vanilla.md#gang-operations)
* [Map operation](../../../../user-guide/data-processing/operations/map.md)
* [Reduce operation](../../../../user-guide/data-processing/operations/reduce.md)
* [MapReduce operation](../../../../user-guide/data-processing/operations/mapreduce.md)
* [Vanilla operation](../../../../user-guide/data-processing/operations/vanilla.md)
