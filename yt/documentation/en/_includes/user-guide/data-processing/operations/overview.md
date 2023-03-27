# Operation types

This section defines the data processing operations available in the {{product-name}} system. It also introduces the concepts of a user script and a job, which make up an operation, and describes the possible states of operations and jobs.

## Overview

{{product-name}} supports the following data processing operations:

- **[Map](../../../../user-guide/data-processing/operations/map.md), [Reduce](../../../../user-guide/data-processing/operations/reduce.md), and [MapReduce](../../../../user-guide/data-processing/operations/mapreduce.md)** execute user-provided code against input data.
- **[Sort](../../../../user-guide/data-processing/operations/sort.md)** sorts input tables.
- **[Merge](../../../../user-guide/data-processing/operations/merge.md)** merges tables.
- **[Erase](../../../../user-guide/data-processing/operations/erase.md)** deletes specified data from a table.
- **[RemoteCopy](../../../../user-guide/data-processing/operations/remote-copy.md)** copies data between clusters.
- **[Vanilla](../../../../user-guide/data-processing/operations/vanilla.md)** launches an appropriate number of **user scripts** on cluster nodes and keeps them running.

Each operation has a `spec` parameter, which, as the name implies, conveys the operation's **specification** (aka its options). The specification is a hierarchical structure that exists in the [YSON format](../../../../user-guide/storage/formats.md#yson). Each operation type has its own specification structure, whose detailed description can be found in the subsections dedicated to specific operation types.

**User script** is a user-defined string that is executed via a `bash -c` call, plus the parameters detailing the execution. User scripts run in a `sandbox` environment, where they are executed by a service user that is not formally present in the system (referring to Linux system users) and that only has read/write rights for the current directory and for the `tmp` directory. To learn more about user script parameters, see [Operation options](../../../../user-guide/data-processing/operations/operations-options.md#user_script_options).

Operations are run in **compute pools**, which are structured as a tree and serve to allocate the cluster's compute resources. For more information, see [Scheduler and pools](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).

Operations are comprised of **jobs**. Jobs as units enable the parallelism of an operation. Input data fed to an operation is divided into parts, with each such part processed by one job.

## Parameters of a running operation

{% note info "Note" %}

You can use the **Operations** screen of the web interface to keep track of operation progress and results.

{% endnote %}

Another way to get the info you need is the get_operation call,
which returns a set of operation attributes, the following being especially useful:

- `state` — current state of the operation.
- `progress` — operation progress broken down by job.
- `spec` — specification of the running operation.
- `result` — execution result (error report in case the operation ended with the `failed` status).

By calling list_jobs, you can get the list of the operation's jobs (both running and completed).
For use cases of Python's `get_operation` and `list_jobs`, see "Python Wrapper".

### Operation statuses { #status }

In the course of execution, an operation can have one of the following statuses:

- `running` — the operation is executing.
- `pending` — the operation is queued for launch. Each [pool](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) has a limit on the number of simultaneously running operations, and, if it is exceeded, operations are sent to a queue.
- `completed` — the operation was successfully completed.
- `failed` — the operation encountered an error. The main possible reasons are failure to start due to incorrect parameters, or the threshold of jobs stopped due to an error being exceeded.
- `aborted` — the operation was aborted. The main possible reasons are it being interrupted by the user, or the transaction housing the operation being interrupted.
- `reviving` — the operation is being recovered following the restart of the [scheduler](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).
- `initializing`, `preparing`, `materializing`, `completing`, `aborting`, `failing` — intermediate states of an operation that facilitate the step-by-step change of meta-information on the {{product-name}} master server.

### Job states

Each operation comprises a number of jobs. Like operations, jobs can have various states:

- `running` — the job is executing.
- `completed` — the job completed successfully.
- `aborted` — the job was interrupted. This normally occurs as a result of **preemption** or network errors. To learn more about preemption, see [Scheduler and pools](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).
- `failed` — the job encountered an error. This state usually results from errors in the user script. An error log (stderr) for the failed job has been generated and can be viewed. Such cases may also occur if the [account](../../../../user-guide/storage/cypress.md) in use has run out of space, meaning the job cannot write the result of its work.
- `waiting` — the job was sent to a cluster node, but is awaiting resources in a queue.
- `aborting` — the scheduler decided to interrupt the job, but the process on the cluster node has not yet ended.

The operation page in the web interface shows the number of `pending` jobs. This is the number of unscheduled jobs that the scheduler assumes need to be executed before the end of the operation. The number of `pending` jobs may differ from the number of jobs that will actually be completed: data slicing depends on the job completion speed.

#### Aborted jobs { #aborted_jobs }

Jobs with the `aborted` state contain information on the class of reasons that led to the interruption:

* `scheduler` — the job was interrupted by the scheduler, exact reason unknown.
* `failed_chunks` — the job was interrupted by the cluster node due to inability to read input data or write output data.
* `resource_overdraft` — the job was aborted by the cluster node because the memory limit was exceeded. By default, the scheduler allocates to the job less memory than requested by the user, and if the allocated memory is not enough for the job (in other words, the memory on the cluster node runs out), the job is interrupted and restarted.
* `other` — the job was interrupted by the cluster node due to an unclassified reason.
* `preemption` — the scheduler interrupted the job to run a different job.
* `user_request` — the job was interrupted by the user.
* `node_offline` — the job was interrupted because the scheduler lost connection with the cluster node.
* `waiting_timeout` — the job was interrupted due to being queued on the cluster node longer than permitted.
* `account_limit_exceeded` — the job was interrupted after trying to to write data past the user account's limit. Such jobs are deemed `failed`, unless `suspend_operation_if_account_limit_exceeded` is enabled.
* `unknown` — the job was interrupted for an unknown reason. This behavior occurs when the compute nodes and the scheduler running on the cluster have different versions and the cluster node reports to the scheduler a reason that is unknown to the latter.
* reasons appended with `scheduling_` refer to cases where the scheduler has scheduled the job, but ended up canceling it before making a schedule for it to run on a cluster node. This type of job wastes the resources of the scheduler, but not of the cluster:
   * `scheduling_timeout` — the scheduler was unable to schedule the job in the allotted time. May occur with big operations, where the input data numbers in the petabytes, or in cases of scheduler CPU overload.
   * `scheduling_resource_overcommit` — after the job was scheduled, it turned out that the `resource_limits` of the operation or one of the parent pools were exceeded.
   * `scheduling_operation_suspended` — after the job was scheduled by the controller, it turned out that the operation was suspended.
   * `scheduling_job_spec_throttling` — after the job is scheduled, a specification is generated for it, which, among other things, contains meta-information on the job's input chunks. Where there are too many input chunks, a mechanism for limiting the creation of jobs in the particular operation is activated. Some of the operation's jobs may be interrupted in the process.
   * `scheduling_intermediate_chunk_limit_exceeded` — the job was interrupted due to the intermediate chunk limit being exceeded. The limit determines how many such chunks an operation can create with automatic merging of output chunks enabled in `economy` and `manual` modes;
   * `scheduling_other` — the job was interrupted by the scheduler for an unclassified reason.

#### Completed jobs { #completed_jobs }
Sometimes, the scheduler decides to interrupt a job and stops inputting new data to the job as per the operation's guarantees, giving some time for the job to complete. If the job completes within the allotted time, it switches to the `сompleted` state — not `aborted` despite having been interrupted. Such `completed` jobs have the `interrupt_reason` attribute with a value other than `none`, specifying the reason for the scheduler interrupting the job.
In cases like this, the values of the job interruption reason can be:

* `none` — the job completed successfully, without interruption.
* `preemption` — the scheduler interrupted the job to run a different job.
* `user_request` — the job was interrupted by the user.
* `job_split` — the job was interrupted to be split into more jobs.
* `unknown` — the job was interrupted for an unknown reason. This happens when the decision to interrupt the job was sent to the cluster node, and then the scheduler restarted.

#### Standard error stream

Data in an operation's job can be written to the standard error stream (stderr) to be saved and available for reading. This can also be useful for debugging purposes.
To get a job's stderr, use the `get_job_stderr` command, to get a job list — `list_jobs`. For convenience, you can adjust the `list_jobs` parameters to only show the jobs where stderr was written to. The `list_jobs` and `get_job_stderr` commands are available in Python (see "Python Wrapper").
To prevent stderr operations from taking up too much space, writing is governed by the following policies:

1. No more than `max_stderr_size` bytes written to stderr are saved. Anything written beyond this limit is ignored.
2. No more than 150 jobs with such stderr log files can be saved (for schedulers).
3. No more than 20 jobs with such stderr log files can be saved (for operations). May take the value up to 150.

## Unavailable data { #chunk_strategy }
### Strategies and tactics
Specification features two settings for each operation: `unavailable_chunk_strategy` and `unavailable_chunk_tactics`. Their possible values are `wait`, `fail`, and `skip`. These values determine the behavior of the scheduler when the necessary input data for the operation is not available:

* `wait` — wait for the data for an indefinite period
* `fail` — immediately abort the operation
* `skip` — skip unavailable data and perform calculations on an incomplete set

Strategy and tactics come into play at different stages of the operation:

* **strategy** is used upon operation start and during initial scheduling of the work scope
* **tactics** are used when handling unavailable data for an operation that is already running

Theoretically, 9 combinations are possible, but not all of them are useful. The following combinations are useful:

* `wait` strategy, `wait` tactic — used by default, calculations are only performed on complete data, even at the cost of waiting.
* `fail` strategy, `wait` tactic — data integrity is checked at startup, and if any losses are detected you must wait for the missing data to become available.
* `skip` strategy, `skip` tactic — calculations are performed on any available data without delay.
* `skip` strategy, `wait` tactic — calculations are started immediately with the losses committed, but no additional losses are allowed — wait for the missing data to become available.
* `skip` strategy, `fail` tactic — calculations are started immediately with the losses committed, but no additional losses are allowed — interrupt the calculations if unavailable data is detected.
* `fail` strategy, `fail` tactic — calculations are only performed on complete data, any data unavailability is reported as soon as possible.

The scheduler periodically queries the master for the status of an operation's input chunks. If the scheduler detects unavailable data, the respective part of the calculation stops. Jobs using this data are not started, but chunks are still requested from the master. Each `erasure` request for a chunk causes it to move up in the queue of chunks awaiting recovery ("move to front"). The chunks required by the current operations to run are recovered first.

These procedures are fully automated and do not require manual set-up.

## C++ and Python Wrapper

Running operations via С~++ or Python Wrapper is associated with a number of priming works in addition to the calling of the corresponding driver command. Those include:
* Uploading files to the [File cache](../../../../user-guide/storage/file-cache.md).

   **Note**: Automatic file upload to the cache always takes place outside of the user transaction, which may lead to conflicts on Cypress nodes if the file cache directory was not created in advance.
* Сreating output tables or clearing them using the `erase` command. When it comes to the system, all output tables should already be there at the start of the operation.
* Deleting files after the operation is completed.
* Setting the `jobcount` and `memorylimit` parameters, if they are specified by the user.
* Some parameters may have default values that are different from the system ones.

## Limits on the number of tables and jobs

The system has restrictions on operation parameters in place that need to be taken into account.

1. There is a limit on the number of input tables for any operation. By default, this limit on "big" production clusters is 3,000. Testing clusters, as well as clusters that do not run large-scale MapReduce operations, can have lower restrictions, which can still be increased to 3,000 if necessary.
2. There is no special limit on the number of output tables, but keep in mind that each output table has memory buffer requirements. If an operation has 1,000 output tables, its jobs will have impossible memory requirements and therefore not run. In practice, the upper bound for output tables is approximately 100. If you need more output tables, split the operation into several smaller ones and stagger the processing.
3. There is a limit on the number of jobs an operation can have. For [Map](../../../../user-guide/data-processing/operations/map.md), [Merge](merge.md), [Reduce](../../../../user-guide/data-processing/operations/reduce.md), and [RemoteCopy](../../../../user-guide/data-processing/operations/remote-copy.md) it is 100,000 jobs. In the cases of [Reduce](../../../../user-guide/data-processing/operations/reduce.md) and sorted [Merge](../../../../user-guide/data-processing/operations/merge.md), it may be impossible to generate so many jobs due to the scheduler not having enough samples. For operations that include the shuffle stage ([Sort](../../../../user-guide/data-processing/operations/sort.md) and [MapReduce](../../../../user-guide/data-processing/operations/mapreduce.md)), there is a limit of 5,000 partitions. To learn more about partitions, see the corresponding sections dedicated to specific operation types.
4. There is a limit on the product of an operation's jobs multiplied by the number of output tables. This restriction helps prevent operations from generating a large number of chunks, which can exert an excessive load on the master servers. For "big" production clusters, this limit is 2,000,000.

## Transactions in data processing { #transactions }

Any operation can be viewed as a black box with a set of inputs and outputs (I/O), as well as a number of artifacts used in its work (files and porto layers). The general logic of the operation is quite simple: it takes exclusive locks on output tables within a special output transaction and snapshot locks on input tables (and artifacts) within a special input transaction. Then, if and when all calculations have been successfully performed, output transactions are committed. If the operation is interrupted or ends in an error, the output transaction is aborted.

There are certain caveats:
* If a table is present in both input and output tables, then only the exclusive lock in the output transaction is taken for that table, and the data is both read and written from this version of the table.
* If the output table is flagged with `append`, it takes a shared lock. The previous item's logic does not work if the tables are present in both the input and output.
* The operation can be run as part of a user transaction, in which case the input and output transactions are created as the user transaction's subtransactions.
* Scheduler transactions are created with a timeout sufficient to survive cluster downtimes which can take hours. Then they are pinged by the scheduler. When a standard scheduler update takes place, it reuses operations' existing transactions. In the case of long shutdown drills or major updates of the master server, transactions are interrupted and operations are restarted from scratch, with locks on tables being taken anew.

Operations that contain core or stderr tables can also have a debug transaction. This transaction houses the writing of core files and the stderr, with the transaction being committed even if it is interrupted or ends in an error.

These transactions can be viewed via operation attributes:
1. `input_transaction_id`, `nested_input_transaction_ids` – transaction with locks on input tables and artifacts. This can also be a list of transactions if `transaction_id` on input tables is used.
2. `output_transaction_id` – transaction within which data is written to output tables.
3. `debug_transaction_id` – transaction within which core and stderr tables are written.
4. `async_scheduler_transaction_id` – transaction within which temporary data is written under an operation that always interrupts upon completion. For example, this is where intermediate data lives.
5. `output_completion_transaction_id`, `debug_completion_transaction_id` – transactions serving a technical purpose, used upon successful completion of an operation to atomically commit the operation's result.

