#### **Q: Where do I get the simplest client and a step-by-step procedure for running MapReduce?**

**A:** We recommend reviewing the Trial section as well as reading about working with {{product-name}} from the console.

------
#### **Q: Are table numbers/names always available in transactions?**

**A:** Table numbers are available at all stages except the [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md) reduce phase. Table numbers are also available in the native C++ wrapper.

------
#### **Q: If a cluster is running several tasks concurrently, how do they get assigned slots?**

**A:** {{product-name}} assigns slots based on the fair share algorithm with the number of slots recomputed dynamically while the task is running. For more information, see [Scheduler and pools](../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).

------
#### **Q: What are the factors that go into the overhead of merging a table with a small delta of the same table?**

**A:** Overhead depends on the number of affected chunks that rows from the delta will be written to.

------
#### **Q: Can I start mappers from the python interactive shell?**

**A:** No, this functionality is not supported.

------
#### **Q: Reading a table or a file produces the following message: "Chunk ... is unavailable." What should I do?**
#### **Q: The operation running on the cluster has slowed or stopped. The following message is displayed: "Some input chunks are not available." What should I do?** { #lostinputchunks }
#### **Q: The operation running on the cluster has slowed or stopped. The following message is displayed: "Some intermediate outputs were lost and will be regenerated." What should I do?** { #lostintermediatechunks }

**A:** Some of the data has become unavailable because cluster nodes have failed. This unavailable condition may be related to the disappearance of a data replica (if there is erasure coding) or the complete disappearance of all replicas (if there is no erasure coding). In any case, you need to wait for the data to be recovered or the failed cluster nodes to be repaired and force the operation to complete. You can monitor cluster status via the web interface's System tab (Lost Chunks, Lost Vital Chunks, Data Missing Chunks, Parity Missing Chunks parameters). You can terminate an operation waiting on missing data early and get some intermediate output. To do this, use the `complete-op` command in the CLI or the **Complete** button on the web interface's operation page.

------
#### **Q: I am getting the following error: "Table row is too large: current weight ..., max weight ... or Row weight is too large." What is this and what do I do about it?**

**A:** A table row weight is computed as the sum of the lengths of all the row's column values. The system has a limitation on row weight which helps control the size of the buffers used for table writes. The default limit is 16 MB. To increase this value, you need to set the `max_row_weight` option in the table_writer configuration.

Value lengths are a function of type:

- `int64`, `uint64`, `double`: 8 bytes.
- `boolean`: 1 byte.
- `string`: String length.
- `any`: Length of structure serialized as binary yson, bytes.
- `null`: 0 bytes.

If you are getting this error when you start a MapReduce, you need to configure the specific table_writer servicing the relevant step in the operation: `--spec '{JOB_IO={table_writer={max_row_weight=...}}}'`.

The name of the `JOB_IO` section is selected as follows:

1. For operations with a single job type ([Map](../../../user-guide/data-processing/operations/map.md), [Reduce](../../../user-guide/data-processing/operations/reduce.md), [Merge](../../../user-guide/data-processing/operations/merge.md), and so on), `JOB_IO = job_io`.
2. For Sort operations, `JOB_IO = partition_job_io | sort_job_io | merge_job_io`, and we recommend doubling all limits until you find the right ones.
3. For [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md) operations, `JOB_IO = map_job_io | sort_job_io | reduce_job_io`. You can increase certain limits if you are sure where exactly large rows occur.

The maximum value is `max_row_weight` equal to 128 MB.

------
#### **Q: I am getting error "Key weight is too large." What is this and what do I do about it?**

**A:** A row key weight is computed as the sum of the lengths of all the row's key columns. There are limits of row key weights. The default limit is 16 K. You can increase the limit via the `max_key_weight` option.

Value lengths are a function of type:

- `int64`, `uint64`, `double`: 8 bytes.
- `boolean`: 1 byte.
- `string`: String length.
- `any`: Length of structure serialized as binary yson, bytes.
- `null`: 0 bytes.

If you are getting this error when you start a MapReduce, you need to configure the table_writer serving the relevant step in the operation:

```bash
--spec '{JOB_IO={table_writer={max_key_weight=...}}}'
```

The name of the `JOB_IO` section is selected as follows:

1. For operations with a single job type ([Map](../../../user-guide/data-processing/operations/map.md), [Reduce](../../../user-guide/data-processing/operations/reduce.md), [Merge](../../../user-guide/data-processing/operations/merge.md), etc), `JOB_IO = job_io`.
2. For sort operations, `JOB_IO = partition_job_io | sort_job_io | merge_job_io`, we recommend increasing all the limits right away.
3. For [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md) operations, `JOB_IO = map_job_io | sort_job_io | reduce_job_io`. You can raise individual limits if you are certain where exactly large rows occur but it is better to increase all limits as well.

The maximum value of `max_key_weight` is 256 KB.

{% note warning "Attention!" %}

Chunk boundary keys are stored on master servers; therefore, raising limits is prohibited except when repairing the production version. Prior to increasing a limit, you must write the system administrator and advise that a limit is about to be increased providing rationale.

{% endnote %}

<!-- ------
#### **Q: При работе джобов, написанных с использованием пакета yandex-yt-python, возникает ошибка «Unicode symbols above 255 are not supported». Что делать?**

**A:** Следует прочитать в разделе [Форматы](../../../user-guide/storage/formats#json) про формат JSON. Можно либо отказаться от использования JSON в пользу [YSON](../../../user-guide/storage/formats#yson), либо указать `encode_utf8=false`. -->

------
#### **Q: Why is reduce_combiner taking a long time? What should I do?**

**A:** Possibly, the job code is fairly slow, and it would make sense to reduce job size. `reduce_combiner` is triggered if partition size exceeds `data_size_per_sort_job`. The amount of data in `reduce_combiner` equals `data_size_per_sort_job`. The `data_size_per_sort_job` default is specified in the {{product-name}} scheduler configuration, but can be overridden via an operation specification (in bytes).
`yt map_reduce ... --spec '{data_size_per_sort_job = N}'`

------
#### **Q: I feed several input tables to MapReduce without specifying a mapper. At the same time, I am unable to get input table indexes in the reducer. What seems to be the problem?**

**A:** Input table indexes are available to mappers only. If you fail to specify a mapper, it will automatically be replaced with a trivial one. Since a table index is an input row attribute rather than part of the data, a trivial mapper will not retain table index information. To resolve the problem, you need to write a custom mapper where you would save the table index in some field.

------
#### **Q: How do I terminate pending jobs without crashing the operation?**

**A:** You can terminate jobs individually via the web interface.
You can terminate the entire operation using the CLI's `yt complete-op <id>`

------
#### **Q: What do I do if am unable to start operations from IPython Notebook?**

**A:** If your error message looks like this:

```python
Traceback (most recent call last):
  File "_py_runner.py", line 113, in <module>
    main()
  File "_py_runner.py", line 40, in main
    ('', 'rb', imp.__dict__[__main_module_type]))
  File "_main_module_9_vpP.py", line 1
    PK
      ^
SyntaxError: invalid syntax
```
You need to do the following prior to starting all operations:

```python
def custom_md5sum(filename):
    with open(filename, mode="rb") as fin:
        h = hashlib.md5()
        h.update("salt")
        for buf in chunk_iter_stream(fin, 1024):
            h.update(buf)
    return h.hexdigest()

yt.wrapper.file_commands.md5sum = custom_md5sum
```

------
#### **Q: Which account will own stored intermediate data for MapReduce and Sort?**

**A:** The default account used is `intermediate` but you can change this behavior by overriding the `intermediate_data_account` parameter in the operation spec. For more information, see [Operation settings.](../../../user-guide/data-processing/operations/operations-options.md)

------
#### **Q: Which account will own stored operation output?**

**A:** The account the output tables happen to belong to. If the output tables did not exist prior to operation start, they will be created automatically with the account inherited from the parent directory. To override these settings, you can create the output tables in advance and configure their attributes any way you want.

------

#### **Q: How do I increase the number of Reduce jobs? The job_count option is not working.**

**A:** Most likely, the output table is too small, and the scheduler does not have enough key samples to spawn a larger number of jobs. To get more jobs out of a small table, you will have forcibly to move the table creating more chunks. You can do this with `merge` using the `desired_chnk_size` option. To create 5-MB chunks, for instance, you need to run the command below:

```bash
yt merge --src _table --dst _table --spec '{job_io = {table_writer = {desired_chunk_size = 5000000}}; force_transform = %true}'
```
An alternative way of solving the problem is by using the `pivot_keys` option explicitly to define the boundary keys between which jobs must be started.

------
#### **Q: I am attempting to use sorted MapReduce output. And using input keys for the output. The jobs are crashing and returning "Output table ... is not sorted: job outputs have overlapping key ranges" or "Sort order violation". What seems to be the problem?**

**A:** Sorted operation output is only possible if jobs produce collections of rows in non-intersecting ranges. In [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md), input rows are grouped based on a hash of the key. Therefore, in the scenario described, job ranges will intersect. To work around the issue, you need to use [Sort](../../../user-guide/data-processing/operations/sort.md) and [Reduce](../../../user-guide/data-processing/operations/reduce.md) in combination.

------
#### **Q: When I start an operation, I get "Maximum allowed data weight ... exceeded». What do I do about it?**

**A:** The error means that the system has spawned a job with an input that is too big: over 200 GB of input data. This job would take too long to process, so the {{product-name}} system is preemptively protecting users from this type of error.

------
#### **Q: When I launch a Reduce or a MapReduce, I can see that the amount of data coming in to the reduce jobs varies greatly. What is the reason, and how do I make the split more uniform?**

The large amounts of data may be the result of skewed input meaning that some keys have noticeably more data corresponding to them than others. In this case, you might want to come up with a different solution for the problem being worked, such as try using [combiners](../../../user-guide/data-processing/operations/reduce.md#rabota-s-bolshimi-klyuchami-—-reduce_combiner).

If the error arises in a [Reduce](../../../user-guide/data-processing/operations/reduce.md) whose input includes more than one table (normally, dozens or hundreds), the scheduler may not have enough samples to break input data down more precisely and achieve uniformity. It is a good idea to utilize [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md) instead of [Reduce](../../../user-guide/data-processing/operations/reduce.md) in this case.

------
#### **Q: A running Sort or MapReduce generates "Intermediate data skew is too high (see "Partitions" tab). Operation is likely to have stragglers». What should I do?** { #intermediatedataskew }

**A:** This means that partitioning broke data down in a grossly non-uniform manner. For a [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md), this makes an input data skew highly likely (some keys have noticeably more data corresponding to them than others).

This is also possible for a [Sort](../../../user-guide/data-processing/operations/sort.md) and is related to the nature of the data and the sampling method. There is no simple solution for this issue as far as [Sort](../../../user-guide/data-processing/operations/sort.md) is concerned. We recommend contacting the system administrator.

------
#### **Q: How do I reduce the limit on job crashes that causes the entire operation to end in an error?**

**A:** The limit is controlled by the `max_failed_job_count` setting. For more information, see [Operation settings.](../../../user-guide/data-processing/operations/operations-options.md)

------
#### **Q: The operation page displays "Average job duration is smaller than 25 seconds, try increasing data_size_per_job in operation spec"?** { #shortjobsduration }

**A:** The message is an indication that the operation jobs are too short, and their launch overhead is slowing the operation down and reducing cluster resource performance. To correct the situation, you need to increase the amount of data being fed to the job as inputs. To do this, you need to increase the relevant settings in the operation spec:

* **Map, Reduce, JoinReduce, Merge**: `data_size_per_job`.
* **MapReduce**:
   * For `map`/`partition` jobs: `data_size_per_map_job`.
   * For `reduce` jobs: `partition_data_size`.
* **Sort**:
   * For `partition` jobs: `data_size_per_partition_job`.
   * For `final_sort` jobs: `partition_data_size`.

The default values are listed in the [sections](../../../user-guide/data-processing/operations/overview.md) on specific operation types.

------
#### **Q: The operation page is displaying "Aborted jobs time ratio ... is is too high. Scheduling is likely to be inefficient. Consider increasing job count to make individual jobs smaller"?** { #longabortedjobs }

**A:** The message means that the jobs are too long. Given that the pool's cluster resource allocation changes constantly with the arrival and departure of other users that run operations, an operation's jobs launch and are subsequently displaced. That is why the percentage of time wasted by the jobs becomes very large. The overall recommendation is to make jobs reasonably short. The best job duration is in the single minutes. You can accomplish this by reducing the amount of data fed to a single job either using the `data_size_per_job` or by optimizing and accelerating the code.

------
#### **Q: The operation page is displaying the following message: "Average CPU wait time of some of your job types is significantly high..."?** { #highcpuwait }

**A:** The message means that the jobs spent significant amounts of time (on the order tenths of the total job runtime) waiting for data from {{product-name}} or were hung up reading data from local disk/over the network. In the general case, it means that you are not utilizing the CPU efficiently. If there is waiting for data from {{product-name}}, you can look to reducing your jobs' `cpu_limit` or try moving your data to SSD for faster reading. If this is a feature of your process because it reads something large from the job's local disk and goes somewhere online, you should either consider optimizing your process or also reducing `cpu_limit`. Optimization implies the restructuring of the user process in a job to prevent disk reads or network requests from becoming a bottleneck.

------
#### **Q: What is the easiest method of sampling a table?**

**A:** In the {{product-name}} system, you can request input sampling for any operation.
In particular, you can start a trivial map and get what you need as follows:
`yt map cat --src _path/to/input --dst _path/to/output --spec '{job_io = {table_reader = {sampling_rate = 0.001}}}' --format yson`

Or simply read the data:
`yt read '//path/to/input' --table-reader '{sampling_rate=0.001}' --format json`

------
#### **Q: A running operation generates the following warning: "Account limit exceeded" and stops. What does it mean?** { #operationsuspended }

**A:** The message indicates that the `suspend_operation_if_account_limit_exceeded` specification parameter is enabled. Also, the account that hosts the operation's output tables is out of one of its quotas. Such as, the disk space quota. You need to figure out why this happened and resume the operation. You can view the account's quotas on the **Accounts** page of the web interface.

------
#### **Q: A running operation remains in pending mode a long time. When will it execute?** { #operationpending }

**A:** The {{product-name}} system has a limitation on the number of concurrently **executing** operations in each pool (as opposed to operations launched, or accepted for execution). By default, this limit is not large (around 10). Whenever a pool's limit on the number of executing operations is reached, new operations are queued. Queued operations will proceed when previous operations in the same pool exit. The limit on the number of executing operations is applicable at all levels of the pool hierarchy, that is to say, that if an operation is launched in pool A, it may be classified as pending not only if the limit is reached in pool A itself but also in any of pool A's parents. For more information on pools and pool configuration, please see [Scheduler and pools](../../../user-guide/data-processing/scheduler/scheduler-and-pools.md). If there is a reasonable need to run more operations concurrently, you need to send a request to the system administrator.

------
#### **Q: A running operation generates the following warning: "Excessive job spec throttling is detected". What does it mean?** { #excessivejobspecthrottling }

**A:** This message is an indication that an operation is computationally intensive from the standpoint of the resources used by the scheduler itself in the operation setup. This situation is normal behavior for a cluster at load. If you believe that the operation is taking unacceptably long to complete and continues in a starving state a long time, you need to advise your system administrator accordingly.

------
#### **Q: A running operation generates the following message: "Average cpu usage... is lower than requested 'cpu_limit'». What does it mean?** { #lowcpuusage }

**A:** The message means that the operation is using much less CPU than requested. By default, a single HyperThreading core is requested. This results in an operation blocking more CPU resources than it is using thereby making the use of the pool's CPU quota inefficient. If this behavior is expected, you should reduce the operation's cpu_limit (you can set it to a fraction), or else, you might review the operation jobs' runtime [statistics](../../../user-guide/problems/jobstatistics.md) profiling the job while it is running to understand what it is doing.

------
#### **Q: A running operation displays the following warning: "Estimated duration of this operation is about ... days". What does it mean?** { #operationtoolong }

**A:** The message is an indication that the expected time to complete the operation is too long. Expected completion time is computed as an optimistic estimate to complete running and pending jobs. As a cluster will update from time to time, and operations may restart, a large amount of utilized resources may go to waste. We recommend breaking operations down into smaller ones or looking for ways significantly to increase the quota under which an operation launches.

------
#### **Q: A running operation generates the following warning: "Scheduling job in controller of operation <operation_id> timed out". What does it mean?** { #schedulejobtimedout }

**A:** The warning means that an operation controller is not able to launch an operation job in the time allotted. This may occur if the operation is very heavy or if the scheduler is under heavy load. If you believe that an operation is taking very long to run and continues in a starving state for a long time, you should advise your system administrator accordingly.

------

#### **Q: A running operation displays warning "Failed to assign slot index to operation". What does it mean?** { #slotindexcollision }

**A:** If this happens, contact the administrator.

------
#### **Q: A running operation generates warning "Operation has jobs that use less than F% of requested tmpfs size". What does it mean?** { #unusedtmpfsspace }

**A:** You request a tmpfs for jobs in the specification (you can view the warning attributes to find out which specific jobs) but are not using the entire file system (apart from certain thresholds)??. tmpfs size is included into the memory limit, which means that a job requests a lot of memory but does not use it in the end. First, this reduces actual memory utilization in your cluster. Second, large tmpfs requests may slow down job scheduling since it is much more likely that the cluster will have a slot with 1 GB of memory than one with 15 GB. You should order as much tmpfs as your jobs actually need. You can review warning attributes or look at the [statistic](../../../user-guide/problems/jobstatistics.md) for `user_job/tmpfs_size` to find out about actual use of tmpfs by jobs.

------
#### **Q: When I lunch an operation, I get error "No online node can satisfy the resource demand". What do I do?**

**A:** The message is an indication that the cluster does not have a single suitable node to start the operation job. This normally happens in the following situations, for instance:

* The operation has CPU or memory requirements so large that a single cluster node's resources are not sufficient. For example, if there is an order for 1 TB of memory and 1000 CPUs for a job, this operation will not run returning an error to the client since {{product-name}} clusters do not have nodes with these properties.
* This specifies a `scheduling_tag_filter` that none of the cluster nodes match.

------
#### **Q: When I start a Merge, Reduce operation, I get error "Maximum allowed data weight violated for a sorted job: xxx > yyy"**

**A:** When jobs are bing built, the scheduler estimates that one job is getting too much data (hundreds of gigabytes), and the scheduler is unable to make a smaller job. The following options are available:

* When you are using [Reduce](../../../user-guide/data-processing/operations/reduce.md), and the input table has a monster key meaning that a single row in the first table corresponds to a large number of rows in another, as a result of the [Reduce](../../../user-guide/data-processing/operations/reduce.md) guarantee, all rows with this key must go into a single job, and the job will run indefinitely. You should use [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md) with the trivial mapper and the reduce combiner to pre-process monster keys.
* There are very many input tables being fed to an operation (100 or more) because chunks at the range boundary are not being counted precisely. The general observation is that the more input tables the less efficient the use of sorted input. You may want to use [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md).
* When using [Merge](../../../user-guide/data-processing/operations/merge.md), this error may result from suboptimal scheduler operation. You should contact the todo mailbox.

The above recommendations notwithstanding, if you are certain that you would like to launch the operation anyway and are ready for it to take a very long time, you can increase the value of the `max_data_weight_per_job` parameter, which will start the operation.

------
#### **Q: A running operation produces the following warning: "Legacy live preview suppressed", and live preview is not available. What does it mean?** { #legacylivepreviewsuppressed }

**A:** Live preview is a mechanism that creates heavy load for the master servers; therefore, by default, it is disabled for operations launched under robotic users.

If you wish to force activate live preview, use the `enable_legacy_live_preview = %true` option in the operation spec.

If you wish to disable this warning, use the `enable_legacy_live_preview = %false` option in the operation spec.

