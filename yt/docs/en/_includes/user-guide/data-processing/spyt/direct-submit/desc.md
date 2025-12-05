# Direct SPYT Submit

This section provides a high-level description of direct submit to {{product-name}} and its components.

The launch of Spark applications in direct submit mode is described in a separate [section](../../../../../user-guide/data-processing/spyt/launch.md#submit).

## Operating Principle {#explanation}

The key idea is that Spark delegates resource management to the {{product-name}} scheduler.

### Resource Allocation for the Driver {#driver-resources}

- **Cluster mode**: The driver is also launched as a separate Vanilla operation within the {{product-name}} cluster. In this mode, the driver's lifecycle (including logging, monitoring, and restart on failure) is fully controlled by YTsaurus. Suitable for production tasks.
- **Client mode**: The driver is launched on a client host — for example, on a developer's local machine or in a Jupyter notebook. This mode is convenient for debugging and interactive work, as it provides direct access to the driver's logs and state.

  {% note warning "Warning" %}

  In client mode, network access between executors and the driver must be ensured.

  {% endnote %}

### Resource Allocation for Executors {#executor-resources}

In this mode, the scheduler allocates resources to executors directly as part of a separate Vanilla operation on the {{product-name}} cluster. A job within the operation corresponds to one Spark executor. The specifications for executors (number of CPUs, amount of memory, disk type, etc.) are described in the Task section of the operation specification. It is possible to describe different resource profiles for Spark executors for tasks with heterogeneous workload during the lifecycle.

![](../../../../../../images/spyt-direct-submit-operation-concept.png){ .center }

### Configuring {{product-name}} Operation {#operation-config}

In addition to the standard Spark parameters for the driver and executors, you can further configure operations and tasks through the corresponding conf parameters `spark.ytsaurus.{driver|executor}.{operation|task}.parameters`. The value is specified as a YSON string with valid Vanilla operation parameters. For example, you can set the type and amount of attached disks:

```bash
--conf spark.ytsaurus.executor.task.parameters='{"disk_request"={"disk_space"=1234567;
                                                 "account"="disk_account_name";
                                                 "medium_name"="ssd_slots_physical";};
                                                }'
```
The full list of parameters can be found in the section [operation options](../../../../../user-guide/data-processing/operations/operations-options.md).

### Advantages of the Approach {#advantages}

- **Efficient resource usage** — allocation on demand, minimal downtime.
- **Simple management** — a single level of control instead of duplication (YTsaurus + Standalone).
- **Flexibility** — operation without losses at low load.
- **Fast task startup** — no need to pre-launch the cluster.
- **Easy migration** — compatibility with other clusters (including Hadoop) due to standard SparkAPI.

## Executing Local Files {#submit-local}

Starting with SPYT version 2.4.0, when launching tasks directly in {{product-name}}, you can specify local files as executable modules and dependencies without pre-loading them to Cypress. In this case, local files will be pre-loaded into the file cache on Cypress and then used from there in the application. On re-launch, the cached files will be used. An example of the `spark-submit` command for this method is provided below:

```bash
spark-submit --master ytsaurus://<cluster name> \
             --deploy-mode cluster \
             --num-executors 5 \
             --executor-cores 4 \
             --py-files ~/path/to/my/dependencies.py \
             ~/path/to/my/script.py
```
