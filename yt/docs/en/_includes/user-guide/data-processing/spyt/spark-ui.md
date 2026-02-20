# Spark UI

SPYT operations launch the Spark UI web interface. Usually, it cannot be opened directly: for example, if cluster nodes are in a closed internal network (as in Kubernetes) or job addresses change when moving to other machines.

To access the interface, [Task proxy](../../../../user-guide/proxy/task.md) is used. It creates a stable link that works even when jobs move between nodes, and verifies access rights upon entry.

{% note info %}

To work with Task proxy, the cluster administrator must first [deploy](../../../../admin-guide/install-task-proxy.md) this component.

{% endnote %}

## Configuring Spark cluster launch

For Spark UI to work through Task proxy, when launching a Standalone cluster, you need to enable the special configuration flag `spark.ui.reverseProxy`. To do this, add launch parameters:

```bash
spark-launch-yt ... --params '{"spark_conf"={"spark.ui.reverseProxy"="true"}}'
```

If the cluster is already running without this flag, to fix the UI operation, it will need to be restarted with the required parameter.

## How to access the UI

To open Spark UI, follow these steps:

1.  **Find out the operation ID.**
    It can be found on the **Operations** page in the {{product-name}} interface or in the output (stdout) of the `spark-launch-yt` / `spark-submit` console commands.

2.  **Find the service address.**
    Task proxy publishes information about all active web services in the Cypress system table: `//sys/task_proxies/services`. Find rows in this table corresponding to your `operation_id`.

    Example table contents:

    | __domain__                               | __operation_id__ (example)         |  __task_name__      | __service__ | __protocol__ |
    |------------------------------------------|-----------------------------------|---------------------|-------------|--------------|
    | 2ef4261c.my-cluster.ytsaurus.example.net | a6e04b98... | master              | ui          | http         |
    | 51a6d485.my-cluster.ytsaurus.example.net | a6e04b98... | history             | ui          | http         |
    | 37a5f11c.my-cluster.ytsaurus.example.net | 6699a5a9... | driver              | ui          | http         |

    - Row 1 (`master`): UI of the master node of a [standalone](../../../../user-guide/data-processing/spyt/cluster/cluster-start.md) cluster.
    - Row 2 (`history`): UI of the history server.
    - Row 3 (`driver`): UI of the driver when launched via [direct submit](../../../../user-guide/data-processing/spyt/direct-submit/desc.md).

3.  **Open the link.**
    Copy the value from the `domain` column (for example, `2ef4261c.my-cluster.ytsaurus.example.net`) and paste it into the browser address bar.
