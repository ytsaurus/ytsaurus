
# Troubleshooting

## Cluster generates errors on startup

To get information on a running transaction, use the `spark-discovery-yt` utility:

```bash
spark-discovery-yt <cluster-name> --discovery-path my_discovery_path
```
A task list for this transaction always includes a master, a worker, and the Spark History Server. If the master is non-functional, so are the other components. If everything is running properly, the worker log will include data on the accepted task and the start of task execution.

If a task produces an error at startup, you need to:
- Open the task log via Master Web UI. It has links to the driver and the executor logs.
- Open Worker UI via Master Web UI and review the application startup sequence.
- View the [Event Log](../../../../user-guide/data-processing/spyt/logs.md).

## Error "Cannot get rpc proxies" { #cannot-get-rpc-proxies }

The `Cannot get rpc proxies` message means that the SPYT client did not get a list of available RPC proxies of the cluster. There are two possible reasons: either [discovery](../../../../admin-guide/cluster-access-proxy/index.md#discovery-roles) returned an empty list, or updating the proxy list failed (for example, because of a timeout or an HTTP error).

To diagnose the problem, locate the failing process, determine its settings, and check discovery with the same parameters:

1. Use the log or Spark UI to locate the error: application submission, the driver, or an executor. In the driver, distinguish the operation-management connection from data connections. For their purposes, see the [connection model](../../../../user-guide/data-processing/spyt/direct-submit/desc.md#proxy-roles).
1. Find the nested `Caused by` exception in the error stack. If present, the proxy list update failed. Use the exception text to check the cluster address, network connectivity, authorization, and discovery availability. Without a nested exception, discovery most likely returned an empty list. The following steps help check this case.
1. Use the process and deploy mode to select the role parameter and the sources to check:

    #|
    || **Error location** | **Role parameter** | **Applicable sources** ||
    || Application submission or operation management from the driver in `client` mode | `spark.hadoop.yt.proxyRole` | `--conf` or the application's `SparkConf` ||
    || Data client of the driver in `client` mode | `spark.hadoop.yt.proxyRole` (`yt.proxyRole` in the Hadoop configuration) | The configuration used to create the data client; for Hadoop paths, the driver's saved configuration ||
    || Driver in `cluster` mode or an executor | `spark.hadoop.yt.clusterProxyRole`, falling back to `spark.hadoop.yt.proxyRole` | User configuration, then release and global configurations. In `client` mode, take a separate executor role from the launch parameters ||
    || Data Source V1 write in Spark 3.5.5 | Role from the driver's saved Hadoop configuration | Account for the [V1 write limitation](../../../../user-guide/data-processing/spyt/direct-submit/desc.md#v1-write-proxy-role) if the cluster role is set only in the global configuration ||
    |#

1. Determine the effective value of the selected parameter. Check the applicable sources in priority order: user `--conf` or `SparkConf`, the SPYT release configuration, and `//home/spark/conf/global`. Check the result against the configuration used to create the client: a saved Hadoop configuration may differ from the current `SparkConf`. If no separate cluster role is set, the client role is used. If no source sets a role, discovery requests `default`. For the configuration order, see [Configuration sources and priority](../../../../user-guide/data-processing/spyt/direct-submit/desc.md#config-sources). For parameter values, see the [reference](../../../../user-guide/data-processing/spyt/thesaurus/configuration.md#proxy-roles).
1. Check the discovery response for that role. Send `GET /api/v4/discover_proxies?type=rpc&role=<role>` to the cluster address, such as `yt.example.com`. Before sending the request, select the network parameter for the same client:

    #|
    || **Client** | **Discovery request parameter** ||
    || Application submission or operation management from the driver in `client` mode | Add `network_name=<network>` if `spark.hadoop.yt.proxyNetworkName` is set ||
    || Data client of the driver in `client` mode outside a {{product-name}} job (without `YT_JOB_ID`) | Add `network_name=<network>` if `proxyNetworkName` is set in that client's configuration ||
    || Operation management from the driver in `cluster` mode | Omit `network_name`. This client ignores `spark.hadoop.yt.proxyNetworkName` ||
    || A standard SPYT data client in a {{product-name}} job with `YT_JOB_ID` present | Omit `network_name`. This client ignores the setting ||
    |#

    The request parameters must match the client's settings. Otherwise, you may get a proxy list for a different network. If `role` is omitted, discovery requests `default`. An empty `proxies` list confirms that no proxies are available for the role and network being checked.

1. Make sure that RPC proxies with the required role exist and are available. For proxy groups with separate roles, see [Project isolation (for RPC)](../../../../admin-guide/cluster-access-proxy/manage-traffic.md#projects). For role definitions, see [The concept of a role](../../../../admin-guide/cluster-access-proxy/index.md#proxy-role). If SPYT runs outside the cluster, also check the [SPYT access setup](../../../../admin-guide/cluster-access-proxy/spyt.md).
1. Correct the role in the identified source, or ask the cluster administrator to create RPC proxies with that role. For separate client and cluster roles, use the [launch examples](../../../../user-guide/data-processing/spyt/launch.md#proxy-roles-examples).

{% note warning %}

Do not remove the role from the configuration without checking. A missing role and an explicitly specified `default` role are not equivalent: if the role is not set in one source, the value from the next source by priority applies (for example, `custom` from `//home/spark/conf/global`), while an explicit `default` role overrides it. If the role is set intentionally, for example, so that SPYT uses a dedicated group of proxies, removing it may switch the application to a different role, and it stops working with the dedicated group.

{% endnote %}
