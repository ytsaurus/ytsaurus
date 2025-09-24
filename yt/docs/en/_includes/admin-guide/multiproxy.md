# Reading from remote clusters via RPC proxy
Starting from version {{product-name}} 25.2, it became possible to perform requests to remote clusters through the RPC Proxy.

## Configuration
This functionality is configured through the [dynamic config](../../admin-guide/cluster-operations.md#ytdyncfgen).

For RPC proxies, the configuration of this functionality is located at `/api/multiproxy` in the dynamic config of RPC proxies. In the case of an RPC Proxy embedded into a Job Proxy, the configuration is located at `/exec_node/job_controller/job_proxy/job_proxy_api_service/multiproxy` in the dynamic config of the nodes.

The configuration consists of two parts:
1. Description of presets with allowed methods.
2. Mapping from a cluster name to a preset name.

Example configuration for reading from the cluster `remote_cluster` using the SDK:
```
{
  "presets" = {
    "allow_sdk_read" = {
      "enabled_methods" = "read";
      "method_overrides" = {
        "StartTransaction" = "explicitly_enabled";
        "AbortTransaction" = "explicitly_enabled";
        "AttachTransaction" = "explicitly_enabled";
        "PingTransaction" = "explicitly_enabled";
        "LockNode" = "explicitly_enabled";
      }
    }
  };
  "cluster_presets" = {
    "remote_cluster" = "allow_sdk_read";
  }
}
```

The `remote_cluster` must be present in the cluster list in `//sys/clusters`.
