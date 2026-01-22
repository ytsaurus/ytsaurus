# Configuration overrides

The {{product-name}} operator automatically generates static configuration files for all cluster components based on the `Ytsaurus` specification. In rare cases, you may need to override specific configuration parameters that are not exposed through the specification fields&mdash;for example, for enabling debug logging or tuning RPC timeouts. For this purpose, you can use the `configOverrides` field.

## How it works {#how-it-works}

Overrides are applied using a patching approach. You create a separate [Kubernetes ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) that contains only the parameters you want to change or add. In your {{product-name}} cluster specification, you reference this ConfigMap via configOverrides. The operator reads the overrides and merges them into the generated configuration before starting the components.

{% note info %}

Before using `configOverrides`, make sure that the specified option exists in the target component's configuration and is supported in your current {{product-name}} version&mdash;the operator does not perform this validation. If an option in the override is incorrect, the changes will be silently ignored.

{% endnote %}

## Configuration format {#configuration-format}

The ConfigMap must contain configuration fragments in YSON format.

## ConfigMap structure {#configmap-structure}

Use the following structure when creating the ConfigMap:
- Each key is the name of a component configuration file. For example, `ytserver-http-proxy.yson` for the HTTP proxy or `ytserver-master.yson` for the master server.
- Each value is a YSON configuration fragment with the parameters to override.

## Example {#example}

Here's an example of using `configOverrides` to configure the cookie domain for HTTP proxies:

**Step 1:** Create a ConfigMap with configuration overrides:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: overrides
data:
  ytserver-http-proxy.yson: |
    {
        "auth" = {
            "cypress_cookie_manager" = {
                "cookie_generator" = {
                    "domain" = ".test.ytsaurus.keenable.ai";
                }
            };
        };
    }
```

**Step 2:** Reference the ConfigMap in your `Ytsaurus` specification:

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Ytsaurus
metadata:
  name: ytdemo
spec:
  configOverrides:
    name: overrides

  # ... rest of your specification
```

## Available configuration files {#available-configuration-files}

You can override configuration for any component by using the appropriate configuration file name:

- `ytserver-master.yson` - Master servers
- `ytserver-http-proxy.yson` - HTTP proxies
- `ytserver-rpc-proxy.yson` - RPC proxies
- `ytserver-data-node.yson` - Data nodes
- `ytserver-exec-node.yson` - Exec nodes
- `ytserver-tablet-node.yson` - Tablet nodes
- `ytserver-scheduler.yson` - Schedulers
- `ytserver-controller-agent.yson` - Controller agents
- `ytserver-discovery.yson` - Discovery service
