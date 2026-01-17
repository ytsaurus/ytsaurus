# Changing the cluster configuration

## Configuration overrides { #config-overrides }

The {{product-name}} operator automatically generates static configuration files for all cluster components based on the `Ytsaurus` specification. In rare cases, you may need to override specific configuration parameters that are not exposed through the specification fields. For this purpose, you can use the `configOverrides` field.

### When to use configOverrides

The `configOverrides` mechanism should only be used when:

- You need to configure advanced parameters not available in the `Ytsaurus` specification
- You're implementing a temporary workaround for a specific issue
- You need to fine-tune component behavior for specific use cases

{% note warning %}

Using `configOverrides` bypasses the operator's configuration management and should be avoided when possible. Prefer using native specification fields whenever they are available.

{% endnote %}

### How it works

The `configOverrides` field references a Kubernetes ConfigMap that contains YSON configuration snippets for specific components. These snippets are merged with the operator-generated configuration.

### Configuration format

Create a ConfigMap where:
- Each key represents a component configuration file name (e.g., `ytserver-http-proxy.yson`)
- Each value contains a YSON configuration snippet to override or extend the default configuration

### Example

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

### Available configuration files

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