# Managing dynamic table bundles

YTsaurus provides flexible management of dynamic table bundles. For each bundle, you can set the number of tablet nodes, distribute threads across thread pools, and allocate memory by categories. If a node fails, automation will assign a new node from the spare pool to the bundle.

## Overview

The system that manages bundles is called the Bundle controller. It manages instances of tablet nodes. Each node is linked to a certain bundle or is in the spare pool. Each bundle and instance belongs to a specific zone — a set of instances that share the same spare pool. Currently, only one zone is supported: `zone_default`.

The Bundle controller sets the `node_tag_filter` attribute on each bundle, which looks like `zone_default/<bundle_name>`. This value is also written to the `user_tags` attribute of all nodes assigned to the bundle. Each node can belong to no more than one bundle. If a node fails, the Bundle controller automatically assigns the bundle a new one.

You can specify a configuration for each bundle: the number of tablet nodes, the number of tablet cells per node, the distribution of memory by categories, and the number of threads in certain thread pools. You can manage these settings through the user interface on the bundle page.

The Bundle controller also manages the accounts where the bundle's tablet cell changelogs and snapshots are stored.

## Limitations

Under the Bundle controller model, bundles are completely isolated from each other across individual instances of tablet nodes. This means your cluster should have at least as many nodes as there are bundles. Specifically, since clusters have two bundles (`default` and `sys`) set up by default, **the Bundle controller must have at least two tablet nodes to function**.

Currently, the Bundle controller can only work with clusters where all tablet node instances have the same amount of CPU and RAM.

## Initial setup { #setup }

### Installing the service

The Bundle controller is a separate cluster component, similar to the master, scheduler, and other components. It is executed using the `ytserver-bundle-controller` binary.

If you're using the k8s operator, add the following section to the specification:

```
spec:
  bundleController:
    instanceCount: N
    loggers: *loggers
```

If you manage the cluster manually, run `ytserver-bundle-controller` with the config. For a sample config, see the [k8s operator repository](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/b880afc047a0793f3f9e7134e2a5a0aff4f81fff/pkg/ytconfig/bundle_controller.go#L43).

A single Bundle controller instance is sufficient for proper operation; however, to ensure fault tolerance, we recommend deploying multiple instances.

### Cluster configuration

After launching the component, you need to configure the cluster by properly marking up bundles and tablet nodes to allow the Bundle controller to take over.

To simplify the setup of a typical scenario, you can use a script: [bundle_controller_tools.py](https://github.com/ytsaurus/ytsaurus/tree/main/yt/yt/scripts/dynamic_tables/bundle_controller_tools). There are two ways to run the script.
- Compile a binary in advance using the `ya make` command.
- Run `__main__.py` directly using the Python interpreter. This requires the system to have the `ytsaurus-client` pip package installed.
For the script to work, save the token in `~/.yt/token` or use one of the alternative token configuration methods described in the Python API [documentation][../api/python/userdoc#configuration_token]. The cluster address is specified using the `YT_PROXY` variable or the `--proxy` argument.

We recommend running the script using the `init --init-all` command. When you run the script, you must specify the `--cpu` and `--memory` flags to indicate the number of CPU cores and the amount of RAM (in bytes) for the tablet node instances. If the cluster contains instances of different sizes, it is recommended to set parameters based on the smallest instance.

The following sections describe the setup steps performed by the script.

### System directories

**When running `bundle_controller_tools.py init`, system directories are created automatically. To skip this step, you can use the `--no-init-system-directories` flag.**

To start using the Bundle controller, create the following directories:

- `//sys/bundle_controller/coordinator`
- `//sys/bundle_controller/controller/zones/zone_default`
- `//sys/bundle_controller/controller/bundles_state`

You must also create an account named `bundle_system_quotas`.

### Zone zone\_default

**To automatically set up the zone, run `bundle_controller_tools.py init` with the `--init-default-zone` flag.**

Instances of each type are linked to a certain bundle or are in the spare pool. Each bundle and instance belongs to a specific zone. In the zone config, specify the size of the instances (CPU and RAM) present in the cluster, as well as the default settings for thread pools and node memory categories.

{% note warning "Attention" %}

Currently, the Bundle controller can only work with clusters where all instances are of the same size. This means that the `resource_guarantee` field in the zone config, in all bundle configs, and in all tablet node annotations must have the same value.

If the cluster contains instances of different sizes, it is recommended to set `resource_guarantee` based on the smallest instance.

{% endnote %}

The zone config is located at `//sys/bundle_controller/controller/zones/zone_default`. Each config field is an attribute of the specified directory.

#### Tablet nodes

The configuration of tablet nodes is specified in the `tablet_node_sizes` attribute. This is a map consisting of a single `regular` field (instance type). It looks like this:

```
{
    "default_config" = {
        "cpu_limits" = {
            "lookup_thread_pool_size" = 8;
            "write_thread_pool_size" = 10;
            "query_thread_pool_size" = 8;
        };
        "memory_limits" = {
            "tablet_dynamic" = 1000000;
            "versioned_chunk_meta" = 1000000;
            "uncompressed_block_cache" = 200000;
            "tablet_static" = 0;
            "compressed_block_cache" = 1000000;
            "reserved" = 5000000;
            "lookup_row_cache" = 0;
        };
    };
    "resource_guarantee" = {
        "net_bytes" = 0;
        "vcpu" = 6000;
        "memory" = 1073741824;
    };
}
```

Here, `default_config` is the default distribution of thread pools and memory categories for all bundles, and `resource_guarantee` is the size of the tablet node's instance. `memory` corresponds to the RAM size in bytes, while `vcpu` corresponds to the number of the container's CPU cores multiplied by 1000. The `net_bytes` field is currently not used.

For more information about the distribution of thread pools and memory categories, see [Creating a bundle](#create_bundle).

#### RPC proxy

{% note info "Note" %}

In the current version, RPC proxy management is not fully supported.

{% endnote %}

The RPC proxy configuration is specified in the `rpc_proxy_sizes` attribute. This is a map consisting of a single `regular` field (instance type). It looks like this:

```
 {
   "resource_guarantee" = {
        "net_bytes" = 0;
        "vcpu" = 6000;
        "memory" = 1073741824;
    };
}
```

The meanings of the fields are the same as for tablet nodes.

### Instance annotations

**To automatically configure instances, run `bundle_controller_tools.py init` with the `--init-tablet-nodes` flag.**

All tablet nodes managed by the Bundle controller must be labeled with certain attributes. These attributes are described in detail below, see [Adding nodes to the cluster](#new_instance).

### Bundle setup

**To automatically configure bundles, run `bundle_controller_tools.py init` with the `--init-bundle <bundle-name>`, `--init-all-bundles`, or `--init-all` flag.**

For the Bundle controller to start managing existing bundles, set the attributes specified in the [Creating a bundle](#create_bundle) section. If the `node_tag_filter` attribute was set on those existing bundles, first set it to `""` (an empty string).

The `bundle_controller_tools.py` script calculates the necessary number of nodes for each existing bundle based on the current number of tablet cells. If you need to assign more or fewer nodes, use the `--bundle-node-count` argument, which accepts a YSON map in the format `{<bundle_name>=<node_count>}`.

#### Accounts for changelogs and snapshots

**To automatically configure changelog and snapshot accounts, run `bundle_controller_tools.py init` with the `--init-bundle-system-quotas` or `--init-all` flag.**

The Bundle controller can manage accounts that store changelogs and snapshots of tablet cells. The accounts are controlled by the `changelog_account` and `snapshot_account` fields in the bundle options in the `//sys/tablet_cell_bundles/<bundle_name>/@options` attribute. Accounts managed by the Bundle controller are named `<bundle_name>_bundle_system_quotas` and are children of the root account `bundle_system_quotas`. The Bundle controller automatically adjusts the quotas of the parent and child accounts when the number of tablet cells changes.

The `bundle_controller` section of the Bundle controller's static config contains [fields](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/server/cell_balancer/config.h#L78) that regulate the amount of resources needed for one tablet cell.

| Name | Value | Default value |
| ------------------------------ | -------------------------------------------------------------- | --------------------- |
| `node_count_per_cell` | Number of Cypress nodes per tablet cell. | 25 |
| `chunk_count_per_cell` | Number of chunks per table cell. | 100 |
| `journal_disk_space_per_cell` | Disk space for changelogs per tablet cell. | 100 GB |
| `snapshot_disk_space_per_cell` | Disk space for snapshots per tablet cell. | 15 GB |
| `min_node_count` | Minimum number of Cypress nodes assigned to the bundle. | 1000 |
| `min_chunk_count` | Minimum number of chunks assigned to the bundle. | 1000 |

## Creating a bundle { #create_bundle }

**To create a bundle, you can use the `bundle_controller_tools.py create-bundle <bundle_name>` script.**

For a bundle to be managed by the Bundle controller, it must have the `enable_bundle_controller = %true` and `zone = "zone_default"` attributes. You must also set the `bundle_controller_target_config` attribute with the following structure:

```
{
    "cpu_limits" = {
        "lookup_thread_pool_size" = 2;
        "query_thread_pool_size" = 2;
        "write_thread_pool_size" = 2;
    };
    "memory_limits" = {
        "tablet_dynamic" = 1000000;
		"tablet_static" = 0;
		"compressed_block_cache" = 1000000;
		"uncompressed_block_cache" = 200000;
		"versioned_chunk_meta" = 1000000;
		"lookup_row_cache" = 0;
		"reserved" = 5000000;
    };
    "rpc_proxy_count" = 0;
    "rpc_proxy_resource_guarantee" = {
        "memory" = 1073741824;
        "net_bytes" = 0;
        "vcpu" = 6000;
    };
    "tablet_node_count" = 1;
    "tablet_node_resource_guarantee" = {
        "memory" = 1073741824;
        "net_bytes" = 0;
        "vcpu" = 6000;
    };
}
```

The fields have the following meanings:

- `tablet_node_count`: Number of tablet nodes assigned to the bundle.
- `tablet_node_resource_guarantee`: Size of the instance of tablet nodes assigned to the bundle. Must match the value in the zone config.
- `rpc_proxy_count`: Number of RPC proxies assigned to the bundle. *Currently not supported.*
- `rpc_proxy_resource_guarantee`: Size of the instance of RPC proxies assigned to the bundle. Must match the value in the zone config. *Currently not supported.*
- `memory_limits`: Distribution of the bundle's tablet node memory by categories. The total value must not exceed `memory` in `resource_guarantee`.
  - `tablet_dynamic`: Memory buffer for dynamic stores.
  - `tablet_static`: Memory for in-memory tables (with `in_memory_mode` other than `none`).
  - `compressed_block_cache`, `uncompressed_block_cache`: Caches for data blocks used when reading.
  - `versioned_chunk_meta`: Cache of chunk metainformation.
  - `lookup_row_cache`: Memory for the row-level cache.
  - `reserved`: Memory reserved for system needs and unaccounted categories.
- `cpu_limits`: Distribution of the bundle’s tablet node threads across thread pools. Unlike memory, overcommit is allowed here, and the number of threads can exceed `vcpu/1000` in `resource_guarantee`.
  - `lookup_thread_pool_size`: Number of threads for lookup queries.
  - `query_thread_pool_size`: Number of threads for select queries.
  - `write_thread_pool_size`: Number of tablet cells per node.

You must also set the `cpu` and `memory` fields in the bundle’s `resource_limits` attribute. The total `vcpu` and `memory` of the bundle’s instances cannot exceed `cpu*1000` and `memory` from `resource_limits`. For more information, see [Resource quotas](#resource_limits).

## Adding nodes to the cluster { #new_instance }

After adding instances to the cluster, mark them with the `bundle_controller_annotations` attribute. To do this, you can use the `bundle_controller_tools.py` script with the `--init-tablet-nodes` flag.

The `bundle_controller_annotations` attribute looks like this:

```
{
    "allocated" = %true;
    "allocated_for_bundle" = "spare";
    "resources" = {
        "vcpu" = 6000;
        "memory" = 1073741824;
        "type" = "regular";
    };
}
```

Here, `allocated` indicates that the instance is managed by the Bundle controller, `allocated_for_bundle = "spare"` indicates that the instance is in the spare pool and does not belong to any bundle, and `resources` contains the instance's resources and must match the `resource_guarantee` field in the corresponding zone settings section.

## Decommissioning nodes { #maintenance }

To decommission a node, set the `//sys/cluster_nodes/<node_address>/@cms_maintenance_requests` attribute to `{maintenance={}}`. After that, the Bundle controller will gracefully move all tablet cells from this node to another node pulled from the spare pool. Once there are no tablet cells left on the node being decommissioned, it can be safely turned off. The presence of tablet cells is shown in the `tablet_slots` attribute.

If a node fails, the Bundle controller will assign a new node from the spare pool to the bundle, after which the tablet cells will be restored on it.

## Resource quotas { #resource_limits }

Bundle resources are located in the `//sys/tablet_cell_bundles/<bundle_name>/@resource_limits` attribute. The attribute contains the following fields:

- `tablet_count`: Limit on the number of tablets of the bundle's tables.
- `tablet_static_memory`: Limit on the total size of the bundle's in-memory tables. Set automatically.
- `cpu`: Limit on the total `cpu` of the bundle's nodes.
- `memory`: Limit on the total `memory` of the bundle's nodes.

`tablet_count` and `tablet_static_memory` fields affect the tables created in the bundle. `cpu` and `memory` affect the number of tablet nodes that the Bundle controller can assign to the bundle.

`tablet_static_memory` field is set automatically by the Bundle controller. Its value is calculated as `node_count * memory`, where `node_count` is the number of nodes assigned to the bundle, and `memory` is the amount of memory on a tablet node instance defined in the zone config.

To manage resources, you need the `write` permission for the bundle. The cluster administrator is responsible for managing resources. You should grant node quotas so that the total quota of all bundles does not exceed the number of nodes in the cluster, taking into account the spare pool for node failures.

To simplify the setup of `cpu` and `memory` limits, the `bundle_controller_tools.py` script has a special command, `set-bundle-resource-limits`. It accepts a limit on the number of nodes and sets the limits according to the zone config.

## Managing a bundle

You can manage bundles by clicking `Edit bundle` on the necessary bundle page in the UI. The interface enables you to specify the number of nodes, the memory distribution by categories, and the number of threads in the thread pools for lookup and select queries.

To manage a bundle, you need the `manage` permission for it.

## Disabling the Bundle controller

To switch the Bundle controller to read-only mode, set the `//sys/@disable_bundle_controller` attribute to `%true`. The Bundle controller will stop performing any actions on the cluster; however, you can still monitor the state of bundles from the user interface.

## Troubleshooting { #faq }

#### The Bundle controller is not working

- Check the `//sys/@disable_bundle_controller` attribute.
- Check for locks at `//sys/bundle_controller/coordinator/lock/@locks`. If there are no locks it means that the Bundle controller cannot register with the cluster — check the container's error log for culprits.

#### The bundle UI shows the `Failed` state

The bundle UI displays two statuses: `Health` is the state of tablet cells, and `State` is the state of the Bundle controller. If the `State` shows `Failed`, click it to find out what went wrong. In some cases, the issue may be caused by violations of the Bundle controller invariants (for example, if `resource_guarantee` in the zone, nodes, and bundles settings do not match). Recommended course of action:

- Disable the Bundle controller by setting the `//sys/@disable_bundle_controller` attribute to `%true`.
- Run the `bundle_controller_tools.py drop-allocations <bundle_name>` command. This command will remove incorrect node allocation requests.
- Make sure the configuration is correct (for example, by running `bundle_controller_tools.py init --init-all --cpu <cpu_guarantee> --memory <memory_guarantee>`).
