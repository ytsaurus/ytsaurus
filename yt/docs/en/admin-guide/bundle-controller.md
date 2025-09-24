# Tablet cell bundle management

YTsaurus provides flexible management of dynamic table bundles. For each bundle you can set the number of tablet nodes, the distribution of threads across thread pools, and memory by categories. If a node fails, automation will assign a new node from the spare pool to the bundle.

## Overview

The system that manages bundles is called the Bundle controller. It manages instances of tablet nodes. Each node is bound to a certain bundle or is in the spare pool. Each bundle and instance belongs to a specific zone—a set of instances that share the same spare pool. Currently, only one zone is supported: `zone_default`.

The Bundle controller sets the `node_tag_filter` attribute on each bundle in the form `zone_default/<bundle_name>`. This value is also written to the `user_tags` attribute of all nodes assigned to the bundle. Each node can belong to no more than one bundle. In case of node failure, the Bundle controller automatically assigns a new node to the bundle.

For each bundle, you can specify its configuration: the number of tablet nodes, the number of tablet cells per node, the distribution of memory by categories, and the number of threads in certain thread pools. You can manage these settings through the user interface on the bundle's page.

The Bundle controller also manages the accounts where the bundle's tablet cell changelogs and snapshots are stored.

## Initial setup {#setup}

To simplify the setup of a typical scenario, a script has been prepared: [bundle\_controller\_tools.py](https://github.com/ytsaurus/ytsaurus/tree/main/yt/yt/scripts/dynamic_tables/bundle_controller_tools). It is recommended to run it with the `--init-all` flag. When launching, you must specify the `--cpu` and `--memory` flags corresponding to the parameters of the tablet node instances. If there are instances of different sizes in the cluster, it is recommended to set the parameters for the smallest instance.

The following sections describe the setup steps performed by the script.

### System directories

**When running `bundle_controller_tools.py`, system directories are created automatically. To skip this step, you can use the `--no-init-system-directories` flag.**

To start using the Bundle controller, you need to create the following directories:

- `//sys/bundle_controller/coordinator`
- `//sys/bundle_controller/controller/zones/zone_default`
- `//sys/bundle_controller/controller/bundles_state`

You must also create an account named `bundle_system_quotas`.

### Zone zone\_default

**To automatically set up the zone, run `bundle_controller_tools.py` with the `--init-default-zone` flag.**

Instances of each type are bound to a specific bundle or are in the spare pool. Each bundle and instance belongs to a specific zone. In the zone config, you must specify the size of the instances installed in the cluster (CPU and RAM), as well as the default settings for node thread pools and memory categories.

{% note warning "Warning" %}

Currently, the Bundle controller can only work with clusters in which all instances are of the same size. This means that the `resource_guarantee` field in the zone config, in all bundle configs, and in all tablet node annotations must have the same value.

If there are instances of different sizes in the cluster, it is recommended to set `resource_guarantee` to the smallest instance.

{% endnote %}

The zone config is located at `//sys/bundle_controller/controller/zones/zone_default`. Each field of the config is an attribute of the specified directory.

#### Tablet nodes

The configuration of tablet nodes is specified in the `tablet_node_sizes` attribute. This is a map consisting of a single field `regular` (the instance type). It looks like this:

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

Here, `default_config` is the default distribution of thread pools and memory categories for all bundles, and `resource_guarantee` is the size of a tablet node instance. The Memory field corresponds to the RAM size in bytes; the `vcpu` field corresponds to the number of CPU cores of the container multiplied by 1000. The `net_bytes` field is currently not used.

More about the distribution of thread pools and memory categories is written in the [Creating a bundle](#create_bundle) section.

#### RPC proxy

{% note info "Note" %}

In the current version, RPC proxy management is not fully supported.

{% endnote %}

The RPC proxy configuration is specified in the `rpc_proxy_sizes` attribute. This is a map consisting of a single field `regular` (the instance type). It looks like this:

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

**To automatically configure instances, run `bundle_controller_tools.py` with the `--init-tablet-nodes` flag.**

All tablet nodes managed by the Bundle controller must be labeled with certain attributes. The attributes are described in detail in the [Adding nodes to the cluster](#new_instance) section.

### Bundle setup

**To automatically configure bundles, run `bundle_controller_tools.py` with the `--init-bundle <bundle-name>`, `--init-all-bundles`, or `--init-all` flag.**

For the Bundle controller to start managing existing bundles, you need to set the attributes specified in the [Creating a bundle](#create_bundle) section. If the `node_tag_filter` attribute was set on existing bundles, you must first set it to `""` (an empty string).

The `bundle_controller_tools.py` script calculates the required number of nodes for each existing bundle based on the current number of tablet cells. If you need to assign more or fewer nodes, use the `--bundle-node-count` argument, which takes a YSON map in the format `{<bundle_name>=<node_count>}`.

#### Accounts for changelogs and snapshots

**To automatically configure changelog and snapshot accounts, run `bundle_controller_tools.py` with the `--init-bundle-system-quotas` or `--init-all` flag.**

The Bundle controller can manage the accounts that store tablet cell changelogs and snapshots. The accounts are controlled by the `changelog_account` and `snapshot_account` fields in the bundle options in the attribute `//sys/tablet_cell_bundles/<bundle_name>/@options`. Accounts managed by the Bundle controller are named `<bundle_name>_bundle_system_quotas` and are children of the root account `bundle_system_quotas`. The Bundle controller automatically adjusts the quotas of the parent and child accounts when the number of tablet cells changes.

In the static config of the Bundle controller, in the `bundle_controller` section, there are fields that regulate the amount of resources needed for a single tablet cell:

- `node_count_per_cell`: number of Cypress nodes
- `chunk_count_per_cell`: number of chunks
- `journal_disk_space_per_cell`: disk space for changelogs
- `snapshot_disk_space_per_cell`: disk space for snapshots
- `min_node_count`: minimum number of Cypress nodes allocated to a bundle
- `min_chunk_count`: minimum number of chunks allocated to a bundle

## Creating a bundle {#create_bundle}

**To create a bundle, you can use the `bundle_controller_tools.py` script with the `--create-bundle <bundle_name>` flag.**

For a bundle to be managed by the Bundle controller, it must have the attributes `enable_bundle_controller = %true` and `zone = "zone_default"`. You must also set the `bundle_controller_target_config` attribute with the following structure:

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

- `tablet_node_count`: the number of tablet nodes assigned to the bundle.
- `tablet_node_resource_guarantee`: the size of the tablet node instances assigned to the bundle. Must match the value in the zone config.
- `rpc_proxy_count`: the number of RPC proxies assigned to the bundle. Currently not supported.
- `rpc_proxy_resource_guarantee`: the size of the RPC proxy instances assigned to the bundle. Must match the value in the zone config. Currently not supported.
- `memory_limits`: the distribution of the bundle’s tablet node memory by categories. The total value must not exceed `memory` in `resource_guarantee`.
  - `tablet_dynamic`: memory buffer for dynamic stores.
  - `tablet_static`: memory for in-memory tables (with `in_memory_mode` other than `none`).
  - `compressed_block_cache`, `uncompressed_block_cache`: caches for data blocks used when reading.
  - `versioned_chunk_meta`: cache of chunk metadata.
  - `lookup_row_cache`: memory for the row-level cache.
  - `reserved`: memory reserved for system needs and unaccounted categories.
- `cpu_limits`: the distribution of the bundle’s tablet node threads across thread pools. Unlike memory, overcommit is allowed here, and the number of threads can exceed `vcpu/1000` in `resource_guarantee`.
  - `lookup_thread_pool_size`: number of threads for lookup requests.
  - `query_thread_pool_size`: number of threads for select requests.
  - `write_thread_pool_size`: number of tablet cells per node.

You must also set the `cpu` and `memory` fields in the bundle’s `resource_limits` attribute. The total `vcpu` and `memory` of the bundle’s instances cannot exceed `cpu*1000` and `memory` from `resource_limits`. More details are in the [Resource quotas](#resource_limits) section.

## Adding nodes to the cluster {#new_instance}

After adding instances to the cluster, you need to mark them with the `bundle_controller_annotations` attribute. To do this, you can use the `bundle_controller_tools.py` script with the `--init-tablet-nodes` flag.

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

Here, the `allocated` field means that the instance is managed by the Bundle controller, `allocated_for_bundle = "spare"` means that the instance is in the spare pool and does not belong to any bundle, and the `resources` field contains the instance resources and must match the `resource_guarantee` field in the corresponding zone settings section.

## Decommissioning nodes {#maintenance}

To decommission a node, you need to set the attribute `//sys/cluster_nodes/<node_address>/@cms_maintenance_requests` to `{maintenance={}}`. After that, the Bundle controller will gracefully move all tablet cells from this node to another node pulled from the spare pool. Once there are no tablet cells left on the node being decommissioned, it can be safely turned off. The presence of tablet cells is shown in the `tablet_slots` attribute.

In the event of an unexpected node failure, the Bundle controller will assign a new node from the spare pool to the bundle, after which the tablet cells will be restored on it.

## Resource quotas {#resource_limits}

Bundle resources are located in the attribute `//sys/tablet_cell_bundles/<bundle_name>/@resource_limits`. The attribute contains the following fields:

- `tablet_count`: limit on the number of tablets of the bundle's tables.
- `tablet_static_memory`: limit on the total size of the bundle’s in-memory tables. Set automatically.
- `cpu`: limit on the total `cpu` of the bundle's nodes.
- `memory`: limit on the total `memory` of the bundle's nodes.

The `tablet_count` and `tablet_static_memory` fields affect the tables created in the bundle. The `cpu` and `memory` fields affect the number of tablet nodes that the Bundle controller can assign to the bundle.

The `tablet_static_memory` field is automatically set by the Bundle controller. Its value is calculated as `node_count * memory`, where `node_count` is the number of nodes assigned to the bundle, and `memory` is the amount of memory on a tablet node instance defined in the zone config.

To manage resources, you need the `write` permission on the bundle. Resource management should be performed by the cluster administrator. You should grant node quotas so that the total quota of all bundles does not exceed the number of nodes in the cluster, taking into account the spare pool for node failures.

To simplify the setup of `cpu` and `memory` limits, the `bundle_controller_tools.py` script has a `set-bundle-resource-limits` command that takes a limit on the number of nodes and sets the limits according to the zone config.

## Managing a bundle

Use the `Edit bundle` button on the bundle page in the user interface to manage the bundle. The interface allows you to specify the number of nodes, the memory split by categories, and the number of threads in the thread pools for lookup and select requests.

To manage a bundle, you need the `manage` permission on the bundle.

## Disabling the Bundle controller

To switch the Bundle controller to read-only mode, set the attribute `//sys/@disable_bundle_controller` to `%true`. The Bundle controller will stop performing any actions on the cluster; however, the state of bundles can still be observed in the user interface.

## Troubleshooting {#faq}

#### The Bundle controller is not working

- Check the `//sys/@disable_bundle_controller` attribute.
- Check for locks at `//sys/bundle_controller/coordinator/lock/@locks`. The absence of locks means that the Bundle controller cannot register with the cluster. The reasons are often in the error log in the container.

#### The bundle UI shows the `Failed` state

The bundle UI displays two statuses: `Health`—the state of tablet cells, and `State`—the state of the Bundle controller. If `State` is in the `Failed` state, clicking on it shows the error that occurred. In some cases, this state can be caused by violations of the Bundle controller invariants (for example, if `resource_guarantee` in the zone settings, nodes, and bundles do not match). The following steps are recommended:

- disable the Bundle controller by setting the `//sys/@disable_bundle_controller` attribute to `%true`;
- run the command `bundle_controller_tools.py drop-allocations <bundle_name>`. This command will remove incorrect node allocation requests;
- make sure the configuration is correct (for example, by running `bundle_controller_tools.py init --init-all --cpu <cpu_guarantee> --memory <memory_guarantee>`).
