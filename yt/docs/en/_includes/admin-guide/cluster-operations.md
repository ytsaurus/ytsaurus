## Cluster operations { #main }

### Managing data storage quotas

You can manage quotas for data storage using accounts. You can read more about accounts [here](../../user-guide/storage/accounts.md).

This section provides several standard examples of how to work with accounts using the CLI.

{% cut "Viewing accounts" %}

In Cypress, the account tree can be found at `//sys/account_tree`. The full flat list is available at `//sys/accounts` (account names are globally unique, despite the hierarchical organization).

<small>Listing 1 — Tree and flat account representation in Cypress</small>

```bash
$ yt list //sys/account_tree/my_account
my_subaccount1
my_subaccount2

$ yt list //sys/accounts
my_account
my_subaccount1
my_subaccount2
```

Account has [lots of attributes](../../user-guide/storage/accounts.md#account_attributes) for describing resource limits, current resource usage, and more.

You can request the attributes of an account (or any Cypress object) using this command:
```
$ yt get //sys/account_tree/my_account/@
```

{% endcut %}

{% cut "Creating an account" %}

To create an account, specify its name in the `name` attribute and the parent name in the `parent_name` attribute. If no parent name is specified, a topmost account will be created. Only cluster administrators have the right to create topmost accounts.

<small>Listing 2 — Creating a new account</small>

```bash
$ yt create account --attributes='{ name = "my_subaccount3"; parent_name = "my_account" }'
```

{% endcut %}

{% cut "Removing an account" %}

To remove an account, just enter the path to it:

<small>Listing 3 — Removing an account</small>

```bash
$ yt remove //sys/accounts/my_subaccount3

# or

$ yt remove //sys/account_tree/my_account/my_subaccount3
```

{% endcut %}

{% cut "Changing account resources" %}

Here's an example of modifying the number of Cypress nodes in account limits.

<small>Listing 4 — Changing account resources</small>

```bash
# Get the current number
$ yt get //sys/accounts/my_subaccount3/@resource_limits/node_count
1000

# Set a new value
$ yt set //sys/accounts/my_subaccount3/@resource_limits/node_count 2000
```

{% endcut %}

Please note that total disk space managed with accounts isn't limited to the cluster's actual available space.
One of the system administrator's responsibilities is to use account limits to make sure the cluster doesn't run out of physical disk space.

### Managing computational quotas

The scheduler is responsible for allocating computational resources in the cluster, but the descriptions of pool trees and pools (entities that store information about computational quotas) are stored in Cypress. We recommend reading the general [scheduler documentation](../../user-guide/data-processing/scheduler/scheduler-and-pools.md) as well as our [help page on pool management](../../user-guide/data-processing/scheduler/manage-pools.md).

It provides several standard examples of how to work with pools using the CLI.

{% cut "Viewing pools" %}

<small>Listing 1 — Viewing pools</small>
```bash
yt get //sys/pool_trees/{{pool-tree}}
{
    "project-root" = {
        "project-subpool1" = {
        };
        "project-subpool2" = {
        };
    };
}
```

{% endcut %}

{% cut "Creating a nested pool" %}

<small>Listing 2 — Creating a nested pool</small>

```bash
yt create scheduler_pool --attributes='{pool_tree={{pool-tree}};name=project-subpool1;parent_name=project-root}'
```

You can additionally pass pool-specific attributes. These attributes will be validated and if the validation fails, the object will not be created.

{% endcut %}


{% cut "Changing pool attributes" %}

<small>Listing 3 — Pool attributes changes</small>

```bash
# Setting the weight
yt set //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1/@weight 10

# Forbid starting operations in the pool
yt set //sys/pool_trees/{{pool-tree}}/project-root/@forbid_immediate_operations '%true'
```
{% endcut %}

{% cut "Setting and changing pool guarantees" %}

Specifying guaranteed resources of the pool (it is possible only if the parent pool has enough undisitributed resources):

<small>Listing 4 — Initial guarantee specification</small>

```bash
yt set //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1/@strong_guarantee_resources '{cpu=50}'
```

To change guarantee that already set specify concrete resource type:

<small>Listing 5 — Changing a guarantee</small>

```bash
yt set //sys/pool_trees/{{pool-tree}}/project-root/project-subpool1/@strong_guarantee_resources/cpu 100
```

{% endcut %}

Unlike data storage quotas, the scheduler validates that distributed computing quotas meet the actual resources available in the cluster.


### Managing users, groups and access controls

Users and groups are presented by user and group objects, while access rights are managed by the acl attribute. For more information about the access rights model, see the relevant [section](../../user-guide/storage/access-control.md).

As a rule, user, group, and access management is delegated to a third-party system (for example, in [IAM](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html)), but for the small installations it is reasonable to handle user, groups and acls manually.

Examples of user and group management.

{% cut "Viewing users and groups" %}

<small>Listing 1 — Viewing users</small>
```bash
yt get //sys/users
{
    "file_cache" = #;
    "guest" = #;
    "root" = #;
    "scheduler" = #;
    "operations_cleaner" = #;
    "operations_client" = #;
    "queue_agent" = #;
    "tablet_balancer" = #;
}
```

<small>Listing 2 — Viewing groups</small>
```bash
$ yt get //sys/groups
{
    "devs" = #;
    "admins" = #;
    "superusers" = #;
    "everyone" = #;
    "users" = #;
    "admin_snapshots" = #;
}
```

<small>Listing 3 — Viewing group members</small>
```bash
$ yt get //sys/groups/users/@members
[
    "superusers";
]
```

{% endcut %}


{% cut "Creating users and groups" %}

<small>Listing 4 — Creating a user</small>
```bash
$ yt create user --attributes '{name=my_user}'
1-4a7d-101f5-f881885

$ yt exists //sys/users/my_user
true
```

<small>Listing 5 — Creating a group</small>
```bash
$ yt create group --attributes '{name=my_group}'
1-bedc-101f6-45aec437
```

<small>Listing 6 — Adding a user to a group</small>
```bash
$ yt add-member my_user my_group

$ yt get //sys/users/my_user/@member_of
[
    "users";
    "my_group";
]
```

{% endcut %}

{% cut "Changing user's RPS limits" %}

<small>Listing 7 — Changing user's RPS limits</small>
```bash
$ yt get //sys/users/my_user/@read_request_rate_limit
100
$ yt get //sys/users/my_user/@write_request_rate_limit
100

$ yt set //sys/users/my_user/@read_request_rate_limit 300
$ yt set //sys/users/my_user/@write_request_rate_limit 200
```

{% endcut %}

{% cut "Viewing a node's ACL" %}

Note that access rights configured directly on the Cypress node and access rights inherited from the parent may differ

<small>Listing 8 — Viewing a node's ACL</small>

```bash
$ yt create map_node //home/my_user
1-283e-1012f-63684d08
$ yt get //home/my_user/@acl
[]
$ yt get //home/my_user/@inherit_acl
%true
$ yt get //home/my_user/@effective_acl
[
    {
        "action" = "allow";
        "subjects" = [
            "users";
        ];
        "permissions" = [
            "read";
        ];
        "inheritance_mode" = "object_and_descendants";
    };
    {
        "action" = "allow";
        "subjects" = [
            "admins";
        ];
        "permissions" = [
            "write";
            "administer";
            "remove";
            "mount";
        ];
        "inheritance_mode" = "object_and_descendants";
    };
]
```

{% endcut %}

{% cut "Changing a node's ACL" %}

<small>Listing 9 — Changing a node's ACL</small>

```bash
$ yt set //home/my_user/@acl '[{subjects=[my_group];permissions=[read;write;remove;];action=allow}]'
$ yt set //home/my_user/@inherit_acl '%false'
$ yt get //home/my_user/@effective_acl
[
    {
        "action" = "allow";
        "subjects" = [
            "my_group";
        ];
        "permissions" = [
            "read";
            "write";
            "remove";
        ];
        "inheritance_mode" = "object_and_descendants";
    };
]
```

{% endcut %}

#### Recommendations for access management

  * Use groups and grant permissions exclusively to them so you can grant/take away access scope from users by adding them to or excluding them from a group.
  * Grant permissions to large project directories rather than specific tables since tables can be recreated (access is then lost) or moved (the `effective_acl` turns out to be different because of the new Cypress location, which means access might be inherited where it shouldn't be).
  * The `deny` permission should be reserved for exceptional cases when you need to quickly revoke permissions of a specific user. A better way to differentiate permissions is to mindfully manage group composition and use `inherit_acl=%false` to prevent overly broad inheritance of parent node permissions.


### Managing cluster nodes

Significant part of cluster management is nodes operations such as commission and decomimssion of nodes, handling various issues on a specific node.

Common information about cluster nodes is available in the UI. However, it's worth to know how nodes are represented in Cypress and what attributes they have.

{% cut "How to get the list of nodes in a cluster" %}

<small>Listing 1 — Viewing the list of nodes</small>

```bash
$ yt get //sys/cluster_nodes
{
    "localhost:17359" = #;
}
```

{% endcut %}

Node attributes are shown in table 1:

<small>Table 1 — Node attributes</small>

| **Attribute** | **Type** | **Description** |
| ----------------------- | ----------------- | ------------------------------------------------------------ |
| state | ENodeState | The node state from the master point of view |
| flavors | list<ENodeFlavor> | List of node flavors |
| resource_limits | ClusterResources | Available computational resources on this node |
| resource_usage | ClusterResources | Used computation resources on this node |
| alerts | list<TError> | List of alerts on this node |
| version | string | The version of {{product-name}}, the binary file running on this node |
| job_proxy_build_version | string | The version of the binary `ytserver-job-proxy` file used by the exe node |
| tags | list<string> | List of [tags](../../admin-guide/node-tags.md) for this node |
| last_seen_time | DateTime | The time the node last accessed the master |
| registered_time | DateTime | The time the node was registered on the master |

Listed attributes are informational, which means they're based on the cluster configuration and current cluster state.

There are also management attributes that support updating their values with `set` calls. This way, you can update node properties or request some activity for the cluster node.

<small>Table 2 — Node management attributes</small>

| **Attribute** | **Type** | **Description** |
| ------------------------- | ---------------- | ------------------------------------------------------------ |
| resource_limits_overrides | ClusterResources | Override current node resources (doesn't work for `user_slots`) |
| user_tags | list<string> | List of additional node [tags](../../admin-guide/node-tags.md) |
| banned | bool | Setting the `%true` value bans this node |
| decommissioned | bool | Setting the `%true` initiates the transfer of chunks from this node to others in the cluster |
| disable_write_sessions | bool | Setting the `%true` value stop creating new write session on this node |
| disable_scheduler_jobs | bool | Setting the `%true` value stop new jobs from being scheduled and within some timeout interrupts existing jobs on the node |

Nodes that operate correctly and are connected to a cluster have the `online` state. The `offline` state means that the node is either turned off or server is unavailable. There are several intermediate states that a node can be in while being registered on the master.

{% cut "Viewing node attributes" %}

<small>Listing 2 — Viewing node attributes</small>

```bash
$ yt get //sys/cluster_nodes/localhost:17359/@
{
    "state" = "online";
    ...
}
```

{% endcut %}

{% if audience == "internal" %}

It can be useful to know the node's state from the scheduler's perspective. This information can be accessed in the [Orchid](../../user-guide/storage/orchid.md) of the scheduler. For example, you can check the node's `scheduler_state`.

{% cut "Viewing node state in the scheduler" %}

<small>Listing 2 — Viewing node state from the scheduler's perspective</small>

```bash
$ yt get //sys/scheduler/orchid/scheduler/nodes
{
    "localhost:17359" = {
        "scheduler_state" = "online";
        ...
    };
}
```

{% endcut %}

{% else %}{% endif %}

### Dynamic configs { #ytdyncfgen }

Most cluster components support dynamic config management through Cypress. The general concept is that config patch can be specified as a document at the special path in Cypress. Cluster components periodically read this path in Cypress and apply new config patches.

Importantly, not all options support a dynamic modification. It is often difficult or impossible to implement the logic, but usually it is not implemented with no specific reason.

#### Dynamic config of cluster nodes

This config is managed through the `//sys/cluster_nodes/@config` node. This node represented as dictionary with keys corresponding to the node filters, and values corresponding to the dynamic configs that needs to be applied to the nodes matching this filter. Make sure that the filters in the dictionary don't overlap: each cluster node must match no more than one filter from the dictionary.

#### Dynamic config of the scheduler and controller agents

The scheduler config is managed through the `//sys/scheduler/config` node. This node represents a document object with a scheduler config patch. The patch refers directly to the scheduling subsystem (the `scheduler` section from the static config of this component).

Controller agents are configured similarly but the config is managed via the `//sys/controller_agents/config` node and contains a patch to the subsystem for managing operation controllers (the `controller_agent` section from the static config for controller agents).


{% cut "Examples of how to manage scheduler and controller agent configs" %}

<small> Listing 1 — Increasing the maximum file size allowed in job sandboxes </small>

```bash
# See the current value of this option
$ yt get //sys/controller_agents/instances/<controller_agent>/orchid/controller_agent/config/user_file_limits/max_size
10737418240

# Creating a config node if there isn't one already
$ yt create document //sys/controller_agents/config --attributes '{value={}}'

# Setting a new value
$ yt set //sys/controller_agents/config/user_file_limits '{max_size=53687091200}'

# Making sure the controller agent picked up the new value
$ yt get //sys/controller_agents/instances/<controller_agent>/orchid/controller_agent/config/user_file_limits/max_size
53687091200
```

{% endcut %}
