# A clique in the {{product-name}} web interface

Cliques don't have their own web interface, but you can view all clique-related objects using the {{product-name}} web interface.

## Strawberry Node

The clique's configuration and persistent state are stored in Cypress at `//sys/strawberry/chyt/<alias>`.

The node description provides housekeeping information: whether the clique is running, in what pool, what the status of the corresponding {{product-name}} operation is, and so on. This node also contains a YSON document called speclet, which stores all the clique settings set with the `yt clickhouse ctl set-option` command.

In addition, you can retrieve information from the strawberry node using the `yt clickhouse ctl get-speclet` and `yt clickhouse ctl status` commands in our CLI.

<!-- ![](../../../../../../images/chyt_strawberry_node.png) -->

## Access Control Object Node

The clique's ACL is stored in a specialized access control object node located at `//sys/access_control_object_namespaces/chyt/<alias>`. In this node, you can view all the accesses granted for the clique.

<!-- ![](../../../../../../images/chyt_clique_access_control_object_node.png) -->

## YT Operation

To access the web interface of the {{product-name}} system operation used to start the clique instances, follow the link from the strawberry node description or the link from the `yt clickhouse ctl status` command output.

There is a lot of useful information on the operation page to help with the operation of CHYT.

### Description { #description }

The **Description** section contains various system information about the clique and a number of links.

- `ytserver-log-tailer`, `ytserver-clickhouse`, `clickhouse-trampoline` &mdash; these sections describe versions of various components of the CHYT server code.

### Specification { #specification }

The **Specification** section contains information about how exactly the clique was started.

![](../../../../../../images/chyt_operation_specification.png){ .center }

- **Wrapper version**: Contains the version of the launcher that started the clique.
- **Command**: Contains the clique start string.

The last two values are very useful if you need, for example, to restart a more recent version of the clique. You can either address the user who started it or reproduce the start command yourself.

### Jobs { #jobs }

The **Jobs** section contains information about the jobs of the operation in which the clique was started. Remember that one job corresponds to one instance in a clique, that is, one ClickHouse server.

![](../../../../../../images/chyt_operation_jobs.png){ .center }

In this section, in terms of CHYT instances:

- `total`: The total number of instances in a clique.
- `pending`: The number of instances out of the total number that have not yet been started. A non-zero number in this section means that the clique does not have enough resources to keep all instances running at once — a bad situation for the clique.
- `running`: The number of running instances out of the total number, i.e. `running + pending = total`.
- `completed`: The number of *gracefully preempted* instances. This includes those instances that were subject to preemption and were able to complete all the queries running on them before they were forcibly aborted.
- `failed`: The number of failed instances. Instances can fail for various reasons and the most common are: OOM (out of memory) or bugs in the CHYT or ClickHouse code.
- `aborted`: The number of aborted instances. Some aborts happen due to insurmountable circumstances (for example, a node failed) and nothing can be done with them. If there are jobs aborted due to `preemption`, this is a bad sign that indicates a lack of resources.
- `lost`: Jobs must not occur in the clique.
