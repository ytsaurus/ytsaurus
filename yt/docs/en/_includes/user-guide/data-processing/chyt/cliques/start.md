# How to start a private clique

A *private clique* is a clique that can be accessed only by certain users or units.

## Start conditions { #conditions }
To start a private clique, you need a dedicated compute pool with CPU guarantees (`strong_guarantee_resources`). If there is not any, you need to:

- Ask colleagues whether it is possible to find a [compute pool](../../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) with available resources.
- Order new resources for the future.
- Start a clique without guarantees (not recommended).

{% if audience == "internal" %}
{% note warning "Attention!" %}

We do not recommend starting a private click in the **Research** pool (in particular, this will happen if you do not specify the pool when starting a clique).

There are no CPU guarantees in this pool. That's why the jobs of the operation in which the clique is running can be [preempted](../../../../../user-guide/data-processing/chyt/cliques/resources.md) sporadically. When preempted, all running queries are aborted.


{% endnote %}{% endif %}

## How to start { #how-start }

1. Install the [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) by installing the `ytsaurus-client` package.

2. Start the clique. Below is an example of a clique of five instances with default settings (16 cores per instance) in the `chyt` pool called `chyt_example_clique`.

{% note tip "CPU quota request" %}

To create a clique with `N` instances on the `<cluster_name>` cluster, you need to:
- `<cluster_name> <pool_tree_name> <running_operations> `: `1` (a clique is a single {{product-name}} operation that is always running).
- `<cluster_name> <pool_tree_name> <total_operations>`: `1` (must be greater than or equal to the limit on the number of running operations).
- `<cluster_name> <pool_tree_name> <CPU strong guarantee>`: `N * 16` (there are 16 CPU cores per instance by default).


{% endnote %}

To avoid mentioning the {{product-name}} cluster name in each command (in the `--proxy` argument), as the first step, set the default value by the environment variable:

```bash
export YT_PROXY=<cluster_name>
```

Next, create a clique, using the command from the [CHYT Controller CLI](../../../../../user-guide/data-processing/chyt/cliques/controller.md):

```bash
yt clickhouse ctl create chyt_example_clique
```
Once you've created the clique, configure it with the relevant options: Every clique has a mandatory `pool` option with the name of the compute pool where the operation on instances will be started for the clique. Because the operations for the clique are started by the {% if audience == "internal" %}`robot-chyt`{% else %}`robot-strawberry-controller`{% endif %} system robot, first you need to grant the `Use` permission for the specified pool to this robot. {% if audience == "internal" %}
You can do this in the {{product-name}} web interface or IDM.{% endif %}

When setting the `pool` option, make sure that you have the `Use` right for the specified pool (otherwise, the command will fail):

```bash
yt clickhouse ctl set-option pool chyt_example_pool --alias chyt_example_clique
```

To set the instance count, use `instance_count`:

```bash
yt clickhouse ctl set-option instance_count 2 --alias chyt_example_clique
```

The clique is now configured, and all its settings are saved in Cypress. The only thing left to do is to start the configured clique:

```bash
yt clickhouse ctl start chyt_example_clique
```

To view the clique status, use the `status` command. Once the clique's operation is started, the value of the `status` field should change to `Ok`, and `operation_state` should change to `running`:

```bash
$ yt clickhouse ctl status chyt_example_clique
{
    "status" = "Waiting for restart: oplet does not have running yt operation";
}
# a few moments later
$ yt clickhouse ctl status chyt_example_clique
{
    "status" = "Ok";
    "operation_state" = "running";
    "operation_url" = "https://domain.com/<cluster_name>/operations/48bdec5d-ed641014-3fe03e8-4289d62e";
}
```

After starting the operation, you need to wait for the clique instances to start up and start accepting incoming queries. To make sure that the clique works, make a test query to the table `//sys/clickhouse/sample_table`. This table is available on all clusters with CHYT:

```bash
$ yt clickhouse execute --proxy <cluster_name> --alias *chyt_example_clique 'select avg(a) from `//sys/clickhouse/sample_table`'
224.30769230769232
```

If the clique is unavailable for more than 10 minutes, try to identify the problem by following the `operation_url` from the `status` command in the operation's web interface.{% if audience == "public" %} If you don't manage to work it out on your own, write to the [{{product-name}} chat](https://t.me/ytsaurus_ru).{% endif %}
