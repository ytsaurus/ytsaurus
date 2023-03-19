# How to start a private clique

A *private clique* is a clique that is available only to certain users or units.

## Start conditions { #conditions }
To start a private clique, you need a dedicated compute pool with CPU guarantees (`strong_guarantee`). If there is not any, you need to:

- Ask colleagues whether it is possible to find a [compute pool](../../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) with available resources.
- Order new resources for the future.
- Start a clique without guarantees (not recommended).

{% if audience == "public" %} {% else %}
{% note warning "Attention!" %}

We do not recommend starting a private click in the **Research** pool (in particular, this will happen if you do not specify the pool when starting a clique).

There are no CPU guarantees in this pool. That's why the jobs of the operation in which the clique is running can be [preempted](../../../../../user-guide/data-processing/chyt/cliques/resources.md) sporadically. When preempted, all running queries are aborted.


{% endnote %}{% endif %}

## How to start { how-start }

1. Install the [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) by installing the `ytsaurus-client` package.

2. Start the clique. Below is an example of a clique of five instances with default settings (16 cores per instance) in the `chyt` pool. Clicks are identified by &mdash aliases and unique names starting with `*`. In the example, we are starting the clique with the `*example` alias.

{% note tip "Заказ CPU квоты" %}

To create a clique with `N` instances on the `<cluster_name>` cluster, you need to:
- `<cluster_name> <pool_tree_name> <running_operations> `: `1` (a clique is a single {{product-name}} operation that is always running).
- `<cluster_name> <pool_tree_name> <total_operations>`: `1` (must be greater than or equal to the limit on the number of executed operations).
- `<cluster_name> <pool_tree_name> <CPU strong guarantee>`: `N * 16` (there are 16 cores per instance by default).


{% endnote %}

To avoid mentioning the {{product-name}} cluster name in each command (in the `--proxy` argument), as the first step, set the default value by the environment variable:

```bash
export YT_PROXY=<cluster_name>
```

Next, create a clique, using the command from the [CHYT Controller CLI](../../../../../user-guide/data-processing/chyt/cliques/controller.md):

```bash
yt clickhouse ctl create chyt_example_clique
```
Once you have created the clique, configure it with the relevant options: Every clique has a mandatory `pool` option with the name of the compute pool where the operation on instances will be started for the clique. Because the operations for the clique are started by the `robot-chyt` system robot, first you need to grant, to this robot, the `Use` right for the specified pool. {% if audience == "internal" %}
You can do it in the {{product-name}} web interface or IDM.{% else %}{% endif %}

When setting the `pool` option, make sure that you have the `Use` right for the specified pool (otherwise, the command will fail):

```bash
yt clickhouse ctl set-option pool chyt_example_pool --alias chyt_example_clique
```

To set the instance count, use `instance_count`:

```bash
yt clickhouse ctl set-option instance_count 2 --alias chyt_example_clique
```

To start the clique, first activate it by setting the `active` option to `%true`:

```bash
yt clickhouse ctl set-option active %true --alias chyt_example_clique
```

To view the clique status, use the `status` command. Once the clique's operation is started, the value of the `status` field changes to `Ok` and `operation_state` changes to `running`:

```bash
yt clickhouse ctl status chyt_example_clique
{
    "status" = "Waiting for restart: oplet does not have running yt operation";
}
# a few moments later
yt clickhouse ctl status chyt_example_clique
{
    "status" = "Ok";
    "operation_state" = "running";
    "operation_url" = "https://domain.com/<cluster_name>/operations/48bdec5d-ed641014-3fe03e8-4289d62e";
}
```


Make sure that the clique works by making a test query in it to the `//sys/clickhouse/sample_table` table available on all clusters where there is CHYT:

```bash
yt clickhouse execute --proxy <cluster_name> --alias *example 'select avg(a) from `//sys/clickhouse/sample_table`'
224.30769230769232
```

If the clique is unavailable for more than ten minutes, write to the {% if audience == "internal" %}
[CHYT chat ](https://nda.ya.ru/t/Dqb57xyQ5psK3X){% else %}[{{product-name}} chat](https://t.me/ytsaurus_ru){% endif %}.
