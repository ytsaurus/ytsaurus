# A clique in the {{product-name}} web interface

There are several ways to access a clique in the {{product-name}} web interface. When you start a clique using the `start-clique` command, its URL is displayed in the terminal:

```bash
2020-06-03 21:30:29,242 INFO    Operation started: <clique_url_in_ui>
```
{% if audience == "internal" %}
You can also access the clique interface at any time from the **Operations** page. To do this, enter the clique alias (с `*`) in the **Filter operations...** field and then click **Go to operation**.

![](../../../../../../images/chyt_go_to_operation.png){ .center }

{% else %}{% endif %}

{% note warning "Attention!" %}

The clique in the {{product-name}} web interface is represented by the Vanilla operation page. This page does not contain any CHYT-specific logic.

Do not confuse the words from {{product-name}} terminology (transaction, job) with the words from CHYT terminology (clique, instance, query). Consider that a {{product-name}} operation is a container for a clique and {{product-name}} jobs are containers for clique instances. And there is no information about ClickHouse queries in the operation interface.

{% endnote %}

There is a lot of useful information on the operation page to help with the operation of CHYT.

## Description { #description }

The **Description** section contains various system information about the clique and a number of links.


- `ytserver-log-tailer`, `ytserver-clickhouse`, `clickhouse-trampoline` &mdash; these sections describe versions of various components of the CHYT server code.
- `yql_url` &mdash; use this link to go to the YQL interface in which a minimal query to this clique has already been generated.
- `solomon_dashboard_url` &mdash; use this link to go to the [CHYT dashboard](../../../../../user-guide/data-processing/chyt/cliques/administration.md#dashboard) for this clique.
- `solomon_root_url` &mdash; use this link to go to the Solomon interface section with all the metrics for this clique.

## Specification { #specification }

The **Specification** section contains information about how exactly the clique was started.

![](../../../../../../images/chyt_operation_specification.png){ .center }

- **Wrapper version**: Contains the version of the launcher that started the clique.
- **User**: Contains information about which user started the clique.
- **Command**: Contains the clique start string.

The last two values are very useful if you need, for example, to restart a more recent version of the clique. You can either address the user who started it or reproduce the start command yourself.

## Jobs { #jobs }

The **Jobs** section contains information about the jobs of the operation in which the clique was started. Remember that one job corresponds to one instance in a clique, that is, one ClickHouse server.

![](../../../../../../images/chyt_operation_jobs.png){ .center }

In this section, in terms of CHYT instances:

- `total`: The total number of instances in a clique.
- `pending`: The number of instances out of the total number that have not yet been started. A non-zero number in this section means that the clique does not have enough resources to keep all instances running at once — a bad situation for the clique.
- `running`: The number of running instances out of the total number, i.e. `running + pendnig = total`.
- `completed`: The number of *gracefully preempted* instances. This includes those instances that were subject to preemption and were able to complete all the queries running on them before they were forcibly aborted.
- `failed`: The number of failed instances. Instances can fail for various reasons and the most common are: OOM (out of memory) or bugs in the CHYT or ClickHouse code.
- `aborted`: The number of aborted instances. Some aborts are due to insurmountable circumstances (for example, a node failed) and nothing can be done with them. If there are `preemption` aborts among jobs, this is a bad sign that demonstrates a lack of resources.
- `lost`: Jobs must not occur in the clique.

## Control buttons { #control }

Using the buttons in the upper-right corner, you can pause or stop the clique.

![](../../../../../../images/chyt_operation_control_buttons.png){ .center }

- **Abort** stops the clique and aborts the {{product-name}} operation in which it is running and it no longer takes up resources and the alias. To restart the clique, you need to [start](../../../../../user-guide/data-processing/chyt/cliques/start.md) it once again.
- **Complete** in the CHYT context does not differ from the **Abort** button.
- **Suspend/Resume** (with `abort running jobs`) &mdash; use this button to abort all instances in the clique at once, without aborting the clique itself. The operation will still be counted in the running operation count of the pool, but the instances will not consume computing resources.

Using the **Suspend/Resume** button, you can try to "recover" the clique that fell into a bad state (hung due to a bug, received too many heavy queries, etc.).
