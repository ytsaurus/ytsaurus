# Checking failed jobs

When working with cliques in {{product-name}}, failures sometimes occur — for example, queries complete with errors or are interrupted. These failures cause jobs to fail within a YT operation. To quickly identify and resolve the issue, you need to be able to analyze the logs of failed jobs.

{% note warning %}

If the number of failed jobs in an operation exceeds 100, logging will stop — the logs won’t be saved. Therefore, it’s important to respond to failures promptly to avoid losing information.

{% endnote %}

## How to view and analyze logs { #instruction }

![Failed Jobs](../../../../../../images/failed-jobs.png){ .center }

1. Open the YT operation web interface (1), as described in the section [How to open the YT operation web interface](../../../../../user-guide/data-processing/chyt/cliques/yt-operation-ui.md#get-to-section).
1. On the right, in the **Failed** block (2), check the number of failed jobs.
1. Go to the **Jobs** tab (3) in the tab bar.
1. In the job list that opens, use the `State`: `Failed` filter (4) to find the failed jobs.
1. In the row with the failed job, find the **Error / Debug** column.
1. To download the job log, click ![download](../../../../../../images/download-log.png){width=24 height=24} in the **Stderr** field (5) .
1. Review the log and pay attention to the following:

   - error messages (usually contain the keywords *Error*, *Exception*, *Failed*);
   - timestamps (to understand exactly when the failure occurred);
   - mentions of resources (for example, tables or files) that the job was working with.

{% note info %}

Save the logs of failed jobs — they will be useful for:

- monitoring system stability;
- analyzing recurring errors;
- providing data to support teams.

{% endnote %}
