# Query parameters

This section contains information about additional query parameters. For more information about commands, see the [Commands](../../api/commands.md) section.

## Prerequisites { #conditions }

Here is a list of the commands for which you can specify success conditions:

- `set`;
- `remove`;
- `create`;
- `lock`;
- `copy`;
- `move`;
- `link`;
- `create`.

There are two types of conditions:

- `prerequisite_revisions`: A set of hints including `path`, `transaction_id`, `revision`. The query will be executed only if all the paths provided in the hint exist (in the context of the given transactions) and the revisions are as hinted.
- `prerequisite_transactions` includes a list of [transactions](../../user-guide/storage/transactions.md) that must exist when the query is executed.

Example:

```python
revision = client.get("//tmp/my_table/@revision")
#... some calculations ...
revision_parameter = yt.create_revision_parameter(path="//tmp/my_table", revision=revision)
revision_client = yt.create_client_with_command_params(prerequisite_revisions=[revision_parameter])
revision_client.move("//tmp/transformed_table", "//tmp/my_table")
```

## Data source for reading { #data-source }

In [non-mutating](../../api/commands.md#concepts) queries to [Cypress](../../user-guide/storage/cypress.md), you can specify a data source for reading, using the `read_from`  parameter.




A popular value for the `read_from` parameter is `cache`.  In this case, the response is read from the cache (usually hosted on dedicated instances in the cluster). This allows decreasing the load on the master server. This is important when running numerous heavy queries against Cypress: for example, if you need to traverse a large subtree.

Example:

```python
for obj in client.search("//tmp/my_table", read_from="cache"):
    if "abc" in str(obj):
        ...
```
