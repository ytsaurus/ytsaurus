---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/use.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/use.md
---
# USE

Specifying the "database". As a rule, one of the {{product-name}} clusters is used as a database. This database will be used by default to search for tables whenever the database hasn't been specified explicitly.

If the query doesn't include `USE`, then the cluster must be specified at the beginning of the table path in the format ```cluster.`path/to/table```` , for example ```hahn.`home/yql/test````. Backticks are used to automatically escape special characters (in this case, slashes).

Usually the cluster name is specified explicitly, but you can use an expression for it. For example, this will let you use the parameters declared by [DECLARE](../declare.md).
In this case, `USE` must have the notation ```USE yt:$cluster_name```, where `$cluster_name` is the [named expression](../expressions.md#named-nodes) of the `String` type.
Alternatively, you can specify the cluster right at the beginning of the table path in the format ```yt:$cluster_name.`path/to/table````.

In this case, `USE` itself can be used inside [actions](../action.md) or [subquery templates](../subquery.md). The current cluster value is inherited in the declarations of nested actions or subqueries. The influence area of `USE` stops after the end of the action or subquery template in which it was declared.

**Examples:**

```yql
USE hahn;
```

{% note info "Note" %}

`USE` **doesn't** guarantee that a query will be executed on the specified cluster. A query can be executed on another cluster if no input data are used in it (e.g. `USE foo; SELECT 2 + 2;`) or if a full path to a table in another cluster is specified (e.g. `USE foo; SELECT * FROM bar.``path/to/table``;`).

{% endnote %}
