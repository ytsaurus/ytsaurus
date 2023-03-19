---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/in.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/in.md
---
## IN {#in}
Checking whether a value is inside of a set of values. It's logically equivalent to a chain of equality comparisons using `OR` but implemented more efficiently.

{% note warning "Attention!" %}

Unlike the same keyword in Python, in YQL `IN` **DOES NOT** involve searching for a substring in a string. You can use [String::Contains](../../../udf/list/string.md) or [LIKE / REGEXP](#like) described above to search for a substring.

{% endnote %}

You can specify the [hint](../../lexer.md#sql-hints) `COMPACT` right after `IN`.
If `COMPACT` is not specified, then `IN` with a subquery is executed as a relevant `JOIN` (`LEFT SEMI` for `IN` and `LEFT ONLY` for `NOT IN`).

The presence of `COMPACT` forces an in-memory execution strategy: a hash table is built in memory based on the content of the right part of `IN`, and then the left part is filtered based on the table.

The `COMPACT` hint must be used with care. Because the hash table is built in memory, the query may throw an error if the right part of `IN` contains a lot of large and/or different elements.

Because there's a limit on the byte size of queries (approx. 1 MB) in YQL, to get a larger list of values you should attach them to the query via URL and use the ParseFile function.

**Examples**

```yql
SELECT column IN (1, 2, 3)
FROM my_table;
```

```yql
SELECT * FROM my_table
WHERE string_column IN ("a", "b", "c");
```

```yql
$foo = AsList(1, 2, 3);
SELECT 1 IN $foo;
```

```yql
$values = (SELECT column + 1 FROM table);
SELECT * FROM my_table WHERE
    -- filtering by in-memory hash table based on the table
    column1 IN /*+ COMPACT() */ $values AND
    -- with subsequent LEFT ONLY JOIN with other_table
    column2 NOT IN (SELECT other_column FROM other_table);
```
