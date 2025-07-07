# Errors

## Expected comparable type, but got: Yson
This might happen if the table has been created in {{product-name}} without a schema and then sorted. In this case, the system (YT) assigns the `any` type to the sort column in the table schema. In YQL, this is equivalent to the `Yson` column type (arbitrary YSON data).

For example, for this query:
```yql
SELECT * FROM my_table
WHERE key = 'www.youtube.com/watch?v=Xn599R0ZBwg';
```
... you can make a workaround:
```yql
SELECT * FROM my_table
WHERE Yson::ConvertToString(Yson::Parse(key)) = 'www.youtube.com/watch?v=Xn599R0ZBwg';
```
**The drawback of this solution:**
The queries used in the examples output all the rows that match a certain key. If the table has been sorted by this key, the query execution time is usually independent of the table size. But for this workaround, such optimization won't work, and you'll have to scan the entire table.

{% note info "Note" %}

This issue doesn't occur with tables that have a <!--[корректно схематизированных](../misc/schema.md)--> valid schema. Configure the schema if you know how to do it. If not, ask the people responsible for the tables to do it for you.

{% endnote %}

More [about Yson UDF](../udf/list/yson.md).

## Cannot add type String (String?) and String (String?)
This error arises when you try to concatenate strings using the '+' operator.
Use the '||' operator instead:
```yql
SELECT "Hello, " || "world" || "!";
```

## Expression has to be an aggregation function or key column, because aggregation is used elsewhere in this subquery
This issue most often arises when you try to use aliases used in your `SELECT` clause, further in the `HAVING` clause. Aliases are names of fields that follow `AS` in a `SELECT` statement. They define a projection and are applied after the `HAVING` clause (that's why you can't use them in HAVING).

An example of an invalid query (such a query **WON'T** work):
```yql
USE {{production-cluster}};
SELECT
    region,
    COUNT(age) AS counter
FROM `home/yql/tutorial/users`
GROUP BY region
HAVING counter > 3
```

To avoid calling aggregate functions twice, add a subquery, moving the logic from the `HAVING` to the `WHERE` clause.

```yql
USE {{production-cluster}};
SELECT *
FROM
    (SELECT
        region,
        COUNT(age) AS counter
    FROM `home/yql/tutorial/users`
    GROUP BY region)
WHERE counter > 3
```

## Value type "double" is not a number
The reason for this error is, most often, division by zero: it results in an NaN value (not a number) not supported by {{product-name}} in some cases.

## Row weight is too large
This error occurs when the maximum memory allocated for a row is exhausted. Use the relevant [PRAGMA](../syntax/pragma#yt) to increase the maximum row length in your {{product-name}} tables.
The default value is "16M", the maximum value is "128M". If you exceed the maximum value, change your computing logic, for example, split your table into parts or apply compression algorithms ([Compress UDF](../udf/list/compress_decompress.md))
```yql
PRAGMA yt.MaxRowWeight = "32M";
```

## Key weight is too large
This error arises when the value in a key field of the table is too large. One of the methods to resolve the issue is to use a [PRAGMA](../syntax/pragma#yt) that increases the maximum length of key table fields in {{product-name}}, where fields are used for `ORDER BY`, `GROUP BY`, `PARTITION BY`, `DISTINCT`, `REDUCE`.

The default value is "16K" and the maximum value is "256K".
```yql
PRAGMA yt.MaxKeyWeight = "32K";
```
{% if audience == "internal"%}
{% note warning "Attention!" %}

Before using this PRAGMA, get an approval from {{admin-email}}.

{% endnote %}
{% endif %}
## The same table column includes values of different types {#badtabledata}

When processing such tables, you might encounter different errors depending on query settings:
* ``` ReadYsonValue(): requirement cmd == Int64Marker failed, message: Expected char: "\2", but read: "\1"```
* ``` { 'Table input pipe failed' { 'Unexpected type of "score" column, expected: "int64" found "uint64"' } }```

To read or process such a table, you need to patch the column type to `Yson?` using [WITH COLUMNS](../syntax/select/with.md) and then parse the data manually using [Yson UDF](../udf/list/yson.md):
```yql
SELECT Yson::ConvertToDouble(bad_column, Yson::Options(false as Strict)) ?? CAST(Yson::ConvertToString(bad_column, Yson::Options(false as Strict)) as Double) as bad_column
FROM `path/to/bad_table`
WITH COLUMNS Struct<bad_column: Yson?>;
```

If the types can be converted automatically, you can do like this:
```yql
SELECT Yson::ConvertToDouble(bad_column, Yson::Options(true as AutoConvert)) as bad_column
FROM `path/to/bad_table`
WITH COLUMNS Struct<bad_column: Yson?>;
```

## Access denied for user <>: "read" permission for node <> is not allowed by any matching ACE {#symlinkaccess}

### Requesting rights in {{product-name}}

Check whether you can open the table in the {{product-name}} interface.{% if audience == "internal" %} You can view the [users]({{cluster-ui}}/navigation?path=//home/yql/tutorial/users) table as an example.{% endif %} If you don't have access rights, {% if audience == "internal" %}request them in the [IDM]({{yt-docs-root}}/user-guide/storage/acl-manage){% else %}request them in the web interface {{product-name}} or in the CLI{% endif %}.

### Rights for table symlinks

If you can open the table in the {{product-name}} interface but YQL can't see it, the issue may be in symlinks to tables, when the user has access rights for the underlying table but doesn't have a read access to its symlink. To resolve the underlying table in {{product-name}}, you don't need read access to its symlink: that's why in {{product-name}} the contents of the table would display normally. However, besides resolving the table, YQL also reads its symlink attributes because the attributes can override, for example, the table schema. To resolve this issue, request the rights to read the symlink.

{% if audience == "internal" %}
Validation of rights to read symlinks will be fixed in [this ticket](https://nda.ya.ru/t/aYwU2M8o7FKATU).
{% endif %}
## Maximum allowed data weight per sorted job exceeds the limit {#key_monster}

In most cases, the error arises when executing a `JOIN` if its inputs include a monster key (a huge number of records with the same key value). To check for a monster key, click the red circle in the query execution plan and drill down to the failed {{product-name}} operation. After that, in the {{product-name}} interface, open the `Partition sizes` tab of the failed operation.

![](../../../images/partition_sizes.png)

If you see large partitions (more than 200 GB), this indicates that you have monster keys. To find the value of the problem key, expand the error message in the operation's `Details` tab. Most probably, you'll find it there.

![](../../../images/max_allowed_data_error.png)

In this example, the problem key is an empty string. <!--Другие способы узнать проблемные ключи описаны в разделе [Производительность](performance.md).-->

The main way to resolve this issue is to filter the problem key values at JOIN inputs. Monster keys typically include irrelevant values (NULL, empty string) that you can safely filter out. If the values of monster keys are critical for you, process them separately.

Besides JOIN, such an error may also arise in the following cases:
1. Huge amounts of data in `GROUP BY`, `JOIN`. Solution: try to decrease the data amount.
2. The [window](../syntax/window) includes the entire table. Solution: review the partitioning conditions for the window function
3. Huge amounts of data for certain values of the `GROUP BY` key. The method used to find a problem key in this case is the same as for `JOIN`. Solution: filter out the problem key, add more columns to the group key.

## Reading multiple times from the same source is not supported
DQ can't execute the query because the resulting query execution plan splits the input into multiple streams. {{product-name}} does not currently support this behavior, and you can't correctly execute this query.{% if audience == "internal" %} To learn more, see YQL-13817.{% endif %}

<!--## Too large table(s) for evaluation pass: NNN > 1048576 {#eval_error}

См. описание [этапов исполнения запроса](../misc/exec_steps.md)-->

## Strict Yson type is not allowed to write, please use Optional<Yson>

This is a current {{product-name}} restriction that doesn't allow writing non-optional Yson columns in `type_v3` mode. The error occurs when using the [yt.UseNativeYtTypes](../syntax/pragma#yt-usenativeyttypes) pragma in the query. Please note that this pragma is enabled by default.

To fix the error:
- Make sure that the query doesn't explicitly generate Yson-type data at any level (for example, the `List<Yson>` type can't be written either).
- Make sure that your query tables don't contain any columns with non-optional Yson at any level.{% if audience == "internal" %} [LogFeller]({{logfeller-link}}) often generates tables with the `_rest` column of the `Dict<String,Yson>` type. To use such tables in the query, their schemas must be explicitly defined via `WITH COLUMNS Struct<_rest:Dict<String,Yson?>?>`. Alternatively, you can use [WeakField](../builtins/basic.md#weakfield).{% endif %}
- Some aggregate functions (for example, `AGG_LIST`) inherently remove optionality from their elements. Even if the original column was of the `Optional<Yson>` type, `AGG_LIST` will result in the `List<Yson>` type that can't be written. In this case, convert the column type into any other type before executing `AGG_LIST`.
- You can also disable the `PRAGMA yt.UseNativeYtTypes` pragma. Please note that if you disable the pragma, you will lose the benefits of strong typing, since native types will be represented as strings.

