# PROCESS

Convert the input table (using UDF) to C++, Python or JavaScript, or [lambda function](../../syntax/expressions.md#lambda), which is applied sequentially to each input string and is able to create zero, one, or multiple output strings per input string (similar to Map in terms of MapReduce).

Search for the table by name in the database specified by the [USE](../use.md) operator.

The parameters of the function call after the `USING` keyword explicitly specify the values of columns from which to pass values for each input row and in which order.

You can use functions that return the result of one of three composite types derived from `OutputType` (supported `OutputType` options are described below):

* `OutputType`: Each input row must always have an output row with the schema determined by the structure type.
* `OutputType?` — functions reserve the right to skip strings returning empty values (`TUnboxedValue()` in C++, `None` in Python, or `null` in JavaScript).
* `Stream<OutputType>` or `List<OutputType>` — the ability to return multiple strings.

Regardless of the option selected above, the result is converted to a flat table with columns defined by the `OutputType` type.

As `OutputType`, you can use one of the types:

* `Struct<...>` — `PROCESS` will only have one output containing records for the specified structure, which is a flat table with columns that correspond to `Struct<...>` fields.
* `Variant<Struct<...>,...>` — `PROCESS` will have the same number of outputs as the number of alternatives in `Variant`. The entries of each output are represented by a flat table with columns based on fields from the relevant variant. In this case, multiple `PROCESS` outputs can be accessed as a tuple (`Tuple`) of lists that can be unpacked into separate [named expressions](../expressions.md#named-nodes) and used as independent expressions.

In the list of function arguments after the `USING` keyword, you can pass one of the two special named expressions:

* `TableRow()`: The entire current row in the form of a structure.
* `TableRows()`: A lazy iterator by strings, in terms of the types `Stream<Struct<...>>` In this case, the output function type can only be `Stream<OutputType>` or `List<OutputType>`.

{% note info "Note" %}

After running `PROCESS` as part of the same query on the resulting table(s), you can run [SELECT](../select.md), [REDUCE](../reduce.md), [INSERT INTO](../insert_into.md), another `PROCESS`, etc. depending on the desired output.

{% endnote %}

You don't have to use the keyword `USING` and specify the function. If they're not specified, the original table returns. This might be helpful when using a [subquery template](subquery.md).

You can pass multiple inputs (input can mean a table, [range of tables](../select.md#range), subquery, or [named expression](../expressions.md#named-nodes)) separated with commas to `PROCESS`. To the function from `USING`, you can only pass in this case special named expressions `TableRow()` or  `TableRows()` that will have the following type:

* `TableRow()`: A `Variant` where each element has an entry structure type from the relevant input. For each input row in the Variant, the element corresponding to the occurrence ID for this row is non-empty
* `TableRows()`: A lazy iterator by Variants, in terms of the types `Stream<Variant<...>>`. The alternative has the same semantics as for `TableRow()`

After `USING` in `PROCESS`, you can optionally specify `ASSUME ORDER BY` with the list of columns. The result of this `PROCESS` will be considered sorted but without actual sorting. Sort check is performed at the query execution stage. It supports setting the sort order using the keywords `ASC` (ascending order) and `DESC` (descending order). Expressions are not supported in `ASSUME ORDER BY`.

**Examples:**

```yql
PROCESS my_table
USING MyUdf::MyProcessor(value)
```

```yql
$udfScript = @@
def MyFunc(my_list):
    return [(int(x.key) % 2, x) for x in my_list]
@@;

-- The function returns the alternatives iterator
$udf = Python3::MyFunc(Callable<(Stream<Struct<...>>) -> Stream<Variant<Struct<...>, Struct<...>>>>,
    $udfScript
);

-- PROCESS outputs the tuple of lists
$i, $j = (PROCESS my_table USING $udf(TableRows()));

SELECT * FROM $i;
SELECT * FROM $j;
```

```yql
$udfScript = @@
def MyFunc(stream):
    for r in stream:
        yield {"alt": r[0], "key": r[1].key}
@@;

-- The function accepts the alternatives iterator as input
$udf = Python::MyFunc(Callable<(Stream<Variant<Struct<...>, Struct<...>>>) -> Stream<Struct<...>>>,
    $udfScript
);

PROCESS my_table1, my_table2 USING $udf(TableRows());
```

