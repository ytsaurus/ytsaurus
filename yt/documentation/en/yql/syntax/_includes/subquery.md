---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/subquery.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/subquery.md
---
# Subquery templates

## DEFINE SUBQUERY {#define-subquery}

`DEFINE SUBQUERY` lets you declare a subquery template as a parameterizable block consisting of several upper-tier statements and then use it repeatedly through application in the `FROM` section of the [SELECT](../select.md) statement or in input data in [PROCESS](../process.md)/[REDUCE](../reduce.md), with parameters indicated.
Unlike [actions](../action.md), a subquery template must end with the `SELECT`/`PROCESS`/`REDUCE` statement. Its result is the returnable subquery value. Here, the upper-tier `SELECT`/`PROCESS`/`REDUCE` statement cannot be used more than once, similarly to modifying statements (for instance, `INSERT`).

After `DEFINE SUBQUERY`, specify:

1. A [named expression](../expressions.md#named-nodes) that will be used to access the declared template further in the query.
2. The round brackets contain a list of named expressions you can use to access parameters inside the subquery template.
3. `AS `keyword.
4. The list of top-level expressions.
5. `END DEFINE` acts as a marker of the last expression inside the subquery template.

One or more of the last subquery parameters can be marked with a question as optional: unless they haven't been specified when calling subquery, they will be assigned the `NULL` value.


{% note info "Note" %}

In large queries, you can use separate files for action definition and include them to the main query using [EXPORT](../export_import.md#export) + [IMPORT](../export_import.md#import) so that instead of one long text you can have several logical parts that are easier to navigate. An important nuance: the `USE my_cluster`; directive in the import query doesn't affect behavior of actions declared in other subquery files.

{% endnote %}


Even if the list of parameters in the subquery template definition is empty, when using it in `FROM`, specify the parentheses `()`. This may help to limit the scope of named expressions used in only one subquery.

In certain cases, instead of `DEFINE SUBQUERY`, it's better to use an equivalent form appearing as a [lambda function](../expressions.md#lambda).
In this case, the lambda function must accept, as the first argument, the special object called `world` that passes dependencies to make certain PRAGMA or COMMIT statements visible at the query template's point of use. Also, make sure to pass this object as the first argument along with the other arguments (if any) to other query templates, if you use them in your lambda function.
The return value of the lambda function must have the structure list type (output table) or a list of variants over a tuple of structures (multiple output tables). In the latter case, the following unpacking is usually used at the query template's point of use:

```yql
$out1, $out2 = PROCESS $mySubquery($myParam1, $myParam2);
-- $out1 and $out2 are further used as individual tables.
```


**Examples**

```yql
DEFINE SUBQUERY $hello_world($name, $suffix?) AS
    $name = $name ?? ($suffix ?? "world");
    SELECT "Hello, " || $name || "!";
END DEFINE;

SELECT * FROM $hello_world(NULL); -- Hello, world!
SELECT * FROM $hello_world("John"); -- Hello, John!
SELECT * FROM $hello_world(NULL, "Earth"); -- Hello, Earth!
```

```yql
DEFINE SUBQUERY $dup($x) AS
   SELECT * FROM $x(1)  -- apply transmitted query template with one argument
   UNION ALL
   SELECT * FROM $x(2); -- ... and with another argument
END DEFINE;

DEFINE SUBQUERY $sub($n) AS
   SELECT $n * 10;
END DEFINE;

SELECT * FROM $dup($sub); -- transmit $sub query template as a parameter
-- Result:
-- 10
-- 20
```

```yql
/* Move the utilized named expressions $a and $b to a separate area of visibility*/
DEFINE SUBQUERY $clean() AS
   $a = 10;
   $b = $a * $a;
   SELECT $a AS a, $b AS b;
END DEFINE;

SELECT * FROM $clean(); -- a: 10, b: 100
```

```yql
USE hahn;

DEFINE SUBQUERY $input() as
    SELECT * FROM `home/yql/tutorial/users`;
END DEFINE;

DEFINE SUBQUERY $myProcess1($nestedQuery, $lambda) AS
    PROCESS $nestedQuery() -- parentheses () must be used here
    USING $lambda(TableRow());
END DEFINE;

$myProcess2 = ($world, $nestedQuery, $lambda) -> {
    -- If ListFlatMap or YQL::OrderedFlatMap is used, this results in Ordered {{product-name}} Map operation
    return YQL::FlatMap($nestedQuery($world), $lambda);
};

-- With such use, $myProcess1 and $myProcess2 implementations are identical
SELECT * FROM $myProcess1($input, ($x) -> { RETURN AsList($x, $x) });
SELECT * FROM $myProcess2($input, ($x) -> { RETURN AsList($x, $x) });
```

```yql
USE hahn;

DEFINE SUBQUERY $runPartition($table) AS
    $paritionByAge = ($row) -> {
        $recordType = TypeOf($row);
        $varType = VariantType(TupleType($recordType, $recordType));
        RETURN If($row.age % 2 == 0,
            Variant($row, "0", $varType),
            Variant($row, "1", $varType),
        );
    };

    PROCESS $table USING $paritionByAge(TableRow());
END DEFINE;

-- Unpacking two results
$i, $j = (PROCESS $runPartition("home/yql/tutorial/users"));

SELECT * FROM $i;

SELECT * FROM $j;
```

## Merging subquery templates SubqueryExtend, SubqueryUnionAll, SubqueryMerge, SubqueryUnionMerge {#subquery-extend} {#subquery-unionall} {#subquery-merge} {#subquery-unionmerge}

These functions combine the results of one or more subquery templates passed by arguments. The number of parameters in such subquery templates must be the same.

* `SubqueryExtend` requires matching of subquery schemas.
* `SubqueryUnionAll` follows the same rules as `ListUnionAll`.
* `SubqueryMerge` uses the same constraints as `SubqueryExtend` and also outputs a sorted result if all subqueries have the same sort order.
* `SubqueryUnionMerge` uses the same constraints as `SubqueryUnionAll` and also outputs a sorted result if all subqueries have the same sort order.

**Examples:**

```yql
DEFINE SUBQUERY $sub1() as
    SELECT 1 as x;
END DEFINE;

DEFINE SUBQUERY $sub2() as
    SELECT 2 as x;
END DEFINE;

$s = SubqueryExtend($sub1,$sub2);
PROCESS $s();
```

## Merging subquery templates after substituting a list element from SubqueryExtendFor, SubqueryUnionAllFor, SubqueryMergeFor, or SubqueryUnionMergeFor {#subquery-extend-for} {#subquery-unionall-for} {#subquery-merge-for} {#subquery-unionmerge-for}

The functions take the following arguments:

* A non-empty list of values.
* A subquery template that must have exactly one parameter.

They substitute each item from the list into the subquery template as a parameter and then combine the obtained subqueries.

* `SubqueryExtendFor` requires matching of subquery schemas.
* `SubqueryUnionAllFor` follows the same rules as `ListUnionAll`.
* `SubqueryMergeFor` uses the same constraints as `SubqueryExtendFor` and also outputs a sorted result if all subqueries have the same sort order.
* `SubqueryUnionMergeFor` uses the same constraints as `SubqueryUnionAllFor` and also outputs a sorted result if all subqueries have the same sort order.

**Examples:**

```yql
DEFINE SUBQUERY $sub($i) as
    SELECT $i as x;
END DEFINE;

$s = SubqueryExtendFor([1,2,3],$sub);
PROCESS $s();
```

## Adding sorting to the SubqueryOrderBy template or indicating the presence of this SubqueryAssumeOrderBy

The functions take the following arguments:

* A subquery template without parameters.
* A list of pairs (string indicating the column name and Boolean value: True for sorting in ascending order or False for sorting in descending order).

And they build a new query template without parameters where sorting is performed or a comment on the use of sorting is added to the result. To use the resulting query template, call the `PROCESS` function, since, when using a `SELECT`, sorting is ignored.

**Examples:**

```yql
DEFINE SUBQUERY $sub() as
   SELECT * FROM (VALUES (1,'c'), (1,'a'), (3,'b')) AS a(x,y);
end define;

$sub2 = SubqueryOrderBy($sub, [('x',false), ('y',true)]);

PROCESS $sub2();
```
