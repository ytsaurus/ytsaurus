# Recipes for code generation

## Introduction
When working with tables of many columns, you might spend too much effort listing them repeatedly in different parts of your SQL query. This happens when you can't select all your columns by `*` and, optionally, by `WITHOUT`. Sometimes, column selection may depend both on the column name and its data type. To avoid writing lengthy SQL queries manually, you can use [type introspection](../builtins/types.md) and [code generation](../builtins/codegen.md). Code will be generated before the main query is executed, and the resulting programs are fully equivalent to those written manually.

When you use code generation, keep in mind that as it is executed before the main process starts, so you can only use column types when generating code, but not column values. You can also use constants and expressions that do not depend on table columns, without any limitation.

Now let's discuss typical use cases and how to implement them.

## Use cases

### Declaring a UDF type by computations

```yql
$tuple = AsTuple(1, "foo", AsList(-1.2, 3.0));

$tupleType = TypeOf($tuple);
$reverseTupleType = EvaluateType(TupleTypeHandle(
    ListReverse(TupleTypeComponents(TypeHandle($tupleType)))));

$script = @@
def reverse_any_tuple(t):
   return tuple(reversed(list(t)))
@@;


$udfType = CallableType(0, $reverseTupleType, $tupleType);
$udf = {{python-lib-qt}}::reverse_any_tuple($udfType, $script);

SELECT $udf($tuple);
```

Links to documentation:


* [EvaluateType](../builtins/types.md#evaluatetype)


### Filtering columns by a list of names

```yql
USE {{production-cluster}};

$makeFieldsSelector = ($fields) -> {
    RETURN EvaluateCode(LambdaCode(($row) -> {
        $items = ListMap($fields, ($f) -> {
            $atom = AtomCode($f);
            RETURN ListCode($atom, FuncCode("Member", $row, $atom));
        });

        RETURN FuncCode("AsStruct", $items);
    }))
};

$selectFields1 = $makeFieldsSelector(AsList("age", "name", "region"));
$selectFields2 = $makeFieldsSelector(AsList("age", "name"));

INSERT INTO @tmp1
SELECT * FROM (
    SELECT $selectFields1(TableRow()) FROM `home/yql/tutorial/users`
    WHERE region != 225
) FLATTEN COLUMNS;

INSERT INTO @tmp2
SELECT * FROM (
    SELECT $selectFields2(TableRow()) FROM `home/yql/tutorial/users`
    WHERE region = 225
) FLATTEN COLUMNS;

COMMIT;

SELECT * FROM @tmp1;
SELECT * FROM @tmp2;
```

Links to documentation:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [AsStruct]({{yql.s-expressions-link}}/functions#asstruct)
{% endif %}
* [ListMap](../builtins/list.md#listmap)
* [Lambda functions](../syntax/expressions.md#lambda)
* [TableRow](../builtins/basic.md#tablerow)


### Searching in all string columns

```yql
USE {{production-cluster}};

$containsAnyColumns = ($strValue, $needle) -> {
    -- Lambda function, a predicate that will be applied to the value of each column
    $func = ($where) -> {
        RETURN String::Contains($where, $needle);
    };

    $code = EvaluateCode(LambdaCode(($strCode) -> {
        -- Select a list of struct fields
        $members = StructTypeComponents(TypeHandle(TypeOf($strValue)));

        $filteredMembers = ListFilter($members, ($x) -> {
             $type = $x.Type;
             -- Remove the optional top-level type
             $cleanType = If (TypeKind($type) == "Optional", OptionalItemType($type), $type);
             -- Leave only String and String? columns
             RETURN TypeKind($cleanType) == "Data" AND
                DataTypeComponents($cleanType)[0] == "String";
        });

        -- Generate an expression in the format $func(column1) OR $func(column2)...
        RETURN Yql::Fold($filteredMembers, ReprCode(false), ($item, $state) -> {
            $member = FuncCode("Member", $strCode, AtomCode($item.Name));
            $apply = FuncCode("Apply", QuoteCode($func), $member);
            RETURN FuncCode("Or", $state, $apply);
        });
    }));

    RETURN $code($strValue);
};

SELECT * FROM `home/yql/tutorial/users`
WHERE $containsAnyColumns(TableRow(), "comment");
```

Links to documentation:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListFilter](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [Apply]({{yql.s-expressions-link}}/functions#apply)
* [Fold]({{yql.s-expressions-link}}/functions#fold)
{% endif %}
* [Lambda functions](../syntax/expressions.md#lambda)
* [TableRow](../builtins/basic.md#tablerow)

### Merging structures left-to-right

```yql

-- Remove the high-level Optional from the data type
$removeOptional = ($type) -> {
    RETURN IF(TypeKind($type) == "Optional", OptionalItemType($type), $type);
};

$combineMembersLeft = ($str1, $str2) -> {
    $code = EvaluateCode(LambdaCode(($str1code, $str2code) -> {
        -- Get lists of fields from both structures
        $leftMembers = StructTypeComponents($removeOptional(TypeHandle(TypeOf($str1))));
        $rightMembers = StructTypeComponents($removeOptional(TypeHandle(TypeOf($str2))));

        -- Generate field retrieval from the left structure
        $members1 = ListMap($leftMembers, ($x) -> {
            $atom = AtomCode($x.Name);
            RETURN ListCode($atom, FuncCode("Member", $str1code, $atom));
        });

        -- Collect the names of fields in the left structure into a set to facilitate search
        $leftNames = ToSet(ListExtract($leftMembers, "Name"));

        -- Generate retrieval of fields from the right structure unless they are already in the left structure
        $members2 = ListFlatMap($rightMembers, ($x) -> {
            $atom = AtomCode($x.Name);
            $member = ListCode($atom, FuncCode("Member", $str2code, $atom));
            RETURN IF(DictContains($leftNames, $x.Name), NULL, $member);
        });

        -- Generate a final structure
        RETURN FuncCode("AsStruct", ListExtend($members1, $members2));
    }));

    RETURN $code($str1, $str2);
};

SELECT $combineMembersLeft(
    Just(AsStruct(1 as a, 2 as b)),
    AsStruct(3 as c, 4 as b)
);
-- (a:1, b:2, c:3)
```

Links to documentation:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [AsStruct]({{yql.s-expressions-link}}/functions#asstruct)
{% endif %}
* [Lambda functions](../syntax/expressions.md#lambda)

### Reading a column by the name written to another column

```yql
$input = AsList(
    AsStruct(1 as key, "value1" as index, "foo" as value1),
    AsStruct(2 as key, "value2" as index, "bar" as value2),
    AsStruct(3 as key, "value3" as index)
);

-- Build a Lambda function that returns, for the input table row,
two columns - a key copied unchanged and a value
-- retrieved from a column whose name is written to the index column.
$makeDynamicAccessor = ($keyColumn, $indexColumn, $valueColumn) -> {
    RETURN ($row) -> {
        RETURN EvaluateCode(LambdaCode(($rowCode) -> {
            -- Build a list of all fields except the key field and index field
            $names = ListExtract(StructTypeComponents(TypeHandle(TypeOf($row))), "Name");
            $names = ListFilter($names, ($x) -> {
                    return $x != $keyColumn and $x != $indexColumn
            });

            -- Build a dictionary from the column name contained in variant
            $dict = EvaluateCode(FuncCode("AsDict",
                ListMap($names,
                    ($x) -> {
                        RETURN ListCode(ReprCode($x),
                            FuncCode("Variant", FuncCode("Void"), AtomCode($x),
                            FuncCode("VariantType", FuncCode("StructType",
                                ListMap($names, ($x) -> {
                                    RETURN ListCode(AtomCode($x), FuncCode("VoidType")) })))))
                    })));

            -- Index column value
            $indexValue = FuncCode("Member", $rowCode, AtomCode($indexColumn));

            -- Take the column value unchanged
            $keyMember = ListCode(AtomCode($keyColumn),
                FuncCode("Member", $rowCode, AtomCode($keyColumn)));

            -- Search in the dictionary
            $enumLookup = FuncCode("Lookup", ReprCode($dict), $indexValue);

            -- Process the value found in the dictionary
            $value = FuncCode("FlatMap", $enumLookup, LambdaCode(($x) -> {
                -- Build a list of handlers for each column name
                $handlers = ListFlatMap($names, ($name) -> {
                    RETURN AsList(AtomCode($name),
                        LambdaCode(($u) -> {
                            RETURN FuncCode("Member", $rowCode, AtomCode($name));
                        }));
                });

                -- Apply a visitor to the found value
                RETURN FuncCode("Visit", $x, $handlers);
            }));

            $valueMember = ListCode(AtomCode($valueColumn), $value);

            -- Build an output structure of the key and value
            RETURN FuncCode("AsStruct", AsList($keyMember, $valueMember));
        }))($row);
    };
};

SELECT * FROM (
    SELECT $makeDynamicAccessor("key","index","value")(TableRow())
    FROM AS_TABLE($input)
) FLATTEN COLUMNS
```

Links to documentation:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [AsStruct]({{yql.s-expressions-link}}/functions#asstruct)
* [Visit]({{yql.s-expressions-link}}/functions#visit)
{% endif %}
* [Lambda functions](../syntax/expressions.md#lambda)

### Calculate a sum across tuple items

```yql
$t = AsTuple(1,2,3);

$sumTuple = ($tupleValue) -> {
    $code = EvaluateCode(LambdaCode(($tupleCode) -> {
        -- Count the tuple size
        $count = ListLength(TupleTypeComponents(TypeHandle(TypeOf($tupleValue))));
        -- Generate the sum
        $pair = Yql::Fold(ListFromRange(0ul, $count), AsTuple(ReprCode(0), 0), ($item, $state) -> {
            -- $state is a pair of the sum and current index taken from the code
            RETURN AsTuple(
                FuncCode("+", $state.0, FuncCode("Nth", $tupleCode, AtomCode(
                    CAST($state.1 as string)))),
                $state.1 + 1
            )
        });

        RETURN $pair.0;
    }));

    RETURN $code($tupleValue);
};

SELECT $sumTuple($t);
```

Links to documentation:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
{% if audience == "internal" %}
* [Nth]({{yql.s-expressions-link}}/functions#nth)
* [Fold]({{yql.s-expressions-link}}/functions#fold)
{% endif %}
* [Lambda functions](../syntax/expressions.md#lambda)

### Aggregating values of all numeric columns

Select all the numeric columns in a table, apply several aggregate functions to them, and write the values to output columns, adding the name of the aggregate function to the output column names.

```yql
USE {{production-cluster}};

-- Remove the top-level Optional from the data type
$removeOptional = ($type) -> {
    RETURN IF(TypeKind($type) == "Optional", OptionalItemType($type), $type);
};

-- Filter only numeric columns and put them in the nums struct
$filterOnlyNumericColumns = ($strValue) -> {
    $code = EvaluateCode(LambdaCode(($str) -> {
        $members = StructTypeComponents(TypeHandle(TypeOf($strValue)));
        $filteredMembers = ListFilter($members, ($x) -> {
            $type = $x.Type;
            $cleanType = $removeOptional($type);
            RETURN TypeKind($cleanType) == "Data" AND
                DataTypeComponents($cleanType)[0] REGEXP "Int[0-9]+|Uint[0-9]+|Float|Double"
        });

        $list = ListMap($filteredMembers, ($x) -> {
            RETURN ListCode(AtomCode($x.Name), FuncCode("Member", $str, AtomCode($x.Name)));
        });

        RETURN FuncCode("AsStruct",$list);
    }));

    RETURN $code($strValue);
};

$nums = (SELECT $filterOnlyNumericColumns(TableRow()) as nums FROM `home/yql/tutorial/users`);

SELECT * FROM $nums;

-- Apply the set aggregate function to the entire nums struct at once.
-- Aggregation results are nested structures for the columns count/min/...
$agg = (SELECT
    MULTI_AGGREGATE_BY(nums, AggregationFactory("count")) as count,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("min")) as min,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("max")) as max,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("avg")) as avg,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("percentile", 0.9)) as p90
FROM $nums);

SELECT * FROM $agg;

-- Expand the structures so that the columns have the format originalcolumn_aggrfunc.
-- You can't manage with FLATTEN COLUMNS here only, because there are conflicts: in count/min, etc.
-- the nested fields are identical.

$rotateStruct = ($value) -> {
    $code = EvaluateCode(LambdaCode(($strValue) -> {
        -- Retrieve the top-level structure columns
        $topMembers = StructTypeComponents(TypeHandle(TypeOf($value)));
        $list = ListFlatMap($topMembers, ($x) -> {
            $topMember = FuncCode("Member", $strValue, AtomCode($x.Name));
            -- Traverse fields in the structures
            $nestedMembers = StructTypeComponents($removeOptional($x.Type));
            RETURN ListMap($nestedMembers, ($y) -> {
                RETURN ListCode(AtomCode($y.Name || "_" || $x.Name),
                    FuncCode("Member", $topMember, AtomCode($y.Name)));
            })
        });

        RETURN FuncCode("AsStruct", $list);
    }));

    RETURN $code($value);
};

$rotate = (SELECT $rotateStruct(TableRow()) FROM $agg);

SELECT * FROM $rotate;
```

Links to documentation:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListFilter](../builtins/list.md#listfilter)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [AsStruct]({{yql.s-expressions-link}}/functions#asstruct)
{% endif %}
* [Lambda functions](../syntax/expressions.md#lambda)
* [MULTI_AGGREGATE_BY](../builtins/aggregation.md#multiaggregateby)
* [AggregationFactory](../builtins/basic.md#aggregationfactory)

### Transposing a table (PIVOT)

```yql
-- Sample data
$input = AsList(
    AsStruct("2012" as year, 1 as key, 3.0 as value),
    AsStruct("2013" as year, 1 as key, 4.0 as value),
    AsStruct("2013" as year, 2 as key, 5.0 as value),
    AsStruct("2014" as year, 1 as key, 6.0 as value),
    AsStruct("2014" as year, 1 as key, 7.0 as value)
);

$makePivot = ($nameColumn, $valueColumn, $nameList) -> {
    RETURN EvaluateCode(LambdaCode(($row) -> {
        $name = FuncCode("Member", $row, AtomCode($nameColumn));
        $value = FuncCode("Member", $row, AtomCode($valueColumn));
        $row = FuncCode("RemoveMember", $row, AtomCode($nameColumn));
        $row = FuncCode("RemoveMember", $row, AtomCode($valueColumn));

        $ensureOptional = ($x) -> {
            RETURN YQL::MatchType($x, AsAtom("Optional"), () -> { RETURN $x }, () -> { RETURN Just($x) });
        };

        $structItems = ListMap($nameList, ($item) -> {
            $adjustedValue = FuncCode("Apply", QuoteCode($ensureOptional), $value);
            $ifValue = FuncCode("FlatOptionalIf",
                FuncCode("Coalesce", FuncCode("==", $name, ReprCode($item)), ReprCode(false)),
                $adjustedValue);
            RETURN ListCode(AtomCode($item), $ifValue);
        });

        $struct = FuncCode("AsStruct", $structItems);
        RETURN FuncCode("AddMember", $row, AtomCode($valueColumn), $struct);
    }));
};

-- Build a PIVOT operation:
-- 1. The `value` field is populated with the structure where you created the columns from the last list.
-- 2. Depending on `year` field value, a certain field in the output `value` structure is populated.
-- 3. After that, the `year` field is deleted

$pivot = $makePivot("year", "value", AsList("2012", "2013", "2014"));

-- Source data
SELECT * FROM AS_TABLE($input);

$x = (SELECT * FROM (
    SELECT $pivot(TableRow()) FROM AS_TABLE($input)
) FLATTEN COLUMNS);

-- Result
SELECT * FROM (
    SELECT
        AsStruct(key as key),
        MULTI_AGGREGATE_BY(value, AggregationFactory("sum"))
    FROM $x GROUP BY key
) FLATTEN COLUMNS;
```

Links to documentation:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [AddMember]({{yql.s-expressions-link}}/functions#addmember)
* [RemoveMember]({{yql.s-expressions-link}}/functions#removemember)
* [AsStruct]({{yql.s-expressions-link}}/functions#asstruct)
{% endif %}
* [Lambda functions](../syntax/expressions.md#lambda)
* [MULTI_AGGREGATE_BY](../builtins/aggregation.md#multiaggregateby)
* [AggregationFactory](../builtins/basic.md#aggregationfactory)

### Merging subquery templates

```yql
$combineQueries = ($query, $list) -> {
    RETURN EvaluateCode(LambdaCode(($world) -> {
        -- An implicit parameter in the subquery template is the world argument used to pass
        -- dependencies, for example, visible PRAGMA or COMMIT operations at the subquery template point of use.
        $queries = ListMap($list, ($arg) -> {
                -- Pass world further, as the first argument of the subquery template
                RETURN FuncCode("Apply", QuoteCode($query), $world, ReprCode($arg))
            });

        -- Merge all the results into a common list (this requires that types are the same).
        -- For weaker merging, you can use the UnionAll function.
        RETURN FuncCode("Extend", $queries);
    }));
};

DEFINE SUBQUERY $sub($n) AS
   SELECT $n;
END DEFINE;

-- Build a query template on merging
-- the results of applying the $sub subquery template for each value between 0 and 9.
$fullQuery = $combineQueries($sub, ListFromRange(0, 10));
SELECT * FROM $fullQuery();
```

Links to documentation:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Apply]({{yql.s-expressions-link}}/functions#apply)
* [Extend]({{yql.s-expressions-link}}/functions#extend)
{% endif %}
* [Lambda functions](../syntax/expressions.md#lambda)
* [Subquery templates](../syntax/subquery.md)

### Building a subquery template with sorting by a list of column

```yql
USE {{production-cluster}};

$sorted = ($world, $input, $orderByColumns, $asc) -> {
    $n = ListLength($orderByColumns);

    $keySelector = LambdaCode(($row) -> {
        $items = ListMap($orderByColumns,
            ($x) -> {
                RETURN FuncCode("Member", $row, AtomCode($x));
            });
        RETURN ListCode($items);
    });

    $sort = EvaluateCode(LambdaCode(($x) -> {
        return FuncCode("Sort",
            $x,
            ListCode(ListReplicate(ReprCode($asc), $n)),
            $keySelector)
    }));

    RETURN $sort($input($world));
};

DEFINE SUBQUERY $source() AS
    PROCESS `home/yql/tutorial/users`;
END DEFINE;

PROCESS $sorted($source, AsList("name","age"), true);
PROCESS $sorted($source, AsList("name"), true);
PROCESS $sorted($source, ListCreate(TypeOf("")), true);
```

Links to documentation:


* [EvaluateCode](../builtins/codegen.md#evaluatecode)
* [ListMap](../builtins/list.md#listmap)
{% if audience == "internal" %}
* [Member]({{yql.s-expressions-link}}/functions#member)
* [Sort]({{yql.s-expressions-link}}/functions#sort)
{% endif %}
* [Lambda functions](../syntax/expressions.md#lambda)
* [Subquery templates](../syntax/subquery.md)
