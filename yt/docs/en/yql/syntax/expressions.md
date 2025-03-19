# Expressions


## String concatenation {#concatenation}

Executed using the binary operator `||`.

As with other binary operators, if the data on either side is `NULL`, the result is also `NULL`.

Don't confuse this operator with a logical "or": in SQL, it's denoted by the `OR` keyword. It's also not worth doing concatenation using `+`.

**Examples**

```sql
SELECT "fo" || "o";
```



## Matching a string by pattern {#check-match}

`REGEXP` and `RLIKE` are aliases and offer a short way to record for a [Re2::Grep](../udf/list/re2.md#match) call. `MATCH` — similar to [Re2::Match](../udf/list/re2.md#match).

`LIKE` works as follows:

* Patterns can include two special characters:
   * `%`: Zero or more of any characters.
   * `_`: Exactly one of any character.

      All other characters are literals that represent themselves.
* As opposed to `REGEXP`, `LIKE` must be matched exactly. For example, to search a substring, add `%` at the beginning and end of the pattern.
* `ILIKE` is a case-insensitive version of `LIKE`.
* If `LIKE` is applied to the key column of the sorted table and the pattern doesn't start with a special character, filtering by prefix drills down directly to the cluster level, which in some cases lets you avoid the full table scan. This optimization is disabled for `ILIKE`.
* To escape special characters, specify the escaped character after the pattern using the `ESCAPE '?'` keyword. Instead of `?` you can use any character except `%` and `_`. For example, if you use a question mark as an escaping symbol, then `?%`, `?_` and `??` expressions will match their second character in the template: percent, underscore, and question mark, respectively. The escape character is undefined by default.

The most popular way to use the `LIKE` and `REGEXP` keywords is to filter a table using the statements with the `WHERE` clause. However, there are no restrictions on using templates in this context: you can use them in most of contexts involving strings, for example, with concatenation by using `||`.

**Examples**
```yql
SELECT * FROM my_table
WHERE string_column REGEXP '\\d+';
-- requires a second slash because all
-- standard string literals in SQL
-- may accept C-escaped strings.
```

```yql
SELECT
    string_column LIKE '___!_!_!_!!!!!!' ESCAPE '!'
    -- searches for a string of exactly 9 symbols:
    --   3 random,
    --   then 3 underscores
    --   and 3 exclamation marks
FROM my_table;
```

```yql
SELECT * FROM my_table
WHERE key LIKE 'foo%bar';
-- if there's table sorting by key, it only scans keys,
-- starting with foo, and then among them
-- leaves only those that end with bar
```



## Operators

### Arithmetic operators {#math-operators}

The operators `+`, `-`, `*`, `/`, `%` are defined for [primitive data types](../types/primitive.md) that are variations of numbers.

For the Decimal data type, bankers rounding is used (to the nearest even integer).

**Examples**

```yql
SELECT 2 + 2;
```

```yql
SELECT 0.0 / 0.0;
```

### Comparison operators {#comparison-operators}

`=`, `==`, `!=`, `<>`, `>`, `<` operators are defined for:

* Primitive data types except Yson and Json.
* Tuples and structures with the same set of fields. No order is defined for structures, but you can check for (non-)equality. Tuples are compared element-by-element left to right.

**Examples**

```yql
SELECT 2 > 1;
```

### Logical operators {#logic-operators}

Use the operators `AND`, `OR`, `XOR` for logical operations on Boolean values (`Bool`).

**Examples**

```yql
SELECT 3 > 0 AND false;
```

### Bitwise operators {#bit-operators}

Bitwise operations on numbers:

* `&`, `|`, `^`: AND, OR, and XOR, respectively. Don't confuse bitwise operations with the related keywords. The keywords `AND`, `OR`, and `XOR` are used for *Boolean values only*, but not for numbers.
* ` ~ ` — negation.
* `<<`, `>>`: Left or right shifts.
* `|<<`, `>>|`: Circular left or right shifts.

**Examples**

```yql
SELECT
    key << 10 AS key,
    ~value AS value
FROM my_table;
```

### Precedence and associativity of operators {#operator-priority}

Operator precedence determines the order of evaluation of an expression that contains different operators.
For example, the expression `1 + 2 * 3` is evaluated as `1 + (2 * 3)`
because the multiplication operator has a higher precedence than the addition operator.

Associativity determines the order of evaluating expressions containing operators of the same type.
For example, the expression `1 + 2 + 3` is evaluated as `(1 + 2) + 3` because the addition operator is left-associative.
On the other hand, the expression `a ?? b ?? c` is evaluated as `a ?? (b ?? c)` because the `??` operator is right-associative

The table below shows precedence and associativity of YQL operators.
The operators in the table are listed in descending order of precedence.

| Priority | Operator | Description | Associativity |
| --- | --- | --- | --- |
| 1 | <code>a[], a.foo, a()</code> | Accessing a container item, calling a function | Left |
| 2 | <code>+a, -a, ~a, NOT a</code> | Unary operators: plus, minus, bitwise and logical negation | Right |
| 3 | <code>a &#124;&#124; b</code> | [String concatenation](#concatenation) | Left |
| 4 | <code>a*b, a/b, a%b</code> | Multiplication, division, remainder of division | Left |
| 5 | <code>a+b, a-b</code> | Addition/Subtraction | Left |
| 6 | <code>a ?? b</code> | Operator notation for NVL/COALESCE | Right |
| 7 | <code>a<<b, a>>b, a&#124;<<b, a>>&#124;b,</code> <code>a&#124;b, a^b, a&b</code> | Shift operators and logical bit operators | Left |
| 8 | <code>a<b, a<=b, a>=b, a>b</code> | Comparison | Left |
| 9 | <code>a IN b</code> | Occurrence of an element in a set | Left |
| 9 | <code>a==b, a=b, a!=b, a<>b,</code> <code>a is (not) distinct from b</code> | Comparison for (non-)equality | Left |
| 10 | <code>a XOR b</code> | Logical XOR | Left |
| 11 | <code>a AND b</code> | Logical AND | Left |
| 12 | <code>a OR b</code> | Logical OR | Left |



## IS \[NOT\] NULL {#is-null}

Matching an empty value (`NULL`). Since `NULL` is a special value that [equals nothing](../types/optional.md#null_expr), regular [comparison operators](#comparison-operators) are not suitable for this task.

**Examples**

```yql
SELECT key FROM my_table
WHERE value IS NOT NULL;
```



## IS \[NOT\] DISTINCT FROM {#is-distinct-from}

Comparing of two values. Unlike regular [comparison operators](#comparison-operators), NULLs are considered equal to each other.
More precisely, the comparison is carried out according to the following rules:
1) `IS DISTINCT FROM`/`IS NOT DISTINCT FROM` operators are defined for those (and only those) arguments for which `!=` and `=` operators are defined.
2) The result of `IS NOT DISTINCT FROM` is equal to the logical negation of the `IS DISTINCT FROM` result for these arguments.
3) If the result of the `==` operator is not equal to zero for some arguments, then it is equal to the result of the `IS NOT DISTINCT FROM` operator for the same arguments.
4) If both arguments are empty `Optional` or `NULL`s, then the value of `IS NOT DISTINCT FROM` is `True`.
5) The result of `IS NOT DISTINCT FROM` for an empty `Optional` or `NULL` and filled-in `Optional` or non-`Optional` value is `False`.

For values of composite types, these rules are used recursively.


## BETWEEN {#between}

Checking whether a value is in a range. It's equivalent to two conditions with `>=` and `<=` (range boundaries are included). Can be used with the `NOT` prefix to support inversion.

**Examples**

```yql
SELECT * FROM my_table
WHERE key BETWEEN 10 AND 20;
```




## IN {#in}
Checking whether a value is inside of a set of values. It's logically equivalent to a chain of equality comparisons using `OR` but implemented more efficiently.

{% note warning "Attention!" %}

Unlike the same keyword in Python, in YQL `IN` **DOES NOT** involve searching for a substring in a string. You can use [String::Contains](../udf/list/string.md) or [LIKE / REGEXP](#like) described above to search for a substring.

{% endnote %}

You can specify the [hint](lexer.md#sql-hints) `COMPACT` right after `IN`.
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



## AS {#as}

Can be used in the following scenarios:

* Adding a short name (alias) for columns or tables within the query.
* Using named arguments in function calls.
* If you perform explicit casting to specify the target type, see [CAST](#cast).

**Examples:**

```yql
SELECT key AS k FROM my_table;
```

```yql
SELECT t.key FROM my_table AS t;
```

```yql
SELECT
    MyFunction(key, 123 AS my_optional_arg)
FROM my_table;
```



## CAST {#cast}

Tries to cast the value to the specified type. The attempt may fail and return `NULL`. When used with numbers, it may lose precision or most significant bits.
For lists and dictionaries, it can either delete or replace with `NULL` the elements whose conversion failed.
For structures and tuples, it deletes elements that are omitted in the target type.
To learn more about conversions, see [here](../types/cast.md).

{% include [decimal_args](../_includes/decimal_args.md) %}

**Examples**

{% include [cast_examples](../_includes/cast_examples.md) %}



## BITCAST {#bitcast}
Performs a bitwise conversion of an integer value to the specified integer type. The conversion is always successful, but may lose precision or high-order bits.

**Examples**
```yql
SELECT
    BITCAST(100000ul AS Uint32),     -- 100000
    BITCAST(100000ul AS Int16),      -- -31072
    BITCAST(100000ul AS Uint16),     -- 34464
    BITCAST(-1 AS Int16),            -- -1
    BITCAST(-1 AS Uint16);           -- 65535
```



## CASE {#case}

Conditional expressions and branching. It's similar to `if`, `switch` and ternary operators in the imperative programming languages.

If the result of the `WHEN` expression is `true`, the value of the `CASE` expression becomes the result
following the condition, and the rest of the `CASE` expression isn't calculated. If the condition is not met,
all the `WHEN` clauses that follow are checked. If none
of the `WHEN` conditions is fulfilled, the result recorded in the `ELSE` sentence becomes the `CASE` value.
The `ELSE` branch is mandatory in the `CASE` expression. Expressions in `WHEN` are checked sequentially, from top to bottom.

Since its syntax is quite sophisticated, it's often more convenient to use the built-in `IF` function.

**Examples**
```yql
SELECT
  CASE
    WHEN value > 0
    THEN "positive"
    ELSE "negative"
  END
FROM my_table;
```

```yql
SELECT
  CASE value
    WHEN 0 THEN "zero"
    WHEN 1 THEN "one"
    ELSE "not zero or one"
  END
FROM my_table;
```



## Named expressions {#named-nodes}

Complex queries may be sophisticated, containing lots of nested levels and/or repeating parts. In YQL, you can use named expressions to assign a name to an arbitrary expression or subquery. Named expressions can be referenced in other expressions or subqueries. In this case, the original expression/subquery is actually substituted at point of use.

A named expression is defined as follows:
```
<named-expr> = <expression> | <subquery>;
```
Here `<named-expr>` consists of a $ character and an arbitrary non-empty identifier (for example, `$foo`).

If the expression on the right is a tuple, you can automatically unpack it by specifying several named expressions separated by commas on the left:
```
<named-expr1>, <named-expr2>, <named-expr3> ... = <expression-returning-tuple>;
```
In this case, the number of expressions must match the tuple size.

Each named expression has a scope. It starts immediately after defining the named expression and ends at the end of the closest enveloping scope of names (e.g. at the end of the query or at the end of the [lambda function](#lambda) body, [ACTION](action.md#define-action), [SUBQUERY](subquery.md#define-subquery) or [EVALUATE FOR](action.md#evaluate-for)) cyclic path.
Redefining a named expression with the same name hides the previous expression from the current scope.

If the named expression has never been used, a warning is issued. To avoid such a warning, use the underscore as the first character in the ID (for example, `$_foo`).
`$_` named expression is called an anonymous named expression and processed in a special way. It works as if `$_` was automatically replaced with `$_<some_uniq_name>`.
Anonymous named expressions are convenient when you don't need the expression value. For example, to fetch the second element from a tuple of three elements, you can write:
```yql
$_, $second, $_ = AsTuple(1, 2, 3);
select $second;
```

An attempt to reference an anonymous named expression results in an error:
```yql
$_ = 1;
select $_; --- error: Unable to reference anonymous name $_
export $_; --- error: Can not export anonymous name $_
```
Also, you can't import a named expression using an anonymous alias:
```yql
import utils symbols $sqrt as $_; --- error: Can not import anonymous name $_
```
Anonymous argument names are also supported for [lambda functions](#lambda), [ACTION](action.md#define-action), [SUBQUERY](subquery.md#define-subquery) and in [EVALUATE FOR](action.md#evaluate-for).

{% note info %}

If named expression substitution results in completely identical subgraphs in the query execution graph, the graphs are combined to execute a subgraph only once.

{% endnote %}

**Examples**

```yql
$multiplier = 712;
SELECT
  a * $multiplier, -- $multiplier is 712
  b * $multiplier,
  (a + b) * $multiplier
FROM abc_table;
$multiplier = c;
SELECT
  a * $multiplier -- $multiplier is column c
FROM abc_table;
```

```yql
$intermediate = (
  SELECT
    value * value AS square,
    value
  FROM my_table
);
SELECT a.square * b.value
FROM $intermediate AS a
INNER JOIN $intermediate AS b
ON a.value == b.square;
```

```yql
$a, $_, $c = AsTuple(1, 5u, "test"); -- tuple unpacking
SELECT $a, $c;
```

```yql
$x, $y = AsTuple($y, $x); -- swap expression values
```



## Table expressions {#table-contexts}

A table expression is an expression that returns a table. Table expressions in YQL are as follows:
* Subqueries: `(SELECT key, subkey FROM T)`
* [named subqueries](#named-nodes): `$foo = SELECT * FROM T;` (use of a `$foo` named subquery is a table expression)
* [subquery templates](subquery.md#define-subquery): `DEFINE SUBQUERY $foo($name) AS ... END DEFINE;` (`$foo("InputTable")` call is a table expression).

Semantics of a table expression depends on the context where it is used. In YQL, table expressions can be used in the following contexts:
* Table context: after [FROM](select/from.md).
   In this case, table expressions work as expected: for example, `$input = SELECT a, b, c FROM T; SELECT * FROM $input` returns a table with three columns.
   The table context also appears after [UNION ALL](select/union.md#union-all), [JOIN](join.md#join), [PROCESS](process.md#process), and [REDUCE](reduce.md#reduce);
* the vector context appears after [IN](#in). In this context, the table expression must contain exactly one column (the name of this column doesn't affect the expression result in any way).
   A table expression in a vector context is typed as a list (the type of the list element is the same as the column type in this case). Example: `SELECT * FROM T WHERE key IN (SELECT k FROM T1)`;
* A scalar context arises _in all the other cases_. As in a vector context,
   a table expression must contain exactly one column, but the value of the table expression is a scalar, that is,
   an arbitrarily selected value of this column (if no rows are returned, the result is `NULL`). Example: `$count = SELECT COUNT(*) FROM T; SELECT * FROM T ORDER BY key LIMIT $count / 2`;

The order of rows in a table context, the order of elements in a vector context, and the rule for selecting a value from a scalar context (if multiple values are returned), aren't defined. This order also cannot be affected by `ORDER BY`: `ORDER BY` without `LIMIT` is ignored in table expressions with a warning, and `ORDER BY` with `LIMIT` defines a set of elements rather than the order within that set.


There is an exception to this rule. A named expression containing [PROCESS](process.md#process), when used in scalar context, behaves as if it existed in table context:

```yql
$input = SELECT 1 AS key, 2 AS value;
$process = PROCESS $input;

SELECT FormatType(TypeOf($process)); -- $process used in scalar context,
                                     -- but the result of SELECT in this case is - List<Struct<'key':Int32,'value':Int32>>

SELECT $process[0].key; -- returns 1

SELECT FormatType(TypeOf($input)); -- error: $input in scalar context must contain one column
```
{% note warning "Attention!" %}

A common error is to use an expression in a scalar context rather than a table context or vector context. For example:

```yql
$dict = SELECT key, value FROM T1;

DEFINE SUBQUERY $merge_dict($table, $dict) AS
SELECT * FROM $table LEFT JOIN $dict USING(key);
END DEFINE;

SELECT * FROM $merge_dict("Input", $dict); -- $dict here is used in scalar context.
                                           -- an error: exactly one column is expected in a scalar context

```

A correct notation in this case is:

```yql
DEFINE SUBQUERY $dict() AS
SELECT key, value FROM T1;
END DEFINE;

DEFINE SUBQUERY $merge_dict($table, $dict) AS
SELECT * FROM $table LEFT JOIN $dict() USING(key); -- use of the table expression $dict()
                                                   -- (subquery template call) in table context
END DEFINE;

SELECT * FROM $merge_dict("Input", $dict); -- $dict - subquery template (not a table expression)
                                           -- passed as a table expression argument
```

{% endnote %}



## Lambda functions {#lambda}

Let you combine multiple expressions into a single callable value.

List arguments in round brackets, following them by the arrow and lambda function body. The body of the lambda consists of an expression enclosed in parentheses or of curly brackets enclosing the optional chain of expressions and assigns [named expressions](#named-nodes) and the output of a call after the `RETURN` keyword in the last expression.

The scope for the lambda body: first the local named expressions, then arguments, then named expressions defined above by the lambda function at the top level of the query.

Only use pure expressions inside the lambda body (those might also be other lambdas, possibly passed through arguments). But you can't use [SELECT](select/index.md), [INSERT INTO](insert_into.md) and other top-level statements inside lambdas.

One or more of the last lambda parameters can be marked with a question mark as optional: if they haven't been specified when calling lambda, they are assigned the `NULL` value.

**Examples**

```yql
$f = ($y) -> {
    $prefix = "x";
    RETURN $prefix || $y;
};

$g = ($y) -> ("x" || $y);

$h = ($x, $y?) -> ($x + ($y ?? 0));

SELECT $f("y"), $g("z"), $h(1), $h(2, 3); -- "xy", "xz", 1, 5
```

```yql
-- if the lambda output is calculated as a single expression, you can use a more compact syntax variant:
$f = ($x, $_) -> ($x || "suffix"); -- the second argument isn't used by
SELECT $f("prefix_", "whatever");
```


<!--[Example in tutorial](https://cluster-name.yql/Tutorial/yt_16_Lambda_functions)-->


## Accessing containers {#items-access}

For accessing the values inside containers:

* `Struct<>`, `Tuple<>` and `Variant<>`, use a **dot**. The set of keys (for the tuple and the corresponding variant — indexes) is known at the query compilation time. The key is **validated** before beginning the query execution.
* `List<>` and `Dict<>`, use **square brackets**. The set of keys (set of indexes for keys) is known only at the query execution time. The key is **not validated** before beginning the query execution. If no value is found, an empty value (NULL) is returned

[Description and list of available containers](../types/containers.md).

When using this syntax to access containers within table columns, be sure to specify the full column name, including the table name or table alias separated by a dot (see the first example below).

**Examples**

```yql
SELECT
  t.struct.member,
  t.tuple.7,
  t.dict["key"],
  t.list[7]
FROM my_table AS t;
```

```yql
SELECT
  Sample::ReturnsStruct().member;
```


<!--[Example in tutorial](https://cluster-name.yql/Tutorial/yt_12_Containers)-->
