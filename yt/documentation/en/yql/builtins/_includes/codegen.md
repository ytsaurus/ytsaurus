---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/codegen.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/codegen.md
---
# Functions for working with code generation

During computations, you can generate code consisting of [S-expressions](/docs/s_expressions) nodes. To do this, use a mechanism for representing code packed in a [resource](../../types/special.md). After constructing the code, you can place it into the main program using the [EvaluateCode](#evaluatecode) function. For debugging, you can convert the code into a string using the [FormatCode](#formatcode) function.

Possible node types in S-expressions that can be used to generate code:

* Atom: An untyped string of zero or more characters.
* List: A sequence of zero or more nodes. Corresponds to the `tuple` type in SQL.
* A built-in function invocation: Consists of a name expressed by an atom and a sequence of zero or more nodes that are arguments of that function.
* A lambda function declaration: Consists of the declaration of argument and node names that is the root of the body of that lambda function.
* A lambda function argument: A node that can only be used inside the body of the lambda function.
* World: A special node that marks I/O operations.

S-expressions nodes form an oriented graph. Atoms are always leaf nodes, since they cannot contain daughter nodes.

S-expressions are written as follows in text form:

* Atom: ```'"foo"```. The apostrophe symbol (') is a sign of quoting a subsequent string usually enclosed in quotation marks.
* List: ```'("foo" "bar")```. The apostrophe symbol (') is a sign that there will be no function invocation in brackets.
* A built-in function invocation: ```(foo "bar")```. The first item inside the brackets is a mandatory function name, and then its arguments are specified.
* A lambda function declaration: ```(lambda '(x y) (+ x y))```. The `lambda` keyword is followed by a list of argument names and the body of the lambda function.
* A lambda function argument: ```x```. Unlike an atom, a string without an apostrophe symbol (') is a reference to the name in the current scope. When you declare a lambda function, the argument names are added to the scope of the body, hiding the name from the enclosing scope if necessary.
* World: ```world```.

<!-- {% note info %}

There is a [separate introductory article](../../guides/codegen.md) about the code generation mechanism. Below is a reference on the specific functions.

{% endnote %} -->

## FormatCode

Code serialization as [S-expressions](/docs/s_expressions). The code must not contain free function arguments, i.e. to serialize the lambda function code, you must pass it in its entirety, not expressions that potentially contain lambda function arguments.

**Examples:**
```yql
SELECT FormatCode(AtomCode("foo"));
-- (
-- (return '"foo")
-- )
```

## WorldCode

Build a code node with the `world` type.

**Examples:**
```yql
SELECT FormatCode(WorldCode());
-- (
-- (return world)
-- )
```

## AtomCode

Build a code node with the `atom` type from a string passed to the argument.

**Examples:**
```yql
SELECT FormatCode(AtomCode("foo"));
-- (
-- (return '"foo")
-- )
```

## ListCode

Build a code node with the `list` type from a set of nodes or lists of code nodes passed to the arguments. The lists from the arguments are embedded as separately listed code nodes.

**Examples:**
```yql
SELECT FormatCode(ListCode(
    AtomCode("foo"),
    AtomCode("bar")));
-- (
-- (return '('"foo" '"bar"))
-- );

SELECT FormatCode(ListCode(AsList(
    AtomCode("foo"),
    AtomCode("bar"))));
-- (
-- (return '('"foo" '"bar"))
-- )
```

## FuncCode

Build a code node with the `built-in function invocation` type from a string with the function name and a set of nodes or lists of code nodes passed to the arguments. The lists from the arguments are embedded as separately listed code nodes.

**Examples:**
```yql
SELECT FormatCode(FuncCode(
    "Baz",
    AtomCode("foo"),
    AtomCode("bar")));
-- (
-- (return (Baz '"foo" '"bar"))
-- )

SELECT FormatCode(FuncCode(
    "Baz",
    AsList(
        AtomCode("foo"),
        AtomCode("bar"))));
-- (
-- (return (Baz '"foo" '"bar"))
-- )
```

## LambdaCode

You can build a code node with the `lambda function declaration` type from the:

* [Lambda function](../../syntax/expressions.md#lambda) if the number of arguments is known in advance. In this case, nodes of the `argument` type will be passed as arguments of this lambda function.
* Number of arguments and [lambda function](../../syntax/expressions.md#lambda) with one argument. In this case, a list of nodes of the `argument` type will be passed as an argument of this lambda function.

**Examples:**
```yql
SELECT FormatCode(LambdaCode(($x, $y) -> {
    RETURN FuncCode("+", $x, $y);
}));
-- (
-- (return (lambda '($1 $2) (+ $1 $2)))
-- )

SELECT FormatCode(LambdaCode(2, ($args) -> {
    RETURN FuncCode("*", Unwrap($args[0]), Unwrap($args[1]));
}));
-- (
-- (return (lambda '($1 $2) (* $1 $2)))
-- )
```

## EvaluateCode

Placing the code node passed to the argument into the main program.

**Examples:**
```yql
SELECT EvaluateCode(FuncCode("Int32", AtomCode("1"))); -- 1

$lambda = EvaluateCode(LambdaCode(($x, $y) -> {
    RETURN FuncCode("+", $x, $y);
}));
SELECT $lambda(1, 2); -- 3
```

## ReprCode

Placing the code node that is a representation of the result of computing the expression passed to the argument into the main program.

**Examples:**
```yql
$add3 = EvaluateCode(LambdaCode(($x) -> {
    RETURN FuncCode("+", $x, ReprCode(1 + 2));
}));
SELECT $add3(1); -- 4
```

## QuoteCode

Placing the code node that is a representation of the expression or [lambda function](../../syntax/expressions.md#lambda) passed to the argument into the main program. If free lambda function arguments are found during placing, they are computed and placed into the code as in the [ReprCode](#reprcode) function.

**Examples:**
```yql
$lambda = ($x, $y) -> { RETURN $x + $y };
$makeClosure = ($y) -> {
    RETURN EvaluateCode(LambdaCode(($x) -> {
        RETURN FuncCode("Apply", QuoteCode($lambda), $x, ReprCode($y))
    }))
};

$closure = $makeClosure(2);
SELECT $closure(1); -- 3
```
