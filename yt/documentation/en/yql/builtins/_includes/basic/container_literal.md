---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/container_literal.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/container_literal.md
---
## Container literals {#containerliteral}

For some containers, an operator format of their literal values is possible:

* Tuple: `(value1, value2...)`.
* Structure: `<|name1: value1, name2: value2...|>`.
* List: `[value1, value2,...]`.
* Dict: `{key1: value1, key2: value2...}`.
* Set: `{key1, key2...}`.

An insignificant trailing comma is allowed in all cases. This comma is mandatory for a tuple with one item: ```(value1,)```.
For field names in the structure literal, you can use an expression that can be counted in evaluation time, for example, string literals, as well as identifiers (including in backticks).

For a list inside, the [AsList](#aslist) function is used, for a dict — the [AsDict](#asdict) function, for a set — the [AsSet](#asset) function, for a tuple — the [AsTuple](#astuple) function, and for a structure — the [AsStruct](#asstruct) function.

**Examples**
```yql
$name = "computed " || "member name";
SELECT
  (1, 2, "3") AS `tuple`,
  <|
    `complex member name`: 2.3,
    b: 2,
    $name: "3",
    "inline " || "computed member name": false
  |> AS `struct`,
  [1, 2, 3] AS `list`,
  {
    "a": 1,
    "b": 2,
    "c": 3,
  } AS `dict`,
  {1, 2, 3} AS `set`
```
