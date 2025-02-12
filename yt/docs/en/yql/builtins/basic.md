# Basic built-in functions

Below are the general-purpose functions. For specialized functions, there are separate articles: [aggregate](aggregation.md), [window](window.md), as well as for [lists](list.md), [dictionaries](dict.md), [structures](struct.md), [data types](types.md), and [code generation](codegen.md).



## COALESCE {#coalesce}

Goes through the arguments from left to right and returns the first non-empty argument found. For the result to be guaranteed non-empty (not [optional type](../types/optional.md)), the rightmost argument must be of this type (a literal is often used). If there is one argument, returns it without change.

**Signature**
```
COALESCE(T?, ..., T)->T
COALESCE(T?, ..., T?)->T?
```

Enables you to pass potentially empty values to functions that cannot process them.

A short write format in the form of the `??` operator which has a low priority (below boolean operations) is available. The `NVL` alias can be used.

**Examples**
```yql
SELECT COALESCE(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

```yql
SELECT
  maybe_empty_column ?? "it's empty!"
FROM my_table;
```

```yql
SELECT NVL(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

<span style="color: gray;">(all three examples above are equivalent)</span>



## LENGTH {#length}

Returns the length of the string in bytes. This function is also available under the `LEN` name .

**Signature**
```
LENGTH(T)->Uint32
LENGTH(T?)->Uint32?
```

**Examples**
```yql
SELECT LENGTH("foo");
```
```yql
SELECT LEN("bar");
```

{% note info %}

You can use the function [Unicode::GetLength](../udf/list/unicode.md) to calculate the length of a string in Unicode characters.<br><br>To get the number of elements in the list, use the function [ListLength](list.md#listlength).

{% endnote %}



## SUBSTRING {#substring}

Returns a substring.

**Signature**
```
Substring(String[, Uint32? [, Uint32?]])->String
Substring(String?[, Uint32? [, Uint32?]])->String?
```

Mandatory arguments:

* Source string;
* Position: The offset from the beginning of the string in bytes (integer) or `NULL` meaning "from the beginning".

Optional arguments:

* Substring length: The number of bytes starting from the specified position (an integer, or the default `NULL` meaning "up to the end of the source string").

Indexing starts from zero. If the specified position and length are beyond the string, returns an empty string.
If the input string is optional, the result is also optional.

**Examples**
```yql
SELECT SUBSTRING("abcdefg", 3, 1); -- d
```
```yql
SELECT SUBSTRING("abcdefg", 3); -- defg
```
```yql
SELECT SUBSTRING("abcdefg", NULL, 3); -- abc
```



## FIND {#find}

Finding the position of a substring in a string.

**Signature**
```
Find(String, String[, Uint32?])->Uint32?
Find(String?, String[, Uint32?])->Uint32?
Find(Utf8, Utf8[, Uint32?])->Uint32?
Find(Utf8?, Utf8[, Uint32?])->Uint32?
```

Mandatory arguments:

* Source string;
* The substring being searched for.

Optional arguments:

* A position in bytes to start the search with (an integer or `NULL` by default that means "from the beginning of the source string").

Returns the first substring position found or `NULL` (meaning that the desired substring hasn't been found starting from the specified position).

**Examples**
```yql
SELECT FIND("abcdefg_abcdefg", "abc"); -- 0
```
```yql
SELECT FIND("abcdefg_abcdefg", "abc", 1); -- 8
```
```yql
SELECT FIND("abcdefg_abcdefg", "abc", 9); -- null
```

## RFIND {#rfind}

Reverse finding the position of a substring in a string, from the end to the beginning.

**Signature**
```
RFind(String, String[, Uint32?])->Uint32?
RFind(String?, String[, Uint32?])->Uint32?
RFind(Utf8, Utf8[, Uint32?])->Uint32?
RFind(Utf8?, Utf8[, Uint32?])->Uint32?
```

Mandatory arguments:

* Source string;
* The substring being searched for.

Optional arguments:

* A position in bytes to start the search with (an integer or `NULL` by default, meaning "from the end of the source string").

Returns the first substring position found or `NULL` (meaning that the desired substring hasn't been found starting from the specified position).

**Examples**
```yql
SELECT RFIND("abcdefg_abcdefg", "bcd"); -- 9
```
```yql
SELECT RFIND("abcdefg_abcdefg", "bcd", 8); -- 1
```
```yql
SELECT RFIND("abcdefg_abcdefg", "bcd", 0); -- null
```



## StartsWith, EndsWith {#starts_ends_with}

Checking for a prefix or suffix in a string.

**Signatures**
```
StartsWith(Utf8, Utf8)->Bool
StartsWith(Utf8[?], Utf8[?])->Bool?
StartsWith(String, String)->Bool
StartsWith(String[?], String[?])->Bool?

EndsWith(Utf8, Utf8)->Bool
EndsWith(Utf8[?], Utf8[?])->Bool?
EndsWith(String, String)->Bool
EndsWith(String[?], String[?])->Bool?
```

Mandatory arguments:

* Source string;
* The substring being searched for.

The arguments can be of the `String` or `Utf8` type and can be optional.

**Examples**
```yql
SELECT StartsWith("abc_efg", "abc") AND EndsWith("abc_efg", "efg"); -- true
```
```yql
SELECT StartsWith("abc_efg", "efg") OR EndsWith("abc_efg", "abc"); -- false
```
```yql
SELECT StartsWith("abcd", NULL); -- null
```
```yql
SELECT EndsWith(NULL, Utf8("")); -- null
```



## IF {#if}

Checks the condition: `IF(condition_expression, then_expression, else_expression)`.

It's a simplified alternative for [CASE WHEN ... THEN ... ELSE ... END](../syntax/expressions.md#case).

**Signature**
```
IF(Bool, T, T)->T
IF(Bool, T)->T?
```

You may omit the `else_expression` argument. In this case, if the condition is false (`condition_expression` returned `false`), an empty value is returned with the type corresponding to `then_expression` and allowing for `NULL`. Hence, the result will have an [optional data type](../types/optional.md).

**Examples**
```yql
SELECT
  IF(foo > 0, bar, baz) AS bar_or_baz,
  IF(foo > 0, foo) AS only_positive_foo
FROM my_table;
```



## NANVL {#nanvl}

Replaces the values `NaN` (not a number) in the expressions that have the type `Float`, `Double`, or [Optional](../types/optional.md).

**Signature**
```
NANVL(Float, Float)->Float
NANVL(Double, Double)->Double
```

Arguments:

1. The expression where you want to make a replacement.
2. The value to replace `NaN`.

If one of the arguments is `Double`, the result `isDouble`, otherwise, it's `Float`. If one of the arguments is `Optional`, then the result is `Optional`.

**Examples**
```yql
SELECT
  NANVL(double_column, 0.0)
FROM my_table;
```



## Random... {#random}

Generates a pseudorandom number:

* `Random()`: A floating point number (Double) from 0 to 1.
* `RandomNumber()`: An integer from the complete Uint64 range.
* `RandomUuid()` — [Uuid version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

**Signatures**
```
Random(T1[, T2, ...])->Double
RandomNumber(T1[, T2, ...])->Uint64
RandomUuid(T1[, T2, ...])->Uuid
```

No arguments are used for random number generation: they are only needed to control the time of the call. A new random number is returned at each call. Therefore:

* If Random is called again within a **same query** and with a same set of arguments, the same set of random numbers is returned. Keep in mind that we mean the arguments themselves (i.e., the text between parentheses) rather than their values.
* Calling of Random with the same set of arguments in **different** queries returns different sets of random numbers.

{% note warning %}

If Random is used in [named expressions](../syntax/expressions.md#named-nodes), one-time calculation isn't guaranteed for it. Depending on the optimizers and the execution environment, it can be calculated either once or many times. To guarantee one-time calculation in this case, materialize your named expression into a table.

{% endnote %}

Use cases:

* `SELECT RANDOM(1);`: Get one random value for the entire query and use it multiple times (to get multiple random values, you can pass various constants of any type).
* `SELECT RANDOM(1) FROM table;`: The same random number for each row in the table.
* `SELECT RANDOM(1), RANDOM(2) FROM table;`: Two random numbers for each row of the table, all the numbers in each of the columns are the same.
* `SELECT RANDOM(some_column) FROM table;`: Different random numbers for each row in the table.
* `SELECT RANDOM(some_column), RANDOM(some_column) FROM table;`: Different random numbers for each row of the table, but two identical numbers within the same row.
* `SELECT RANDOM(some_column), RANDOM(some_column + 1) FROM table;` or `SELECT RANDOM(some_column), RANDOM(other_column) FROM table;`: Two columns, with different numbers in both.

**Examples**
```yql
SELECT
    Random(key) -- [0, 1)
FROM my_table;
```

```yql
SELECT
    RandomNumber(key) -- [0, Max<Uint64>)
FROM my_table;
```

```yql
SELECT
    RandomUuid(key) -- Uuid version 4
FROM my_table;
```

```yql
SELECT
    RANDOM(column) AS rand1,
    RANDOM(column) AS rand2, -- same as rand1
    RANDOM(column, 1) AS randAnd1, -- different from rand1/2
    RANDOM(column, 2) AS randAnd2 -- different from randAnd1
FROM my_table;
```



## CurrentUtc... {#current-utc}

`CurrentUtcDate()`, `CurrentUtcDatetime()` and `CurrentUtcTimestamp()`: Getting the current date and/or time in UTC. The result data type is specified at the end of the function name.

**Signatures**
```
CurrentUtcDate(...)->Date
CurrentUtcDatetime(...)->Datetime
CurrentUtcTimestamp(...)->Timestamp
```

The arguments are optional and work the same as [RANDOM](#random).

**Examples**
```yql
SELECT CurrentUtcDate();
```
```yql
SELECT CurrentUtcTimestamp(TableRow()) FROM my_table;
```



## CurrentTz... {#current-tz}

`CurrentTzDate()`, `CurrentTzDatetime()`, and `CurrentTzTimestamp()`: Get the current date and/or time in the [IANA time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) specified in the first argument. The result data type is specified at the end of the function name.

**Signatures**
```
CurrentTzDate(String, ...)->TzDate
CurrentTzDatetime(String, ...)->TzDatetime
CurrentTzTimestamp(String, ...)->TzTimestamp
```

The arguments that follow are optional and work the same as [RANDOM](#random).

**Examples**
```yql
SELECT CurrentTzDate("Europe/Moscow");
```
```yql
SELECT CurrentTzTimestamp("Europe/Moscow", TableRow()) FROM my_table;
```

## AddTimezone

Adding the time zone information to the date/time in UTC. In the result of `SELECT` or after `CAST`, a `String` will be subject to the time zone rules used to calculate the time offset.

**Signature**
```
AddTimezone(Date, String)->TzDate
AddTimezone(Date?, String)->TzDate?
AddTimezone(Datetime, String)->TzDatetime
AddTimezone(Datetime?, String)->TzDatetime?
AddTimezone(Timestamp, String)->TzTimestamp
AddTimezone(Timestamp?, String)->TzTimestamp?
```

Arguments:

1. Date: the type is `Date`/`Datetime`/`Timestamp`.
2. [IANA name of the time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).

Result type: `TzDate`/`TzDatetime`/`TzTimestamp`, depending on the input data type.

**Examples**
```yql
SELECT AddTimezone(Datetime("2018-02-01T12:00:00Z"), "Europe/Moscow");
```

## RemoveTimezone

Removing the time zone data and converting the value to date/time in UTC.

**Signature**
```
RemoveTimezone(TzDate)->Date
RemoveTimezone(TzDate?)->Date?
RemoveTimezone(TzDatetime)->Datetime
RemoveTimezone(TzDatetime?)->Datetime?
RemoveTimezone(TzTimestamp)->Timestamp
RemoveTimezone(TzTimestamp?)->Timestamp?
```

Arguments:

1. Date: the type is `TzDate`/`TzDatetime`/`TzTimestamp`.

Result type: `Date`/`Datetime`/`Timestamp`, depending on the input data type.

**Examples**
```yql
SELECT RemoveTimezone(TzDatetime("2018-02-01T12:00:00,Europe/Moscow"));
```



## MAX_OF, MIN_OF, GREATEST и LEAST {#max-min}

Returns the minimum or maximum among N arguments. Those functions let you replace the SQL standard statement `CASE WHEN a < b THEN a ELSE b END` that would be too sophisticated for N more than two.

**Signatures**
```
MIN_OF(T[,T,...})->T
MAX_OF(T[,T,...})->T
```

The argument types must be mutually castable and accept `NULL`.

`GREATEST` is a synonym for `MAX_OF` and `LEAST` is a synonym for `MIN_OF`.

**Examples**
```yql
SELECT MIN_OF(1, 2, 3);
```



## AsTuple, AsStruct, AsList, AsDict, AsSet, AsListStrict, AsDictStrict and AsSetStrict {#as-container}

Creates containers of the appropriate types. [Operator writing](#containerliteral) of container literals is also available.

Features:

* Container items are passed through arguments, so the number of items in the resulting container is equal to the number of passed arguments, except when the dict keys are repeated.
* In `AsTuple` and `AsStruct`, can be invoked without arguments and arguments can have different types.
* Field names in `AsStruct` are set via `AsStruct(field_value AS field_name)`.
* You need at least one argument to create a list if you need to output item types. To create an empty list with the specified item type, use the [ListCreate](list.md#listcreate) function. You can create an empty list as an `AsList()` invocation without arguments. In this case, this expression will have the `EmptyList` type.
* You need at least one argument to create a dict if you need to output item types. To create an empty dict with the specified item type, use the [DictCreate](dict.md#dictcreate) function. You can create an empty dict as an `AsDict()` invocation without arguments. In this case, this expression will have the `EmptyDict` type.
* You need at least one argument to create a set if you need to output item types. To create an empty set with the specified item type, use the [SetCreate](dict.md#setcreate) function. You can create an empty set as an `AsSet()` invocation without arguments. In this case, this expression will have the `EmptyDict` type.
* `AsList` outputs the general type of list items. A type error is generated if there are incompatible types.
* `AsDict` outputs the general types of keys and values separately. A type error is generated if there are incompatible types.
* `AsSet` outputs the general types of keys. A type error is generated if there are incompatible types.
* `AsListStrict`, `AsDictStrict`, and `AsSetStrict` require the same type for arguments.
* In `AsDict` and `AsDictStrict`, `Tuple` of two items (key and value) is expected as arguments. If keys are repeated, only the value for the first key remains in the dict.
* In `AsSet` and `AsSetStrict`, keys are expected as arguments.

**Examples**
```yql
SELECT
  AsTuple(1, 2, "3") AS `tuple`,
  AsStruct(
    1 AS a,
    2 AS b,
    "3" AS c
  ) AS `struct`,
  AsList(1, 2, 3) AS `list`,
  AsDict(
    AsTuple("a", 1),
    AsTuple("b", 2),
    AsTuple("c", 3)
  ) AS `dict`,
  AsSet(1, 2, 3) AS `set`
```



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



## Variant, AsVariant {#variant}

`Variant()` creates a variant value over a tuple or structure.

**Signature**
```
Variant(T, String, Type<Variant<...>>)->Variant<...>
```

Arguments:

* Value
* String with a field name or tuple index
* Variant type

**Example**
```yql
$var_type = Variant<foo: Int32, bar: Bool>;

SELECT
   Variant(6, "foo", $var_type) as Variant1Value,
   Variant(false, "bar", $var_type) as Variant2Value;
```

`AsVariant()` creates a value of a [variant on a structure](../types/containers.md) with one field. This value can be implicitly converted to any variant over a structure that has a matching data type for this field name and might include more fields with other names.

**Signature**
```
AsVariant(T, String)->Variant
```

Arguments:

* Value
* A string with the field name

**Example**
```yql
SELECT
   AsVariant(6, "foo") as VariantValue
```




## Enum, AsEnum {#enum}

`Enum()` creates an enumeration value.

**Signature**
```
Enum(String, Type<Enum<...>>)->Enum<...>
```

Arguments:

* A string with the field name
* Enumeration type

**Example**
```yql
$enum_type = Enum<Foo, Bar>;
SELECT
   Enum("Foo", $enum_type) as Enum1Value,
   Enum("Bar", $enum_type) as Enum2Value;
```

`AsEnum()` creates an [enumeration](../types/containers.md) value with one element. This value can be implicitly cast to any enumeration containing such a name.

**Signature**
```
AsEnum(String)->Enum<'tag'>
```

Arguments:

* A string with the name of an enumeration item

**Example**
```yql
SELECT
   AsEnum("Foo");
```



## AsTagged, Untag {#as-tagged}

Wraps a value in [Tagged data type](../types/special.md) with the specified tag preserving the physical data type. `Untag`: Reverse operation.

**Signature**
```
AsTagged(T, tagName:String)->Tagged<T,tagName>
AsTagged(T?, tagName:String)->Tagged<T,tagName>?

Untag(Tagged<T, tagName>)->T
Untag(Tagged<T, tagName>?)->T?
```

Mandatory arguments:

1. Value of an arbitrary type.
2. Tag name.

Returns a copy of the value from the first argument with the specified tag in the data type.

Examples of use cases:

* Returning to the client to display media files of base64-encoded strings in the web interface. <!--Support for tags in the YQL web UI is [described here](../interfaces/web_tagged.md).-->
* Protecting at the boundaries of the UDF call against the transfer of incorrect values.
* Additional clarifications at the returned column type level.




## TablePath {#tablepath}

Access to the current table name, which might be needed when using [CONCAT](../syntax/select.md#concat), [RANGE](../syntax/select.md#range), and other related mechanisms.

**Signature**
```
TablePath()->String
```

No arguments. Returns a string with the full path or an empty string and warning when used in an unsupported context (for example, when working with a subquery or a range of 1000+ tables).

{% note info "Note" %}

The [TablePath](#tablepath), [TableName](#tablename), and [TableRecordIndex](#tablerecordindex) functions do no support temporary and anonymous tables (they return an empty string or 0 for [TableRecordIndex](#tablerecordindex)).
These functions are calculated when the `SELECT` projection is [executed](../syntax/select.md#selectexec), and the current table might already be temporary at that point.
To avoid such a situation, create a subquery for calculating these functions, as shown in the second example below.

{% endnote %}

**Examples**
```yql
SELECT TablePath() FROM CONCAT(table_a, table_b);
```

```yql
SELECT key, tpath_ AS path FROM (SELECT a.*, TablePath() AS tpath_ FROM RANGE(`my_folder`) AS a)
WHERE key IN $subquery;
```

## TableName {#tablename}

Get the table name based on the table path. You can obtain the path using the [TablePath](#tablepath) function or as the `Path` column when using the table function [FOLDER](../syntax/select.md#folder).

**Signature**
```
TableName()->String
TableName(String)->String
TableName(String, String)->String
```

Optional arguments:

* Path to the table, `TablePath()` is used by default (see also its limitations).
* Specifying the system ("yt") whose rules are used to determine the table name. You need to specify the system only if [USE](../syntax/select.md#use) doesn't specify the current cluster.

**Examples**
```yql
USE hahn;
SELECT TableName() FROM CONCAT(table_a, table_b);
```

```yql
SELECT TableName(Path, "yt") FROM hahn.FOLDER(folder_name);
```

## TableRecordIndex {#tablerecordindex}

Access to the current sequence number of a row in the physical source table, **starting from 1** (depends on the storage implementation).

**Signature**
```
TableRecordIndex()->Uint64
```

No arguments. When used together with [CONCAT](../syntax/select.md#concat), [RANGE](../syntax/select.md#range), and other related mechanisms, numbering is reset for each input table. If used in an incorrect context, it returns 0.

**Example**
```yql
SELECT TableRecordIndex() FROM my_table;
```




## TableRow, JoinTableRow {#tablerow}

Getting the entire table row as a structure. No arguments. `JoinTableRow` in case of `JOIN` always returns a structure with table prefixes.

**Signature**
```
TableRow()->Struct
```

**Example**
```yql
SELECT TableRow() FROM my_table;
```




## FileContent and FilePath {#file-content-path}

You can add arbitrary named files to your query both in the console and web interfaces. With these functions, you can use the name of the attached file to get its contents or the path in the sandbox, and then use it as you like in the query.

**Signatures**
```
FilePath(String)->String
FileContent(String)->String
```

The `FileContent` and `FilePath` argument is a string with an alias.

**Examples**
```yql
SELECT "Content of "
  || FilePath("my_file.txt")
  || ":\n"
  || FileContent("my_file.txt");
```
## FolderPath {#folderpath}

Getting the path to the root of a directory with several "attached" files with the common prefix specified.

**Signature**
```
FolderPath(String)->String
```

The argument is a string with a prefix among aliases.

See also [PRAGMA File](../syntax/pragma.md#file) and [PRAGMA Folder](../syntax/pragma.md#folder).

**Examples**
```yql
PRAGMA File("foo/1.txt", "http://url/to/somewhere");
PRAGMA File("foo/2.txt", "http://url/to/somewhere/else");
PRAGMA File("bar/3.txt", "http://url/to/some/other/place");

SELECT FolderPath("foo"); -- The directory at the return path will
                          -- include the files 1.txt and 2.txt downloaded by the above links
```

## ParseFile

Get a list of values from the attached text file. You can use it together with [IN](../syntax/expressions.md#in) and file attachment using URL <span style="color:gray;">(instructions on how to attach files in the web interface and in the client)</span>.

Only one file format is supported: one value per line.
<!-- For something more sophisticated, you should write a small UDF in [Python](../udf/python.md) or [JavaScript](../udf/javascript.md). -->

**Signature**
```
ParseFile(String, String)->List<T>
```

Two required arguments:

1. List cell type: only strings and numeric types are supported.
2. The name of the attached file.

{% note info "Note" %}

The return value is a lazy list. For repeat use, wrap it in the function [ListCollect](list.md#listcollect)

{% endnote %}

**Examples:**
```yql
SELECT ListLength(ParseFile("String", "my_file.txt"));
```
```yql
SELECT * FROM my_table
WHERE int_column IN ParseFile("Int64", "my_file.txt"));
```




## WeakField {#weakfield}

Fetches a table column from a strong schema, if it is in a strong schema, or from the `_other` and `_rest` fields. If the value is missing, it returns `NULL`.

Syntax: `WeakField([<table>.]<field>, <type>[, <default_value>])`.

The default value is used only if the column is missing in the data schema. To use the default value in any case, use [COALESCE](#coalesce).

**Examples:**
```yql
SELECT
    WeakField(my_column, String, "no value"),
    WeakField(my_table.other_column, Int64)
FROM my_table;
```


{% if audience == internal %}

To learn more about how to work with schematized tables, see [here](https://wiki.yandex-team.ru/logfeller/schema/yt/#osobennostirabotysosxematizirovannymitablicami)

{% endif %}



## Ensure... {#ensure}

Checking for the user conditions:

* `Ensure()`: Checking whether the predicate is true at query execution.
* `EnsureType()`: Checking that the expression type exactly matches the specified type.
* `EnsureConvertibleTo()`: A soft check of the expression type (with the same rules as for implicit type conversion).

If the check fails, the entire query fails.

**Signatures**
```
Ensure(T, Bool, String)->T
EnsureType(T, Type<T>, String)->T
EnsureConvertibleTo(T, Type<T>, String)->T
```

Arguments:

1. An expression that will result from a function call if the check is successful. It's also checked for the data type in the corresponding functions.
2. Ensure uses a Boolean predicate that is checked for being `true`. The other functions use the data type that can be obtained using the [relevant functions](types.md) or a string literal with the [text description of the type](../types/type_string.md).
3. An optional string with an error comment to be included in the overall error message when the query is complete. The data itself can't be used for type checks, since the data check is performed at query validation (or can be an arbitrary expression in the case of Ensure).

To check the conditions based on the final calculation result, it's convenient to combine Ensure with [DISCARD SELECT](../syntax/discard.md).

**Examples**
```yql
SELECT Ensure(
    value,
    value < 100,
    "value out or range"
) AS value FROM my_table;
```

```yql
SELECT EnsureType(
    value,
    TypeOf(other_value),
    "expected value and other_value to be of same type"
) AS value FROM my_table;
```

```yql
SELECT EnsureConvertibleTo(
    value,
    Double?,
    "expected value to be numeric"
) AS value FROM my_table;
```



## AssumeStrict {#assumestrict}

**Signature**
```
AssumeStrict(T)->T
```

The `AssumeStrict` function returns its argument. Using this function is a way to tell the YQL optimizer that the expression in the argument is _strict_, i.e. free of runtime errors.
Most of the built-in YQL functions and operators are strict, but there are exceptions like [Unwrap](#optional-ops) and [Ensure](#ensure).
Besides that, a non-strict expression is considered a UDF call.

If you are sure that no runtime errors actually occur when computing the expression, we recommend using `AssumeStrict`.

**Example**
```yql
SELECT * FROM T1 AS a JOIN T2 AS b USING(key)
WHERE AssumeStrict(Unwrap(CAST(a.key AS Int32))) == 1;
```

In this example, we assume that all values of the `a.key` text column in table `T1` are valid numbers, so Unwrap does not cause an error.
If there is `AssumeStrict`, the optimizer will be able to perform filtering first and then JOIN.
Without `AssumeStrict`, such optimization is not performed: the optimizer must take into account the situation when there are non-numeric values in the `a.key` column which are filtered out by `JOIN`.



## Likely {#likely}

**Signature**
```
Likely(Bool)->Bool
Likely(Bool?)->Bool?
```

The `Likely` function returns its argument. This function is a hint for the optimizer that tells it that in most cases its argument will be `True`.
For example, if such a function is used in `WHERE`, it means that the filter is weekly selective.

**Example**
```yql
SELECT * FROM T1 AS a JOIN T2 AS b USING(key)
WHERE Likely(a.amount > 0)  -- This condition is almost always true
```

When you use `Likely`, the optimizer won't attempt to perform filtering before `JOIN`.




## EvaluateExpr, EvaluateAtom {#evaluate_expr_atom}

Evaluate an expression before the start of the main calculation and input its result to the query as a literal (constant). In many contexts where the standard SQL would only expect a constant (for example in table names, number of rows in [LIMIT](../syntax/select.md#limit), and so on), this functionality is automatically activated implicitly.

EvaluateExpr can be used where the grammar already expects an expression. For example, you can use it to:

* Round the current time to days, weeks, or months and add the time to the query. This will ensure [valid query caching](../syntax/pragma.md#yt.querycachemode), although normally, when you use [current-time functions](#currentutcdate), caching is disabled completely.
* Run a heavy calculation with a small result once per query instead of once per job.

Using EvaluateAtom, you can create an [atom](../types/special.md) dynamically, but since atoms are mainly controlled from a lower [s-expressions](/docs/s_expressions/functions) level, generally, it's not recommended to use this function directly.

The only argument for both functions is the expression for calculation and substitution.

Restrictions:

* The expression must not trigger MapReduce operations.
* This functionality is fully locked in YQL over YDB.

**Examples:**
```yql
$now = CurrentUtcDate();
SELECT EvaluateExpr(
    DateTime::MakeDate(DateTime::StartOfWeek($now)
    )
);
```




## Literals of primitive types {#data-type-literals}

For primitive types, you can create literals based on string literals.

**Syntax**

`<Primitive type>( <string>[, <additional attributes>] )`

Unlike `CAST("myString" AS MyType)`:

* The check for literal's castability to the desired type occurs at validation.
* The result is non-optional.

For the `Date`, `Datetime`, `Timestamp`, and `Interval` data types, literals are supported only in the format conforming to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). `Interval` has the following differences from the standard:

* It supports the negative sign for shifts to the past.
* Microseconds can be expressed as fractional parts of seconds.
* You can't use units of measurement exceeding one week.
* The options with the beginning/end of the interval and with repetitions, are not supported.

For the `TzDate`, `TzDatetime`, `TzTimestamp` data types, literals are also set in the format conforming to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), but instead of the optional Z suffix, they specify the [IANA name of the time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones), separated by a comma (for example, GMT or Europe/Moscow).

{% include [decimal args](../_includes/decimal_args.md) %}

**Examples**
```yql
SELECT
  Bool("true"),
  Uint8("0"),
  Int32("-1"),
  Uint32("2"),
  Int64("-3"),
  Uint64("4"),
  Float("-5"),
  Double("6"),
  Decimal("1.23", 5, 2), -- до 5 десятичных знаков, из которых 2 после запятой
  String("foo"),
  Utf8("привет"),
  Yson("<a=1>[3;%false]"),
  Json(@@{"a":1,"b":null}@@),
  Date("2017-11-27"),
  Datetime("2017-11-27T13:24:00Z"),
  Timestamp("2017-11-27T13:24:00.123456Z"),
  Interval("P1DT2H3M4.567890S"),
  TzDate("2017-11-27,Europe/Moscow"),
  TzDatetime("2017-11-27T13:24:00,America/Los_Angeles"),
  TzTimestamp("2017-11-27T13:24:00.123456,GMT"),
  Uuid("f9d5cc3f-f1dc-4d9c-b97e-766e57ca4ccb");
```




## Access to the metadata of the current operation {#metadata}

When you run YQL operations via the web interface or HTTP API, you get access to the following data:

* `CurrentOperationId()`: The private ID of the operation.
* `CurrentOperationSharedId()`: The public ID of the operation.
* `CurrentAuthenticatedUser()`: The username of the current user.

**Signatures**
```
CurrentOperationId()->String
CurrentOperationSharedId()->String
CurrentAuthenticatedUser()->String
```

No arguments.

If this data is missing, for example, when you run operations in the embedded mode, the functions return an empty string.

**Examples**
```yql
SELECT
    CurrentOperationId(),
    CurrentOperationSharedId(),
    CurrentAuthenticatedUser();
```




## ToBytes и FromBytes {#to-from-bytes}

Conversion of [primitive data types](../types/primitive.md) to a string with their binary representation and back. Numbers are represented in the [little endian](https://en.wikipedia.org/wiki/Endianness#Little-endian) encoding.

**Signatures**
```
ToBytes(T)->String
ToBytes(T?)->String?

FromBytes(String, Type<T>)->T?
FromBytes(String?, Type<T>)->T?
```

**Examples**
```yql
SELECT
    ToBytes(123), -- "\u0001\u0000\u0000\u0000"
    FromBytes(
        "\xd2\x02\x96\x49\x00\x00\x00\x00",
        Uint64
    ); -- 1234567890ul
```



## ByteAt {#byteat}

Getting the byte value in the string by the index from its beginning. If the index is invalid, `NULL` is returned.

**Signature**
```
ByteAt(String, Uint32)->Uint8
ByteAt(String?, Uint32)->Uint8?

ByteAt(Utf8, Uint32)->Uint8
ByteAt(Utf8?, Uint32)->Uint8?
```

Arguments:

1. String: `String` or `Utf8`.
2. Index: `Uint32`.

**Examples**
```yql
SELECT
    ByteAt("foo", 0), -- 102
    ByteAt("foo", 1), -- 111
    ByteAt("foo", 9); -- NULL
```



## ...Bit {#bitops}

`TestBit()`, `ClearBit()`, `SetBit()`, and `FlipBit()`: Test, clear, set, or flip a bit in an unsigned number by the specified bit sequence number.

**Signatures**
```
TestBit(T, Uint8)->Bool
TestBit(T?, Uint8)->Bool?

ClearBit(T, Uint8)->T
ClearBit(T?, Uint8)->T?

SetBit(T, Uint8)->T
SetBit(T?, Uint8)->T?

FlipBit(T, Uint8)->T
FlipBit(T?, Uint8)->T?
```

Arguments:

1. An unsigned number to perform the desired operation on. TestBit is also implemented for strings.
2. Bit number.

TestBit returns `true/false`. Other functions return a copy of their first argument with the corresponding transformation performed.

**Examples:**
```yql
SELECT
    TestBit(1u, 0), -- true
    SetBit(8u, 0); -- 9
```



## Abs {#abs}

Absolute value of the number.

**Signature**
```
Abs(T)->T
Abs(T?)->T?
```

**Examples**
```yql
SELECT Abs(-123); -- 123
```



## Just, Unwrap, Nothing {#optional-ops}

`Just()`: Change the data type of the value to an [optional](../types/optional.md) type derived from the current data type (for example, `T` becomes `T?`).

**Signature**
```
Just(T)->T?
```

**Examples**
```yql
SELECT
  Just("my_string"); --  String?
```

`Unwrap()`: Converting the [optional](../types/optional.md) value of the data type to the corresponding non-optional type, raising a runtime error if the data is `NULL`. This means that `T?` becomes `T`.

If the value is not [optional](../types/optional.md), the function returns its first argument without change.

**Signature**
```
Unwrap(T?)->T
Unwrap(T?, Utf8)->T
Unwrap(T?, String)->T
```

Arguments:

1. Value to be converted.
2. An optional string with a comment for the error text.

The reverse operation is [Just](#just).

**Examples**
```yql
$value = Just("value");

SELECT Unwrap($value, "Unexpected NULL for $value");
```

`Nothing()`: Create an empty value for the specified [Optional](../types/optional.md) data type.

**Signature**
```
Nothing(Type<T?>)->T?
```

**Examples**
```yql
SELECT
  Nothing(String?); -- A NULL value with the String? type
```

[Learn more about ParseType and other functions for data types](types.md).



## Callable {#callable}

Create a callable value with the given signature from a lambda function. Usually used to place callable values in containers.

**Signature**
```
Callable(Type<Callable<(...)->T>>, lambda)->Callable<(...)->T>
```

Arguments:

1. Type.
2. Lambda function.

**Examples:**
```yql
$lambda = ($x) -> {
    RETURN CAST($x as String)
};

$callables = AsTuple(
    Callable(Callable<(Int32)->String>, $lambda),
    Callable(Callable<(Bool)->String>, $lambda),
);

SELECT $callables.0(10), $callables.1(true);
```



## Pickle, Unpickle {#pickle}

`Pickle()` and `StablePickle()` serialize an arbitrary object into a sequence of bytes, if possible. Typical non-serializable objects are Callable and Resource. The serialization format is not versioned and can be used within a single query. For the Dict type, the StablePickle function pre-sorts the keys, and for Pickle, the order of dictionary elements in the serialized representation isn't defined.

`Unpickle()` is the inverse operation (deserialization), where with the first argument being the data type of the result and the second argument is the string with the result of `Pickle()` or `StablePickle()`.

**Signatures**
```
Pickle(T)->String
StablePickle(T)->String
Unpickle(Type<T>, String)->T
```

Examples:
```yql
SELECT *
FROM my_table
WHERE Digest::MurMurHash32(
        Pickle(TableRow())
    ) % 10 == 0; -- from practical viewpoint, it's better to use TABLESAMPLE

$buf = Pickle(123);
SELECT Unpickle(Int32, $buf);
```



## StaticMap

Transforms a structure or tuple by applying a lambda function to each item.

**Signature**
```
StaticMap(Struct<...>, lambda)->Struct<...>
StaticMap(Tuple<...>, lambda)->Tuple<...>
```

Arguments:

* Structure or tuple.
* Lambda for processing items.

Result: a structure or tuple with the same number and naming of items as in the first argument, and with item data types determined by lambda results.

**Examples:**
```yql
SELECT *
FROM (
    SELECT
        StaticMap(TableRow(), ($item) -> {
            return CAST($item AS String);
        })
    FROM my_table
) FLATTEN COLUMNS; -- преобразование всех колонок в строки
```




## StaticZip

Merges structures or tuples element-by-element. All arguments (one or more) must be either structures with the same set of fields or tuples of the same length.
The result will be a structure or tuple, respectively.
Each item of the result is a tuple comprised of items taken from arguments.

**Signature**
```
StaticZip(Struct, Struct)->Struct
StaticZip(Tuple, Tuple)->Tuple
```

**Examples:**
```yql
$one = <|k1:1, k2:2.0|>;
$two = <|k1:3.0, k2:4|>;

-- поэлементное сложение двух структур
SELECT StaticMap(StaticZip($one, $two), ($tuple)->($tuple.0 + $tuple.1)) AS sum;
```




## AggregationFactory {#aggregationfactory}

Create the [aggregate function](aggregation.md) factory to separate the process of describing how to aggregate data and which data to apply it to.

Arguments:

1. A string in quotation marks that is the name of an aggregate function, for example, ["MIN"](aggregation.md#min).
2. Optional aggregate function parameters that do not depend on the data. For example, the percentile value in [PERCENTILE](aggregation.md#percentile).

The resulting factory can be used as a second parameter of the [AGGREGATE_BY](aggregation.md#aggregateby) function.
If the aggregate function works on two columns instead of one, for example, [MIN_BY](aggregation.md#minby), `Tuple` of two values is passed as a first argument in [AGGREGATE_BY](aggregation.md#aggregateby). For more information, see the description of this aggregate function.

**Examples:**
```yql
$factory = AggregationFactory("MIN");
SELECT
    AGGREGATE_BY(value, $factory) AS min_value -- apply MIN aggregation to value column
FROM my_table;
```

## AggregateTransform... {#aggregatetransform}

`AggregateTransformInput()` transforms the [aggregate function](aggregation.md) factory, for example, the one obtained via the [AggregationFactory](#aggregationfactory) function, into another factory in which the specified transformation of input items is performed before the aggregation starts.

Arguments:

1. Aggregate function factory.
2. Lambda function with one argument which transforms an input item.

**Examples:**
```yql
$f = AggregationFactory("sum");
$g = AggregateTransformInput($f, ($x) -> (cast($x as Int32)));
$h = AggregateTransformInput($f, ($x) -> ($x * 2));
select ListAggregate([1,2,3], $f); -- 6
select ListAggregate(["1","2","3"], $g); -- 6
select ListAggregate([1,2,3], $h); -- 12
```

`AggregateTransformOutput()` transforms the [aggregate function](aggregation.md) factory, for example, the one obtained via the [AggregationFactory](#aggregationfactory) function, into another factory in which the specified transformation of the result is performed after the aggregation is completed.

Arguments:

1. Aggregate function factory.
2. Lambda function with one argument which transforms the result.

**Examples:**
```yql
$f = AggregationFactory("sum");
$g = AggregateTransformOutput($f, ($x) -> ($x * 2));
select ListAggregate([1,2,3], $f); -- 6
select ListAggregate([1,2,3], $g); -- 12
```

## AggregateFlatten {#aggregateflatten}

Adapts the [aggregate function](aggregation.md) factory, for example, the one obtained via the [AggregationFactory](#aggregationfactory) function, so that it performs aggregation on input items — lists. This operation is similar to [FLATTEN LIST BY](../syntax/flatten.md): each item in the list is aggregated.

Arguments:

1. Aggregate function factory.

**Examples:**
```yql
$i = AggregationFactory("AGGREGATE_LIST_DISTINCT");
$j = AggregateFlatten($i);
select AggregateBy(x, $j) from (
   select [1,2] as x
   union all
   select [2,3] as x
); -- [1, 2, 3]

```


