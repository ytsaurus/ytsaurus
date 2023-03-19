---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/struct.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/struct.md
---
# Functions for working with structures

## TryMember {#trymember}

An attempt to get the value of a field from the structure and if it is not among the fields or null in the structure value, use the default value.
The `default_value` type must match the `key` field type from the structure.

**Signature**
```
TryMember(struct:Struct<...>, key:String, default_value:T) -> T
TryMember(struct:Struct<...>?, key:String, default_value:T) -> T?
```

Arguments:

1. struct: Original structure.
2. key: Field name.
3. default_value: Default value if the field is missing

{% note info "Limitation" %}

The field name (key) cannot depend on lambda data or arguments. In this case, the TryMember function cannot be prototyped.

{% endnote %}

**Examples**
```yql
$struct = <|a:1|>;
SELECT
  TryMember(
    $struct,
    "a",
    123
  ) AS a, -- 1
  TryMember(
    $struct,
    "b",
    123
  ) AS b; -- 123
```

## ExpandStruct {#expandstruct}

Adding one or more new fields to the structure. A new extended structure returns.  If there are duplicates in the set of fields, an error will be returned.

**Signature**
```
ExpandStruct(struct:Struct<...>, value_1:T1 AS key_1:K, value_2:T2 AS key_2:K, ....) -> Struct<...>
```

Arguments:

* The source structure for the extension is passed to the first argument.
* All other arguments must be named, each argument adds a new field, and the argument name is used as the field name (similar to [AsStruct](../basic.md#asstruct)).

**Examples**
```yql
$struct = <|a:1|>;
SELECT
  ExpandStruct(
    $struct,
    2 AS b,
    "3" AS c
  ) AS abc;  -- ("a": 1, "b": 2, "c": "3")
```

## AddMember {#addmember}

Adding one new field to the structure. If you need to add multiple fields, better use [ExpandStruct](#expandstruct).

If there are duplicates in the set of fields, an error will be returned.

**Signature**
```
AddMember(struct:Struct<...>, new_key:String, new_value:T) -> Struct<...>
```

Arguments:

1. struct: Original structure.
2. new_key: Name of the new field.
3. new_value: Value of the new field.

**Examples**
```yql
$struct = <|a:1|>;
SELECT
  AddMember(
    $struct,
    "b",
    2
  ) AS ab; -- ("a": 1, "b": 2)
```

## RemoveMember {#removemember}

Removing a field from the structure. If the specified field did not exist, an error will be returned.

**Signature**
```
RemoveMember(struct:Struct<...>, key_to_delete:String) -> Struct<...>
```

Arguments:

1. Original structure.
2. Name of the field to be removed

**Examples**
```yql
$struct = <|a:1, b:2|>;
SELECT
  RemoveMember(
    $struct,
    "b"
  ) AS a; -- ("a": 1)
```

## ForceRemoveMember {#forceremovemember}

Removing a field from the structure.

If the specified field did not exist, an error will not be returned unlike [RemoveMember](#removemember).

**Signature**
```
ForceRemoveMember(struct:Struct<...>, key_to_delete:String) -> Struct<...>
```

Arguments:

1. Original structure.
2. Name of the field to be removed.

**Examples**
```yql
$struct = <|a:1, b:2|>;
SELECT
  ForceRemoveMember(
    $struct,
    "c"
  ) AS ab; -- ("a": 1, "b": 2)
```

## ChooseMembers {#choosemembers}

Selecting fields with given names from the structure. The new structure is returned only from the specified fields.

If any of the fields did not exist, an error will be returned.

**Signature**
```
ChooseMembers(struct:Struct<...>, list_of_keys:List<String>) -> Struct<...>
```

Arguments:

1. Original structure.
2. List of field names.

**Examples**
```yql
$struct = <|a:1, b:2, c:3|>;
SELECT
  ChooseMembers(
    $struct,
    ["a", "b"]
  ) AS ab; -- ("a": 1, "b": 2)
```

## RemoveMembers {#removemembers}

Removing fields with given names from the structure.

If any of the fields did not exist, an error will be returned.

**Signature**
```
RemoveMembers(struct:Struct<...>, list_of_delete_keys:List<String>) -> Struct<...>
```

Arguments:

1. Original structure.
2. List of field names.

**Examples**
```yql
$struct = <|a:1, b:2, c:3|>;
SELECT
  RemoveMembers(
    $struct,
    ["a", "b"]
  ) AS c; -- ("c": 3)
```

## ForceRemoveMembers {#forceremovemembers}

Removing fields with given names from the structure.

If any of the fields did not exist, it is ignored.

**Signature**
```
ForceRemoveMembers(struct:Struct<...>, list_of_delete_keys:List<String>) -> Struct<...>
```

Arguments:

1. Original structure.
2. List of field names.

**Examples**
```yql
$struct = <|a:1, b:2, c:3|>;
SELECT
  ForceRemoveMembers(
    $struct,
    ["a", "b", "z"]
  ) AS c; -- ("c": 3)
```

## CombineMembers {#combinemembers}

Combining fields of several structures into a new structure.

If there are duplicates in the resulting set of fields, an error will be returned.

**Signature**
```
CombineMembers(struct1:Struct<...>, struct2:Struct<...>, .....) -> Struct<...>
CombineMembers(struct1:Struct<...>?, struct2:Struct<...>?, .....) -> Struct<...>
```

Arguments: two or more structures.

**Examples**
```yql
$struct1 = <|a:1, b:2|>;
$struct2 = <|c:3|>;
SELECT
  CombineMembers(
    $struct1,
    $struct2
  ) AS abc; -- ("a": 1, "b": 2, "c": 3)
```

## FlattenMembers {#flattenmembers}

Combining the fields of several new structures into a new structure with prefix support.

If there are duplicates in the resulting set of fields, an error will be returned.

**Signature**
```
FlattenMembers(prefix_struct1:Tuple<String, Struct<...>>, prefix_struct2:Tuple<String, Struct<...>>, ...) -> Struct<...>
```

Arguments: two or more tuples from two elements: prefix and structure.

**Examples**
```yql
$struct1 = <|a:1, b:2|>;
$struct2 = <|c:3|>;
SELECT
  FlattenMembers(
    AsTuple("foo", $struct1), -- fooa, foob
    AsTuple("bar", $struct2)  -- barc
  ) AS abc; -- ("barc": 3, "fooa": 1, "foob": 2)
```

## StructMembers {#structmembers}

Returns an unordered list of field names (possibly removing one optionality level) for a single argument — the structure. An empty list of strings is returned for a `NULL` argument.

**Signature**
```
StructMembers(struct:Struct<...>) -> List<String>
StructMembers(struct:Struct<...>?) -> List<String>
StructMembers(NULL) -> []
```

Argument: structure

**Examples**
```yql
$struct = <|a:1, b:2|>;
SELECT
  StructMembers($struct) AS a, -- ['a', 'b']
  StructMembers(NULL) AS b; -- []
```

## RenameMembers {#renamemembers}

Renames fields in the passed structure. The original field can be renamed into several new ones. All fields not mentioned in the process of renaming as source fields are transferred to the resulting structure. If an original field is not in the list for renaming, an error is displayed. For an optional structure or `NULL`, so is the result.

**Signature**
```
RenameMembers(struct:Struct<...>, rename_rules:List<Tuple<String, String>>) -> Struct<...>
```

Arguments:

1. Original structure.
2. A list of field names in the form of a tuple list: original name, new name.

**Examples**
```yql
$struct = <|a:1, b:2|>;
SELECT
  RenameMembers($struct, [('a', 'c'), ('a', 'e')]); -- (b:2, c:1, e:1)
```

## ForceRenameMembers {#forecerenamemembers}

Renames fields in the passed structure. The original field can be renamed into several new ones. All fields not mentioned in the process of renaming as source fields are transferred to the resulting structure. If an original field is not in the list for renaming, it is ignored. For an optional structure or `NULL`, so is the result.

**Signature**
```
ForceRenameMembers(struct:Struct<...>, rename_rules:List<Tuple<String, String>>) -> Struct<...>
```

Arguments:

1. Original structure.
2. A list of field names in the form of a tuple list: original name, new name.

**Examples**
```yql
$struct = <|a:1, b:2|>;
SELECT
  ForceRenameMembers($struct, [('a', 'c'), ('d', 'e')]); -- (b:2, c:1)
```

## GatherMembers {#gathermembers}

Returns an unordered tuple list from the field name and value. For the `NULL` argument, `EmptyList` is returned. Can be used only when the item types in the structure are identical or compatible. For an optional structure, an optional list is returned.

**Signature**
```
GatherMembers(struct:Struct<...>) -> List<Tuple<String,V>>
GatherMembers(struct:Struct<...>?) -> List<Tuple<String,V>>?
GatherMembers(NULL) -> []
```

Argument: structure

**Examples**
```yql
$struct = <|a:1, b:2|>;
SELECT
  GatherMembers($struct), -- [('a', 1), ('b', 2)]
  GatherMembers(null); -- []
```

## SpreadMembers {#spreadmembers}

Creates a structure with a given list of fields and applies a given list of corrections in the (field name, value) format to it. All field types of the resulting structure are the same and equal to the type of values in the list of corrections with the added optionality (if they weren't already). If the field was not mentioned among the list of editable fields, it is returned as `NULL`. Among all the corrections for one field, the last one is saved. If there is a field in the list of corrections that is not in the list of expected fields, an error is displayed.

**Signature**
```
SpreadMembers(list_of_tuples:List<Tuple<String, T>>, result_keys:List<String>) -> Struct<...>
```

Arguments:

1. Tuple list: field name, field value.
2. A list of all possible field names in the structure.

**Examples**
```yql
SELECT
  SpreadMembers([('a',1),('a',2)],['a','b']); -- (a: 2, b: null)
```

## ForceSpreadMembers {#forcespreadmembers}

Creates a structure with a given list of fields and applies a given list of corrections in the (field name, value) format to it. All field types of the resulting structure are the same and equal to the type of values in the list of corrections with the added optionality (if they weren't already). If the field was not mentioned among the list of editable fields, it is returned as `NULL`. Among all the corrections for one field, the last one is saved. If there is a field in the list of corrections that is not in the list of expected fields, this correction is ignored.

**Signature**
```
ForceSpreadMembers(list_of_tuples:List<Tuple<String, T>>, result_keys:List<String>) -> Struct<...>
```

Arguments:

1. Tuple list: field name, field value.
2. A list of all possible field names in the structure.

**Examples**
```yql
SELECT
  ForceSpreadMembers([('a',1),('a',2),('c',100)],['a','b']); -- (a: 2, b: null)
```
