
## AsTuple, AsStruct, AsList, AsDict, AsSet, AsListStrict, AsDictStrict and AsSetStrict {#as-container}

Creates containers of the appropriate types. [Operator writing](#containerliteral) of container literals is also available.

Features:

* Container items are passed through arguments, so the number of items in the resulting container is equal to the number of passed arguments, except when the dict keys are repeated.
* In `AsTuple` and `AsStruct`, can be invoked without arguments and arguments can have different types.
* Field names in `AsStruct` are set via `AsStruct(field_value AS field_name)`.
* You need at least one argument to create a list if you need to output item types. To create an empty list with the specified item type, use the [ListCreate](../../list.md#listcreate) function. You can create an empty list as an `AsList()` invocation without arguments. In this case, this expression will have the `EmptyList` type.
* You need at least one argument to create a dict if you need to output item types. To create an empty dict with the specified item type, use the [DictCreate](../../dict.md#dictcreate) function. You can create an empty dict as an `AsDict()` invocation without arguments. In this case, this expression will have the `EmptyDict` type.
* You need at least one argument to create a set if you need to output item types. To create an empty set with the specified item type, use the [SetCreate](../../dict.md#setcreate) function. You can create an empty set as an `AsSet()` invocation without arguments. In this case, this expression will have the `EmptyDict` type.
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
