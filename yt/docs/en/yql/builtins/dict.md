
# Functions for working with dicts

## DictCreate {#dictcreate}

#### Signature

```yql
DictCreate(K,V)->Dict<K,V>
```

Construct an empty dict. Two arguments are passed — for the key and the value. In each of them, a string describing the data type or the type itself obtained using the [functions intended for it](types.md) are specified. There are no dicts with an unknown key or value type in YQL.

A key type can be:

* A [primitive data type](../types/primitive.md) (except for `Yson` and `Json`).
* A primitive data type (except for `Yson` and `Json`) with an optionality sign.
* A tuple with the length of at least two of the types listed above.

[Type description format documentation](../types/type_string.md).

#### Examples

```yql
SELECT DictCreate(String, Tuple<String,Double?>);
```

```yql
SELECT DictCreate(Tuple<Int32?,String>, OptionalType(DataType("String")));
```

```yql
SELECT DictCreate(ParseType("Tuple<Int32?,String>"), ParseType("Tuple<String,Double?>"));
```

## SetCreate {#setcreate}

#### Signature

```yql
SetCreate(T)->Set<T>
```

Construct an empty set. The argument is passed: a key type probably obtained using the [functions intended for it](types.md). There are no sets with an unknown key type in YQL. Restrictions on the key type are the same as on the key type for the dict. Note that a set is a dict with the `Void` value type. A set can also be created using the `DictCreate` function. This also means that all functions that take `Dict<K,V>` as input can also take `Set<K>` as input.

[Type description format documentation](../types/type_string.md).

#### Examples

```yql
SELECT SetCreate(String);
```

```yql
SELECT SetCreate(Tuple<Int32?,String>);
```

## DictLength {#dictlength}

#### Signature

```yql
DictLength(Dict<K,V>)->Uint64
DictLength(Dict<K,V>?)->Uint64?
```

Number of items in the dict.

#### Examples

```yql
SELECT DictLength(AsDict(AsTuple(1, AsList("foo", "bar"))));
```

```yql
SELECT DictLength(dict_column) FROM my_table;
```

## DictHasItems {#dicthasitems}

#### Signature

```yql
DictHasItems(Dict<K,V>)->Bool
DictHasItems(Dict<K,V>?)->Bool?
```

Checking that the dict contains at least one item.

#### Examples

```yql
SELECT DictHasItems(AsDict(AsTuple(1, AsList("foo", "bar")))) FROM my_table;
```

```yql
SELECT DictHasItems(dict_column) FROM my_table;
```

## DictItems {#dictitems}

#### Signature

```yql
DictItems(Dict<K,V>)->List<Tuple<K,V>>
DictItems(Dict<K,V>?)->List<Tuple<K,V>>?
```

Getting the contents of the dict as a list of tuples with key-value pairs (`List<Tuple<key_type,value_type>>`).

#### Examples

```yql
SELECT DictItems(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ ( 1, [ "foo", "bar" ] ) ]
```

```yql
SELECT DictItems(dict_column)
FROM my_table;
```

## DictKeys {#dictkeys}

#### Signature

```yql
DictKeys(Dict<K,V>)->List<K>
DictKeys(Dict<K,V>?)->List<K>?
```

Getting a list of dict keys.

#### Examples

```yql
SELECT DictKeys(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ 1 ]
```

```yql
SELECT DictKeys(dict_column)
FROM my_table;
```

## DictPayloads {#dictpayloads}

#### Signature

```yql
DictPayloads(Dict<K,V>)->List<V>
DictPayloads(Dict<K,V>?)->List<V>?
```

Getting a list of dict values.

#### Examples

```yql
SELECT DictPayloads(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ [ "foo", "bar" ] ]
```

```yql
SELECT DictPayloads(dict_column)
FROM my_table;
```

## DictLookup {#dictlookup}

#### Signature

```yql
DictLookup(Dict<K,V>, K)->V?
DictLookup(Dict<K,V>?, K)->V?
DictLookup(Dict<K,V>, K?)->V?
DictLookup(Dict<K,V>?, K?)->V?
```

Getting a dict item by key.

#### Examples

```yql
SELECT DictLookup(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 1);
-- [ "foo", "bar" ]
```

```yql
SELECT DictLookup(dict_column, "foo")
FROM my_table;
```

## DictContains {#dictcontains}

#### Signature

```yql
DictContains(Dict<K,V>, K)->Bool
DictContains(Dict<K,V>?, K)->Bool
DictContains(Dict<K,V>, K?)->Bool
DictContains(Dict<K,V>?, K?)->Bool
```

Checking presence of an item in the dict by key. Returns true or false.

#### Examples

```yql
SELECT DictContains(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 42);
-- false
```

```yql
SELECT DictContains(dict_column, "foo")
FROM my_table;
```

## DictAggregate {#dictaggregate}

#### Signature

```yql
DictAggregate(Dict<K,List<V>>, List<V>->T)->Dict<K,T>
DictAggregate(Dict<K,List<V>>?, List<V>->T)->Dict<K,T>?
```

Apply the [aggregate function factory](basic.md#aggregationfactory) for the passed dict where each value is a list. The factory is applied individually within each key.
If the list is empty, the aggregation result will be the same as for an empty table: 0 for the `COUNT` function and `NULL` for other functions.
If the list in the passed dict is empty by a key, then such a key is removed from the result.
If the passed dict is optional and contains the `NULL` value, the result will also be `NULL`.

Arguments:

1. Dict.
2. [Aggregate function factory](basic.md#aggregationfactory).


#### Examples

```yql
SELECT DictAggregate(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("baz", "qwe"))),
    AggregationFactory("Max"));
-- {1 : "foo", 2 : "qwe" }
```

## SetIsDisjoint {#setisjoint}

#### Signature

```yql
SetIsDisjoint(Dict<K,V1>, Dict<K,V2>)->Bool
SetIsDisjoint(Dict<K,V1>?, Dict<K,V2>)->Bool?
SetIsDisjoint(Dict<K,V1>, Dict<K,V2>?)->Bool?
SetIsDisjoint(Dict<K,V1>?, Dict<K,V2>?)->Bool?

SetIsDisjoint(Dict<K,V1>, List<K>)->Bool
SetIsDisjoint(Dict<K,V1>?, List<K>)->Bool?
SetIsDisjoint(Dict<K,V1>, List<K>?)->Bool?
SetIsDisjoint(Dict<K,V1>?, List<K>?)->Bool?
```

Checking that the dict and the list or other dict do not intersect by keys.

Thus, there are two invocation variants:

* With the `Dict<K,V1>` and `List<K>` arguments.
* With the `Dict<K,V1>` and `Dict<K,V2>` arguments.

#### Examples

```yql
SELECT SetIsDisjoint(ToSet(AsList(1, 2, 3)), AsList(7, 4)); -- true
SELECT SetIsDisjoint(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- false
```

## SetIntersection {#setintersection}

#### Signature

```yql
SetIntersection(Dict<K,V1>, Dict<K,V2>)->Set<K>
SetIntersection(Dict<K,V1>?, Dict<K,V2>)->Set<K>?
SetIntersection(Dict<K,V1>, Dict<K,V2>?)->Set<K>?
SetIntersection(Dict<K,V1>?, Dict<K,V2>?)->Set<K>?

SetIntersection(Dict<K,V1>, Dict<K,V2>, (K,V1,V2)->U)->Dict<K,U>
SetIntersection(Dict<K,V1>?, Dict<K,V2>, (K,V1,V2)->U)->Dict<K,U>?
SetIntersection(Dict<K,V1>, Dict<K,V2>?, (K,V1,V2)->U)->Dict<K,U>?
SetIntersection(Dict<K,V1>?, Dict<K,V2>?, (K,V1,V2)->U)->Dict<K,U>?
```

Builds an intersection of two dicts by keys.

Arguments:

* Two dicts: `Dict<K,V1>` and `Dict<K,V2>`.
* An optional function that combines values from the source dicts to build values of the output dict. If the type of this function is `(K,V1,V2) -> U`, the result type is `Dict<K,U>`. If the function is not set, the result type is `Dict<K,Void>` and values from the source dicts are ignored.

#### Examples

```yql
SELECT SetIntersection(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 3 }
SELECT SetIntersection(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 1 : ("foo", "baz") }
```

{% note info %}

The [lambda function](../syntax/expressions.md#lambda) was used in the example.

{% endnote %}

## SetIncludes {#setincludes}

#### Signature

```yql
SetIncludes(Dict<K,V1>, List<K>)->Bool
SetIncludes(Dict<K,V1>?, List<K>)->Bool?
SetIncludes(Dict<K,V1>, List<K>?)->Bool?
SetIncludes(Dict<K,V1>?, List<K>?)->Bool?

SetIncludes(Dict<K,V1>, Dict<K,V2>)->Bool
SetIncludes(Dict<K,V1>?, Dict<K,V2>)->Bool?
SetIncludes(Dict<K,V1>, Dict<K,V2>?)->Bool?
SetIncludes(Dict<K,V1>?, Dict<K,V2>?)->Bool?
```

Checking that the keys of the given dict include all the list items or the keys of the second dict.

Thus, there are two invocation variants:

* With the `Dict<K,V1>` and `List<K>` arguments.
* With the `Dict<K,V1>` and `Dict<K,V2>` arguments.

#### Examples

```yql
SELECT SetIncludes(ToSet(AsList(1, 2, 3)), AsList(3, 4)); -- false
SELECT SetIncludes(ToSet(AsList(1, 2, 3)), ToSet(AsList(2, 3))); -- true
```

## SetUnion {#setunion}

#### Signature

```yql
SetUnion(Dict<K,V1>, Dict<K,V2>)->Set<K>
SetUnion(Dict<K,V1>?, Dict<K,V2>)->Set<K>?
SetUnion(Dict<K,V1>, Dict<K,V2>?)->Set<K>?
SetUnion(Dict<K,V1>?, Dict<K,V2>?)->Set<K>?

SetUnion(Dict<K,V1>, Dict<K,V2>,(K,V1?,V2?)->U)->Dict<K,U>
SetUnion(Dict<K,V1>?, Dict<K,V2>,(K,V1?,V2?)->U)->Dict<K,U>?
SetUnion(Dict<K,V1>, Dict<K,V2>?,(K,V1?,V2?)->U)->Dict<K,U>?
SetUnion(Dict<K,V1>?, Dict<K,V2>?,(K,V1?,V2?)->U)->Dict<K,U>?
```

Builds a union of two dicts by keys.

Arguments:

* Two dicts: `Dict<K,V1>` and `Dict<K,V2>`.
* An optional function that combines values from the source dicts to build values of the output dict. If the type of this function is `(K,V1?,V2?) -> U`, the result type is `Dict<K,U>`. If the function is not set, the result type is `Dict<K,Void>` and values from the source dicts are ignored.

#### Examples

```yql
SELECT SetUnion(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2, 3, 4 }
SELECT SetUnion(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 1 : ("foo", "baz"), 2 : (null, "qwe"), 3 : ("bar", null) }
```

## SetDifference {#setdifference}

#### Signature

```yql
SetDifference(Dict<K,V1>, Dict<K,V2>)->Dict<K,V1>
SetDifference(Dict<K,V1>?, Dict<K,V2>)->Dict<K,V1>?
SetDifference(Dict<K,V1>, Dict<K,V2>?)->Dict<K,V1>?
SetDifference(Dict<K,V1>?, Dict<K,V2>?)->Dict<K,V1>?
```

Builds a dict which has all keys with corresponding values of the first dict for which there is no key in the second dict.

#### Examples

```yql
SELECT SetDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2 }
SELECT SetDifference(
    AsDict(AsTuple(1, "foo"), AsTuple(2, "bar")),
    ToSet(AsList(2, 3)));
-- { 1 : "foo" }
```

## SetSymmetricDifference {#setsymmetricdifference}

#### Signature

```yql
SetSymmetricDifference(Dict<K,V1>, Dict<K,V2>)->Set<K>
SetSymmetricDifference(Dict<K,V1>?, Dict<K,V2>)->Set<K>?
SetSymmetricDifference(Dict<K,V1>, Dict<K,V2>?)->Set<K>?
SetSymmetricDifference(Dict<K,V1>?, Dict<K,V2>?)->Set<K>?

SetSymmetricDifference(Dict<K,V1>, Dict<K,V2>,(K,V1?,V2?)->U)->Dict<K,U>
SetSymmetricDifference(Dict<K,V1>?, Dict<K,V2>,(K,V1?,V2?)->U)->Dict<K,U>?
SetSymmetricDifference(Dict<K,V1>, Dict<K,V2>?,(K,V1?,V2?)->U)->Dict<K,U>?
SetSymmetricDifference(Dict<K,V1>?, Dict<K,V2>?,(K,V1?,V2?)->U)->Dict<K,U>?
```

Builds a symmetric difference of two dicts by keys.

Arguments:

* Two dicts: `Dict<K,V1>` and `Dict<K,V2>`.
* An optional function that combines values from the source dicts to build values of the output dict. If the type of this function is `(K,V1?,V2?) -> U`, the result type is `Dict<K,U>`. If the function is not set, the result type is `Dict<K,Void>` and values from the source dicts are ignored.

#### Examples

```yql
SELECT SetSymmetricDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2, 4 }
SELECT SetSymmetricDifference(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 2 : (null, "qwe"), 3 : ("bar", null) }
```

## DictInsert {#dictinsert}

#### Signature

```yql
DictInsert(Dict<K,V>,K,V)->Dict<K,V>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Returns a new dict with the specified key and value added to it. If this key already existed, the dict remains unchanged.
When working with a `Set`, pass the `Void()` function as the value.

#### Examples

```yql
SELECT DictInsert({'foo':1}, 'bar', 2); -- {'foo':1,'bar':2}
SELECT DictInsert({'foo':1}, 'foo', 2); -- {'foo':1}
SELECT DictInsert({'foo'}, 'bar', Void()); -- {'foo','bar'}
```

## DictUpsert {#dictupsert}

#### Signature

```yql
DictUpsert(Dict<K,V>,K,V)->Dict<K,V>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Returns a new dict with the specified key and value either replaced or added to it. If this key already existed, the value is updated.

#### Examples

```yql
SELECT DictUpsert({'foo':1}, 'bar', 2); -- {'foo':1,'bar':2}
SELECT DictUpsert({'foo':1}, 'foo', 2); -- {'foo':2}
```

## DictUpdate {#dictupdate}

#### Signature

```yql
DictUpdate(Dict<K,V>,K,V)->Dict<K,V>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Returns a new dict with the value for the specified key replaced. If this key did not exist, the dict remains unchanged.

#### Examples

```yql
SELECT DictUpdate({'foo':1}, 'bar', 2); -- {'foo':1}
SELECT DictUpdate({'foo':1}, 'foo', 2); -- {'foo':2}
```

## DictRemove {#dictremove}

#### Signature

```yql
DictRemove(Dict<K,V>,K)->Dict<K,V>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Returns a new dict without the specified key. If this key did not exist, the dict remains unchanged.

#### Examples

```yql
SELECT DictRemove({'foo':1}, 'bar'); -- {'foo':1}
SELECT DictRemove({'foo':1}, 'foo'); -- {}
```

## ToMutDict {#tomutdict}

#### Signature

```yql
ToMutDict(Dict<K,V>,dependArg1...)->Linear<mutDictType for Dict<K,V>>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Converts a dict into its mutable version. You also need to pass one or more dependent expressions, for example, using the `lambda` argument in the [`Block`](basic.md#block) function.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        return FromMutDict($dict);
    }); -- {'foo':1}
```

## MutDictCreate {#mutdictcreate}

#### Signature

```yql
MutDictCreate(KeyType,ValueType,dependArg1...)->Linear<mutDictType for Dict<K,V>>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Constructs an empty mutable dict with the specified key and value types. You also need to pass one or more dependent expressions, for example, using the `lambda` argument in the [`Block`](basic.md#block) function.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        return FromMutDict($dict);
    }); -- {}
```

## FromMutDict {#frommutdict}

#### Signature

```yql
FromMutDict(Linear<mutDictType for Dict<K,V>>)->Dict<K,V>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Absorbs a mutable dict and converts it into an immutable one.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        return FromMutDict($dict);
    }); -- {'foo':1}
```

## MutDictInsert {#mutdictinsert}

#### Signature

```yql
MutDictInsert(Linear<mutDictType for Dict<K,V>>,K,V)->Linear<mutDictType for Dict<K,V>>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Adds the specified key and value to a mutable dict and returns this mutable dict. If this key already existed in the dict, the dict remains unchanged.
When working with a `Set`, pass the `Void()` function as the value.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictInsert($dict,'foo',2);
        return FromMutDict($dict);
    }); -- {'foo':1}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictInsert($dict,'bar',2);
        return FromMutDict($dict);
    }); -- {'foo':1,'bar':2}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo'}, $arg);
        $dict = MutDictInsert($dict,'bar', Void());
        return FromMutDict($dict);
    }); -- {'foo','bar'}
```

## MutDictUpsert {#mutdictupsert}

#### Signature

```yql
MutDictUpsert(Linear<mutDictType for Dict<K,V>>,K,V)->Linear<mutDictType for Dict<K,V>>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Adds the specified key and value to a mutable dict or replaces them and returns this mutable dict. If this key already existed in the dict, the value is updated.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpsert($dict,'foo',2);
        return FromMutDict($dict);
    }); -- {'foo':2}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpsert($dict,'bar',2);
        return FromMutDict($dict);
    }); -- {'foo':1,'bar':2}
```

## MutDictUpdate {#mutdictupdate}

#### Signature

```yql
MutDictUpdate(Linear<mutDictType for Dict<K,V>>,K,V)->Linear<mutDictType for Dict<K,V>>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Replaces a value for the specified key in a mutable dict and returns this mutable dict. If this key did not exist in the dict, the dict remains unchanged.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpdate($dict,'foo',2);
        return FromMutDict($dict);
    }); -- {'foo':2}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpdate($dict,'bar',2);
        return FromMutDict($dict);
    }); -- {'foo':1}
```

## MutDictRemove {#mutdictremove}

#### Signature

```yql
MutDictRemove(Linear<mutDictType for Dict<K,V>>,K)->Linear<mutDictType for Dict<K,V>>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Removes a value for the specified key from a mutable dict and returns this mutable dict. If this key did not exist in the dict, the dict remains unchanged.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictRemove($dict,'foo');
        return FromMutDict($dict);
    }); -- {}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictRemove($dict,'bar');
        return FromMutDict($dict);
    }); -- {'foo':1}
```


## MutDictPop {#mutdictpop}

#### Signature

```yql
MutDictPop(Linear<mutDictType for Dict<K,V>>,K)->Tuple<Linear<mutDictType for Dict<K,V>>,V?>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Removes a value for the specified key from a mutable dict and returns this mutable dict and the value for the removed key. If this key did not exist in the dict, the dict remains unchanged and an empty Optional is returned.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictPop($dict,'foo');
        return (FromMutDict($dict), $val);
    }); -- ({},1)

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictPop($dict,'bar');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},null)
```

## MutDictContains {#mutdictcontains}

#### Signature

```yql
MutDictContains(Linear<mutDictType for Dict<K,V>>,K)->Tuple<Linear<mutDictType for Dict<K,V>>,Bool>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Checks if the specified key exists in a mutable dict and returns this mutable dict along with the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictContains($dict,'foo');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},True)

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictContains($dict,'bar');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},False)
```

## MutDictLookup {#mutdictlookup}

#### Signature

```yql
MutDictLookup(Linear<mutDictType for Dict<K,V>>,K)->Tuple<Linear<mutDictType for Dict<K,V>>,V?>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Gets a value for the specified key in a mutable dict and returns this mutable dict and the optional result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictLookup($dict,'foo');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},1)

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictLookup($dict,'bar');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},null)
```

## MutDictHasItems {#mutdicthasitems}

#### Signature

```yql
MutDictHasItems(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,Bool>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Checks if a mutable dict is empty and returns this mutable dict and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictHasItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},True)

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictHasItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},False)
```

## MutDictLength {#mutdictlength}

#### Signature

```yql
MutDictLength(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,Uint64>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Gets the number of items in a mutable dict and returns this mutable dict and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictLength($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},1)

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictLength($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},0)
```

## MutDictKeys {#mutdictkeys}

#### Signature

```yql
MutDictKeys(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,List<K>>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Gets a list of keys in a mutable dict and returns this mutable dict and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictKeys($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},['foo'])

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictKeys($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},[])
```

## MutDictPayloads {#mutdictpayloads}

#### Signature

```yql
MutDictPayloads(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,List<V>>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Gets a list of values in a mutable dict and returns this mutable dict and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictPayloads($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},['1'])

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictPayloads($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},[])
```

## MutDictItems {#mutdictitems}

#### Signature

```yql
MutDictItems(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,List<Tuple<K,V>>>
```

Introduced in version [2025.04](../changelog/2025.04.md).
Gets a list of tuples with key-value pairs in a mutable dict and returns this mutable dict and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},[('foo',1)])

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},[])
```
