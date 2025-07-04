# Functions for lists

## ListCreate {#list-create}

Construct an empty list. The only argument specifies a string describing the data type of the list cell, or the type itself obtained using [relevant functions](types.md). YQL doesn't support lists with an unknown cell type.

[Type description format documentation](../types/type_string.md).

#### Examples

```yql
SELECT ListCreate(Tuple<String,Double?>);
```

```yql
SELECT ListCreate(OptionalType(DataType("String")));
```

#### Signature

```yql
ListCreate(T)->List<T>
```

## asList and AsListStrict {#aslist}

Construct a list based on one or more arguments. The argument types must be compatible in the case of `AsList` and strictly match in the case of `AsListStrict`.

#### Examples

```yql
SELECT AsList(1, 2, 3, 4, 5);
```

#### Signature

```yql
AsList(T..)->List<T>
```

## ListLength {#listlength}

The count of items in the list.

#### Examples

```yql
SELECT ListLength(list_column) FROM my_table;
```

#### Signature

```yql
ListLength(List<T>)->Uint64
ListLength(List<T>?)->Uint64?
```

## ListHasItems

Check that the list contains at least one item.

#### Examples

```yql
SELECT ListHasItems(list_column) FROM my_table;
```

#### Signature

```yql
ListHasItems(List<T>)->Bool
ListHasItems(List<T>?)->Bool?
```

## ListCollect {#listcollect}

Convert a lazy list (which can be built by such functions as  [ListFilter](#listmap), [ListMap](#listmap), [ListFlatMap](#listmap)) to an eager list. In contrast to a lazy list, where each new pass re-calculates the list contents, in an eager list the content is built at once by consuming more memory.

#### Examples

```yql
SELECT ListCollect(list_column) FROM my_table;
```

#### Signature

```yql
ListCollect(LazyList<T>)->List<T>
ListCollect(LazyList<T>?)->List<T>?
```

## ListSort, ListSortAsc и ListSortDesc {#listsort}

Sort the list. By default, the ascending sorting order is applied (`ListSort` is an alias for `ListSortAsc`).

Arguments:

1. List.
2. An optional expression to get the sort key from a list element (it's the element itself by default).

#### Examples

```yql
SELECT ListSortDesc(list_column) FROM my_table;
```

```yql
$list = AsList(
    AsTuple("x", 3),
    AsTuple("xx", 1),
    AsTuple("a", 2)
);

SELECT ListSort($list, ($x) -> {
    RETURN $x.1;
});
```

{% note info %}

The [lambda function](../syntax/expressions.md#lambda) was used in the example.

{% endnote %}

#### Signature

```yql
ListSort(List<T>)->List<T>
ListSort(List<T>?)->List<T>?

ListSort(List<T>, (T)->U)->List<T>
ListSort(List<T>?, (T)->U)->List<T>?
```

## ListExtend and ListExtendStrict {#listextend}

Sequentially join lists (concatenation of lists). The arguments can be lists, optional lists, and `NULL`.
The types of list elements must be compatible in the case of `ListExtend` and strictly match in the case of `ListExtendStrict`.
If at least one of the lists is optional, then the result is also optional.
If at least one argument is `NULL`, then the result type is `NULL`.

#### Examples

```yql
SELECT ListExtend(
    list_column_1,
    list_column_2,
    list_column_3
) FROM my_table;
```

```yql
$l1 = AsList("a", "b");
$l2 = AsList("b", "c");
$l3 = AsList("d", "e");

SELECT ListExtend($l1, $l2, $l3);  -- ["a","b","b","c","d","e"]
```

#### Signature

```yql
ListExtend(List<T>..)->List<T>
ListExtend(List<T>?..)->List<T>?
```

## ListUnionAll {#listunionall}

Sequentially join lists of structures (concatenation of lists). A field is added to the output list of structures if it exists in at least one source list, but if there is no such field in any list, it is added as NULL. In the case when a field is present in two or more lists, the output field is cast to the common type.

If at least one of the lists is optional, then the result is also optional.

#### Examples

```yql
SELECT ListUnionAll(
    list_column_1,
    list_column_2,
    list_column_3
) FROM my_table;
```

```yql
$l1 = AsList(
    <|value:1|>,
    <|value:2|>
);
$l2 = AsList(
    <|key:"a"|>,
    <|key:"b"|>
);
SELECT ListUnionAll($l1, $l2);  -- result: [("value":1),("value":2),("key":"a"),("key":"b")]
                                -- schema: List<Struct<key : String?, value : Int32?>>
```

#### Signature

```yql
ListUnionAll(List<Struct<..>>, List<Struct<..>>..)->List<Struct<..>>
ListUnionAll(List<Struct<..>>?, List<Struct<..>>?..)->List<Struct<..>>?
```

## ListZip and ListZipAll {#listzip}

Based on the input lists, build a list of pairs containing the list elements with corresponding indexes (`List<Tuple<first_list_element_type,second_list_element_type>>`).

The length of the returned list is determined by the shortest list for ListZip and the longest list for ListZipAll.
When the shorter list is exhausted, a `NULL` value of a relevant [optional type](../types/optional.md).

#### Examples

```yql
SELECT
    ListZip(list_column_1, list_column_2, list_column_3),
    ListZipAll(list_column_1, list_column_2)
FROM my_table;
```

```yql
$l1 = AsList("a", "b");
$l2 = AsList(1, 2, 3);

SELECT ListZip($l1, $l2);  -- [("a",1),("b",2)]
SELECT ListZipAll($l1, $l2);  -- [("a",1),("b",2),(null,3)]
```

#### Signature

```yql
ListZip(List<T1>, List<T2>)->List<Tuple<T1, T2>>
ListZip(List<T1>?, List<T2>?)->List<Tuple<T1, T2>>?

ListZipAll(List<T1>, List<T2>)->List<Tuple<T1?, T2?>>
ListZipAll(List<T1>?, List<T2>?)->List<Tuple<T1?, T2?>>?
```

## ListEnumerate {#listenumerate}

Build a list of pairs (Tuple) containing the element number and the element itself (`List<Tuple<Uint64,list_element_type>>`).

#### Examples

```yql
SELECT ListEnumerate(list_column) FROM my_table;
```

#### Signature

```yql
ListEnumerate(List<T>)->List<Tuple<Uint64, T>>
ListEnumerate(List<T>?)->List<Tuple<Uint64, T>>?
```

## ListReverse {#listreverse}

Reverse the list.

#### Examples

```yql
SELECT ListReverse(list_column) FROM my_table;
```

#### Signature

```yql
ListReverse(List<T>)->List<T>
ListReverse(List<T>?)->List<T>?
```

## ListSkip {#listskip}

Returns a copy of the list, skipping the specified number of its first elements.

The first argument specifies the source list and the second argument specifies how many elements to skip.

#### Examples

```yql
SELECT
    ListSkip(list_column, 3)
FROM my_table;
```

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListSkip($l1, 2);  -- [3,4,5]
```

#### Signature

```yql
ListSkip(List<T>, Uint64)->List<T>
ListSkip(List<T>?, Uint64)->List<T>?
```

## ListTake {#listtake}

Returns a copy of the list containing a limited number of elements from the second list.

The first argument specifies the source list and the second argument specifies the maximum number of elements to be taken from the beginning of the list.

#### Examples

```yql
SELECT ListTake(list_column, 3) FROM my_table;
```

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListTake($l1, 2);  -- [1,2]
```

#### Signature

```yql
ListTake(List<T>, Uint64)->List<T>
ListTake(List<T>?, Uint64)->List<T>?
```

## ListSample and ListSampleN {#listsample}

Returns a sample of elements from the list without duplicates.

- `ListSample` selects each element independently with a specified probability.

- `ListSampleN` selects a fixed number of elements (if the list is smaller than the sample size, the original list is returned).

If the probability or sample size is NULL, the original list is returned.

An additional argument is used to control randomness. For more information, see the [documentation for `Random`](basic.md#random).

#### Examples

```yql
$list = AsList(1, 2, 3, 4, 5);

SELECT ListSample($list, 0.5);  -- [1, 2, 5]
SELECT ListSampleN($list, 2);  -- [4, 2]
```

#### Signature

```yql
ListSample(List<T>, Double?[, U])->List<T>
ListSample(List<T>?, Double?[, U])->List<T>?

ListSampleN(List<T>, Uint64?[, U])->List<T>
ListSampleN(List<T>?, Uint64?[, U])->List<T>?
```

## ListShuffle {#listshuffle}

Returns a copy of the list with the elements rearranged in random order. An additional argument is used to control randomness. For more information, see the [documentation for `Random`](basic.md#random).

#### Examples

```yql
$list = AsList(1, 2, 3, 4, 5);

SELECT ListShuffle($list);  -- [1, 3, 5, 2, 4]
```

#### Signature

```yql
ListShuffle(List<T>[, U])->List<T>
ListShuffle(List<T>?[, U])->List<T>?
```

## ListIndexOf {#listindexof}

Searches the list for an element with the specified value and returns its index at the first occurrence. Indexes count from 0. If such element is missing, it returns `NULL`.

#### Examples

```yql
SELECT
    ListIndexOf(list_column, 123)
FROM my_table;
```

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListIndexOf($l1, 2);  -- 1
```

#### Signature

```yql
ListIndexOf(List<T>, T)->Uint64?
ListIndexOf(List<T>?, T)->Uint64?
```

## ListMap, ListFlatMap and ListFilter {#listmap}

Apply the function specified as the second argument to each list element. The functions differ in their returned result:

* `ListMap` returns a list with results.
* `ListFlatMap` returns a list with results, combining and expanding the first level of results (lists or optional values) for each item.
* `ListFilter` leaves only those elements where the function returned `true`.

{% note info %}

In `ListFlatMap`, optional values in function results are deprecated, use the combination of  [`ListNotNull`](#listnotnull) and `ListMap` instead.

{% endnote %}

Arguments:

1. Source list.
2. Functions for processing list elements, such as:

    * [Lambda function](../syntax/expressions.md#lambda).
    * `Module::Function` - С++ UDF;

If the source list is optional, then the output list is also optional.

#### Examples

```yql
SELECT
    ListMap(list_column, ($x) -> { RETURN $x > 2; }),
    ListFlatMap(list_column, My::Udf)
FROM my_table;
```

```yql
$list = AsList("a", "b", "c");

$filter = ($x) -> {
    RETURN $x == "b";
};

SELECT ListFilter($list, $filter);  -- ["b"]
```

```yql
$list = AsList(1,2,3,4);
$callable = Python::test(Callable<(Int64)->Bool>, "def test(i): return i % 2");
SELECT ListFilter($list, $callable);  -- [1,3]
```

#### Signature

```yql
ListMap(List<T>, (T)->U)->List<U>
ListMap(List<T>?, (T)->U)->List<U>?

ListFlatMap(List<T>, (T)->List<U>)->List<U>
ListFlatMap(List<T>?, (T)->List<U>)->List<U>?
ListFlatMap(List<T>, (T)->U?)->List<U>
ListFlatMap(List<T>?, (T)->U?)->List<U>?

ListFilter(List<T>, (T)->Bool)->List<T>
ListFilter(List<T>?, (T)->Bool)->List<T>?
```

## ListNotNull {#listnotnull}

Applies transformation to the source list, skipping empty optional items and strengthening the item type to non-optional. For a list with non-optional items, it returns the unchanged source list.

If the source list is optional, then the output list is also optional.

#### Examples

```yql
SELECT ListNotNull([1,2]),   -- [1,2]
    ListNotNull([3,null,4]); -- [3,4]
```

#### Signature

```yql
ListNotNull(List<T?>)->List<T>
ListNotNull(List<T?>?)->List<T>?
```

## ListFlatten {#listflatten}

Expands the list of lists into a flat list, preserving the order of items. As the top-level list item, you can use an optional list that is interpreted as an empty list in the case of `NULL`.

If the source list is optional, then the output list is also optional.

#### Examples

```yql
SELECT ListFlatten([[1,2],[3,4]]),   -- [1,2,3,4]
    ListFlatten([null,[3,4],[5,6]]); -- [3,4,5,6]
```

#### Signature

```yql
ListFlatten(List<List<T>?>)->List<T>
ListFlatten(List<List<T>?>?)->List<T>?
```

## ListUniq and ListUniqStable {#listuniq}

Returns a copy of the list containing only distinct elements. For ListUniq, the order of elements in the resulting list is undefined. For ListUniqStable, elements appear in the same order as in the source list.

#### Examples

```yql
SELECT ListUniq([1, 2, 3, 2, 4, 5, 1]) -- [5, 4, 2, 1, 3]
SELECT ListUniqStable([1, 2, 3, 2, 4, 5, 1]) -- [1, 2, 3, 4, 5]
SELECT ListUniqStable([1, 2, null, 7, 2, 8, null]) -- [1, 2, null, 7, 8]
```

#### Signature

```yql
ListUniq(List<T>)->List<T>
ListUniq(List<T>?)->List<T>?

ListUniqStable(List<T>)->List<T>
ListUniqStable(List<T>?)->List<T>?
```

## ListAny and ListAll {#listany}

Returns `true` for a list of Boolean values if:

* `ListAny`: At least one element is `true`.
* `ListAll`: All elements are `true`.

Otherwise, it returns `false`.

Behavior with empty lists:

* `ListAny` returns `false`.
* `ListAll` returns `true`.

#### Examples

```yql
SELECT
    ListAll(bool_column),
    ListAny(bool_column)
FROM my_table;
```

#### Signature

```yql
ListAny(List<Bool>)->Bool
ListAny(List<Bool>?)->Bool?
ListAll(List<Bool>)->Bool
ListAll(List<Bool>?)->Bool?
```

## ListHas {#listhas}

Show whether the list contains the specified element. In this case, `NULL` values are considered equal to each other. If the input list is `NULL`, the result is always `false`.

#### Examples

```yql
SELECT
    ListHas(list_column, "my_needle")
FROM my_table;
```

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListHas($l1, 2);  -- true
SELECT ListHas($l1, 6);  -- false
```

#### Signature

```yql
ListHas(List<T>, U)->Bool
ListHas(List<T>?, U)->Bool
```

## ListHead, ListLast {#listheadlast}

Returns the first and last item of the list.

#### Examples

```yql
SELECT
    ListHead(numeric_list_column) AS head,
    ListLast(numeric_list_column) AS last
FROM my_table;
```

#### Signature

```yql
ListHead(List<T>)->T?
ListHead(List<T>?)->T?
ListLast(List<T>)->T?
ListLast(List<T>?)->T?
```

## ListMin, ListMax, ListSum and ListAvg {#listminy}

Apply the appropriate aggregate function to all elements of the numeric list.

#### Examples

```yql
SELECT
    ListMax(numeric_list_column) AS max,
    ListMin(numeric_list_column) AS min,
    ListSum(numeric_list_column) AS sum,
    ListAvg(numeric_list_column) AS avg
FROM my_table;
```

#### Signature

```yql
ListMin(List<T>)->T?
ListMin(List<T>?)->T?
```

## ListFold, ListFold1 {#listfold}

List folding.

Arguments:

1. List
2. Initial value U for ListFold, initLambda(item:T)->U for ListFold1
3. updateLambda(item:T, state:U)->U

Return type:
U for ListFold, optional U for ListFold1.

#### Examples

```yql
$l = [1, 4, 7, 2];
$y = ($x, $y) -> { RETURN $x + $y; };
$z = ($x) -> { RETURN 4 * $x; };

SELECT
    ListFold($l, 6, $y) AS fold,                       -- 20
    ListFold([], 3, $y) AS fold_empty,                 -- 3
    ListFold1($l, $z, $y) AS fold1,                    -- 17
    ListFold1([], $z, $y) AS fold1_empty;              -- Null
```

#### Signature

```yql
ListFold(List<T>, U, (T, U)->U)->U
ListFold(List<T>?, U, (T, U)->U)->U?

ListFold1(List<T>, (T)->U, (T, U)->U)->U?
ListFold1(List<T>?, (T)->U, (T, U)->U)->U?
```

## ListFoldMap, ListFold1Map {#listfoldmap}

Converts each element i in the list by calling handler(i, state).

Arguments:

1. List
2. Initial state S for ListFoldMap, initLambda(item:T)->tuple (U S) for ListFold1Map
3. handler(item:T, state:S)->tuple (U S)

Return type:
List of elements U.

#### Examples

```yql
$l = [1, 4, 7, 2];
$x = ($i, $s) -> { RETURN ($i * $s, $i + $s); };
$t = ($i) -> { RETURN ($i + 1, $i + 2); };

SELECT
    ListFoldMap([], 1, $x),                -- []
    ListFoldMap($l, 1, $x),                -- [1, 8, 42, 26]
    ListFold1Map([], $t, $x),              -- []
    ListFold1Map($l, $t, $x);              -- [2, 12, 49, 28]
```

#### Signature

```yql
ListFoldMap(List<T>, S, (T, S)->Tuple<U,S>)->List<U>
ListFoldMap(List<T>?, S, (T, S)->Tuple<U,S>)->List<U>?

ListFold1Map(List<T>, (T)->Tuple<U,S>, (T, S)->Tuple<U,S>)->List<U>
ListFold1Map(List<T>?, (T)->Tuple<U,S>, (T, S)->Tuple<U,S>)->List<U>?
```

## ListFromRange {#listfromrange}

Generate a sequence of numbers or dates with the specified step. It's similar to `xrange` in Python 2, but additionally supports dates and floating points.

Arguments:

1. Start
2. End
3. Step. Optional, 1 by default for numeric sequences, 1 day for `Date`/`TzDate`, 1 second for `Datetime`/`TzDatetime`, and 1 microsecond`Timestamp`/`TzTimestamp`/`Interval`

Features:

* *The end is not included, i.e. `ListFromRange(1,3) == AsList(1,2)`.
*  The type for the resulting elements is selected as the broadest from the argument types. For example, `ListFromRange(1, 2, 0.5)` results in a `Double` list.
* If start and end have one of the date types, the step must have the `Interval` type.
* The list is "lazy", but if it's used incorrectly, it can still consume a lot of RAM.
* If the step is positive and the end is less than or equal to the start, the result list is empty.
* If the step is negative and the end is greater than or equal to the start, the result list is empty.
* If the step is neither positive nor negative (0 or NaN), the result list is empty.
* If one of the parameters is optional, the result will be an optional list.
* If one of the parameters is `NULL`, the result is `NULL`.

#### Examples

```yql
SELECT
    ListFromRange(-2, 2), -- [-2, -1, 0, 1]
    ListFromRange(2, 1, -0.5); -- [2.0, 1.5]
```

```yql
SELECT ListFromRange(Datetime("2022-05-23T15:30:00Z"), Datetime("2022-05-30T15:30:00Z"), DateTime::IntervalFromDays(1));
```

#### Signature

```yql
ListFromRange(T{Flags:AutoMap}, T{Flags:AutoMap}, T?)->LazyList<T> -- T is a numeric type
ListFromRange(T{Flags:AutoMap}, T{Flags:AutoMap}, I?)->LazyList<T> -- T is a date/time type; I is an interval
```

## ListReplicate {#listreplicate}

Creates a list containing multiple copies of the specified value.

Mandatory arguments:

1. Value.
2. Number of copies.

#### Examples

```yql
SELECT ListReplicate(true, 3); -- [true, true, true]
```

#### Signature

```yql
ListReplicate(T, Uint64)->List<T>
```

## ListConcat {#listconcat}

Concatenates a list of strings into a single string.
You can set a separator as the second parameter.

#### Examples

```yql
SELECT
    ListConcat(string_list_column),
    ListConcat(string_list_column, "; ")
FROM my_table;
```

```yql
$l1 = AsList("h", "e", "l", "l", "o");

SELECT ListConcat($l1);  -- "hello"
SELECT ListConcat($l1, " ");  -- "h e l l o"
```

#### Signature

```yql
ListConcat(List<String>)->String?
ListConcat(List<String>?)->String?

ListConcat(List<String>, String)->String?
ListConcat(List<String>?, String)->String?
```

## ListExtract {#listextract}

For a list of structures, it returns a list of contained fields having the specified name.

#### Examples

```yql
SELECT
    ListExtract(struct_list_column, "MyMember")
FROM my_table;
```

```yql
$l = AsList(
    <|key:"a", value:1|>,
    <|key:"b", value:2|>
);
SELECT ListExtract($l, "key");  -- ["a", "b"]
```

#### Signature

```yql
ListExtract(List<Struct<..>>, String)->List<T>
ListExtract(List<Struct<..>>?, String)->List<T>?
```

## ListTakeWhile, ListSkipWhile {#listtakewhile}

`ListTakeWhile` returns a list from the beginning while the predicate is true, then the list ends.

`ListSkipWhile` skips the list segment from the beginning while the predicate is true, then returns the rest of the list disregarding the predicate.
`ListTakeWhileInclusive` returns a list from the beginning while the predicate is true. Then the list ends, but it also includes the item on which the stopping predicate triggered.
`ListSkipWhileInclusive` skips a list segment from the beginning while the predicate is true, then returns the rest of the list disregarding the predicate, but excluding the element that matched the predicate and starting with the next element after it.

Mandatory arguments:

1. List.
2. Predicate.

If the input list is optional, then the result is also optional.

#### Examples

```yql
$data = AsList(1, 2, 5, 1, 2, 7);

SELECT
    ListTakeWhile($data, ($x) -> {return $x <= 3}), -- [1, 2]
    ListSkipWhile($data, ($x) -> {return $x <= 3}), -- [5, 1, 2, 7]
    ListTakeWhileInclusive($data, ($x) -> {return $x <= 3}), -- [1, 2, 5]
    ListSkipWhileInclusive($data, ($x) -> {return $x <= 3}); -- [1, 2, 7]
```

#### Signature

```yql
ListTakeWhile(List<T>, (T)->Bool)->List<T>
ListTakeWhile(List<T>?, (T)->Bool)->List<T>?
```

## ListAggregate {#listaggregate}

Apply the [aggregation factory](basic.md#aggregationfactory) to the passed list.
If the passed list is empty, the aggregation result is the same as for an empty table: 0 for the `COUNT` function and `NULL` for other functions.
If the passed list is optional and `NULL`, the result is also `NULL`.

Arguments:

1. List.
2. [Aggregate function factory](basic.md#aggregationfactory).

#### Examples

```yql
SELECT ListAggregate(AsList(1, 2, 3), AggregationFactory("Sum")); -- 6
```

#### Signature

```yql
ListAggregate(List<T>, AggregationFactory)->T
ListAggregate(List<T>?, AggregationFactory)->T?
```

## ToDict and ToMultiDict {#todict}

Convert a list of tuples containing key-value pairs to a dictionary. If there are conflicting keys in the input list, `ToDict` leaves the first value and `ToMultiDict` builds a list of all the values.

It means that:

* `ToDict` converts `List<Tuple<K, V>>` to `Dict<K, V>`
* `ToMultiDict` converts `List<Tuple<K, V>>` to `Dict<K, List<V>>`

Optional lists are also supported, resulting in an optional dictionary.

#### Examples

```yql
SELECT
    ToDict(tuple_list_column)
FROM my_table;
```

```yql
$l = AsList(("a",1), ("b", 2), ("a", 3));
SELECT ToDict($l);  -- {"a": 1,"b": 2}
```

#### Signature

```yql
ToDict(List<Tuple<K,V>>)->Dict<K,V>
ToDict(List<Tuple<K,V>>?)->Dict<K,V>?
```

## ToSet {#toset}

Converts a list to a dictionary where the keys are unique elements of this list, and values are omitted and have the type `Void`. For the `List<T>` list, the result type is `Dict<T, Void>`.
An optional list is also supported, resulting in an optional dictionary.

Inverse function: get a list of keys for the [DictKeys](dict.md#dictkeys) dictionary.

#### Examples

```yql
SELECT
    ToSet(list_column)
FROM my_table;
```

```yql
$l = AsList(1,1,2,2,3);
SELECT ToSet($l);  -- {1,2,3}
```

#### Signature

```yql
ToSet(List<T>)->Set<T>
ToSet(List<T>?)->Set<T>?
```

## ListFromTuple

Creates a list from a tuple with compatible element types. For an optional tuple, returns an optional list. For a NULL argument, returns NULL. For an empty tuple, returns EmptyList.

#### Examples

```yql
$t = (1,2,3);
SELECT ListFromTuple($t);  -- [1,2,3]
```

#### Signature

```yql
ListFromTuple(Null)->Null
ListFromTuple(Tuple<>)->EmptyList
ListFromTuple(Tuple<T1,T2,...>)->List<T>
ListFromTuple(Tuple<T1,T2,...>?)->List<T>?
```

## ListToTuple

Creates a tuple from a list and an explicitly specified tuple width. All elements in the resulting tuple will have the same type as the list elements. If the list length doesn't match the specified tuple width, returns an error. For an optional list, returns an optional tuple. For a NULL argument, returns NULL.

#### Examples

```yql
$l = [1,2,3];
SELECT ListToTuple($l, 3);  -- (1,2,3)
```

#### Signature

```yql
ListToTuple(Null,N)->Null
ListToTuple(EmptyList,N)->()) -- N must be 0
ListToTuple(List<T>, N)->Tuple<T,T,...T> -- tuple width is N
ListToTuple(List<T>?, N)->Tuple<T,T,...T>? -- tuple width is N
```

## ListTop, ListTopAsc, ListTopDesc, ListTopSort, ListTopSortAsc, and ListTopSortDesc {#listtop}

Select the top values from the list. `ListTopSort*` additionally sorts the list of the returned values. By default, the lowest values are selected. Functions without a suffix are aliases for `*Asc` functions. `*Desc` functions select the highest values.

`ListTopSort` is more efficient than calling `ListTop` followed by `ListSort`, because `ListTop` may partially pre-sort the list when looking for the desired values. However, `ListTop` is more efficient than `ListTopSort` if you don't need to sort the output.

Arguments:

1. List.
2. Sample size.
3. An optional expression to get the sort key from a list element (it's the element itself by default).

#### Signature

```yql
ListTop(List<T>{Flags:AutoMap}, N)->List<T>
ListTop(List<T>{Flags:AutoMap}, N, (T)->U)->List<T>
```

The signatures of other functions match those of `ListTop`.
