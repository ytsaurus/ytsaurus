# Custom aggregate functions in YQL

## Introduction
If the [built-in aggregate functions](../builtins/aggregation.md) do not meet your needs for whatever reason, you can define your own function in YQL. This mechanism is referred to as `UDAF` (User Defined Aggregation Functions) and works the same as standard aggregate functions.

Here are the peculiarities/benefits of this approach compared to Map/Reduce ([PROCESS](../syntax/process.md) + [REDUCE](../syntax/reduce.md) or working with {{product-name}} directly without YQL):

1. You can easily apply the same logic to several fields or expressions within a single computing run (that is, we calculate different non-related aggregates, reading the data only once). Moreover, you can automatically skip Null values.
2. Support for `DISTINCT`, that is, calling a function only by unique values.
3. Automatic creation of a map-side combiner. Often this allows you to substantially save on the query execution time without having to implement a map-side combiner yourself.

{% note info %}

If you have built or want to build an aggregate function which, in your opinion, could be useful to many users, write to us {% if audience == "internal" %}at {{yql.ml}} {% else %} in the [Telegram chat]({{community-tg}}){% endif %}. We'll consider adding it to standard functions.

{% endnote %}

## How it works
UDAFs operate with the following abstractions:

* `Item`: Aggregated value.
* `State`: Aggregation state.
* `Result`: Aggregation result.
* `Serialized` <span class="gray;">(optional)</span>: A serialized representation of State.
* `Parent` <span class="gray;">(optional)</span>: A node that should be used as a parent node if the handler is written in lambda. This is needed when the state includes mutable resources.

In each specific case, there should be an arbitrary [data type](../types/primitive.md) behind each of these abstractions.
However, the type corresponding to each abstraction should be the same for different functions. If one function uses `String` as `Item` and another function uses `Int64` as Item, you will get a type mismatch error.

{% if audience == "inetrnal"%}
**[Example in Tutorial]({{yql.link}}/Tutorial/yt_22_User_Defined_Aggregation_Functions)**.
{% endif %}

UDAF calling procedure in high-level terms:

1. To perform each aggregation step, the user provides from three to seven simple functions (Callable values, to be precise) that operate with the above abstractions. The main methods to get them is by using [lambda](../syntax/expressions.md#lambda){% if audience == "internal" %}, [JavaScript UDF](../udf/javascript.md){% endif %} or [Python UDF](../udf/python.md). Main options:

  * 3: Minimal.
  * 4: If State isn't a result.
  * 6: If State is non-serializable.
  * 7: If you need to override the default `NULL` value.
2. The user calls a special aggregate `UDAF` function. To this function, as the first argument, the user passes a column or an aggregated expression. Other arguments include callable values. The order of callable values is described in the table below.
3. Benefits.

List of callable values. Required values are highlighted in bold.

| No. | Name | Description | Signature |
| --- | --- | --- | --- |
| 1 | **Create** | Initializing State by the first non-Null Item. | `(Item)->State`. Input arguments for lambda (Item, Parent) |
| 2 | **Add** | Adding the next Item to State. | `(State,Item)->State`. Input arguments for lambda (State, Item, Parent) |
| 3 | **Merge** | Merging of two States. | `(State,State)->State` |
| 4 | Get Result | Getting the aggregation result from the final State.<br>If omitted, the final State is used as a result. | `(State)->Result` |
| 5 | Serialize | Serialization of State, if it isn't serializable by itself.<br>Any combination of basic types and containers is serializable. That's why this case applies only when you use a Resource as a State. | `(State)->Serialized` |
| 6 | Deserialize | Recovering a State after serialization. | `(Serialized)->State` |
| 7 | Default Value | The result in the situation when the aggregate function hasn't been called (zero rows were input) The default value is `NULL`. | `()->Result` or `(TypeOf(Result))->Result` |

They are passed one-by-one as individual arguments after the aggregated value. The aggregate `UDAF` function can have between 4 and 8 arguments.

If several values in the query are aggregated in the same manner using `UDAF`, then, to avoid enumerating the callable values every time, use the [aggregate function factory](../builtins/basic.md#aggregationfactory). In this case, "UDAF" is passed instead of a data column, and the call is executed using [AGGREGATE_BY](../builtins/aggregation.md#aggregateby).

### Examples
```yql
-- Emulate the aggregate COUNT function
$create = ($item, $parent) -> { return 1ul };
$add = ($state, $item, $parent) -> { return 1ul + $state };
$merge = ($state1, $state2) -> { return $state1 + $state2 };
$get_result = ($state) -> { return $state };
$serialize = ($state) -> { return $state };
$deserialize = ($state) -> { return $state };
$default = 0ul;

$udaf_factory = AggregationFactory(
    "UDAF",
    $create,
    $add,
    $merge,
    $get_result,
    $serialize,
    $deserialize,
    $default
);

SELECT
    AGGREGATE_BY(value1, $udaf_factory) AS cnt1,
    AGGREGATE_BY(value2, $udaf_factory) AS cnt2 -- Count non-NULL values
                                                -- in the value1 and value2 columns.
FROM my_table;
```

