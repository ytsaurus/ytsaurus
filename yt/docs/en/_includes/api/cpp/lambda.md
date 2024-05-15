# Lambda expressions

In some {{product-name}} operations, calculations are so simple that creating a class for each of them using the [C++ API](../../../api/cpp/description.md) feels excessive. In this case, you can use [lambda expressions](https://en.wikipedia.org/wiki/Anonymous_function) to describe operations.

{% note warning %}

Such lambda expressions currently can't capture variables, meaning they must have an empty capture block. This makes them equivalent to C functions. This is because generic lambda are problematic to serialize correctly in modern C++. To pass parameters to a lambda, you can sometimes use `static const` and [TSaveable](#saveable) global variables.

{% endnote %}

{% note warning %}

This only works from programs built statically for Linux, so you can't use settings like `JobBinaryLocalPath`.

{% endnote %}

## Using the yt_lambda library

{% if audience == 'internal' %}Connect `PEERDIR(yt/cpp/mapreduce/library/lambda)` in ya.make{% else %}Connect the [yt_lambda](https://github.com/ytsaurus/ytsaurus/blob/main/yt/cpp/mapreduce/library/lambda/yt_lambda.h)library{% endif %} and include the following header in the program:
```
#include <yt/cpp/mapreduce/library/lambda/yt_lambda.h>
```

[Initialize {{product-name}}](../../../api/cpp/description.md#init) and get the `client`. Pass the `client` or [transaction](../../../api/cpp/description.md#transactions) as the first parameter to the functions described in this document.

This library's functions have good compatibility with the use of records in [protobuf](../../../api/cpp/protobuf.md) format.

## Operations overview

All of the operations in this library take as input lambda-expressed operators on a single record, which are then repeatedly applied. In general, an operation can also be viewed as a chain of conversions of types:

![](../../../../images/lambda-normal-op.png){ .center}

### Additive operations

A variant of ordinary operations where [Reduce](../../../user-guide/data-processing/operations/reduce.md) has the same input and output type and is associative. In this case, you can further simplify the implementation of the Reduce transformation and use it as a combiner for free.

![](../../../../images/lambda-additive-op.png){ .center}

## CopyIf

This function filters records in the table without modifying them.

```c++
template <class T>
void CopyIf(const IClientBasePtr& client, const TRichYPath& from, const TRichYPath& to, bool (*p)(const T&));
```

| **Parameter** | **Description** |
| -------- | ------------------------------------------------------------ |
| `T` | Record type (TNode or a specific [protobuf message](../../../api/cpp/protobuf.md) type) |
| `from` | Input table name |
| `to` | Output table name |
| `p` | Filtering predicate |

Notes:

- The order of records in the table isn't changed, and the `Ordered` flag that may slightly slow down the operation is set.
- A known defect: if there is only one input table, it has a schema, and is sorted, the sorting information doesn't reach the output table schema.

### Application

```c++
NYT::CopyIf<TMyProtoMsg>(client, "//home/project/inTable", "//home/project/outTable",
    [](auto& rec) { return rec.GetSomeField() > 10000; });
```

## TransformCopyIf, or Map

This function performs a classic [Map](../../../user-guide/data-processing/operations/map.md): it reads each record from the table of one format and writes them to the table of another format. Some records aren't included in the final table.

```c++
template <class R, class W>
void TransformCopyIf(const IClientBasePtr& client, const TRichYPath& from, const TRichYPath& to, bool (*mapper)(const R&, W&));
```

| **Parameter** | **Description** |
| -------- | ------------------------------------------------------------ |
| `R` | Input record type (TNode or a specific protobuf message type) |
| `W` | Output record type (TNode or a specific protobuf message type) |
| `from` | Input table name |
| `to` | Output table name |
| `mapper` | A conversion function that takes an `src` record of the `R` type as input and fills the `dst` record of the `W` type. For `dst` to be written, return `true`. For performance reasons, the library passes as `dst` the same buffer variable that isn't even cleared between `mapper` calls. This is generally not a problem, because the `mapper` usually fills in the same `dst` fields. |

Note:

- The order of records in the table isn't changed, and the `Ordered` flag that may slightly slow down the operation is set.

### Application

```c++
NYT::TransformCopyIf<TDataMsg, TStatsMsg>(
    client, "//home/project/inTable", "//home/project/outTable",
    [](auto& src, auto& dst) {
        dst.SetField(src.GetField());
        return true;
    });
```

## AdditiveReduce

```c++
template <class T, class TFin>
void AdditiveReduce(
    const IClientBasePtr& client,
    const TKeyBase<TRichYPath>& from,
    const TRichYPath& to,
    const TKeyColumns& reduceFields,
    void (*reducer)(const T&, T&),
    bool (*finalizer)(const T&, TFin&));
```

| **Parameter** | **Description** |
| -------------- | ------------------------------------------------------------ |
| `T` | Input and output record type for reducer (TNode or a specific protobuf message type) |
| `TFin`Opt | **Optional parameter.** Output record type (TNode or a specific protobuf message type). <span style="color: green;">**Specified only in the version with a finalizer**</span> |
| `from` | Name of an input table (input tables) |
| `to` | Output table name |
| `reduceFields` | Which keys' identical values will be used for sorting and folding (Reduce) |
| `reducer` | A conversion function that takes a new `src` record of the `T` type as input and modifies the `dst` record of the same `T` type. The first record of the current range of records with the same `reduceFields` keys is initially included in `dst`. If there is one record in the current range, the `reducer` won't be called. You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |
| `finalizer`Opt | **Optional parameter.** A conversion function that takes a new `src` record of the `T` type as input and fills the `dst` record of the `TFin` type. Called once after Reduce has processed a range of input records to generate the final version of the output record. `dst` is initialized by default. You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |

## Reduce

```c++
template <class R, class W, class TFin>
void Reduce(
    const IClientBasePtr& client,
    const TKeyBase<TRichYPath>& from,
    const TRichYPath& to,
    const TKeyColumns& reduceFields,
    void (*reducer)(const R&, W&),
    bool (*finalizer)(const W&, TFin&));
```

| **Parameter** | **Description** |
| -------------- | ------------------------------------------------------------ |
| `R` | Input record type (TNode or a specific protobuf message type) |
| `W` | Reducer's output record type (TNode or protobuf) |
| `TFin`Opt | **Optional parameter.** Output record type (TNode or a specific protobuf message type). <span style="color: green;">**Specified only in the version with a finalizer**</span> |
| `from` | Name of an input table (input tables) |
| `to` | Output table name |
| `reduceFields` | Which keys' identical values will be used for sorting and folding (Reduce) |
| `reducer` | A conversion function that takes a new `src` record of the `R` type as input and modifies the `dst` record of the `W` type. `dst` is initialized by default (in case of TNode, there will be an empty TNode, that is, `Undefined`; protobuf is usually initialized with zeros and empty strings, which makes it more practical). You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |
| `finalizer`Opt | **Optional parameter.** A conversion function that takes a new `src` record of the `W` type as input and fills the `dst` record of the `TFin` type. Called once after Reduce has processed a range of input records to generate the final version of the output record. `dst` is initialized by default. You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |

## AdditiveMapReduce[Sorted] { #additive_mapreduce }

The `AdditiveMapReduce` function generates a merged [MapReduce operation](../../../user-guide/data-processing/operations/mapreduce.md), that is, it converts records of the input table (or tables), sorts the result, and performs Reduce. The reducer must be an [associative binary operation](https://en.wikipedia.org/wiki/Associative_property) on same-type records — most often, this will be an addition of numbers. As a bonus, YT can use such a reducer as a combiner if needed.

Unfortunately, merged MapReduce operations in YT always generate an unsorted table. There's a workaround in the form of the `AdditiveMapReduceSorted` function, which invokes `AdditiveMapReduce` and then sorts the output table.

```c++
template <class R, class W, class TFin>
void AdditiveMapReduce(
    const IClientBasePtr& client,
    const TKeyBase<TRichYPath>& from,
    const TRichYPath& to,
    const TKeyColumns& reduceFields,
    bool (*mapper)(const R&, W&),
    void (*reducer)(const W&, W&),
    bool (*finalizer)(const W&, TFin&));
```

| **Parameter** | **Description** |
| -------------- | ------------------------------------------------------------ |
| `R` | Input record type (TNode or a specific protobuf message type) |
| `W` | Output record type for reducer (TNode or a specific protobuf message type) |
| `TFin`Opt | **Optional parameter.** Output record type (TNode or a specific protobuf message type). <span style="color: green;">**Specified only in the version with a finalizer**</span> |
| `from` | Name of an input table (input tables) |
| `to` | Output table name |
| `reduceFields` | Which keys' identical values will be used for sorting and folding (Reduce) |
| `mapper` | A conversion function that takes an `src` record of the `R` type as input and fills the `dst` record of the `W` type. For `dst` to be written, return `true`. `mapper` may be `nullptr`. For performance reasons, the library passes as `dst` the same buffer variable that isn't even cleared between `mapper` calls. This is generally not a problem, because the `mapper` usually fills in the same `dst` fields. |
| `reducer` | A conversion function that takes a new `src` record of the `W` type and modifies the `dst` record of the same `W` type. The first record of the current range of records with the same `reduceFields` keys is initially included in `dst`. If there is one record in the current range, the `reducer` won't be called. You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |
| `finalizer`Opt | **Optional parameter.** A conversion function that takes a new `src` record of the `W` type as input and fills the `dst` record of the `TFin` type. Called once after Reduce has processed a range of input records to generate the final version of the output record. `dst` is initialized by default. You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |

### Application

```c++
NYT::AdditiveMapReduceSorted<TDataMsg, TStatsMsg>(
    client,
    inTable, // or inTables
    outTable,
    "name", // sort key, matches TStatsMsg::Name
    [](auto& src, auto& dst) {
        // dst is a buffer that may contain garbage.
        // We don't need to clear it because we set all fields.
        dst.SetName(src.GetSomeField());
        dst.SetCount(src.GetSomeCountField());
        return true;
    },
    [](auto& src, auto& dst) {
        // dst is initialized by the first record of equal key range.
        // This lambda function is called starting from the 2nd record.
        // dst.Name is filled with correct value and must not be modified.
        dst.SetCount(src.GetCount() + dst.GetCount());
    });
```

## MapReduce[Sorted] { #mapreduce }

The MapReduce function generates a merged MapReduce operation, that is, it converts records of the input table (or tables), sorts the result, and performs Reduce. Unlike [AdditiveMapReduce](#additive_mapreduce), the reducer function doesn't have to be associative, so it can have different input and output record types (and thus can't be used as a combiner). To make the output clearer, you can use an additional lambda function — finalizer, — which will be called on the reducer output to generate the final version of the record.

Unfortunately, merged MapReduce operations in YT always generate an unsorted table. There's a workaround in the form of the `MapReduceSorted` function, which invokes `MapReduce` and then sorts the output table.

```c++
template <class R, class TMapped, class TReducerData, class W>
void MapReduce(
    const IClientBasePtr& client,
    const TKeyBase<TRichYPath>& from,
    const TRichYPath& to,
    const TKeyColumns& reduceFields,
    bool (*mapper)(const R& src, TMapped& dst),
    void (*reducer)(const TMapped& src, TReducerData& dst),
    bool (*finalizer)(const TReducerData& src, W& dst));
```

| **Parameter** | **Description** |
| ----------------- | ------------------------------------------------------------ |
| `R` | Input record type (TNode or a specific protobuf message type) |
| `TMapped` | Output record type for mapper and input record type for reducer (TNode or protobuf) |
| `TReducerData`Opt | **Optional parameter.** Output record type for the reducer and input record type for the finalizer (any type, including C++ struct). <span style="color: green;">**Specified only in the MapReduce version with a finalizer**</span> |
| `W` | Output record type (TNode or a specific protobuf message type) |
| `from` | Name of an input table (input tables) |
| `to` | Output table name |
| `reduceFields` | Which keys' identical values will be used for sorting and folding (Reduce) |
| `mapper` | A conversion function that takes an `src` record of the `R` type as input and fills the `dst` record of the `TMapped` type. For `dst` to be written, return `true`. `mapper` may be `nullptr`. For performance reasons, the library passes as `dst` the same buffer variable that isn't even cleared between `mapper` calls. This is generally not a problem, because the `mapper` usually fills in the same `dst` fields. |
| `reducer` | A conversion function that takes a new `src` record of the `TMapped` type as input and modifies the `dst` record of the `W` type (or `TReducerData` when using a finalizer). `dst` is initialized by default (in case of TNode, there will be an empty TNode, that is, `Undefined`; protobuf is usually initialized with zeros and empty strings, which makes it more practical). You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |
| `finalizer`Opt | **Optional parameter.** A conversion function that takes a new `src` record of the `TReducerData` type as input and fills the `dst` record of the `W` type. Called once after the reducer has processed a range of input records to generate the final version of the output record. `dst` is initialized by default. You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |

### Application { #mapreduce_example }

```c++
message TSimpleKeyValue {
    required string Key = 1          [(NYT.column_name) = "key"];
    required uint64 Value = 2        [(NYT.column_name) = "value"];
}

message TKeyStat {
    required string Key = 1          [(NYT.column_name) = "key"];
    required double Mean = 2         [(NYT.column_name) = "mean"];
    required double Sigma = 3        [(NYT.column_name) = "sigma"];
}message TSimpleKeyValue {
    required string Key = 1          [(NYT.column_name) = "key"];
    required uint64 Value = 2        [(NYT.column_name) = "value"];
}

message TKeyStat {
    required string Key = 1          [(NYT.column_name) = "key"];
    required double Mean = 2         [(NYT.column_name) = "mean"];
    required double Sigma = 3        [(NYT.column_name) = "sigma"];
}
```

```c++
struct TDispersionData {
    ui64 Count = 0;
    long double Sum = 0.;
    long double SumSquared = 0.;
};

void CalculateDispersion(IClientPtr& client, TString input, TString output) {
    NYT::MapReduceSorted<TDataMsg, TSimpleKeyValue, TDispersionData, TKeyStat>(
        client,
        input,
        output,
        "key",
        [](auto& src, auto& dst) { // mapper
            dst.SetName(src.GetSomeField());
            dst.SetCount(src.GetSomeCountField());
            return true;
        },
        [](auto& src, auto& dst) { // reducer
            double value = src.GetValue();
            dst.Count++;
            dst.Sum += value;
            dst.SumSquared += value * value;
        },
        [](auto& src, auto& dst) { // finalizer
            double mean = (double)src.Sum / src.Count;
            double dispersion = (double)src.SumSquared / src.Count - mean * mean;
            dst.SetMean(mean);
            dst.SetSigma(std::sqrt(dispersion));
        });
}
```

## MapReduceCombined[Sorted] { #mapreduce_combined }

The MapReduceCombined function generates a merged MapReduce operation, that is, it converts records of the input table (or tables), sorts the result, and performs Reduce similarly to [MapReduce](#mapreduce), but with combiner mandatorily enabled. To make the output clearer, you can use an additional lambda function — finalizer, — which will be called on the reducer output to generate the final version of the record.

Unfortunately, merged MapReduce operations in YT always generate an unsorted table. There's a workaround in the form of the `MapReduceCombinedSorted` function, which invokes `MapReduceCombined` and then sorts the output table.

```c++
template <class R, class TMapped, class W, class TFin>
void MapReduceCombined(
    const IClientBasePtr& client,
    const TKeyBase<TRichYPath>& from,
    const TRichYPath& to,
    const TKeyColumns& reduceFields,
    bool (*mapper)(const R&, TMapped&),
    void (*combiner)(const TMapped&, W&),
    void (*reducer)(const W&, W&),
    bool (*finalizer)(const W&, TFin&));
```

| **Parameter** | **Description** |
| -------------- | ------------------------------------------------------------ |
| `R` | Input record type (TNode or a specific protobuf message type) |
| `TMapped` | Output record type for mapper and input record type for reducer (TNode or protobuf) |
| `W` | Output record type for the combiner, input record type for the reducer and finalizer (TNode or a specific protobuf message type) |
| `TFin`Opt | **Optional parameter.** Output entry type for finalizer (TNode or a specific protobuf message type). <span style="color: green;">**Specified only in the version with a finalizer**</span> |
| `from` | Name of an input table (input tables) |
| `to` | Output table name |
| `reduceFields` | Which keys' identical values will be used for sorting and folding (Reduce) |
| `mapper` | A conversion function that takes an `src` record of the `R` type as input and fills the `dst` record of the `TMapped` type. For `dst` to be written, return `true`. `mapper` may be `nullptr`. For performance reasons, the library passes as `dst` the same buffer variable that isn't even cleared between `mapper` calls. This is generally not a problem, because the `mapper` usually fills in the same `dst` fields. |
| `combiner` | A conversion function that takes a new `src` record of the `TMapped` type as input and modifies the `dst` record of the `W` type. Essentially, it does the same thing that the reducer does in [MapReduce](#mapreduce). `dst` is initialized by default (in case of TNode, there will be an empty TNode, that is, `Undefined`; protobuf is usually initialized with zeros and empty strings, which makes it more practical). You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |
| `reducer` | An associative conversion function that takes a new `src` record as input and modifies the `dst` record of the same `W` type. The first record of the current range of records with the same `reduceFields` keys is initially included in `dst`. If there is one record in the current range, the `reducer` won't be called. You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |
| `finalizer`Opt | **Optional parameter.** A conversion function that takes a new `src` record of the `W` type as input and fills the `dst` record of the `TFin` type. Called once after Reduce has processed a range of input records to generate the final version of the output record. `dst` is initialized by default. You don't need to write anything in the `dst` fields that correspond to the keys — everything is already there |

### Application

In addition to the protobuf messages defined in the [example for MapReduce](#mapreduce_example):

```c++
message TDispersionDataMsg {
    required string Key = 1          [(NYT.column_name) = "key"];
    required uint64 Count = 2        [(NYT.column_name) = "count"];
    required double Sum = 3          [(NYT.column_name) = "sum"];
    required double SumSquared = 4   [(NYT.column_name) = "sum_squared"];
};
```

The function itself:

```c++
void CalculateDispersionWithCombiner(IClientPtr& client, TString input, TString output) {
    NYT::MapReduceCombinedSorted<TDataMsg, TSimpleKeyValue, TDispersionDataMsg, TKeyStat>(
        client,
        input,
        output,
        "key",
        [](auto& src, auto& dst) { // mapper
            dst.SetName(src.GetSomeField());
            dst.SetCount(src.GetSomeCountField());
            return true;
        },
        [](auto& src, auto& dst) { // combiner
            double value = src.GetValue();
            dst.SetCount(dst.GetCount() + 1);
            dst.SetSum(dst.GetSum() + value);
            dst.SetSumSquared(dst.GetSumSquared() + value * value);
        },
        [](auto& src, auto& dst) { // reducer
            dst.SetCount(src.GetCount() + dst.GetCount());
            dst.SetSum(src.GetSum() + dst.GetSum());
            dst.SetSumSquared(src.GetSumSquared() + dst.GetSumSquared());
        },
        [](auto& src, auto& dst) { // finalizer
            double mean = src.GetSum() / src.GetCount();
            double dispersion = src.GetSumSquared() / src.GetCount() - mean * mean;
            dst.SetMean(mean);
            dst.SetSigma(std::sqrt(dispersion));
        });
}
```

### Choosing between MapReduce and MapReduceCombined

The `AdditiveMapReduce` function, which also provides the combiner, leaves it up to YT to choose whether to use the combiner or not. The `MapReduceCombined` function always forces the system to use the combiner even if omitting it would be more efficient. This is because writing lambdas in this function would be problematic, as this would allow inputting different types of records (from map and from combine) to the reducer.

In general, the observed effect is as follows: the `MapReduceCombined` operations consume a few percent more CPU resources on the cluster than fully identical MapReduce operations, but are completed much faster. Since using [MapReduceCombined](#mapreduce_combined) is as simple as using [MapReduce](#mapreduce), it may be worth trying both during the development stage and leave the one that best suits your needs.

## Combining lambdas with the regular API

You may want to use a lambda when calling operations in a standard way via `IClientBase` methods. For example, you wrote the mapper as a large class, and the reducer remained simple. Then it makes sense to use named wrapper classes (`TCopyIfMapper`, `TTransformMapper`, `TAdditiveReducer`, etc.).

### Example { #lambdas_mixed_example }

```c++
auto createReducer = []() {
    return new TAdditiveReducer<TOutStatsProto>([](auto& src, auto& dst) {
        dst.SetCount(src.GetCount() + dst.GetCount());
    });
};

client->MapReduce(
    TMapReduceOperationSpec()
        .AddInput<TInputProto>(inTable)
        .AddOutput<TOutStatsProto>(outTable)
        .ReduceBy(fields),
    new TMyComplicatedMapper(),
    createReducer(), // combiner
    createReducer(), // reducer
    TOperationOptions().Spec(TNode()("map_selectivity_factor", 0.005)));
```

## TSaveable — global variables accessible from lambda operations { #saveable }

If you create a global variable of the `NYT::TSaveable<TYourStruct>` type, where `TYourStruct` has access to the `ysaveload` variant serialization, then all the above functions described in the `yt_lambda` library will save the variable in the job state at the beginning of their work and restore it on the cluster before executing the job.

Example:

```c++
struct TGlobalSettings {
    TString MailSuffix;
    Y_SAVELOAD_DEFINE(MailSuffix);
};
NYT::TSaveable<TGlobalSettings> GlobalSettings;
int main() {
    GlobalSettings.MailSuffix = "@domain-name";
    ...
    /// use GlobalSettings.MailSuffix in lambda
}
```
{% note warning "Attention" %}

- Naturally, the contents of `TSaveable` aren't saved In ordinary (non-lambda) operations. In the [example above](#lambdas_mixed_example), you can't use the `TSaveable` variable from the `TMyComplicatedMapper` class, but you can use it from the lambda created inside `createReducer()`. If necessary, you can manually add calls to `TSaveablesRegistry::Get()->SaveAll(stream)` and `LoadAll(stream) ` in the `Save` and `Load` functions of your mapper or reducer.

- Don't use `TSaveable` if modification of these global variables and start of operations can be initiated from different threads to avoid data races and crashes.

- You can create only one `TSaveable` variable for each user-defined type. This is intended and won't be changed in the near future.

{% endnote %}
