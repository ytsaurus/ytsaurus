#include "ql_helpers.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TOwningKey& key, ::std::ostream* os)
{
    *os << KeyToYson(key);
}

void PrintTo(const TUnversionedValue& value, ::std::ostream* os)
{
    *os << ToString(value);
}

void PrintTo(const TUnversionedRow& value, ::std::ostream* os)
{
    *os << ToString(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

namespace NYT {
namespace NQueryClient {

void PrintTo(TConstExpressionPtr expr, ::std::ostream* os)
{
    *os << InferName(expr);
}

TKeyColumns GetSampleKeyColumns()
{
    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    keyColumns.push_back("l");
    keyColumns.push_back("m");
    return keyColumns;
}

TKeyColumns GetSampleKeyColumns2()
{
    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    keyColumns.push_back("l");
    keyColumns.push_back("m");
    keyColumns.push_back("s");
    return keyColumns;
}

TTableSchema GetSampleTableSchema()
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64),
        TColumnSchema("b", EValueType::Int64),
        TColumnSchema("c", EValueType::Int64),
        TColumnSchema("s", EValueType::String),
        TColumnSchema("u", EValueType::String)
    });
    return tableSchema;
}

TFuture<void> WrapVoidInFuture()
{
    return MakeFuture(TErrorOr<void>());
}

TDataSplit MakeSimpleSplit(const TRichYPath& path, ui64 counter)
{
    TDataSplit dataSplit;

    ToProto(
        dataSplit.mutable_chunk_id(),
        MakeId(EObjectType::Table, 0x42, counter, 0xdeadbabe));

    SetTableSchema(&dataSplit, GetSampleTableSchema());

    return dataSplit;
}

TDataSplit MakeSplit(const std::vector<TColumnSchema>& columns)
{
    TDataSplit dataSplit;

    ToProto(
        dataSplit.mutable_chunk_id(),
        MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

    TTableSchema tableSchema(columns);
    SetTableSchema(&dataSplit, tableSchema);

    return dataSplit;
}

TFuture<TDataSplit> RaiseTableNotFound(
    const TRichYPath& path,
    TTimestamp)
{
    return MakeFuture(TErrorOr<TDataSplit>(TError(Format(
        "Could not find table %v",
        path.GetPath()))));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
