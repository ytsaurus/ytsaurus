#include "ql_helpers.h"

#include <yt/ytlib/table_client/helpers.h>

#include <yt/core/yson/public.h>
#include <yt/core/yson/string.h>

#include <yt/core/ytree/convert.h>

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

TValue MakeInt64(i64 value)
{
    return MakeUnversionedInt64Value(value);
}

TValue MakeUint64(i64 value)
{
    return MakeUnversionedUint64Value(value);
}

TValue MakeDouble(i64 value)
{
    return MakeUnversionedDoubleValue(value);
}

TValue MakeBoolean(bool value)
{
    return MakeUnversionedBooleanValue(value);
}

TValue MakeString(TStringBuf value)
{
    return MakeUnversionedStringValue(value);
}

TValue MakeNull()
{
    return MakeUnversionedSentinelValue(EValueType::Null);
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
        TColumnSchema("u", EValueType::String),
        TColumnSchema("ki", EValueType::Int64),
        TColumnSchema("ku", EValueType::Uint64),
        TColumnSchema("kd", EValueType::Double),
    });
    return tableSchema;
}

TDataSplit MakeSimpleSplit(const TYPath& /*path*/, ui64 counter)
{
    TDataSplit dataSplit;

    ToProto(
        dataSplit.mutable_chunk_id(),
        MakeId(EObjectType::Table, 0x42, counter, 0xdeadbabe));

    SetTableSchema(&dataSplit, GetSampleTableSchema());

    return dataSplit;
}

TDataSplit MakeSplit(const std::vector<TColumnSchema>& columns, ui64 counter)
{
    TDataSplit dataSplit;

    ToProto(
        dataSplit.mutable_chunk_id(),
        MakeId(EObjectType::Table, 0x42, counter, 0xdeadbabe));

    TTableSchema tableSchema(columns);
    SetTableSchema(&dataSplit, tableSchema);

    return dataSplit;
}

TFuture<TDataSplit> RaiseTableNotFound(const TYPath& path, TTimestamp /*timestamp*/)
{
    return MakeFuture<TDataSplit>(TError(
        "Could not find table %v",
        path));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
