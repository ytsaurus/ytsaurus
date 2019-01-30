#include "helpers.h"

#include "table_schema.h"

#include <yt/client/table_client/unversioned_row.h>

#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

KeyCondition CreateKeyCondition(
    const Context& context,
    const SelectQueryInfo& queryInfo,
    const TClickHouseTableSchema& schema)
{
    auto pkExpression = std::make_shared<ExpressionActions>(
        schema.KeyColumns,
        context);

    return KeyCondition(queryInfo, context, schema.Columns, schema.PrimarySortColumns, std::move(pkExpression));
}

void ConvertToFieldRow(const NTableClient::TUnversionedRow& row, DB::Field* field)
{
    for (auto* value = row.Begin(); value != row.End(); ) {
        *(field++) = ConvertToField(*(value++));
    }
}

Field ConvertToField(const NTableClient::TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Null:
            return Field();
        case EValueType::Int64:
            return Field(static_cast<Int64>(value.Data.Int64));
        case EValueType::Uint64:
            return Field(static_cast<UInt64>(value.Data.Uint64));
        case EValueType::Double:
            return Field(static_cast<Float64>(value.Data.Double));
        case EValueType::Boolean:
            return Field(static_cast<UInt64>(value.Data.Boolean ? 1 : 0));
        case EValueType::String:
            return Field(value.Data.String, value.Length);
        default:
            THROW_ERROR_EXCEPTION("Unexpected data type %v", value.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

