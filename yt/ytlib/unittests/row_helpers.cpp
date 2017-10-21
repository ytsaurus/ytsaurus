#include "row_helpers.h"

#include <yt/core/yson/string.h>
#include <yt/core/ytree/convert.h>

namespace NYT {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static void EnsureTypesMatch(EValueType expected, EValueType actual)
{
    if (expected != actual) {
        THROW_ERROR_EXCEPTION("Expected: %v actual: %v", expected, actual);
    }
}

TUnversionedOwningRow MakeRow(const std::vector<TUnversionedValue>& rows)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& r : rows) {
        builder.AddValue(r);
    }
    return builder.FinishRow();
}

i64 GetInt64(const TUnversionedValue& row)
{
    EnsureTypesMatch(EValueType::Int64, row.Type);
    return row.Data.Int64;
}

ui64 GetUint64(const TUnversionedValue& row)
{
    EnsureTypesMatch(EValueType::Uint64, row.Type);
    return row.Data.Uint64;
}

double GetDouble(const NTableClient::TUnversionedValue& row)
{
    EnsureTypesMatch(EValueType::Double, row.Type);
    return row.Data.Double;
}

bool GetBoolean(const TUnversionedValue& row)
{
    EnsureTypesMatch(EValueType::Boolean, row.Type);
    return row.Data.Boolean;
}

TString GetString(const TUnversionedValue& row)
{
    EnsureTypesMatch(EValueType::String, row.Type);
    return TString(row.Data.String, row.Length);
}

NYTree::INodePtr GetAny(const NTableClient::TUnversionedValue& row)
{
    EnsureTypesMatch(EValueType::Any, row.Type);
    return NYTree::ConvertToNode(NYson::TYsonString(row.Data.String, row.Length));
}

bool IsNull(const NTableClient::TUnversionedValue& row)
{
    return row.Type == EValueType::Null;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
