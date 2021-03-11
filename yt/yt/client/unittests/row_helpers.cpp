#include "row_helpers.h"

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT {

using namespace NTableClient;


////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue TTableField::ToUnversionedValue(const NTableClient::TNameTablePtr& nameTable) const
{
    const int valueId = nameTable->GetIdOrRegisterName(Name_);
    return std::visit([valueId] (const auto& value) -> TUnversionedValue {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, i64>) {
            return MakeUnversionedInt64Value(value, valueId);
        } else if constexpr (std::is_same_v<T, ui64>) {
            return MakeUnversionedUint64Value(value, valueId);
        } else if constexpr (std::is_same_v<T, double>) {
            return MakeUnversionedDoubleValue(value, valueId);
        } else if constexpr (std::is_same_v<T, bool>) {
            return MakeUnversionedBooleanValue(value, valueId);
        } else if constexpr (std::is_same_v<T, TString>) {
            return MakeUnversionedStringValue(value, valueId);
        } else {
            static_assert(std::is_same_v<T, std::nullptr_t>);
            return MakeUnversionedSentinelValue(EValueType::Null, valueId);
        }
    }, Value_);
}
////////////////////////////////////////////////////////////////////////////////

static void EnsureTypesMatch(EValueType expected, EValueType actual)
{
    if (expected != actual) {
        THROW_ERROR_EXCEPTION("Unexpected type of TUnversionedValue: expected %Qlv, actual %Qlv",
            expected,
            actual);
    }
}

TUnversionedOwningRow MakeRow(const std::vector<TUnversionedValue>& values)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& v : values) {
        builder.AddValue(v);
    }
    return builder.FinishRow();
}

NTableClient::TUnversionedOwningRow MakeRow(
    const NTableClient::TNameTablePtr& nameTable,
    const std::initializer_list<TTableField>& values)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& v : values) {
        builder.AddValue(v.ToUnversionedValue(nameTable));
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
    return NYTree::ConvertToNode(NYson::TYsonString(TString(row.Data.String, row.Length)));
}

NYTree::INodePtr GetComposite(const NTableClient::TUnversionedValue& row)
{
    EnsureTypesMatch(EValueType::Composite, row.Type);
    return NYTree::ConvertToNode(NYson::TYsonString(TString(row.Data.String, row.Length)));
}

bool IsNull(const NTableClient::TUnversionedValue& row)
{
    return row.Type == EValueType::Null;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
