#include "select_literals.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/string/join.h>

namespace NYT::NFlow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

std::string FormatValueAsSelectLiteral(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Int64:
            return Format("%v", value.Data.Int64);
        case EValueType::Uint64:
            return Format("%vu", value.Data.Uint64);
        case EValueType::Double:
            return Format("%v", value.Data.Double);
        case EValueType::Boolean:
            return value.Data.Boolean ? std::string("true") : std::string("false");
        case EValueType::String:
            return Format("%Qv", value.AsStringBuf());
        case EValueType::Any:
            return Format("yson_string_to_any(%Qv)", value.AsStringBuf());
        default:
            THROW_ERROR_EXCEPTION("Unsupported value type for SELECT literal: %v",
                value.Type);
    }
}

std::string BuildLiteralTuple(const TKey& key, const TTableSchema& schema)
{
    int count = key.Underlying().GetCount();
    THROW_ERROR_EXCEPTION_UNLESS(count > 0 && count <= schema.GetColumnCount(),
        "Key length %v is not a valid prefix of the schema (column count: %v)",
        count,
        schema.GetColumnCount());
    std::vector<std::string> literals;
    literals.reserve(count);
    for (int i = 0; i < count; ++i) {
        literals.push_back(FormatValueAsSelectLiteral(key.Underlying()[i]));
    }
    return Format("(%v)", JoinSeq(",", literals));
}

std::string BuildColumnTuple(const TTableSchema& schema, int count)
{
    THROW_ERROR_EXCEPTION_UNLESS(count > 0 && count <= schema.GetColumnCount(),
        "Column tuple length %v is not a valid prefix of the schema (column count: %v)",
        count,
        schema.GetColumnCount());
    std::vector<std::string> names;
    names.reserve(count);
    for (int i = 0; i < count; ++i) {
        names.push_back(std::string(schema.Columns()[i].Name()));
    }
    return Format("(%v)", JoinSeq(",", names));
}

std::string BuildColumnTuple(const TTableSchema& schema)
{
    return BuildColumnTuple(schema, schema.GetColumnCount());
}

////////////////////////////////////////////////////////////////////////////////

std::string TSelectPlaceholders::Bind(const std::string& name, const TUnversionedValue& value)
{
    Values_.emplace_back(name, value);
    return Format("{%v}", name);
}

std::string TSelectPlaceholders::BindTuple(const std::string& name, const TKey& key)
{
    int count = key.Underlying().GetCount();
    std::vector<std::string> references;
    references.reserve(count);
    for (int i = 0; i < count; ++i) {
        references.push_back(Bind(Format("%v_%v", name, i), key.Underlying()[i]));
    }
    return Format("(%v)", JoinSeq(",", references));
}

std::string TSelectPlaceholders::BindKeyBound(
    const TTableSchema& schema,
    TStringBuf op,
    const std::string& name,
    const TKey& key)
{
    return Format("%v %v %v",
        BuildColumnTuple(schema, key.Underlying().GetCount()),
        op,
        BindTuple(name, key));
}

NYson::TYsonString TSelectPlaceholders::Build() const
{
    return NYTree::BuildYsonStringFluently()
        .BeginMap()
        .DoFor(Values_, [] (NYTree::TFluentMap fluent, const auto& item) {
            fluent.Item(item.first).Value(static_cast<TUnversionedValue>(item.second));
        })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
