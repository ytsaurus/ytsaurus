#include "block_builder.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <contrib/libs/clickhouse-cpp/clickhouse/columns/bool.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/columns/date.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/columns/lowcardinality.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/columns/nullable.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/columns/numeric.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/columns/string.h>

#include <util/generic/hash.h>
#include <util/string/cast.h>

namespace NYT::NFlow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

EClickHouseBaseType ParseBaseType(std::string_view type)
{
    static const THashMap<std::string_view, EClickHouseBaseType> mapping{
        {"Int8", EClickHouseBaseType::Int8},
        {"Int16", EClickHouseBaseType::Int16},
        {"Int32", EClickHouseBaseType::Int32},
        {"Int64", EClickHouseBaseType::Int64},
        {"UInt8", EClickHouseBaseType::UInt8},
        {"UInt16", EClickHouseBaseType::UInt16},
        {"UInt32", EClickHouseBaseType::UInt32},
        {"UInt64", EClickHouseBaseType::UInt64},
        {"Float32", EClickHouseBaseType::Float32},
        {"Float64", EClickHouseBaseType::Float64},
        {"String", EClickHouseBaseType::String},
        {"Bool", EClickHouseBaseType::Bool},
        {"Date", EClickHouseBaseType::Date},
    };
    if (type == "DateTime" || type.starts_with("DateTime(")) {
        return EClickHouseBaseType::DateTime;
    }
    if (auto it = mapping.find(type); it != mapping.end()) {
        return it->second;
    }
    THROW_ERROR_EXCEPTION("Unsupported ClickHouse column type %Qv", type);
}

ESimpleLogicalValueType PrimarySimpleType(EClickHouseBaseType base)
{
    switch (base) {
        case EClickHouseBaseType::Int8:
            return ESimpleLogicalValueType::Int8;
        case EClickHouseBaseType::Int16:
            return ESimpleLogicalValueType::Int16;
        case EClickHouseBaseType::Int32:
            return ESimpleLogicalValueType::Int32;
        case EClickHouseBaseType::Int64:
            return ESimpleLogicalValueType::Int64;
        case EClickHouseBaseType::UInt8:
            return ESimpleLogicalValueType::Uint8;
        case EClickHouseBaseType::UInt16:
            return ESimpleLogicalValueType::Uint16;
        case EClickHouseBaseType::UInt32:
            return ESimpleLogicalValueType::Uint32;
        case EClickHouseBaseType::UInt64:
            return ESimpleLogicalValueType::Uint64;
        case EClickHouseBaseType::Float32:
            return ESimpleLogicalValueType::Float;
        case EClickHouseBaseType::Float64:
            return ESimpleLogicalValueType::Double;
        case EClickHouseBaseType::String:
        case EClickHouseBaseType::FixedString:
            return ESimpleLogicalValueType::String;
        case EClickHouseBaseType::Bool:
            return ESimpleLogicalValueType::Boolean;
        case EClickHouseBaseType::Date:
            return ESimpleLogicalValueType::Date;
        case EClickHouseBaseType::DateTime:
            return ESimpleLogicalValueType::Datetime;
    }
    YT_ABORT();
}

bool IsStringLike(EClickHouseBaseType base)
{
    return base == EClickHouseBaseType::String || base == EClickHouseBaseType::FixedString;
}

bool StreamTypeMatches(const TClickHouseColumnType& type, const TLogicalTypePtr& actual)
{
    std::vector<ESimpleLogicalValueType> accepted{PrimarySimpleType(type.Base)};
    if (IsStringLike(type.Base)) {
        accepted.push_back(ESimpleLogicalValueType::Utf8);
    }
    for (auto simple : accepted) {
        TLogicalTypePtr expected = SimpleLogicalType(simple);
        if (type.Nullable) {
            expected = OptionalLogicalType(std::move(expected));
        }
        if (*actual == *expected) {
            return true;
        }
    }
    return false;
}

clickhouse::ColumnRef MakeScalarColumn(const TClickHouseColumnType& type)
{
    switch (type.Base) {
        case EClickHouseBaseType::Int8:
            return std::make_shared<clickhouse::ColumnInt8>();
        case EClickHouseBaseType::Int16:
            return std::make_shared<clickhouse::ColumnInt16>();
        case EClickHouseBaseType::Int32:
            return std::make_shared<clickhouse::ColumnInt32>();
        case EClickHouseBaseType::Int64:
            return std::make_shared<clickhouse::ColumnInt64>();
        case EClickHouseBaseType::UInt8:
            return std::make_shared<clickhouse::ColumnUInt8>();
        case EClickHouseBaseType::UInt16:
            return std::make_shared<clickhouse::ColumnUInt16>();
        case EClickHouseBaseType::UInt32:
            return std::make_shared<clickhouse::ColumnUInt32>();
        case EClickHouseBaseType::UInt64:
            return std::make_shared<clickhouse::ColumnUInt64>();
        case EClickHouseBaseType::Float32:
            return std::make_shared<clickhouse::ColumnFloat32>();
        case EClickHouseBaseType::Float64:
            return std::make_shared<clickhouse::ColumnFloat64>();
        case EClickHouseBaseType::String:
            return std::make_shared<clickhouse::ColumnString>();
        case EClickHouseBaseType::FixedString:
            return std::make_shared<clickhouse::ColumnFixedString>(*type.FixedStringLength);
        case EClickHouseBaseType::Bool:
            return std::make_shared<clickhouse::ColumnBool>();
        case EClickHouseBaseType::Date:
            return std::make_shared<clickhouse::ColumnDate>();
        case EClickHouseBaseType::DateTime:
            return std::make_shared<clickhouse::ColumnDateTime>();
    }
    YT_ABORT();
}

clickhouse::ColumnRef MakeColumn(const TClickHouseColumnType& type)
{
    if (type.LowCardinality) {
        if (type.Nullable) {
            THROW_ERROR_EXCEPTION("LowCardinality(Nullable) columns are not supported by the ClickHouse sink");
        }
        if (type.Base == EClickHouseBaseType::String) {
            return std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
        }
        if (type.Base == EClickHouseBaseType::FixedString) {
            return std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnFixedString>>(*type.FixedStringLength);
        }
        THROW_ERROR_EXCEPTION("LowCardinality is supported only over String / FixedString by the ClickHouse sink");
    }
    if (type.Nullable) {
        return std::make_shared<clickhouse::ColumnNullable>(
            MakeScalarColumn(type),
            std::make_shared<clickhouse::ColumnUInt8>());
    }
    return MakeScalarColumn(type);
}

void AppendScalar(
    const clickhouse::ColumnRef& column,
    const TClickHouseColumnType& type,
    const TUnversionedValue& value)
{
    bool isNull = value.Type == EValueType::Null;
    switch (type.Base) {
        case EClickHouseBaseType::Int8:
            column->As<clickhouse::ColumnInt8>()->Append(isNull ? 0 : value.Data.Int64);
            break;
        case EClickHouseBaseType::Int16:
            column->As<clickhouse::ColumnInt16>()->Append(isNull ? 0 : value.Data.Int64);
            break;
        case EClickHouseBaseType::Int32:
            column->As<clickhouse::ColumnInt32>()->Append(isNull ? 0 : value.Data.Int64);
            break;
        case EClickHouseBaseType::Int64:
            column->As<clickhouse::ColumnInt64>()->Append(isNull ? 0 : value.Data.Int64);
            break;
        case EClickHouseBaseType::UInt8:
            column->As<clickhouse::ColumnUInt8>()->Append(isNull ? 0 : value.Data.Uint64);
            break;
        case EClickHouseBaseType::UInt16:
            column->As<clickhouse::ColumnUInt16>()->Append(isNull ? 0 : value.Data.Uint64);
            break;
        case EClickHouseBaseType::UInt32:
            column->As<clickhouse::ColumnUInt32>()->Append(isNull ? 0 : value.Data.Uint64);
            break;
        case EClickHouseBaseType::UInt64:
            column->As<clickhouse::ColumnUInt64>()->Append(isNull ? 0 : value.Data.Uint64);
            break;
        case EClickHouseBaseType::Float32:
            column->As<clickhouse::ColumnFloat32>()->Append(isNull ? 0 : value.Data.Double);
            break;
        case EClickHouseBaseType::Float64:
            column->As<clickhouse::ColumnFloat64>()->Append(isNull ? 0 : value.Data.Double);
            break;
        case EClickHouseBaseType::String:
            column->As<clickhouse::ColumnString>()->Append(isNull ? std::string_view{} : value.AsStringBuf());
            break;
        case EClickHouseBaseType::FixedString:
            column->As<clickhouse::ColumnFixedString>()->Append(isNull ? std::string_view{} : value.AsStringBuf());
            break;
        case EClickHouseBaseType::Bool:
            column->As<clickhouse::ColumnBool>()->Append(!isNull && value.Data.Boolean);
            break;
        case EClickHouseBaseType::Date:
            column->As<clickhouse::ColumnDate>()->AppendRaw(isNull ? 0 : value.Data.Uint64);
            break;
        case EClickHouseBaseType::DateTime:
            column->As<clickhouse::ColumnDateTime>()->AppendRaw(isNull ? 0 : value.Data.Uint64);
            break;
    }
}

void AppendValue(
    const clickhouse::ColumnRef& column,
    const TClickHouseColumnType& type,
    const TUnversionedValue& value)
{
    if (type.LowCardinality) {
        auto stringValue = value.AsStringBuf();
        if (type.Base == EClickHouseBaseType::FixedString) {
            column->As<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnFixedString>>()->Append(stringValue);
        } else {
            column->As<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>()->Append(stringValue);
        }
        return;
    }
    if (type.Nullable) {
        auto nullable = column->As<clickhouse::ColumnNullable>();
        AppendScalar(nullable->Nested(), type, value);
        nullable->Append(value.Type == EValueType::Null);
        return;
    }
    AppendScalar(column, type, value);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TClickHouseColumnType ParseClickHouseType(std::string_view type)
{
    TClickHouseColumnType result;
    for (bool changed = true; changed;) {
        changed = false;
        if (type.starts_with("LowCardinality(") && type.ends_with(")")) {
            result.LowCardinality = true;
            type = type.substr(15, type.size() - 16);
            changed = true;
        } else if (type.starts_with("Nullable(") && type.ends_with(")")) {
            result.Nullable = true;
            type = type.substr(9, type.size() - 10);
            changed = true;
        }
    }

    if (type.starts_with("FixedString(") && type.ends_with(")")) {
        auto lengthText = type.substr(12, type.size() - 13);
        int length = 0;
        if (!TryFromString(lengthText, length) || length <= 0) {
            THROW_ERROR_EXCEPTION("Cannot parse FixedString length from ClickHouse type %Qv", type);
        }
        result.Base = EClickHouseBaseType::FixedString;
        result.FixedStringLength = length;
    } else {
        result.Base = ParseBaseType(type);
    }

    if (result.LowCardinality && !IsStringLike(result.Base)) {
        THROW_ERROR_EXCEPTION("LowCardinality is supported only over String / FixedString by the ClickHouse sink");
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TResolvedColumn> ResolveColumns(
    const std::vector<TClickHouseTableColumn>& tableColumns,
    const TTableSchemaPtr& streamSchema)
{
    std::vector<TResolvedColumn> resolved;
    for (const auto& tableColumn : tableColumns) {
        if (tableColumn.DefaultKind == "MATERIALIZED" || tableColumn.DefaultKind == "ALIAS") {
            continue;
        }
        const auto* streamColumn = streamSchema->FindColumn(tableColumn.Name);
        if (!streamColumn) {
            continue;
        }
        auto type = ParseClickHouseType(tableColumn.Type);
        if (!StreamTypeMatches(type, streamColumn->LogicalType())) {
            THROW_ERROR_EXCEPTION(
                "ClickHouse column %Qv has type %Qv incompatible with stream column type %v",
                tableColumn.Name,
                tableColumn.Type,
                *streamColumn->LogicalType());
        }
        resolved.push_back(TResolvedColumn{.Name = tableColumn.Name, .Type = type});
    }
    if (resolved.empty()) {
        THROW_ERROR_EXCEPTION("The stream produces none of the target ClickHouse table columns");
    }
    return resolved;
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseBlockBuilder::TClickHouseBlockBuilder(std::vector<TResolvedColumn> columns)
    : Columns_(std::move(columns))
{ }

const std::vector<TResolvedColumn>& TClickHouseBlockBuilder::Columns() const
{
    return Columns_;
}

clickhouse::Block TClickHouseBlockBuilder::Build(const std::vector<TOutputMessageConstPtr>& messages) const
{
    clickhouse::Block block;
    if (messages.empty()) {
        return block;
    }

    std::vector<clickhouse::ColumnRef> columns;
    columns.reserve(Columns_.size());
    for (const auto& column : Columns_) {
        columns.push_back(MakeColumn(column.Type));
    }

    for (const auto& message : messages) {
        for (int columnIndex = 0; columnIndex < std::ssize(Columns_); ++columnIndex) {
            auto value = GetColumn(*message, Columns_[columnIndex].Name);
            AppendValue(columns[columnIndex], Columns_[columnIndex].Type, value);
        }
    }

    for (int columnIndex = 0; columnIndex < std::ssize(Columns_); ++columnIndex) {
        block.AppendColumn(Columns_[columnIndex].Name, columns[columnIndex]);
    }

    return block;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
