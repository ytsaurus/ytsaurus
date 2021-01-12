#include "schema.h"

#include "composite.h"
#include "config.h"

#include <yt/client/table_client/schema.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNothing.h>

#include <Storages/ColumnsDescription.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

DB::Names ToNames(const std::vector<TString>& columnNames)
{
    DB::Names result;

    for (const auto& columnName : columnNames) {
        result.emplace_back(columnName.data());
    }

    return result;
}

std::vector<TString> ToVectorString(const DB::Names& columnNames)
{
    std::vector<TString> result;

    for (const auto& columnName : columnNames) {
        result.emplace_back(columnName);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

DB::DataTypePtr ToDataType(const TComplexTypeFieldDescriptor& descriptor, const TCompositeSettingsPtr& settings)
{
    TCompositeValueToClickHouseColumnConverter converter(descriptor, settings);
    return converter.GetDataType();
}

DB::DataTypes ToDataTypes(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings)
{
    DB::DataTypes result;
    result.reserve(schema.GetColumnCount());

    for (const auto& column : schema.Columns()) {
        TComplexTypeFieldDescriptor descriptor(column);
        result.emplace_back(ToDataType(std::move(descriptor), settings));
    }

    return result;
}

DB::NamesAndTypesList ToNamesAndTypesList(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings)
{
    const auto& dataTypes = ToDataTypes(schema, settings);

    DB::NamesAndTypesList result;

    for (int index = 0; index < schema.GetColumnCount(); ++index) {
        result.emplace_back(schema.Columns()[index].Name(), dataTypes[index]);
    }

    return result;
}

DB::Block ToHeaderBlock(const TTableSchema& schema, const TCompositeSettingsPtr& settings)
{
    DB::Block headerBlock;

    auto namesAndTypesList = ToNamesAndTypesList(schema, settings);

    for (const auto& nameAndTypePair : namesAndTypesList) {
        auto column = nameAndTypePair.type->createColumn();
        headerBlock.insert({ std::move(column), nameAndTypePair.type, nameAndTypePair.name });
    }

    return headerBlock;
}

////////////////////////////////////////////////////////////////////////////////

EValueType ToValueType(DB::Field::Types::Which which)
{
    switch (which) {
        case DB::Field::Types::Which::Null:
            return EValueType::Null;
        case DB::Field::Types::Which::Int64:
            return EValueType::Int64;
        case DB::Field::Types::Which::UInt64:
            return EValueType::Uint64;
        case DB::Field::Types::Which::Float64:
            return EValueType::Double;
        case DB::Field::Types::Which::String:
            return EValueType::String;
        default:
            THROW_ERROR_EXCEPTION(
                "ClickHouse physical type %Qv is not supported",
                DB::Field::Types::toString(which));
    }
}

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr RepresentClickHouseType(const DB::DataTypePtr& type)
{
    if (type->isNullable()) {
        return OptionalLogicalType(RepresentClickHouseType(DB::removeNullable(type)));
    }
    switch (type->getTypeId()) {
        case DB::TypeIndex::Int64:
            return SimpleLogicalType(ESimpleLogicalValueType::Int64);
        case DB::TypeIndex::Int32:
            return SimpleLogicalType(ESimpleLogicalValueType::Int32);
        case DB::TypeIndex::Int16:
            return SimpleLogicalType(ESimpleLogicalValueType::Int16);
        case DB::TypeIndex::Int8:
            return SimpleLogicalType(ESimpleLogicalValueType::Int8);
        case DB::TypeIndex::UInt64:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint64);
        case DB::TypeIndex::UInt32:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint32);
        case DB::TypeIndex::UInt16:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint16);
        case DB::TypeIndex::UInt8:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint8);
        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
            return SimpleLogicalType(ESimpleLogicalValueType::Double);
        case DB::TypeIndex::String:
        case DB::TypeIndex::FixedString:
            return SimpleLogicalType(ESimpleLogicalValueType::String);
        case DB::TypeIndex::Date:
            return SimpleLogicalType(ESimpleLogicalValueType::Date);
        case DB::TypeIndex::DateTime:
            return SimpleLogicalType(ESimpleLogicalValueType::Datetime);
        // TODO(dakovalkov): https://github.com/yandex/ClickHouse/pull/7170.
        // case DB::TypeIndex::DateTime64:
        //     return SimpleLogicalType(ESimpleLogicalValueType::Timestamp);
        default:
            THROW_ERROR_EXCEPTION("ClickHouse type %Qv is not supported", type->getFamilyName());
    }
}

TTableSchema ConvertToTableSchema(const DB::ColumnsDescription& columns, const TKeyColumns& keyColumns)
{
    std::vector<TString> columnOrder;
    THashSet<TString> usedColumns;

    for (const auto& keyColumnName : keyColumns) {
        if (!columns.has(keyColumnName)) {
            THROW_ERROR_EXCEPTION("Column %Qv is specified as key column but is missing",
                keyColumnName);
        }
        columnOrder.emplace_back(keyColumnName);
        usedColumns.emplace(keyColumnName);
    }

    for (const auto& column : columns) {
        if (usedColumns.emplace(column.name).second) {
            columnOrder.emplace_back(column.name);
        }
    }

    std::vector<TColumnSchema> columnSchemas;
    columnSchemas.reserve(columnOrder.size());
    for (int index = 0; index < static_cast<int>(columnOrder.size()); ++index) {
        const auto& name = columnOrder[index];
        const auto& column = columns.get(name);
        const auto& type = RepresentClickHouseType(column.type);
        std::optional<ESortOrder> sortOrder;
        if (index < static_cast<int>(keyColumns.size())) {
            sortOrder = ESortOrder::Ascending;
        }
        columnSchemas.emplace_back(name, type, sortOrder);
    }

    return TTableSchema(columnSchemas);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
