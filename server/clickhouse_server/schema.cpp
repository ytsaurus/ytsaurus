#include "schema.h"

#include <yt/client/table_client/schema.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
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

DB::DataTypePtr RepresentYtType(const TLogicalTypePtr& valueType)
{
    // TODO(max42): CHYT-140.
    if (!SimplifyLogicalType(valueType).first) {
        // This is an ultimately rich type (like optional<optional<...>> or list<...> etc).
        // It is physically represented as Any, so we currently treat it as string.
        return std::make_shared<DB::DataTypeString>();
    }

    switch (valueType->GetMetatype()) {
        case ELogicalMetatype::Optional:
            return std::make_shared<DB::DataTypeNullable>(RepresentYtType(valueType->GetElement()));
        case ELogicalMetatype::Simple: {
            auto simpleLogicalType = valueType->AsSimpleTypeRef().GetElement();
            switch (simpleLogicalType) {
                case ESimpleLogicalValueType::Int64:
                case ESimpleLogicalValueType::Interval:
                    return std::make_shared<DB::DataTypeInt64>();
                case ESimpleLogicalValueType::Int32:
                    return std::make_shared<DB::DataTypeInt32>();
                case ESimpleLogicalValueType::Int16:
                    return std::make_shared<DB::DataTypeInt16>();
                case ESimpleLogicalValueType::Int8:
                    return std::make_shared<DB::DataTypeInt8>();

                case ESimpleLogicalValueType::Uint64:
                    return std::make_shared<DB::DataTypeUInt64>();
                case ESimpleLogicalValueType::Uint32:
                    return std::make_shared<DB::DataTypeUInt32>();
                case ESimpleLogicalValueType::Uint16:
                    return std::make_shared<DB::DataTypeUInt16>();
                case ESimpleLogicalValueType::Uint8:
                case ESimpleLogicalValueType::Boolean:
                    return std::make_shared<DB::DataTypeUInt8>();

                case ESimpleLogicalValueType::Double:
                    return std::make_shared<DB::DataTypeFloat64>();

                case ESimpleLogicalValueType::String:
                case ESimpleLogicalValueType::Utf8:
                case ESimpleLogicalValueType::Any:
                    return std::make_shared<DB::DataTypeString>();

                case ESimpleLogicalValueType::Date:
                    return std::make_shared<DB::DataTypeDate>();

                case ESimpleLogicalValueType::Datetime:
                    return std::make_shared<DB::DataTypeDateTime>();

                case ESimpleLogicalValueType::Timestamp:
                    // TODO(dakovalkov): https://github.com/yandex/ClickHouse/pull/7170.
                    // return std::make_shared<DB::DataTypeDateTime>();
                    return std::make_shared<DB::DataTypeUInt64>();

                default:
                    THROW_ERROR_EXCEPTION("YT value type %Qlv is not supported", simpleLogicalType);
            }
        }
        default:
            THROW_ERROR_EXCEPTION("YT metatype %Qlv is not supported", valueType->GetMetatype());
    }
}

DB::DataTypes ToDataTypes(const NTableClient::TTableSchema& schema)
{
    DB::DataTypes result;
    result.reserve(schema.GetColumnCount());

    for (const auto& column : schema.Columns()) {
        result.emplace_back(RepresentYtType(column.LogicalType()));
    }

    return result;
}

DB::NamesAndTypesList ToNamesAndTypesList(const NTableClient::TTableSchema& schema)
{
    const auto& dataTypes = ToDataTypes(schema);

    DB::NamesAndTypesList result;

    for (int index = 0; index < schema.GetColumnCount(); ++index) {
        result.emplace_back(schema.Columns()[index].Name(), dataTypes[index]);
    }

    return result;
}

DB::Block ToHeaderBlock(const TTableSchema& schema)
{
    DB::Block headerBlock;

    auto namesAndTypesList = ToNamesAndTypesList(schema);

    for (const auto& nameAndTypePair : namesAndTypesList) {
        auto column = nameAndTypePair.type->createColumn();
        headerBlock.insert({ std::move(column), nameAndTypePair.type, nameAndTypePair.name });
    }

    return headerBlock;
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
