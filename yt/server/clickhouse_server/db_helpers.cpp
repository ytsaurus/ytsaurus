#include "db_helpers.h"

#include "type_helpers.h"

#include "table.h"
#include "value.h"

#include <DataTypes/DataTypeFactory.h>

#include <IO/WriteHelpers.h>

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

const char* GetTypeName(const TColumn& column)
{
    switch (column.Type) {
        /// Invalid type.
        case EClickHouseColumnType::Invalid:
            break;

        /// Signed integer value.
        case EClickHouseColumnType::Int8:     return "Int8";
        case EClickHouseColumnType::Int16:    return "Int16";
        case EClickHouseColumnType::Int32:    return "Int32";
        case EClickHouseColumnType::Int64:    return "Int64";

        /// Unsigned integer value.
        case EClickHouseColumnType::UInt8:    return "UInt8";
        case EClickHouseColumnType::UInt16:   return "UInt16";
        case EClickHouseColumnType::UInt32:   return "UInt32";
        case EClickHouseColumnType::UInt64:   return "UInt64";

        /// Floating point value.
        case EClickHouseColumnType::Float:    return "Float32";
        case EClickHouseColumnType::Double:   return "Float64";

        /// Boolean value.
        case EClickHouseColumnType::Boolean:  return "UInt8";

        /// DateTime value.
        case EClickHouseColumnType::Date:     return "Date";
        case EClickHouseColumnType::DateTime: return "DateTime";

        /// String value.
        case EClickHouseColumnType::String:   return "String";
    }

    throw Exception(
        "Invalid column type",
        toString(static_cast<int>(column.Type)),
        ErrorCodes::UNKNOWN_TYPE);
}

DB::DataTypePtr GetDataType(const std::string& name)
{
    return DB::DataTypeFactory::instance().get(name);
}

DB::NamesAndTypesList GetTableColumns(const TTable& table)
{
    const auto& dataTypeFactory = DB::DataTypeFactory::instance();

    DB::NamesAndTypesList columns;

    for (const auto& column : table.Columns) {
        std::string name = ToStdString(column.Name);
        DB::DataTypePtr type = dataTypeFactory.get(GetTypeName(column));
        columns.emplace_back(std::move(name), std::move(type));
    }

    return columns;
}

void GetField(const TValue& value, std::vector<Field>& fields)
{
    switch (value.Type) {
        case EClickHouseValueType::Null:
            fields.emplace_back();
            return;

        case EClickHouseValueType::Int:
            fields.emplace_back(static_cast<Int64>(value.Int));
            return;

        case EClickHouseValueType::UInt:
            fields.emplace_back(static_cast<UInt64>(value.UInt));
            return;

        case EClickHouseValueType::Float:
            fields.emplace_back(value.Float);
            return;

        case EClickHouseValueType::Boolean:
            fields.emplace_back(static_cast<UInt64>(value.Boolean ? 1 : 0));
            return;

        case EClickHouseValueType::String:
            fields.emplace_back(value.String, value.Length);
            return;
    }

    throw Exception(
        "Invalid value type",
        toString(static_cast<int>(value.Type)),
        ErrorCodes::UNKNOWN_TYPE);
}

std::vector<Field> GetFields(const TValue* values, size_t count)
{
    std::vector<Field> fields;
    fields.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        GetField(values[i], fields);
    }

    return fields;
}

} // namespace NYT::NClickHouseServer
