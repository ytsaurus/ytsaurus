#include "db_helpers.h"

#include "type_helpers.h"

#include <DataTypes/DataTypeFactory.h>

#include <IO/WriteHelpers.h>

namespace DB {

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}

}   // namespace DB

namespace NYT {
namespace NClickHouse {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

const char* GetTypeName(const NInterop::TColumn& column)
{
    switch (column.Type) {
        /// Invalid type.
        case NInterop::EColumnType::Invalid:
            break;

        /// Signed integer value.
        case NInterop::EColumnType::Int8:     return "Int8";
        case NInterop::EColumnType::Int16:    return "Int16";
        case NInterop::EColumnType::Int32:    return "Int32";
        case NInterop::EColumnType::Int64:    return "Int64";

        /// Unsigned integer value.
        case NInterop::EColumnType::UInt8:    return "UInt8";
        case NInterop::EColumnType::UInt16:   return "UInt16";
        case NInterop::EColumnType::UInt32:   return "UInt32";
        case NInterop::EColumnType::UInt64:   return "UInt64";

        /// Floating point value.
        case NInterop::EColumnType::Float:    return "Float32";
        case NInterop::EColumnType::Double:   return "Float64";

        /// Boolean value.
        case NInterop::EColumnType::Boolean:  return "UInt8";

        /// DateTime value.
        case NInterop::EColumnType::Date:     return "Date";
        case NInterop::EColumnType::DateTime: return "DateTime";

        /// String value.
        case NInterop::EColumnType::String:   return "String";
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

DB::NamesAndTypesList GetTableColumns(const NInterop::TTable& table)
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

void GetField(const NInterop::TValue& value, std::vector<Field>& fields)
{
    switch (value.Type) {
        case NInterop::EValueType::Null:
            fields.emplace_back();
            return;

        case NInterop::EValueType::Int:
            fields.emplace_back(value.Int);
            return;

        case NInterop::EValueType::UInt:
            fields.emplace_back(value.UInt);
            return;

        case NInterop::EValueType::Float:
            fields.emplace_back(value.Float);
            return;

        case NInterop::EValueType::Boolean:
            fields.emplace_back(static_cast<UInt64>(value.Boolean ? 1 : 0));
            return;

        case NInterop::EValueType::String:
            fields.emplace_back(value.String, value.Length);
            return;
    }

    throw Exception(
        "Invalid value type",
        toString(static_cast<int>(value.Type)),
        ErrorCodes::UNKNOWN_TYPE);
}

std::vector<Field> GetFields(const NInterop::TValue* values, size_t count)
{
    std::vector<Field> fields;
    fields.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        GetField(values[i], fields);
    }

    return fields;
}

}   // namespace NClickHouse
}   // namespace NYT
