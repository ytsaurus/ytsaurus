#include "db_helpers.h"

#include "type_helpers.h"

#include <yt/server/clickhouse_server/native/table_schema.h>
#include <yt/server/clickhouse_server/native/value.h>

#include <DataTypes/DataTypeFactory.h>

#include <IO/WriteHelpers.h>

namespace DB {

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}

}   // namespace DB

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

const char* GetTypeName(const NNative::TColumn& column)
{
    switch (column.Type) {
        /// Invalid type.
        case NNative::EClickHouseColumnType::Invalid:
            break;

        /// Signed integer value.
        case NNative::EClickHouseColumnType::Int8:     return "Int8";
        case NNative::EClickHouseColumnType::Int16:    return "Int16";
        case NNative::EClickHouseColumnType::Int32:    return "Int32";
        case NNative::EClickHouseColumnType::Int64:    return "Int64";

        /// Unsigned integer value.
        case NNative::EClickHouseColumnType::UInt8:    return "UInt8";
        case NNative::EClickHouseColumnType::UInt16:   return "UInt16";
        case NNative::EClickHouseColumnType::UInt32:   return "UInt32";
        case NNative::EClickHouseColumnType::UInt64:   return "UInt64";

        /// Floating point value.
        case NNative::EClickHouseColumnType::Float:    return "Float32";
        case NNative::EClickHouseColumnType::Double:   return "Float64";

        /// Boolean value.
        case NNative::EClickHouseColumnType::Boolean:  return "UInt8";

        /// DateTime value.
        case NNative::EClickHouseColumnType::Date:     return "Date";
        case NNative::EClickHouseColumnType::DateTime: return "DateTime";

        /// String value.
        case NNative::EClickHouseColumnType::String:   return "String";
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

DB::NamesAndTypesList GetTableColumns(const NNative::TTable& table)
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

void GetField(const NNative::TValue& value, std::vector<Field>& fields)
{
    switch (value.Type) {
        case NNative::EClickHouseValueType::Null:
            fields.emplace_back();
            return;

        case NNative::EClickHouseValueType::Int:
            fields.emplace_back(static_cast<Int64>(value.Int));
            return;

        case NNative::EClickHouseValueType::UInt:
            fields.emplace_back(static_cast<UInt64>(value.UInt));
            return;

        case NNative::EClickHouseValueType::Float:
            fields.emplace_back(value.Float);
            return;

        case NNative::EClickHouseValueType::Boolean:
            fields.emplace_back(static_cast<UInt64>(value.Boolean ? 1 : 0));
            return;

        case NNative::EClickHouseValueType::String:
            fields.emplace_back(value.String, value.Length);
            return;
    }

    throw Exception(
        "Invalid value type",
        toString(static_cast<int>(value.Type)),
        ErrorCodes::UNKNOWN_TYPE);
}

std::vector<Field> GetFields(const NNative::TValue* values, size_t count)
{
    std::vector<Field> fields;
    fields.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        GetField(values[i], fields);
    }

    return fields;
}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
