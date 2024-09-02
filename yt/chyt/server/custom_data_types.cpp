#include "custom_data_types.h"

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterDataTypeBoolean()
{
    auto& factory = DB::DataTypeFactory::instance();
    // Register YtBoolean as alias for backward compatibility
    factory.registerAlias("YtBoolean", "Bool");
}

DB::DataTypePtr GetDataTypeBoolean()
{
    auto& factory = DB::DataTypeFactory::instance();
    return factory.get("Bool");
}

////////////////////////////////////////////////////////////////////////////////

void RegisterDataTypeTimestamp()
{
    auto& factory = DB::DataTypeFactory::instance();
    factory.registerSimpleDataTypeCustom("YtTimestamp", [] {
        // YT Timestamp logical type stores timestamp in microseconds so scale of underlying Decimal is equal to 6
        DB::DataTypePtr type = std::make_shared<DB::DataTypeDateTime64>(6);
        return std::make_pair(type, std::make_unique<DB::DataTypeCustomDesc>(
            std::make_unique<DB::DataTypeCustomFixedName>("YtTimestamp")));
    });
}

DB::DataTypePtr GetDataTypeTimestamp()
{
    auto& factory = DB::DataTypeFactory::instance();
    return factory.get("YtTimestamp");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
