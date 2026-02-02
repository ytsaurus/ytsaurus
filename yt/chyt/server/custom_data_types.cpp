#include "custom_data_types.h"

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterDataTypeBoolean()
{
    auto& factory = DB::DataTypeFactory::instance();
    // Register YtBoolean as alias for backward compatibility.
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
        // YT Timestamp logical type stores timestamp in microseconds so scale of underlying Decimal is equal to 6.
        DB::DataTypePtr type = std::make_shared<DB::DataTypeDateTime64>(6);
        return std::pair(type, std::make_unique<DB::DataTypeCustomDesc>(
            std::make_unique<DB::DataTypeCustomFixedName>("YtTimestamp")));
    });
}

DB::DataTypePtr GetDataTypeTimestamp()
{
    auto& factory = DB::DataTypeFactory::instance();
    return factory.get("YtTimestamp");
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void RegisterDataTypeTzDate()
{
    auto& factory = DB::DataTypeFactory::instance();
    factory.registerSimpleDataTypeCustom("TzDate", [] {
        DB::DataTypePtr type = std::make_shared<DB::DataTypeDate>();
        return std::pair(type, std::make_unique<DB::DataTypeCustomDesc>(
            std::make_unique<DB::DataTypeCustomFixedName>("TzDate")));
    });
}

void RegisterDataTypeTzDatetime()
{
    auto& factory = DB::DataTypeFactory::instance();
    factory.registerSimpleDataTypeCustom("TzDatetime", [] {
        DB::DataTypePtr type = std::make_shared<DB::DataTypeDateTime>();
        return std::pair(type, std::make_unique<DB::DataTypeCustomDesc>(
            std::make_unique<DB::DataTypeCustomFixedName>("TzDatetime")));
    });
}

void RegisterDataTypeTzTimestamp()
{
    auto& factory = DB::DataTypeFactory::instance();
    factory.registerSimpleDataTypeCustom("TzTimestamp", [] {
        DB::DataTypePtr type = std::make_shared<DB::DataTypeDateTime64>(6);
        return std::pair(type, std::make_unique<DB::DataTypeCustomDesc>(
            std::make_unique<DB::DataTypeCustomFixedName>("TzTimestamp")));
    });
}

void RegisterDataTypeTzDate32()
{
    auto& factory = DB::DataTypeFactory::instance();
    factory.registerSimpleDataTypeCustom("TzDate32", [] {
        DB::DataTypePtr type = std::make_shared<DB::DataTypeDate32>();
        return std::pair(type, std::make_unique<DB::DataTypeCustomDesc>(
            std::make_unique<DB::DataTypeCustomFixedName>("TzDate32")));
    });
}

void RegisterDataTypeTzDateTime64()
{
    auto& factory = DB::DataTypeFactory::instance();
    factory.registerSimpleDataTypeCustom("TzDateTime64", [] {
        DB::DataTypePtr type = std::make_shared<DB::DataTypeDateTime64>(0);
        return std::pair(type, std::make_unique<DB::DataTypeCustomDesc>(
            std::make_unique<DB::DataTypeCustomFixedName>("TzDateTime64")));
    });
}

void RegisterDataTypeTzTimestamp64()
{
    auto& factory = DB::DataTypeFactory::instance();
    factory.registerSimpleDataTypeCustom("TzTimestamp64", [] {
        DB::DataTypePtr type = std::make_shared<DB::DataTypeDateTime64>(6);
        return std::pair(type, std::make_unique<DB::DataTypeCustomDesc>(
            std::make_unique<DB::DataTypeCustomFixedName>("TzTimestamp64")));
    });
}

} // namespace

void RegisterTzDataTypes()
{
    RegisterDataTypeTzDate();
    RegisterDataTypeTzDatetime();
    RegisterDataTypeTzTimestamp();
    RegisterDataTypeTzDate32();
    RegisterDataTypeTzDateTime64();
    RegisterDataTypeTzTimestamp64();
}



DB::DataTypePtr GetDataTypeTzDate()
{
    auto& factory = DB::DataTypeFactory::instance();
    return factory.get("TzDate");
}

DB::DataTypePtr GetDataTypeTzDatetime()
{
    auto& factory = DB::DataTypeFactory::instance();
    return factory.get("TzDatetime");
}

DB::DataTypePtr GetDataTypeTzTimestamp()
{
    auto& factory = DB::DataTypeFactory::instance();
    return factory.get("TzTimestamp");
}

DB::DataTypePtr GetDataTypeTzDate32()
{
    auto& factory = DB::DataTypeFactory::instance();
    return factory.get("TzDate32");
}

DB::DataTypePtr GetDataTypeTzDatetime64()
{
    auto& factory = DB::DataTypeFactory::instance();
    return factory.get("TzDateTime64");
}

DB::DataTypePtr GetDataTypeTzTimestamp64()
{
    auto& factory = DB::DataTypeFactory::instance();
    return factory.get("TzTimestamp64");
}

} // namespace NYT::NClickHouseServer
