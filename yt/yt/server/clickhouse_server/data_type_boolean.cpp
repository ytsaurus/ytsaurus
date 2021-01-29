#include "data_type_boolean.h"

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeCustom.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterDataTypeBoolean()
{
    auto& factory = DB::DataTypeFactory::instance();
    // Boolean type is represented as UInt8.
    factory.registerSimpleDataTypeCustom(
        "YtBoolean",
        [] {
            return std::make_pair(
                std::make_shared<DB::DataTypeUInt8>(),
                std::make_unique<DB::DataTypeCustomDesc>(
                    std::make_unique<DB::DataTypeCustomFixedName>("YtBoolean")));
        });
}

DB::DataTypePtr GetDataTypeBoolean()
{
    auto& factory = DB::DataTypeFactory::instance();
    return factory.get("YtBoolean");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
