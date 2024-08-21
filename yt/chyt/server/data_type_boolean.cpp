#include "data_type_boolean.h"

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeCustom.h>

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

} // namespace NYT::NClickHouseServer
