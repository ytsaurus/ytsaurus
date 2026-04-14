#pragma once

#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterDataTypeBoolean();
DB::DataTypePtr GetDataTypeBoolean();

////////////////////////////////////////////////////////////////////////////////

void RegisterDataTypeTimestamp();
DB::DataTypePtr GetDataTypeTimestamp();

////////////////////////////////////////////////////////////////////////////////

void RegisterTzDataTypes();

DB::DataTypePtr GetDataTypeTzDate();
DB::DataTypePtr GetDataTypeTzDatetime();
DB::DataTypePtr GetDataTypeTzTimestamp();
DB::DataTypePtr GetDataTypeTzDate32();
DB::DataTypePtr GetDataTypeTzDatetime64();
DB::DataTypePtr GetDataTypeTzTimestamp64();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
