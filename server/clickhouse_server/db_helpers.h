#pragma once

#include "private.h"

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>

#include <string>
#include <vector>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const char* GetTypeName(const TClickHouseColumn& column);

DB::DataTypePtr GetDataType(const std::string& name);

DB::NamesAndTypesList GetTableColumns(const TClickHouseTable& table);

std::vector<DB::Field> GetFields(const TValue* values, size_t count);

} // namespace NYT::NClickHouseServer
