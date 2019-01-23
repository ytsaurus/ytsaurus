#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>

#include <string>
#include <vector>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const char* GetTypeName(const TColumn& column);

DB::DataTypePtr GetDataType(const std::string& name);

DB::NamesAndTypesList GetTableColumns(const TTable& table);

std::vector<DB::Field> GetFields(const TValue* values, size_t count);

} // namespace NYT::NClickHouseServer
