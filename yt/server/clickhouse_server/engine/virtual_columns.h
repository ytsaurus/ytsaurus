#pragma once

#include <yt/server/clickhouse_server/native/system_columns.h>

#include <Core/NamesAndTypes.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

const DB::NamesAndTypesList& ListSystemVirtualColumns();

NNative::TSystemColumns GetSystemColumns(const DB::Names& names);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
