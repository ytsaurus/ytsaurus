#pragma once

#include "table_schema.h"

#include <yt/server/clickhouse_server/native/public.h>

#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

NNative::IRangeFilterPtr CreateRangeFilter(
    const DB::Context& context,
    const DB::SelectQueryInfo& queryInfo,
    const TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
