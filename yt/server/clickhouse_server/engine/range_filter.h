#pragma once

#include "table_schema.h"

#include <yt/server/clickhouse_server/interop/api.h>

#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::IRangeFilterPtr CreateRangeFilter(
    const DB::Context& context,
    const DB::SelectQueryInfo& queryInfo,
    const TTableSchema& schema);

}   // namespace NClickHouse
}   // namespace NYT
