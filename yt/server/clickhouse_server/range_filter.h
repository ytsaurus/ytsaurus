#pragma once

#include "table_schema.h"

#include <yt/server/clickhouse_server/public.h>

#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct IRangeFilter
{
    virtual ~IRangeFilter() = default;

    /// Returns True if the filter condition is feasible in the given key range.
    virtual bool CheckRange(
        const TValue* leftKey,
        const TValue* rightKey,
        size_t keySize) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRangeFilterPtr CreateRangeFilter(
    const DB::Context& context,
    const DB::SelectQueryInfo& queryInfo,
    const TClickHouseTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
