#pragma once

#include <yt/server/clickhouse_server/interop/api.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/core/logging/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

// Tables should have identical schemas (native and YQL) and types (static/dynamic)

NInterop::TTablePartList PartitionTables(
    NApi::NNative::IClientPtr client,
    std::vector<TString> tables,
    NInterop::IRangeFilterPtr rangeFilter,
    size_t numParts = 1);

} // namespace NClickHouse
} // namespace NYT
