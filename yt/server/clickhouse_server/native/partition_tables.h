#pragma once

#include "range_filter.h"
#include "table_partition.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/core/logging/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

// Tables should have identical schemas (native and YQL) and types (static/dynamic)

TTablePartList PartitionTables(
    NApi::NNative::IClientPtr client,
    std::vector<TString> tables,
    IRangeFilterPtr rangeFilter,
    size_t numParts = 1);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
