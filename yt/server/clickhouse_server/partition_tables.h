#pragma once

#include "table_partition.h"
#include "public_ch.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/core/logging/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// Tables should have identical schemas (native and YQL) and types (static/dynamic)

TTablePartList PartitionTables(
    NApi::NNative::IClientPtr client,
    std::vector<TString> tables,
    const DB::KeyCondition* keyCondition,
    size_t numParts = 1);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
