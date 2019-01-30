#pragma once

#include "table_partition.h"
#include "public_ch.h"

#include <yt/server/controller_agent/chunk_pools/chunk_stripe.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/logging/public.h>

#include <yt/client/ypath/rich.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// Tables should have identical schemas (native and YQL) and types (static/dynamic)

TTablePartList BuildJobs(
    NApi::NNative::IClientPtr client,
    std::vector<TString> inputTablePaths,
    const DB::KeyCondition* keyCondition,
    int jobCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
