#pragma once

#include "table_partition.h"
#include "private.h"

#include <yt/server/controller_agent/chunk_pools/chunk_stripe.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/logging/public.h>

#include <yt/client/ypath/rich.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TFetchResult
{
    std::vector<NChunkClient::TInputDataSlicePtr> DataSlices;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
    NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory;
};

TFetchResult FetchInput(
    NApi::NNative::IClientPtr client,
    std::vector<TString> inputTablePaths,
    const DB::KeyCondition* keyCondition,
    NTableClient::TRowBufferPtr rowBuffer,
    TSubqueryConfigPtr config);

NChunkPools::TChunkStripeListPtr BuildJobs(
    const std::vector<NChunkClient::TInputDataSlicePtr>& dataSlices,
    int jobCount);

TTablePartList SerializeAsTablePartList(
    const NChunkPools::TChunkStripeListPtr& chunkStripeListPtr,
    const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
