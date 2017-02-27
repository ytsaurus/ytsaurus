#pragma once

#include "chunk_pool.h"

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

struct TSortedChunkPoolOptions
{
    bool EnableKeyGuarantee;
    int PrimaryPrefixLength = 0;
    int ForeignPrefixLength = 0;
    i64 MinTeleportChunkSize = 0;
    i64 MaxTotalSliceCount = 0;
    bool SupportLocality = false;
    IJobSizeConstraintsPtr JobSizeConstraints;

    TOperationId OperationId;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkPool> CreateSortedChunkPool(
    const TSortedChunkPoolOptions& options,
    std::function<NYT::NTableClient::IChunkSliceFetcherPtr()> ChunkSliceFetcherFactory,
    std::vector<TDataSource> sources);

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
