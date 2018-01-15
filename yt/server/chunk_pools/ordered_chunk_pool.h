#pragma once

#include "private.h"
#include "chunk_pool.h"
#include "input_stream.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TOrderedChunkPoolOptions
{
    //! An upper bound for a total number of slices that is allowed. If this value
    //! is exceeded, an exception is thrown.
    i64 MaxTotalSliceCount;
    i64 MinTeleportChunkSize = 0;
    bool SupportLocality = false;
    NControllerAgent::IJobSizeConstraintsPtr JobSizeConstraints;
    NScheduler::TOperationId OperationId;
    bool EnablePeriodicYielder = false;
    bool KeepOutputOrder = false;
    bool ShouldSliceByRowIndices = false;
    TString Task;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkPool> CreateOrderedChunkPool(
    const TOrderedChunkPoolOptions& options,
    TInputStreamDirectory inputStreamDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
