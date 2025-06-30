#pragma once

#include "private.h"
#include "chunk_pool.h"
#include "input_stream.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/core/logging/serializable_logger.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TOrderedChunkPoolOptions
{
    //! An upper bound for a total number of slices that is allowed. If this value
    //! is exceeded, an exception is thrown.
    i64 MaxTotalSliceCount;
    i64 MinTeleportChunkSize = 0;
    NControllerAgent::IJobSizeConstraintsPtr JobSizeConstraints;
    bool EnablePeriodicYielder = false;
    bool ShouldSliceByRowIndices = false;
    // COMPAT(apollo1321): remove in 25.2 release.
    bool UseNewSlicingImplementation = true;
    NLogging::TSerializableLogger Logger;
    TJobSizeAdjusterConfigPtr JobSizeAdjusterConfig;

    PHOENIX_DECLARE_TYPE(TOrderedChunkPoolOptions, 0xa7e43d2a);
};

////////////////////////////////////////////////////////////////////////////////


struct IOrderedChunkPool
    : public IPersistentChunkPool
{
    virtual std::vector<NChunkClient::TChunkTreeId> ArrangeOutputChunkTrees(
        const std::vector<std::pair<TOutputCookie, NChunkClient::TChunkTreeId>>& chunkTrees) const = 0;

    // For tests only.
    virtual std::vector<TOutputCookie> GetOutputCookiesInOrder() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOrderedChunkPool)

////////////////////////////////////////////////////////////////////////////////

IOrderedChunkPoolPtr CreateOrderedChunkPool(
    const TOrderedChunkPoolOptions& options,
    TInputStreamDirectory inputStreamDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
