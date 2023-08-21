#pragma once

#include "private.h"
#include "chunk_pool.h"
#include "input_stream.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/job_tracker_client/public.h>

#include <yt/yt/core/logging/serializable_logger.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TOrderedChunkPoolOptions
{
    //! An upper bound for a total number of slices that is allowed. If this value
    //! is exceeded, an exception is thrown.
    i64 MaxTotalSliceCount;
    i64 MinTeleportChunkSize = 0;
    bool SupportLocality = false;
    NControllerAgent::IJobSizeConstraintsPtr JobSizeConstraints;
    bool EnablePeriodicYielder = false;
    bool KeepOutputOrder = false;
    bool ShouldSliceByRowIndices = false;
    NLogging::TSerializableLogger Logger;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

IPersistentChunkPoolPtr CreateOrderedChunkPool(
    const TOrderedChunkPoolOptions& options,
    TInputStreamDirectory inputStreamDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
