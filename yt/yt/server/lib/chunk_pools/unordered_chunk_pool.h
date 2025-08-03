#pragma once

#include "input_stream.h"
#include "private.h"

#include <yt/yt/server/lib/controller_agent/public.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/core/logging/serializable_logger.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TUnorderedChunkPoolOptions
{
    TJobSizeAdjusterConfigPtr JobSizeAdjusterConfig = nullptr;
    NControllerAgent::IJobSizeConstraintsPtr JobSizeConstraints = nullptr;
    //! Minimum uncompressed size to be teleported.
    i64 MinTeleportChunkSize = std::numeric_limits<i64>::max() / 4;
    //! Minimum data weight to be teleported.
    i64 MinTeleportChunkDataWeight = std::numeric_limits<i64>::max() / 4;
    bool SliceErasureChunksByParts = false;
    // TODO(max42): YT-13335.
    NTableClient::TRowBufferPtr RowBuffer;
    NLogging::TSerializableLogger Logger;
    NScheduler::ESingleChunkTeleportStrategy SingleChunkTeleportStrategy = NScheduler::ESingleChunkTeleportStrategy::Disabled;
    // COMPAT(apollo1321): Remove in 25.2.
    bool UseNewSlicingImplementation = true;
};

IPersistentChunkPoolPtr CreateUnorderedChunkPool(
    const TUnorderedChunkPoolOptions& options,
    TInputStreamDirectory dataSourceDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
