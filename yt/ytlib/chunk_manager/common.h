#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"

#include "../logging/log.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TChunkManagerConfig
{
    TChunkManagerConfig()
        : HolderLeaseTimeout(TDuration::Seconds(60))
    { }

    TDuration HolderLeaseTimeout;    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT

