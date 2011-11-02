#pragma once

#include "../misc/common.h"

#include "../logging/log.h"

#include "../chunk_holder/common.h"
#include "../chunk_holder/replicator.h"

#include "../transaction_manager/common.h"
#include "../transaction_manager/transaction.h"
#include "../transaction_manager/transaction_manager.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

using NChunkHolder::THolderStatistics;
using NChunkHolder::EJobState;
using NChunkHolder::EJobType;
using NChunkHolder::TJobId;

typedef int THolderId;
const int InvalidHolderId = -1;

typedef TGuid TChunkListId;
extern TChunkListId NullChunkListId;

////////////////////////////////////////////////////////////////////////////////

struct TChunkManagerConfig
{
    TChunkManagerConfig()
        : HolderLeaseTimeout(TDuration::Seconds(10))
    { }

    TDuration HolderLeaseTimeout;
};

// TODO: make configurable
extern int MaxReplicationFanOut;
extern int MaxReplicationFanIn;
extern int MaxRemovalJobsPerHolder;
extern TDuration ChunkRefreshDelay;
extern TDuration ChunkRefreshQuantum;
extern int MaxChunksPerRefresh;
extern double MinChunkBalancingFillCoeffDiff;
extern double MinChunkBalancingFillCoeff;
extern double MaxHolderFillCoeff;
extern i64 MinHolderFreeSpace;
extern double ActiveSessionsPenalityCoeff;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

