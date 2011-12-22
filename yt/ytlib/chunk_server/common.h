#pragma once

#include "../misc/common.h"

#include "../logging/log.h"

#include "../chunk_holder/common.h"
#include "../chunk_holder/job_executor.h"

#include "../transaction_server/common.h"
#include "../transaction_server/transaction.h"
#include "../transaction_server/transaction_manager.h"

#include "../misc/configurable.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

typedef i32 THolderId;
const i32 InvalidHolderId = -1;

typedef TGuid TChunkListId;
extern TChunkListId NullChunkListId;

////////////////////////////////////////////////////////////////////////////////

struct TChunkManagerConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TChunkManagerConfig> TPtr;

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

