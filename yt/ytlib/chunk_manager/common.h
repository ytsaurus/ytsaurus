#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"

#include "../logging/log.h"

#include "../chunk_holder/common.h"
#include "../chunk_holder/replicator.h"

#include "../transaction/common.h"
#include "../transaction/transaction.h"
#include "../transaction/transaction_manager.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

using NTransaction::TTransactionId;
using NTransaction::TTransaction;
using NTransaction::TTransactionManager;

using NChunkHolder::THolderStatistics;
using NChunkHolder::EJobState;
using NChunkHolder::EJobType;
using NChunkHolder::TJobId;

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
extern double MinChunkBalancingLoadFactorDiff;
extern double MinChunkBalancingLoadFactor;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT

