#pragma once

#include "../misc/common.h"
#include "../misc/configurable.h"
#include "../logging/log.h"
#include "../transaction_server/id.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

using NTransactionServer::TTransactionId;
using NTransactionServer::NullTransactionId;

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

