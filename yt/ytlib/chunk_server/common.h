#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/logging/log.h>

#include <ytlib/chunk_client/common.h>
#include <ytlib/transaction_server/common.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

typedef i32 THolderId;
const i32 InvalidHolderId = -1;

typedef TGuid TChunkListId;
extern TChunkListId NullChunkListId;

using NChunkClient::TChunkId;
using NChunkClient::NullChunkId;

using NTransactionServer::TTransactionId;
using NTransactionServer::NullTransactionId;

typedef TGuid TChunkTreeId;
extern TChunkTreeId NullChunkTreeId;

extern ui64 ChunkIdSeed;
extern ui64 ChunkListIdSeed;

DECLARE_ENUM(EChunkTreeKind,
    (Chunk)
    (ChunkList)
);

EChunkTreeKind GetChunkTreeKind(const TChunkTreeId& treeId);

////////////////////////////////////////////////////////////////////////////////

struct TChunkManagerConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TChunkManagerConfig> TPtr;

    TDuration HolderLeaseTimeout;
    int MaxReplicationFanOut;
    int MaxReplicationFanIn;
    int MaxRemovalJobsPerHolder;
    TDuration ChunkRefreshDelay;
    TDuration ChunkRefreshQuantum;
    int MaxChunksPerRefresh;
    double MinChunkBalancingFillCoeffDiff;
    double MinChunkBalancingFillCoeff;
    double MaxHolderFillCoeff;
    i64 MinHolderFreeSpace;
    double ActiveSessionsPenalityCoeff;
    TDuration MaxJobDuration;

    TChunkManagerConfig()
    {
        Register("holder_lease_timeout", HolderLeaseTimeout).Default(TDuration::Seconds(10));
        Register("max_replication_fan_out", MaxReplicationFanOut).Default(4);
        Register("max_replication_fan_in", MaxReplicationFanIn).Default(8);
        Register("max_removal_jobs_per_holder", MaxRemovalJobsPerHolder).Default(16);
        Register("chunk_refresh_delay", ChunkRefreshDelay).Default(TDuration::Seconds(15));
        Register("chunk_refresh_quantum", ChunkRefreshQuantum).Default(TDuration::MilliSeconds(100));
        Register("max_chunks_rer_refresh", MaxChunksPerRefresh).Default(1000);
        Register("min_chunk_balancing_fill_coeff_diff", MinChunkBalancingFillCoeffDiff).Default(0.2);
        Register("min_chunk_balancing_fill_coeff", MinChunkBalancingFillCoeff).Default(0.1);
        Register("max_holder_fill_coeff", MaxHolderFillCoeff).Default(0.99);
        Register("min_holder_free_space", MinHolderFreeSpace).Default(10 * 1024 * 1024); // 10MB
        Register("active_sessions_penality_coeff", ActiveSessionsPenalityCoeff).Default(0.1);
        Register("max_job_duration", MaxJobDuration).Default(TDuration::Minutes(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
