#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkManagerConfig
    : public TConfigurable
{
    TDuration OnlineHolderTimeout;
    TDuration RegisteredHolderTimeout;
    TDuration UnconfirmedHolderTimeout;
    TDuration HolderExpirationBackoffTime;
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
    TDuration JobTimeout;

    TChunkManagerConfig()
    {
        Register("online_holder_timeout", OnlineHolderTimeout)
            .Default(TDuration::Seconds(10));
        Register("registered_holder_timeout", RegisteredHolderTimeout)
            .Default(TDuration::Seconds(60));
        Register("unconfirmed_holder_timeout", UnconfirmedHolderTimeout)
            .Default(TDuration::Seconds(30));
        Register("holder_expiration_backoff_time", HolderExpirationBackoffTime)
            .Default(TDuration::Seconds(5));
        Register("max_replication_fan_out", MaxReplicationFanOut)
            .Default(4);
        Register("max_replication_fan_in", MaxReplicationFanIn)
            .Default(8);
        Register("max_removal_jobs_per_holder", MaxRemovalJobsPerHolder)
            .Default(16);
        Register("chunk_refresh_delay", ChunkRefreshDelay)
            .Default(TDuration::Seconds(15));
        Register("chunk_refresh_quantum", ChunkRefreshQuantum)
            .Default(TDuration::MilliSeconds(100));
        Register("max_chunks_per_refresh", MaxChunksPerRefresh)
            .Default(10000);
        Register("min_chunk_balancing_fill_coeff_diff", MinChunkBalancingFillCoeffDiff)
            .Default(0.2);
        Register("min_chunk_balancing_fill_coeff", MinChunkBalancingFillCoeff)
            .Default(0.1);
        Register("active_sessions_penality_coeff", ActiveSessionsPenalityCoeff)
            .Default(0.1);
        Register("job_timeout", JobTimeout)
            .Default(TDuration::Minutes(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
