    #pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TJobSchedulerConfig
    : public TConfigurable
{
    //! Minimum number of holders the cell must have online to enable starting new jobs.
    TNullable<int> MinOnlineHolderCount;
    //! Max lost chunk fraction the cell is allowed to have to enable starting new jobs.
    TNullable<double> MaxLostChunkFraction;
    //! Maximum number of upload targets during replication and balancing.
    int MaxReplicationFanOut;
    //! Maximum number of incoming upload sessions during replication and balancing.
    int MaxReplicationFanIn;
    //! Maximum number of concurrent removal jobs that can be scheduled to a holder.
    int MaxRemovalJobsPerHolder;
    //! Minimum difference in fill coefficient (between the most and the least loaded holders) to start balancing.
    double MinBalancingFillCoeffDiff;
    //! Minimum fill coefficient of the most loaded holder to start balancing.
    double MinBalancingFillCoeff;
    //! Maximum duration a job can run before it is considered dead.
    TDuration JobTimeout;

    TJobSchedulerConfig()
    {
        Register("min_online_holder_count", MinOnlineHolderCount)
            .GreaterThan(0);
        Register("max_lost_chunk_fraction", MaxLostChunkFraction)
            .InRange(0.0, 1.0);
        Register("max_replication_fan_out", MaxReplicationFanOut)
            .Default(4);
        Register("max_replication_fan_in", MaxReplicationFanIn)
            .Default(8);
        Register("max_removal_jobs_per_holder", MaxRemovalJobsPerHolder)
            .Default(16);
        Register("min_chunk_balancing_fill_coeff_diff", MinBalancingFillCoeffDiff)
            .Default(0.2);
        Register("min_chunk_balancing_fill_coeff", MinBalancingFillCoeff)
            .Default(0.1);
        Register("job_timeout", JobTimeout)
            .Default(TDuration::Minutes(5));
    }
};

struct TChunkManagerConfig
    : public TConfigurable
{
    TDuration OnlineHolderTimeout;
    TDuration RegisteredHolderTimeout;
    TDuration UnconfirmedHolderTimeout;
    TDuration HolderExpirationBackoffTime;

    TDuration ChunkRefreshDelay;
    TDuration ChunkRefreshQuantum;
    int MaxChunksPerRefresh;

    double MaxHolderFillCoeff;
    i64 MinHolderFreeSpace;
    double ActiveSessionsPenalityCoeff;
    TJobSchedulerConfigPtr Jobs;

    i32 MaxChunkTreeRank;
    i32 MinChunkListSize;
    i32 MaxChunkListSize;

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
        Register("chunk_refresh_delay", ChunkRefreshDelay)
            .Default(TDuration::Seconds(15));
        Register("chunk_refresh_quantum", ChunkRefreshQuantum)
            .Default(TDuration::MilliSeconds(100));
        Register("max_chunks_per_refresh", MaxChunksPerRefresh)
            .Default(10000);
        Register("active_sessions_penality_coeff", ActiveSessionsPenalityCoeff)
            .Default(0.1);
        Register("jobs", Jobs)
            .DefaultNew();
        Register("max_chunk_tree_rank", MaxChunkTreeRank)
            .GreaterThan(2)
            .Default(10);
        Register("min_chunk_list_size", MinChunkListSize)
            .GreaterThan(0)
            .Default(1024);
        Register("min_chunk_list_size", MaxChunkListSize)
            .GreaterThan(0)
            .Default(2048);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
