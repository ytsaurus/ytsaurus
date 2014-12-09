#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/error.h>

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! If |true| then replicator is disabled regardless of anything else.
    bool DisableChunkReplicator;

    //! When the number of online nodes drops below this margin,
    //! replicator gets disabled.
    TNullable<int> SafeOnlineNodeCount;

    //! When the fraction of lost chunks grows above this margin,
    //! replicator gets disabled.
    TNullable<double> SafeLostChunkFraction;

    //! When the number of lost chunks grows above this margin,
    //! replicator gets disabled.
    TNullable<int> SafeLostChunkCount;

    //! Minimum difference in fill coefficient (between the most and the least loaded nodes) to start balancing.
    double MinBalancingFillFactorDiff;

    //! Minimum fill coefficient of the most loaded node to start balancing.
    double MinBalancingFillFactor;

    //! Maximum duration a job can run before it is considered dead.
    TDuration JobTimeout;

    //! Maximum total size of chunks assigned for replication (per node).
    i64 MaxReplicationJobsSize;

    //! Maximum number of replication/balancing jobs writing to each target node.
    /*!
     *  This limit is approximate and is only maintained when scheduling balancing jobs.
     *  This makes sense since balancing jobs specifically target nodes with lowest fill factor
     *  and thus risk overloading them.
     *  Replication jobs distribute data evenly across he cluster and thus pose no threat.
     */
    int MaxReplicationWriteSessions;

    //! Maximum total size of chunks assigned for repair (per node).
    i64 MaxRepairJobsSize;

    //! Memory usage assigned to every repair job.
    i64 RepairJobMemoryUsage;

    //! Graceful delay before chunk refresh.
    TDuration ChunkRefreshDelay;

    //! Interval between consequent chunk refresh scans.
    TDuration ChunkRefreshPeriod;

    //! Maximum number of chunks to process during a refresh scan.
    int MaxChunksPerRefresh;

    //! Interval between consequent chunk properties update scans.
    TDuration ChunkPropertiesUpdatePeriod;

    //! Maximum number of chunks to process during a properties update scan.
    int MaxChunksPerPropertiesUpdate;

    //! Maximum number of cached replicas to be returned on fetch request.
    int MaxCachedReplicasPerFetch;

    TChunkManagerConfig()
    {
        RegisterParameter("disable_chunk_replicator", DisableChunkReplicator)
            .Default(false);
        RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("safe_lost_chunk_fraction", SafeLostChunkFraction)
            .InRange(0.0, 1.0)
            .Default(0.5);
        RegisterParameter("safe_lost_chunk_count", SafeLostChunkCount)
            .GreaterThan(0)
            .Default(1000);

        RegisterParameter("min_chunk_balancing_fill_factor_diff", MinBalancingFillFactorDiff)
            .InRange(0.0, 1.0)
            .Default(0.2);
        RegisterParameter("min_chunk_balancing_fill_factor", MinBalancingFillFactor)
            .InRange(0.0, 1.0)
            .Default(0.1);

        RegisterParameter("job_timeout", JobTimeout)
            .Default(TDuration::Minutes(5));

        RegisterParameter("max_replication_jobs_size", MaxReplicationJobsSize)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThanOrEqual(0);
        RegisterParameter("max_replication_write_sessions", MaxReplicationWriteSessions)
            .GreaterThanOrEqual(1)
            .Default(128);
        RegisterParameter("max_repair_jobs_size", MaxRepairJobsSize)
            .Default((i64) 4 * 1024 * 1024 * 1024)
            .GreaterThanOrEqual(0);

        RegisterParameter("repair_job_memory_usage", RepairJobMemoryUsage)
            .Default((i64) 256 * 1024 * 1024)
            .GreaterThanOrEqual(0);

        RegisterParameter("chunk_refresh_delay", ChunkRefreshDelay)
            .Default(TDuration::Seconds(15));
        RegisterParameter("chunk_refresh_period", ChunkRefreshPeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("max_chunks_per_refresh", MaxChunksPerRefresh)
            .Default(10000);

        RegisterParameter("chunk_properties_update_period", ChunkPropertiesUpdatePeriod)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("max_chunks_per_properties_update", MaxChunksPerPropertiesUpdate)
            .Default(10000);

        RegisterParameter("max_cached_replicas_per_fetch", MaxCachedReplicasPerFetch)
            .GreaterThanOrEqual(0)
            .Default(20);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
