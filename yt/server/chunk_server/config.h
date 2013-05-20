#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/error.h>

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkManagerConfig
    : public TYsonSerializable
{
public:
    //! When the number of online nodes drops below this margin,
    //! replicator gets disabled.
    TNullable<int> SafeOnlineNodeCount;

    //! When the fraction of lost chunks grows above this margin,
    //! replicator gets disabled.
    TNullable<double> SafeLostChunkFraction;

    //! Minimum difference in fill coefficient (between the most and the least loaded nodes) to start balancing.
    double MinBalancingFillCoeffDiff;

    //! Minimum fill coefficient of the most loaded node to start balancing.
    double MinBalancingFillCoeff;

    //! Maximum duration a job can run before it is considered dead.
    TDuration JobTimeout;

    //! Memory usage assigned to every repair job.
    i64 RepairJobMemoryUsage;

    //! Graceful delay before chunk refresh.
    TDuration ChunkRefreshDelay;

    //! Interval between consequent chunk refresh scans.
    TDuration ChunkRefreshPeriod;

    //! Maximum number of chunks to process during a refresh scan.
    int MaxChunksPerRefresh;

    //! Each active upload session adds |ActiveSessionPenalityCoeff| to effective load factor
    //! when picking an upload target.
    double ActiveSessionPenalityCoeff;

    //! Interval between consequent chunk properties update scans.
    TDuration ChunkPropertiesUpdatePeriod;

    //! Maximum number of chunks to process during a properties update scan.
    int MaxChunksPerPropertiesUpdate;

    TChunkManagerConfig()
    {
        RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("safe_lost_chunk_fraction", SafeLostChunkFraction)
            .InRange(0.0, 1.0)
            .Default(0.5);

        RegisterParameter("min_chunk_balancing_fill_coeff_diff", MinBalancingFillCoeffDiff)
            .Default(0.2);
        RegisterParameter("min_chunk_balancing_fill_coeff", MinBalancingFillCoeff)
            .Default(0.1);

        RegisterParameter("job_timeout", JobTimeout)
            .Default(TDuration::Minutes(5));

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

        RegisterParameter("active_session_penality_coeff", ActiveSessionPenalityCoeff)
            .Default(0.0001);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
