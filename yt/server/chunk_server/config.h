#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/error.h>

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicatorConfig
    : public TYsonSerializable
{
public:
    //! Minimum number of nodes the cell must have online to enable starting new jobs.
    TNullable<int> MinOnlineNodeCount;

    //! Max lost chunk fraction the cell is allowed to have to enable starting new jobs.
    TNullable<double> MaxLostChunkFraction;

    //! Minimum difference in fill coefficient (between the most and the least loaded nodes) to start balancing.
    double MinBalancingFillCoeffDiff;

    //! Minimum fill coefficient of the most loaded node to start balancing.
    double MinBalancingFillCoeff;

    //! Maximum duration a job can run before it is considered dead.
    TDuration JobTimeout;

    TChunkReplicatorConfig()
    {
        RegisterParameter("min_online_node_count", MinOnlineNodeCount)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("max_lost_chunk_fraction", MaxLostChunkFraction)
            .InRange(0.0, 1.0)
            .Default(0.5);
        RegisterParameter("min_chunk_balancing_fill_coeff_diff", MinBalancingFillCoeffDiff)
            .Default(0.2);
        RegisterParameter("min_chunk_balancing_fill_coeff", MinBalancingFillCoeff)
            .Default(0.1);
        RegisterParameter("job_timeout", JobTimeout)
            .Default(TDuration::Minutes(5));
    }
};

class TChunkManagerConfig
    : public TYsonSerializable
{
public:
    TDuration ChunkRefreshDelay;
    TDuration ChunkRefreshPeriod;
    int MaxChunksPerRefresh;

    double ActiveSessionsPenalityCoeff;

    TDuration ChunkRFUpdatePeriod;
    int MaxChunksPerRFUpdate;

    TChunkReplicatorConfigPtr ChunkReplicator;

    TChunkManagerConfig()
    {
        RegisterParameter("chunk_refresh_delay", ChunkRefreshDelay)
            .Default(TDuration::Seconds(15));
        RegisterParameter("chunk_refresh_period", ChunkRefreshPeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("max_chunks_per_refresh", MaxChunksPerRefresh)
            .Default(10000);

        RegisterParameter("chunk_rf_update_period", ChunkRFUpdatePeriod)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("max_chunks_per_rf_update", MaxChunksPerRFUpdate)
            .Default(10000);

        RegisterParameter("active_sessions_penality_coeff", ActiveSessionsPenalityCoeff)
            .Default(0.0001);

        RegisterParameter("chunk_replicator", ChunkReplicator)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
