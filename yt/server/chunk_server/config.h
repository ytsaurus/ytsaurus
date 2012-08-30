#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/error.h>

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReplicatorConfig
    : public TYsonSerializable
{
    //! Minimum number of nodes the cell must have online to enable starting new jobs.
    TNullable<int> MinOnlineNodeCount;
    
    //! Max lost chunk fraction the cell is allowed to have to enable starting new jobs.
    TNullable<double> MaxLostChunkFraction;
    
    //! Maximum number of upload targets during replication and balancing.
    int MaxReplicationFanOut;
    
    //! Maximum number of incoming upload sessions during replication and balancing.
    int MaxReplicationFanIn;
    
    //! Maximum number of concurrent removal jobs that can be scheduled to a node.
    int MaxRemovalJobsPerNode;
    
    //! Minimum difference in fill coefficient (between the most and the least loaded nodes) to start balancing.
    double MinBalancingFillCoeffDiff;
    
    //! Minimum fill coefficient of the most loaded node to start balancing.
    double MinBalancingFillCoeff;
    
    //! Maximum duration a job can run before it is considered dead.
    TDuration JobTimeout;

    TChunkReplicatorConfig()
    {
        Register("min_online_node_count", MinOnlineNodeCount)
            .GreaterThan(0)
            .Default(1);
        Register("max_lost_chunk_fraction", MaxLostChunkFraction)
            .InRange(0.0, 1.0)
            .Default(0.5);
        Register("max_replication_fan_out", MaxReplicationFanOut)
            .Default(4);
        Register("max_replication_fan_in", MaxReplicationFanIn)
            .Default(8);
        Register("max_removal_jobs_per_node", MaxRemovalJobsPerNode)
            .Default(16);
        Register("min_chunk_balancing_fill_coeff_diff", MinBalancingFillCoeffDiff)
            .Default(0.2);
        Register("min_chunk_balancing_fill_coeff", MinBalancingFillCoeff)
            .Default(0.1);
        Register("job_timeout", JobTimeout)
            .Default(TDuration::Minutes(5));
    }
};

struct TChunkTreeBalancerConfig
    : public TYsonSerializable
{
    int MaxChunkTreeRank;
    int MinChunkListSize;
    int MaxChunkListSize;

    TChunkTreeBalancerConfig()
    {
        Register("max_chunk_tree_rank", MaxChunkTreeRank)
            .GreaterThanOrEqual(2)
            .Default(32);
        Register("min_chunk_list_size", MinChunkListSize)
            .GreaterThan(0)
            .Default(1024);
        Register("max_chunk_list_size", MaxChunkListSize)
            .GreaterThan(0)
            .Default(2048);
    }

    virtual void DoValidate() const
    {
        if (MaxChunkListSize <= MinChunkListSize) {
            THROW_ERROR_EXCEPTION("\"max_chunk_list_size\" must be greater than \"min_chunk_list_size\"");
        }
    }
};

struct TChunkManagerConfig
    : public TYsonSerializable
{
    TDuration OnlineNodeTimeout;
    TDuration RegisteredNodeTimeout;
    TDuration UnconfirmedNodeTimeout;

    TDuration ChunkRefreshDelay;
    TDuration ChunkRefreshQuantum;
    int MaxChunksPerRefresh;

    double MaxNodeFillCoeff;
    i64 MinNodeFreeSpace;
    double ActiveSessionsPenalityCoeff;
    TChunkReplicatorConfigPtr ChunkReplicator;
    TChunkTreeBalancerConfigPtr ChunkTreeBalancer;

    TChunkManagerConfig()
    {
        Register("online_node_timeout", OnlineNodeTimeout)
            .Default(TDuration::Seconds(60));
        Register("registered_node_timeout", RegisteredNodeTimeout)
            .Default(TDuration::Seconds(180));
        Register("unconfirmed_node_timeout", UnconfirmedNodeTimeout)
            .Default(TDuration::Seconds(30));
        Register("chunk_refresh_delay", ChunkRefreshDelay)
            .Default(TDuration::Seconds(15));
        Register("chunk_refresh_quantum", ChunkRefreshQuantum)
            .Default(TDuration::MilliSeconds(100));
        Register("max_chunks_per_refresh", MaxChunksPerRefresh)
            .Default(10000);
        Register("active_sessions_penality_coeff", ActiveSessionsPenalityCoeff)
            .Default(0.1);
        Register("chunk_replicator", ChunkReplicator)
            .DefaultNew();
        Register("chunk_tree_balancer", ChunkTreeBalancer)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
