#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/ytree/ephemeral_node_factory.h>

#include <ytlib/table_client/config.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TFairShareStrategyConfig
    : public TYsonSerializable
{
    TDuration FairShareUpdatePeriod;
    TDuration PreemptionCheckPeriod;
    
    double MinShareStarvationFactor;
    TDuration MinSharePreemptionTimeout;

    double FairShareStarvationFactor;
    TDuration FairSharePreemptionTimeout;

    double NewOperationWeightBoostFactor;
    TDuration NewOperationWeightBoostPeriod;

    TFairShareStrategyConfig()
    {
        Register("fair_share_update_period", FairShareUpdatePeriod)
            .Default(TDuration::MilliSeconds(1000));
        Register("preemption_check_period", PreemptionCheckPeriod)
            .Default(TDuration::Seconds(15));
        
        Register("min_share_starvation_factor", MinShareStarvationFactor)
            .InRange(0.0, 1.0)
            .Default(0.9);
        Register("min_share_preemption_timeout", MinSharePreemptionTimeout)
            .Default(TDuration::Seconds(15));

        Register("fair_share_starvation_factor", FairShareStarvationFactor)
            .InRange(0.0, 1.0)
            .Default(0.7);
        Register("fair_share_preemption_timeout", FairSharePreemptionTimeout)
            .Default(TDuration::Seconds(30));

        Register("new_operation_weight_boost_factor", NewOperationWeightBoostFactor)
            .GreaterThanOrEqual(1.0)
            .Default(1.0);
        Register("new_operation_weight_boost_period", NewOperationWeightBoostPeriod)
            .Default(TDuration::Minutes(0));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerConfig
    : public TFairShareStrategyConfig
{
    TDuration ConnectRetryPeriod;

    TDuration TransactionsRefreshPeriod;

    TDuration OperationsUpdatePeriod;

    TDuration WatchersUpdatePeriod;

    ESchedulerStrategy Strategy;

    //! Timeout used for direct RPC requests to nodes.
    TDuration NodeRpcTimeout;

    //! Once this limit is reached the operation fails.
    int FailedJobsLimit;

    //! Number of chunk lists to be allocated when an operation starts.
    int ChunkListPreallocationCount;

    //! Maximum number of chunk lists to request via a single request.
    int MaxChunkListAllocationCount;

    //! Better keep the number of spare chunk lists above this threshold.
    int ChunkListWatermarkCount;

    //! Each time the number of spare chunk lists drops below #ChunkListWatermarkCount or
    //! the controller requests more chunk lists than we currently have,
    //! another batch is allocated. Each time we allocate #ChunkListAllocationMultiplier times
    //! more chunk lists than previously.
    double ChunkListAllocationMultiplier;

    //! Maximum number of chunk trees to attach per request.
    int MaxChildrenPerAttachRequest;

    //! Maximum number of partitions during sort, ever.
    int MaxPartitionCount;

    //! Approximate maximum number of jobs per operation.
    int MaxJobCount;

    NYTree::INodePtr MapOperationSpec;
    NYTree::INodePtr ReduceOperationSpec;
    NYTree::INodePtr EraseOperationSpec;
    NYTree::INodePtr OrderedMergeOperationSpec;
    NYTree::INodePtr UnorderedMergeOperationSpec;
    NYTree::INodePtr SortedMergeOperationSpec;
    NYTree::INodePtr MapReduceOperationSpec;
    NYTree::INodePtr SortOperationSpec;

    // Default environment variables set for every job.
    yhash_map<Stroka, Stroka> Environment;

    TSchedulerConfig()
    {
        Register("connect_retry_period", ConnectRetryPeriod)
            .Default(TDuration::Seconds(15));
        Register("transactions_refresh_period", TransactionsRefreshPeriod)
            .Default(TDuration::Seconds(3));
        Register("operations_update_period", OperationsUpdatePeriod)
            .Default(TDuration::Seconds(3));
        Register("watchers_update_period", WatchersUpdatePeriod)
            .Default(TDuration::Seconds(15));
        Register("strategy", Strategy)
            .Default(ESchedulerStrategy::Null);
        Register("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(15));
        Register("failed_jobs_limit", FailedJobsLimit)
            .Default(100)
            .GreaterThanOrEqual(0);
        Register("chunk_list_preallocation_count", ChunkListPreallocationCount)
            .Default(128)
            .GreaterThanOrEqual(0);
        Register("max_chunk_list_allocation_count", MaxChunkListAllocationCount)
            .Default(16384)
            .GreaterThanOrEqual(0);
        Register("chunk_list_watermark_count", ChunkListWatermarkCount)
            .Default(50)
            .GreaterThanOrEqual(0);
        Register("chunk_list_allocation_multiplier", ChunkListAllocationMultiplier)
            .Default(2.0)
            .GreaterThan(1.0);
        Register("max_children_per_attach_request", MaxChildrenPerAttachRequest)
            .Default(10000)
            .GreaterThan(0);
        Register("max_partition_count", MaxPartitionCount)
            .Default(2000)
            .GreaterThan(0);

        auto factory = NYTree::GetEphemeralNodeFactory();
        Register("map_operation_spec", MapOperationSpec)
            .Default(factory->CreateMap());
        Register("reduce_operation_spec", ReduceOperationSpec)
            .Default(factory->CreateMap());
        Register("erase_operation_spec", EraseOperationSpec)
            .Default(factory->CreateMap());
        Register("ordered_merge_operation_spec", OrderedMergeOperationSpec)
            .Default(factory->CreateMap());
        Register("unordered_merge_operation_spec", UnorderedMergeOperationSpec)
            .Default(factory->CreateMap());
        Register("sorted_merge_operation_spec", SortedMergeOperationSpec)
            .Default(factory->CreateMap());
        Register("map_reduce_operation_spec", MapReduceOperationSpec)
            .Default(factory->CreateMap());
        Register("sort_operation_spec", SortOperationSpec)
            .Default(factory->CreateMap());

        Register("max_job_count", MaxJobCount)
            .Default(20000)
            .GreaterThan(0);

        Register("environment", Environment)
            .Default(yhash_map<Stroka, Stroka>());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
