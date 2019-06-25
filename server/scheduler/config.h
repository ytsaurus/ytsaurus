#pragma once

#include "public.h"

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/ypath/public.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TEveryNodeSelectionStrategyConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    bool Enable;
    int IterationPeriod;
    int IterationSplay;

    TEveryNodeSelectionStrategyConfig()
    {
        RegisterParameter("enable", Enable)
            .Default(true);
        RegisterParameter("iteration_period", IterationPeriod)
            .Default(1)
            .GreaterThanOrEqual(1);
        RegisterParameter("iteration_splay", IterationSplay)
            .Default(1)
            .GreaterThanOrEqual(1);
    }
};

DEFINE_REFCOUNTED_TYPE(TEveryNodeSelectionStrategyConfig)

////////////////////////////////////////////////////////////////////////////////

class TPodNodeScoreConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    EPodNodeScoreType Type;
    NYT::NYTree::IMapNodePtr Parameters;

    TPodNodeScoreConfig()
    {
        RegisterParameter("type", Type)
            .Default(EPodNodeScoreType::FreeCpuMemoryShareSquaredMinDelta);
        RegisterParameter("parameters", Parameters)
            .Default(NYT::NYTree::BuildYsonNodeFluently()
                .BeginMap()
                .EndMap()
            ->AsMap());
    }
};

DEFINE_REFCOUNTED_TYPE(TPodNodeScoreConfig)

////////////////////////////////////////////////////////////////////////////////

class TGlobalResourceAllocatorConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TEveryNodeSelectionStrategyConfigPtr EveryNodeSelectionStrategy;
    TPodNodeScoreConfigPtr PodNodeScore;

    TGlobalResourceAllocatorConfig()
    {
        RegisterParameter("every_node_selection_strategy", EveryNodeSelectionStrategy)
            .DefaultNew();
        RegisterParameter("pod_node_score", PodNodeScore)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TGlobalResourceAllocatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TDuration LoopPeriod;
    TDuration FailedAllocationBackoffTime;
    int AllocationCommitConcurrency;
    TGlobalResourceAllocatorConfigPtr GlobalResourceAllocator;

    TSchedulerConfig()
    {
        RegisterParameter("loop_period", LoopPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("failed_allocation_backoff_time", FailedAllocationBackoffTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("allocation_commit_concurrency", AllocationCommitConcurrency)
            .Default(256)
            .GreaterThanOrEqual(1);
        RegisterParameter("global_resource_allocator", GlobalResourceAllocator)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
