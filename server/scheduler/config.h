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

class TNodeScoreFeatureConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TString FilterQuery;
    TNodeScoreValue Weight;

    TNodeScoreFeatureConfig()
    {
        RegisterParameter("filter_query", FilterQuery);
        RegisterParameter("weight", Weight)
            .Default(1);
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeScoreFeatureConfig)

////////////////////////////////////////////////////////////////////////////////

class TNodeScoreConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    std::vector<TNodeScoreFeatureConfigPtr> Features;

    TNodeScoreConfig()
    {
        RegisterParameter("features", Features)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeScoreConfig)

////////////////////////////////////////////////////////////////////////////////

class TGlobalResourceAllocatorConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TEveryNodeSelectionStrategyConfigPtr EveryNodeSelectionStrategy;
    TPodNodeScoreConfigPtr PodNodeScore;
    TNodeScoreConfigPtr NodeScore;

    TGlobalResourceAllocatorConfig()
    {
        RegisterParameter("every_node_selection_strategy", EveryNodeSelectionStrategy)
            .DefaultNew();
        RegisterParameter("pod_node_score", PodNodeScore)
            .DefaultNew();
        RegisterParameter("node_score", NodeScore)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TGlobalResourceAllocatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TPodDisruptionBudgetControllerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    int UpdateConcurrency;
    int UpdatesPerIteration;

    TPodDisruptionBudgetControllerConfig()
    {
        RegisterParameter("update_concurrency", UpdateConcurrency)
            .Default(256)
            .GreaterThanOrEqual(1);
        RegisterParameter("updates_per_iteration", UpdatesPerIteration)
            .Default(1024)
            .GreaterThanOrEqual(1);
    }
};

DEFINE_REFCOUNTED_TYPE(TPodDisruptionBudgetControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    bool Disabled;
    TEnumIndexedVector<ESchedulerLoopStage, bool> DisableStage;
    TDuration LoopPeriod;
    TDuration FailedAllocationBackoffTime;
    int AllocationCommitConcurrency;
    TGlobalResourceAllocatorConfigPtr GlobalResourceAllocator;
    TPodDisruptionBudgetControllerConfigPtr PodDisruptionBudgetController;

    TSchedulerConfig()
    {
        RegisterParameter("disabled", Disabled)
            .Default(false);
        RegisterParameter("disable_stage", DisableStage)
            .Default();
        RegisterParameter("loop_period", LoopPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("failed_allocation_backoff_time", FailedAllocationBackoffTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("allocation_commit_concurrency", AllocationCommitConcurrency)
            .Default(256)
            .GreaterThanOrEqual(1);
        RegisterParameter("global_resource_allocator", GlobalResourceAllocator)
            .DefaultNew();
        RegisterParameter("pod_disruption_budget_controller", PodDisruptionBudgetController)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
