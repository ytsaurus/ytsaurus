#pragma once

#include "public.h"

#include <yp/server/lib/cluster/config.h>

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
            .Default(320)
            .GreaterThanOrEqual(1);
        RegisterParameter("updates_per_iteration", UpdatesPerIteration)
            .Default(10240)
            .GreaterThanOrEqual(1);
    }
};

DEFINE_REFCOUNTED_TYPE(TPodDisruptionBudgetControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPodExponentialBackoffPolicyConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TDuration Start;
    TDuration Max;
    double Base;

    TPodExponentialBackoffPolicyConfig()
    {
        RegisterParameter("start", Start)
            .Default(TDuration::Seconds(20));
        RegisterParameter("max", Max)
            .Default(TDuration::Minutes(10));
        RegisterParameter("base", Base)
            .GreaterThanOrEqual(1.0)
            .LessThanOrEqual(1000.0)
            .Default(2.0);

        RegisterPostprocessor([&] {
            if (Start > Max) {
                THROW_ERROR_EXCEPTION("\"start\" must be less than or equal to \"max\", but got %v > %v",
                    Start,
                    Max);
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TPodExponentialBackoffPolicyConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulePodsStageConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TDuration TimeLimit;
    int PodLimit;

    TSchedulePodsStageConfig()
    {
        RegisterParameter("time_limit", TimeLimit)
            .Default(TDuration::Seconds(10));
        RegisterParameter("pod_limit", PodLimit)
            .GreaterThan(0)
            .Default(1000);
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulePodsStageConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    bool Disabled;
    TEnumIndexedVector<ESchedulerLoopStage, bool> DisableStage;
    TDuration LoopPeriod;
    TPodExponentialBackoffPolicyConfigPtr FailedAllocationBackoff;
    int AllocationCommitConcurrency;
    TGlobalResourceAllocatorConfigPtr GlobalResourceAllocator;
    TPodDisruptionBudgetControllerConfigPtr PodDisruptionBudgetController;
    NCluster::TClusterConfigPtr Cluster;
    TSchedulePodsStageConfigPtr SchedulePodsStage;

    TSchedulerConfig()
    {
        RegisterParameter("disabled", Disabled)
            .Default(false);
        RegisterParameter("disable_stage", DisableStage)
            .Default();
        RegisterParameter("loop_period", LoopPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("failed_allocation_backoff", FailedAllocationBackoff)
            .DefaultNew();
        RegisterParameter("allocation_commit_concurrency", AllocationCommitConcurrency)
            .Default(256)
            .GreaterThanOrEqual(1);
        RegisterParameter("global_resource_allocator", GlobalResourceAllocator)
            .DefaultNew();
        RegisterParameter("pod_disruption_budget_controller", PodDisruptionBudgetController)
            .DefaultNew();
        RegisterParameter("cluster", Cluster)
            .DefaultNew();
        RegisterParameter("schedule_pods_stage", SchedulePodsStage)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
