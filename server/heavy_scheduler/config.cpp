#include "config.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

TYTConnectorConfig::TYTConnectorConfig()
{
    RegisterParameter("connection", Connection);

    RegisterParameter("user", User)
        .Default("robot-yp-heavy-sched");
    RegisterParameter("token", Token)
        .Optional();

    RegisterParameter("root_path", RootPath)
        .Default("//yp/heavy_scheduler");

    RegisterParameter("connect_period", ConnectPeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("leader_transaction_timeout", LeaderTransactionTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TClusterReaderConfig::TClusterReaderConfig()
{
    RegisterParameter("select_batch_size", SelectBatchSize)
        .Default(500)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

TTaskManagerConfig::TTaskManagerConfig()
{
    RegisterParameter("task_time_limit", TaskTimeLimit)
        .Default(TDuration::Minutes(30));
    RegisterParameter("task_slots_per_source", TaskSlotsPerSource_)
        .Default();
}

int TTaskManagerConfig::GetTaskSlots(ETaskSource source) const
{
    return TaskSlotsPerSource_[source]
        ? *TaskSlotsPerSource_[source]
        : 10;
}

////////////////////////////////////////////////////////////////////////////////

TDisruptionThrottlerConfig::TDisruptionThrottlerConfig()
{
    RegisterParameter("validate_pod_disruption_budget", ValidatePodDisruptionBudget)
        .Default(true);
    RegisterParameter("limit_evictions_by_pod_set", LimitEvictionsByPodSet)
        .Default(true);
    RegisterParameter("safe_suitable_node_count", SafeSuitableNodeCount)
        .GreaterThanOrEqual(0)
        .Default(3);
}

////////////////////////////////////////////////////////////////////////////////

TSwapDefragmentatorConfig::TSwapDefragmentatorConfig()
{
    RegisterParameter("starving_pods_per_iteration_limit", StarvingPodsPerIterationLimit)
        .GreaterThanOrEqual(1)
        .Default(100);

    RegisterParameter("victim_candidate_pod_count", VictimCandidatePodCount)
        .GreaterThan(0)
        .Default(10000);
}

////////////////////////////////////////////////////////////////////////////////

TAntiaffinityHealerConfig::TAntiaffinityHealerConfig()
{
    RegisterParameter("pods_per_iteration_soft_limit", PodsPerIterationSoftLimit)
        .GreaterThanOrEqual(1)
        .Default(5000);
}

////////////////////////////////////////////////////////////////////////////////

TEvictionGarbageCollectorConfig::TEvictionGarbageCollectorConfig()
{
    RegisterParameter("time_limit", TimeLimit)
        .Default(TDuration::Days(1));
}

////////////////////////////////////////////////////////////////////////////////

THeavySchedulerConfig::THeavySchedulerConfig()
{
    RegisterParameter("iteration_period", IterationPeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("verbose", Verbose)
        .Default(false);

    RegisterParameter("cluster_reader", ClusterReader)
        .DefaultNew();

    RegisterParameter("node_segment", NodeSegment)
        .Default("default");

    RegisterParameter("safe_cluster_pod_eviction_count", SafeClusterPodEvictionCount)
        .GreaterThanOrEqual(0)
        .Default(10);

    RegisterParameter("task_manager", TaskManager)
        .DefaultNew();
    RegisterParameter("disruption_throttler", DisruptionThrottler)
        .DefaultNew();
    RegisterParameter("swap_defragmentator", SwapDefragmentator)
        .DefaultNew();
    RegisterParameter("antiaffinity_healer", AntiaffinityHealer)
        .DefaultNew();
    RegisterParameter("eviction_garbage_collector", EvictionGarbageCollector)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

THeavySchedulerProgramConfig::THeavySchedulerProgramConfig()
{
    SetUnrecognizedStrategy(NYT::NYTree::EUnrecognizedStrategy::KeepRecursive);

    RegisterParameter("client", Client)
        .DefaultNew();
    RegisterParameter("monitoring_server", MonitoringServer)
        .Optional();
    RegisterParameter("yt_connector", YTConnector)
        .DefaultNew();
    RegisterParameter("heavy_scheduler", HeavyScheduler)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
