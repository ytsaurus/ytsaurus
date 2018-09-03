#include "config.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyOperationControllerConfig::TFairShareStrategyOperationControllerConfig()
{
    RegisterParameter("max_concurrent_controller_schedule_job_calls", MaxConcurrentControllerScheduleJobCalls)
        .Default(10)
        .GreaterThan(0);

    RegisterParameter("schedule_job_time_limit", ScheduleJobTimeLimit)
        .Default(TDuration::Seconds(60));

    RegisterParameter("schedule_job_fail_backoff_time", ScheduleJobFailBackoffTime)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("schedule_job_timeout_alert_reset_time", ScheduleJobTimeoutAlertResetTime)
        .Default(TDuration::Minutes(15));

    RegisterParameter("schedule_jobs_timeout", ScheduleJobsTimeout)
        .Default(TDuration::Seconds(40));
}

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyTreeConfig::TFairShareStrategyTreeConfig()
{
    RegisterParameter("nodes_filter", NodesFilter)
        .Default();

    RegisterParameter("min_share_preemption_timeout", MinSharePreemptionTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("fair_share_preemption_timeout", FairSharePreemptionTimeout)
        .Default(TDuration::Seconds(30));
    RegisterParameter("fair_share_starvation_tolerance", FairShareStarvationTolerance)
        .InRange(0.0, 1.0)
        .Default(0.8);

    RegisterParameter("min_share_preemption_timeout_limit", MinSharePreemptionTimeoutLimit)
        .Default(TDuration::Seconds(15));
    RegisterParameter("fair_share_preemption_timeout_limit", FairSharePreemptionTimeoutLimit)
        .Default(TDuration::Seconds(30));
    RegisterParameter("fair_share_starvation_tolerance_limit", FairShareStarvationToleranceLimit)
        .InRange(0.0, 1.0)
        .Default(0.8);

    RegisterParameter("max_unpreemptable_running_job_count", MaxUnpreemptableRunningJobCount)
        .Default(10);

    RegisterParameter("max_running_operation_count", MaxRunningOperationCount)
        .Default(200)
        .GreaterThan(0);

    RegisterParameter("max_running_operation_count_per_pool", MaxRunningOperationCountPerPool)
        .Alias("max_running_operations_per_pool")
        .Default(50)
        .GreaterThan(0);

    RegisterParameter("max_operation_count_per_pool", MaxOperationCountPerPool)
        .Alias("max_operations_per_pool")
        .Default(50)
        .GreaterThan(0);

    RegisterParameter("max_operation_count", MaxOperationCount)
        .Default(50000)
        .GreaterThan(0);

    RegisterParameter("enable_pool_starvation", EnablePoolStarvation)
        .Default(true);

    RegisterParameter("default_parent_pool", DefaultParentPool)
        .Default(RootPoolName);

    RegisterParameter("forbid_immediate_operations_in_root", ForbidImmediateOperationsInRoot)
        .Default(true);

    RegisterParameter("job_count_preemption_timeout_coefficient", JobCountPreemptionTimeoutCoefficient)
        .Default(1.0)
        .GreaterThanOrEqual(1.0);

    RegisterParameter("preemption_satisfaction_threshold", PreemptionSatisfactionThreshold)
        .Default(1.0)
        .GreaterThan(0);

    RegisterParameter("aggressive_preemption_satisfaction_threshold", AggressivePreemptionSatisfactionThreshold)
        .Default(0.5)
        .GreaterThan(0);

    RegisterParameter("enable_scheduling_tags", EnableSchedulingTags)
        .Default(true);

    RegisterParameter("heartbeat_tree_scheduling_info_log_period", HeartbeatTreeSchedulingInfoLogBackoff)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("max_ephemeral_pools_per_user", MaxEphemeralPoolsPerUser)
        .GreaterThanOrEqual(1)
        .Default(5);

    RegisterParameter("update_preemptable_list_duration_logging_threshold", UpdatePreemptableListDurationLoggingThreshold)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("enable_operations_profiling", EnableOperationsProfiling)
        .Default(true);

    RegisterParameter("threshold_to_enable_max_possible_usage_regularization", ThresholdToEnableMaxPossibleUsageRegularization)
        .InRange(0.0, 1.0)
        .Default(0.5);

    RegisterParameter("total_resource_limits_consider_delay", TotalResourceLimitsConsiderDelay)
        .Default(TDuration::Seconds(60));

    RegisterParameter("preemptive_scheduling_backoff", PreemptiveSchedulingBackoff)
        .Default(TDuration::Seconds(5));

    RegisterPostprocessor([&] () {
        if (AggressivePreemptionSatisfactionThreshold > PreemptionSatisfactionThreshold) {
            THROW_ERROR_EXCEPTION("Aggressive preemption satisfaction threshold must be less than preemption satisfaction threshold")
                << TErrorAttribute("aggressive_threshold", AggressivePreemptionSatisfactionThreshold)
                << TErrorAttribute("threshold", PreemptionSatisfactionThreshold);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyConfig::TFairShareStrategyConfig()
{
    RegisterParameter("fair_share_update_period", FairShareUpdatePeriod)
        .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
        .Default(TDuration::MilliSeconds(1000));

    RegisterParameter("fair_share_profiling_period", FairShareProfilingPeriod)
        .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
        .Default(TDuration::MilliSeconds(5000));

    RegisterParameter("fair_share_log_period", FairShareLogPeriod)
        .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
        .Default(TDuration::MilliSeconds(1000));

    RegisterParameter("min_needed_resources_update_period", MinNeededResourcesUpdatePeriod)
        .Default(TDuration::Seconds(3));

    RegisterParameter("operation_unschedulable_check_period", OperationUnschedulableCheckPeriod)
        .Default(TDuration::Minutes(1));

    RegisterParameter("operation_unschedulable_safe_timeout", OperationUnschedulableSafeTimeout)
        .Default(TDuration::Minutes(10));

    RegisterParameter("operation_unschedulable_min_schedule_job_attempts", OperationUnschedulableMinScheduleJobAttempts)
        .Default(1000);

    RegisterParameter("max_operation_count", MaxOperationCount)
        .Default(5000)
        .GreaterThan(0)
        // This value corresponds to the maximum possible number of memory tags.
        // It should be changed simultaneously with values of all `MaxTagValue`
        // across the code base.
        .LessThan(MaxMemoryTag);

    RegisterParameter("operations_without_tentative_pool_trees", OperationsWithoutTentativePoolTrees)
        .Default({EOperationType::Sort, EOperationType::MapReduce, EOperationType::RemoteCopy });
}

////////////////////////////////////////////////////////////////////////////////

TTestingOptions::TTestingOptions()
{
    RegisterParameter("enable_random_master_disconnection", EnableRandomMasterDisconnection)
        .Default(false);
    RegisterParameter("random_master_disconnection_max_backoff", RandomMasterDisconnectionMaxBackoff)
        .Default(TDuration::Seconds(5));
    RegisterParameter("master_disconnect_delay", MasterDisconnectDelay)
        .Default(Null);
    RegisterParameter("finish_operation_transition_delay", FinishOperationTransitionDelay)
        .Default(Null);
}

////////////////////////////////////////////////////////////////////////////////

TOperationsCleanerConfig::TOperationsCleanerConfig()
{
    RegisterParameter("enable", Enable)
        .Default(false);
    RegisterParameter("enable_archivation", EnableArchivation)
        .Default(true);
    RegisterParameter("clean_delay", CleanDelay)
        .Default(TDuration::Minutes(5));
    RegisterParameter("analysis_period", AnalysisPeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("remove_batch_size", RemoveBatchSize)
        .Default(256);
    RegisterParameter("remove_batch_timeout", RemoveBatchTimeout)
        .Default(TDuration::Seconds(5));
    RegisterParameter("archive_batch_size", ArchiveBatchSize)
        .Default(100);
    RegisterParameter("archive_batch_timeout", ArchiveBatchTimeout)
        .Default(TDuration::Seconds(5));
    RegisterParameter("fetch_batch_size", FetchBatchSize)
        .GreaterThan(0)
        .Default(100);
    RegisterParameter("max_operation_age", MaxOperationAge)
        .Default(TDuration::Hours(6));
    RegisterParameter("max_operation_count_per_user", MaxOperationCountPerUser)
        .Default(200);
    RegisterParameter("soft_retained_operation_count", SoftRetainedOperationCount)
        .Default(200);
    RegisterParameter("hard_retained_operation_count", HardRetainedOperationCount)
        .Default(4000);
    RegisterParameter("min_archivation_retry_sleep_delay", MinArchivationRetrySleepDelay)
        .Default(TDuration::Seconds(3));
    RegisterParameter("max_archivation_retry_sleep_delay", MaxArchivationRetrySleepDelay)
        .Default(TDuration::Minutes(1));
    RegisterParameter("max_operation_count_enqueued_for_archival", MaxOperationCountEnqueuedForArchival)
        .Default(7000);
    RegisterParameter("archivation_enable_delay", ArchivationEnableDelay)
        .Default(TDuration::Minutes(30));
    RegisterParameter("max_removal_sleep_delay", MaxRemovalSleepDelay)
        .Default(TDuration::Seconds(5));

    RegisterPostprocessor([&] {
        if (MaxArchivationRetrySleepDelay <= MinArchivationRetrySleepDelay) {
            THROW_ERROR_EXCEPTION("\"max_archivation_retry_sleep_delay\" must be greater than "
                "\"min_archivation_retry_sleep_delay\"")
                << TErrorAttribute("min_archivation_retry_sleep_delay", MinArchivationRetrySleepDelay)
                << TErrorAttribute("max_archivation_retry_sleep_delay", MaxArchivationRetrySleepDelay);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TControllerAgentTrackerConfig::TControllerAgentTrackerConfig()
{
    RegisterParameter("light_rpc_timeout", LightRpcTimeout)
        .Default(TDuration::Seconds(30));

    RegisterParameter("heavy_rpc_timeout", HeavyRpcTimeout)
        .Default(TDuration::Minutes(30));

    RegisterParameter("heartbeat_timeout", HeartbeatTimeout)
        .Default(TDuration::Seconds(15));

    RegisterParameter("agent_pick_strategy", AgentPickStrategy)
        .Default(EControllerAgentPickStrategy::Random);

    RegisterParameter("min_agent_count", MinAgentCount)
        .Default(1);

    RegisterParameter("min_agent_available_memory", MinAgentAvailableMemory)
        .Default(10_GB);
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerConfig::TSchedulerConfig()
{
    SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);

    RegisterParameter("node_shard_count", NodeShardCount)
        .Default(4)
        .GreaterThan(0);

    RegisterParameter("connect_retry_backoff_time", ConnectRetryBackoffTime)
        .Default(TDuration::Seconds(15));

    RegisterParameter("node_heartbeat_timeout", NodeHeartbeatTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("watchers_update_period", WatchersUpdatePeriod)
        .Default(TDuration::Seconds(3));
    RegisterParameter("nodes_attributes_update_period", NodesAttributesUpdatePeriod)
        .Default(TDuration::Seconds(15));
    RegisterParameter("profiling_update_period", ProfilingUpdatePeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("alerts_update_period", AlertsUpdatePeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("node_shard_submit_jobs_to_strategy_period", NodeShardSubmitJobsToStrategyPeriod)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("lock_transaction_timeout", LockTransactionTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("job_prober_rpc_timeout", JobProberRpcTimeout)
        .Default(TDuration::Seconds(300));

    RegisterParameter("cluster_info_logging_period", ClusterInfoLoggingPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("exec_node_descriptors_update_period", ExecNodeDescriptorsUpdatePeriod)
        .Default(TDuration::Seconds(10));
    RegisterParameter("jobs_logging_period", JobsLoggingPeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("running_jobs_update_period", RunningJobsUpdatePeriod)
        .Default(TDuration::Seconds(10));
    RegisterParameter("missing_jobs_check_period", MissingJobsCheckPeriod)
        .Default(TDuration::Seconds(10));
    RegisterParameter("transient_operation_queue_scan_period", TransientOperationQueueScanPeriod)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("operation_to_agent_assignment_backoff", OperationToAgentAssignmentBackoff)
        .Default(TDuration::Seconds(1));

    RegisterParameter("max_started_jobs_per_heartbeat", MaxStartedJobsPerHeartbeat)
        .Default()
        .GreaterThan(0);

    RegisterParameter("node_shard_exec_nodes_cache_update_period", NodeShardExecNodesCacheUpdatePeriod)
        .Default(TDuration::Seconds(10));

    RegisterParameter("heartbeat_process_backoff", HeartbeatProcessBackoff)
        .Default(TDuration::MilliSeconds(5000));
    RegisterParameter("soft_concurrent_heartbeat_limit", SoftConcurrentHeartbeatLimit)
        .Default(50)
        .GreaterThanOrEqual(1);
    RegisterParameter("hard_concurrent_heartbeat_limit", HardConcurrentHeartbeatLimit)
        .Default(100)
        .GreaterThanOrEqual(1);

    RegisterParameter("static_orchid_cache_update_period", StaticOrchidCacheUpdatePeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("orchid_keys_update_period", OrchidKeysUpdatePeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("enable_job_reporter", EnableJobReporter)
        .Default(false);
    RegisterParameter("enable_job_spec_reporter", EnableJobSpecReporter)
        .Default(false);
    RegisterParameter("enable_job_stderr_reporter", EnableJobStderrReporter)
        .Default(false);
    RegisterParameter("enable_job_fail_context_reporter", EnableJobFailContextReporter)
        .Default(false);

    RegisterParameter("job_interrupt_timeout", JobInterruptTimeout)
        .Default(TDuration::Seconds(10));

    RegisterParameter("enable_unrecognized_alert", EnableUnrecognizedAlert)
        .Default(true);

    RegisterParameter("memory_distribution_different_node_types_threshold", MemoryDistributionDifferentNodeTypesThreshold)
        .Default(4);

    RegisterParameter("job_revival_abort_timeout", JobRevivalAbortTimeout)
        .Default(TDuration::Minutes(5));

    RegisterParameter("scheduling_tag_filter_expire_timeout", SchedulingTagFilterExpireTimeout)
        .Default(TDuration::Seconds(10));

    RegisterParameter("operations_cleaner", OperationsCleaner)
        .DefaultNew();

    RegisterParameter("operations_update_period", OperationsUpdatePeriod)
        .Default(TDuration::Seconds(3));

    RegisterParameter("finished_job_storing_timeout", FinishedJobStoringTimeout)
        .Default(TDuration::Minutes(30));

    RegisterParameter("finished_operation_job_storing_timeout", FinishedOperationJobStoringTimeout)
        .Default(TDuration::Seconds(10));

    RegisterParameter("testing_options", TestingOptions)
        .DefaultNew();

    RegisterParameter("event_log", EventLog)
        .DefaultNew();

    RegisterParameter("spec_template", SpecTemplate)
        .Default();

    RegisterParameter("controller_agent_tracker", ControllerAgentTracker)
        .DefaultNew();

    RegisterParameter("job_reporter_issues_check_period", JobReporterIssuesCheckPeriod)
        .Default(TDuration::Minutes(1));

    RegisterParameter("job_reporter_write_failures_alert_threshold", JobReporterWriteFailuresAlertThreshold)
        .Default(1000);
    RegisterParameter("job_reporter_queue_is_too_large_alert_threshold", JobReporterQueueIsTooLargeAlertThreshold)
        .Default(10);

    RegisterParameter("node_changes_count_threshold_to_update_cache", NodeChangesCountThresholdToUpdateCache)
        .Default(5);

    RegisterPreprocessor([&] () {
        EventLog->MaxRowWeight = 128_MB;
        if (!EventLog->Path) {
            EventLog->Path = "//sys/scheduler/event_log";
        }
    });

    RegisterPostprocessor([&] () {
        if (SoftConcurrentHeartbeatLimit > HardConcurrentHeartbeatLimit) {
            THROW_ERROR_EXCEPTION("\"soft_limit\" must be less than or equal to \"hard_limit\"")
                << TErrorAttribute("soft_limit", SoftConcurrentHeartbeatLimit)
                << TErrorAttribute("hard_limit", HardConcurrentHeartbeatLimit);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
