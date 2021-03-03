#include "config.h"

#include "experiments.h"

#include <yt/yt/server/lib/node_tracker_server/name_helpers.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TStrategyTestingOptions::TStrategyTestingOptions()
{
    RegisterParameter("delay_inside_fair_share_update", DelayInsideFairShareUpdate)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyControllerThrottling::TFairShareStrategyControllerThrottling()
{
    RegisterParameter("schedule_job_start_backoff_time", ScheduleJobStartBackoffTime)
        .Default(TDuration::MilliSeconds(100));
    RegisterParameter("schedule_job_max_backoff_time", ScheduleJobMaxBackoffTime)
        .Default(TDuration::Seconds(10));
    RegisterParameter("schedule_job_backoff_multiplier", ScheduleJobBackoffMultiplier)
        .Default(1.1);
}

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyOperationControllerConfig::TFairShareStrategyOperationControllerConfig()
{
    RegisterParameter("max_concurrent_controller_schedule_job_calls", MaxConcurrentControllerScheduleJobCalls)
        .Default(100)
        .GreaterThan(0);

    RegisterParameter("max_concurrent_controller_schedule_job_calls_per_node_shard", MaxConcurrentControllerScheduleJobCallsPerNodeShard)
        .Default(5)
        .GreaterThan(0);

    RegisterParameter("schedule_job_time_limit", ScheduleJobTimeLimit)
        .Default(TDuration::Seconds(30));

    RegisterParameter("schedule_job_fail_backoff_time", ScheduleJobFailBackoffTime)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("controller_throttling", ControllerThrottling)
        .DefaultNew();

    RegisterParameter("schedule_job_timeout_alert_reset_time", ScheduleJobTimeoutAlertResetTime)
        .Default(TDuration::Minutes(15));

    RegisterParameter("schedule_jobs_timeout", ScheduleJobsTimeout)
        .Default(TDuration::Seconds(40));

    RegisterParameter("long_schedule_job_logging_threshold", LongScheduleJobLoggingThreshold)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategySchedulingSegmentsConfig::TFairShareStrategySchedulingSegmentsConfig()
{
    RegisterParameter("mode", Mode)
        .Default(ESegmentedSchedulingMode::Disabled);

    RegisterParameter("satisfaction_margins", SatisfactionMargins)
        .Default();

    RegisterParameter("unsatisfied_segments_rebalancing_timeout", UnsatisfiedSegmentsRebalancingTimeout)
        .Default(TDuration::Minutes(5));

    RegisterParameter("data_center_reconsideration_timeout", DataCenterReconsiderationTimeout)
        .Default(TDuration::Minutes(20));

    RegisterParameter("data_centers", DataCenters)
        .Default();

    RegisterPostprocessor([&] {
        for (const auto& dataCenter : DataCenters) {
            ValidateDataCenterName(dataCenter);
        }
    });

    RegisterPostprocessor([&] {
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            if (!IsDataCenterAwareSchedulingSegment(segment)) {
                continue;
            }

            for (const auto& dataCenter : SatisfactionMargins.At(segment).GetDataCenters()) {
                if (!dataCenter) {
                    // This could never happen but I'm afraid to put YT_VERIFY here.
                    THROW_ERROR_EXCEPTION("Satisfaction margin can be specified only for non-null data centers");
                }

                if (DataCenters.find(*dataCenter) == DataCenters.end()) {
                    THROW_ERROR_EXCEPTION("Satisfaction margin can be specified only for configured data centers")
                        << TErrorAttribute("configured_data_centers", DataCenters)
                        << TErrorAttribute("specified_data_center", dataCenter);
                }
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyTreeConfig::TFairShareStrategyTreeConfig()
{
    RegisterParameter("nodes_filter", NodesFilter)
        .Default();

    RegisterParameter("fair_share_preemption_timeout", FairSharePreemptionTimeout)
        .Default(TDuration::Seconds(30));
    RegisterParameter("fair_share_starvation_tolerance", FairShareStarvationTolerance)
        .InRange(0.0, 1.0)
        .Default(0.8);

    RegisterParameter("fair_share_preemption_timeout_limit", FairSharePreemptionTimeoutLimit)
        .Default(TDuration::Seconds(30));
    RegisterParameter("fair_share_starvation_tolerance_limit", FairShareStarvationToleranceLimit)
        .InRange(0.0, 1.0)
        .Default(0.8);

    RegisterParameter("enable_aggressive_starvation", EnableAggressiveStarvation)
        .Default(false);

    RegisterParameter("max_unpreemptable_running_job_count", MaxUnpreemptableRunningJobCount)
        .Default(10);

    RegisterParameter("max_running_operation_count", MaxRunningOperationCount)
        .Default(200)
        .GreaterThan(0);

    RegisterParameter("max_running_operation_count_per_pool", MaxRunningOperationCountPerPool)
        .Default(50)
        .GreaterThan(0);

    RegisterParameter("max_operation_count_per_pool", MaxOperationCountPerPool)
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
        .Default(0.2)
        .GreaterThanOrEqual(0);

    RegisterParameter("enable_scheduling_tags", EnableSchedulingTags)
        .Default(true);

    RegisterParameter("heartbeat_tree_scheduling_info_log_period", HeartbeatTreeSchedulingInfoLogBackoff)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("max_ephemeral_pools_per_user", MaxEphemeralPoolsPerUser)
        .GreaterThanOrEqual(1)
        .Default(1);

    RegisterParameter("update_preemptable_list_duration_logging_threshold", UpdatePreemptableListDurationLoggingThreshold)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("enable_operations_profiling", EnableOperationsProfiling)
        .Default(true);

    RegisterParameter("custom_profiling_tag_filter", CustomProfilingTagFilter)
        .Default();

    RegisterParameter("threshold_to_enable_max_possible_usage_regularization", ThresholdToEnableMaxPossibleUsageRegularization)
        .InRange(0.0, 1.0)
        .Default(0.5);

    RegisterParameter("total_resource_limits_consider_delay", TotalResourceLimitsConsiderDelay)
        .Default(TDuration::Seconds(60));

    RegisterParameter("preemptive_scheduling_backoff", PreemptiveSchedulingBackoff)
        .Default(TDuration::Seconds(5));

    RegisterParameter("tentative_tree_saturation_deactivation_period", TentativeTreeSaturationDeactivationPeriod)
        .Default(TDuration::Seconds(10));

    RegisterParameter("infer_weight_from_strong_guarantee_share_multiplier", InferWeightFromStrongGuaranteeShareMultiplier)
        .Alias("infer_weight_from_min_share_ratio_multiplier")
        .Default()
        .GreaterThanOrEqual(1.0);

    RegisterParameter("packing", Packing)
        .DefaultNew();

    RegisterParameter("non_tentative_operation_types", NonTentativeOperationTypes)
        .Default(std::nullopt);

    RegisterParameter("log_fair_share_ratio_disagreement_threshold", LogFairShareRatioDisagreementThreshold)
        // Effectively disabled.
        .Default(1.0);

    RegisterParameter("best_allocation_ratio_update_period", BestAllocationRatioUpdatePeriod)
        .Default(TDuration::Minutes(1));

    RegisterParameter("enable_by_user_profiling", EnableByUserProfiling)
        .Default(true);

    RegisterParameter("integral_guarantees", IntegralGuarantees)
        .DefaultNew();

    RegisterParameter("use_recent_resource_usage_for_local_satisfaction", UseRecentResourceUsageForLocalSatisfaction)
        .Default(false);

    RegisterParameter("enable_resource_tree_structure_lock_profiling", EnableResourceTreeStructureLockProfiling)
        .Default(true);

    RegisterParameter("enable_resource_tree_usage_lock_profiling", EnableResourceTreeUsageLockProfiling)
        .Default(true);

    RegisterParameter("preemption_check_starvation", PreemptionCheckStarvation)
        .Default(true);

    RegisterParameter("preemption_check_satisfaction", PreemptionCheckSatisfaction)
        .Default(true);

    RegisterParameter("job_interrupt_timeout", JobInterruptTimeout)
        .Default(TDuration::Seconds(10));

    RegisterParameter("job_graceful_interrupt_timeout", JobGracefulInterruptTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("scheduling_segments", SchedulingSegments)
        .DefaultNew();

    RegisterParameter("enable_pools_vector_profiling", EnablePoolsVectorProfiling)
        .Default(true);

    RegisterParameter("enable_operations_vector_profiling", EnableOperationsVectorProfiling)
        .Default(false);

    RegisterParameter("enable_limiting_ancestor_check", EnableLimitingAncestorCheck)
        .Default(true);

    RegisterParameter("profiled_pool_resources", ProfiledPoolResources)
        .Default({
            EJobResourceType::Cpu,
            EJobResourceType::Memory,
            EJobResourceType::UserSlots,
            EJobResourceType::Gpu,
            EJobResourceType::Network
        });

    RegisterParameter("profiled_operation_resources", ProfiledOperationResources)
        .Default({
            EJobResourceType::Cpu,
            EJobResourceType::Memory,
            EJobResourceType::UserSlots,
            EJobResourceType::Gpu,
            EJobResourceType::Network
        });

    RegisterParameter("waiting_job_timeout", WaitingJobTimeout)
        .Default();

    RegisterParameter("min_child_heap_size", MinChildHeapSize)
        .Default(16);
    
    RegisterParameter("main_resource", MainResource)
        .Default(EJobResourceType::Cpu);

    RegisterPostprocessor([&] () {
        if (AggressivePreemptionSatisfactionThreshold > PreemptionSatisfactionThreshold) {
            THROW_ERROR_EXCEPTION("Aggressive preemption satisfaction threshold must be less than preemption satisfaction threshold")
                << TErrorAttribute("aggressive_threshold", AggressivePreemptionSatisfactionThreshold)
                << TErrorAttribute("threshold", PreemptionSatisfactionThreshold);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TPoolTreesTemplateConfig::TPoolTreesTemplateConfig()
{
    RegisterParameter("priority", Priority);
    
    RegisterParameter("filter", Filter);
    
    RegisterParameter("config", Config);
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

    RegisterParameter("resource_metering_period", ResourceMeteringPeriod)
        .Default(TDuration::Minutes(1));

    RegisterParameter("operation_hangup_check_period", OperationHangupCheckPeriod)
        .Alias("operation_unschedulable_check_period")
        .Default(TDuration::Minutes(1));

    RegisterParameter("operation_hangup_safe_timeout", OperationHangupSafeTimeout)
        .Alias("operation_unschedulable_safe_timeout")
        .Default(TDuration::Minutes(60));

    RegisterParameter("operation_hangup_min_schedule_job_attempts", OperationHangupMinScheduleJobAttempts)
        .Alias("operation_unschedulable_min_schedule_job_attempts")
        .Default(1000);

    RegisterParameter("operation_hangup_deactivation_reasons", OperationHangupDeactivationReasons)
        .Alias("operation_unschedulable_deactivation_reasons")
        .Default({EDeactivationReason::ScheduleJobFailed, EDeactivationReason::MinNeededResourcesUnsatisfied});

    RegisterParameter("operation_hangup_due_to_limiting_ancestor_safe_timeout", OperationHangupDueToLimitingAncestorSafeTimeout)
        .Alias("operation_unschedulable_due_to_limiting_ancestor_safe_timeout")
        .Default(TDuration::Minutes(5));

    RegisterParameter("max_operation_count", MaxOperationCount)
        .Default(5000)
        .GreaterThan(0)
        // This value corresponds to the maximum possible number of memory tags.
        // It should be changed simultaneously with values of all `MaxTagValue`
        // across the code base.
        .LessThan(NYTAlloc::MaxMemoryTag);

    RegisterParameter("operations_without_tentative_pool_trees", OperationsWithoutTentativePoolTrees)
        .Default({EOperationType::Sort, EOperationType::MapReduce, EOperationType::RemoteCopy});

    RegisterParameter("default_tentative_pool_trees", DefaultTentativePoolTrees)
        .Default();

    RegisterParameter("enable_schedule_in_single_tree", EnableScheduleInSingleTree)
        .Default(true);

    RegisterParameter("strategy_testing_options", StrategyTestingOptions)
        .DefaultNew();
    
    RegisterParameter("template_pool_tree_config_map", TemplatePoolTreeConfigMap)
        .Default();
    
    RegisterPostprocessor([&] {
        THashMap<int, TStringBuf> priorityToName;
        priorityToName.reserve(std::size(TemplatePoolTreeConfigMap));

        for (const auto& [name, value] : TemplatePoolTreeConfigMap) {
            if (const auto [it, inserted] = priorityToName.try_emplace(value->Priority, name); !inserted) {
                THROW_ERROR_EXCEPTION("\"template_pool_tree_config_map\" has equal priority for templates")
                    << TErrorAttribute("template_names", std::array{it->second, TStringBuf{name}});
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TTestingOptions::TTestingOptions()
{
    RegisterParameter("enable_random_master_disconnection", EnableRandomMasterDisconnection)
        .Default(false);
    RegisterParameter("random_master_disconnection_max_backoff", RandomMasterDisconnectionMaxBackoff)
        .Default(TDuration::Seconds(5));
    RegisterParameter("master_disconnect_delay", MasterDisconnectDelay)
        .Default();
    RegisterParameter("finish_operation_transition_delay", FinishOperationTransitionDelay)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TOperationsCleanerConfig::TOperationsCleanerConfig()
{
    RegisterParameter("enable", Enable)
        .Default(true);
    RegisterParameter("enable_archivation", EnableArchivation)
        .Default(true);
    RegisterParameter("clean_delay", CleanDelay)
        .Default(TDuration::Minutes(5));
    RegisterParameter("analysis_period", AnalysisPeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("remove_batch_size", RemoveBatchSize)
        .Default(256);
    RegisterParameter("remove_subbatch_size", RemoveSubbatchSize)
        .Default(64);
    RegisterParameter("remove_batch_timeout", RemoveBatchTimeout)
        .Default(TDuration::Seconds(5));
    RegisterParameter("archive_batch_size", ArchiveBatchSize)
        .Default(100);
    RegisterParameter("archive_batch_timeout", ArchiveBatchTimeout)
        .Default(TDuration::Seconds(5));
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
        .Default(20000);
    RegisterParameter("archivation_enable_delay", ArchivationEnableDelay)
        .Default(TDuration::Minutes(30));
    RegisterParameter("max_removal_sleep_delay", MaxRemovalSleepDelay)
        .Default(TDuration::Seconds(5));
    RegisterParameter("min_operation_count_enqueued_for_alert", MinOperationCountEnqueuedForAlert)
        .Default(500);
    RegisterParameter("finished_operations_archive_lookup_timeout", FinishedOperationsArchiveLookupTimeout)
        .Default(TDuration::Seconds(30));
    RegisterParameter("parse_operation_attributes_batch_size", ParseOperationAttributesBatchSize)
        .Default(100);

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

TSchedulerIntegralGuaranteesConfig::TSchedulerIntegralGuaranteesConfig()
{
    RegisterParameter("smooth_period", SmoothPeriod)
        .Default(TDuration::Minutes(1));

    RegisterParameter("pool_capacity_saturation_period", PoolCapacitySaturationPeriod)
        .Default(TDuration::Days(1));

    RegisterParameter("relaxed_share_multiplier_limit", RelaxedShareMultiplierLimit)
        .Default(3);
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

    RegisterParameter("incarnation_transaction_timeout", IncarnationTransactionTimeout)
        .Default(TDuration::Seconds(30));

    RegisterParameter("incarnation_transaction_ping_period", IncarnationTransactionPingPeriod)
        .Default();

    RegisterParameter("agent_pick_strategy", AgentPickStrategy)
        .Default(EControllerAgentPickStrategy::Random);

    RegisterParameter("min_agent_count", MinAgentCount)
        .Default(1);

    RegisterParameter("min_agent_available_memory", MinAgentAvailableMemory)
        .Default(1_GB);

    RegisterParameter("min_agent_available_memory_fraction", MinAgentAvailableMemoryFraction)
        .InRange(0.0, 1.0)
        .Default(0.05);

    RegisterParameter("memory_balanced_pick_strategy_score_power", MemoryBalancedPickStrategyScorePower)
        .Default(1.0);
}

////////////////////////////////////////////////////////////////////////////////

TResourceMeteringConfig::TResourceMeteringConfig()
{
    RegisterParameter("enable_new_abc_format", EnableNewAbcFormat)
        .Default(true);

    RegisterParameter("default_abc_id", DefaultAbcId)
        .Default(-1);

    RegisterParameter("default_cloud_id", DefaultCloudId)
        .Default();

    RegisterParameter("default_folder_id", DefaultFolderId)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerConfig::TSchedulerConfig()
{
    SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);

    RegisterParameter("node_shard_count", NodeShardCount)
        .Default(4)
        .InRange(1, MaxNodeShardCount);

    RegisterParameter("connect_retry_backoff_time", ConnectRetryBackoffTime)
        .Default(TDuration::Seconds(15));

    RegisterParameter("node_heartbeat_timeout", NodeHeartbeatTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("node_registration_timeout", NodeRegistrationTimeout)
        .Default(TDuration::Seconds(600));

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

    // NB: This setting is NOT synchronized with the Cypress while scheduler is connected to master.
    RegisterParameter("lock_transaction_timeout", LockTransactionTimeout)
        .Default(TDuration::Seconds(30));
    RegisterParameter("job_prober_rpc_timeout", JobProberRpcTimeout)
        .Default(TDuration::Seconds(300));

    RegisterParameter("cluster_info_logging_period", ClusterInfoLoggingPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("nodes_info_logging_period", NodesInfoLoggingPeriod)
        .Default(TDuration::Seconds(30));
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
    RegisterParameter("pending_by_pool_operation_scan_period", PendingByPoolOperationScanPeriod)
        .Default(TDuration::Minutes(1));

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
        .Default(true);
    RegisterParameter("enable_job_spec_reporter", EnableJobSpecReporter)
        .Default(true);
    RegisterParameter("enable_job_stderr_reporter", EnableJobStderrReporter)
        .Default(true);
    RegisterParameter("enable_job_profile_reporter", EnableJobProfileReporter)
        .Default(true);
    RegisterParameter("enable_job_fail_context_reporter", EnableJobFailContextReporter)
        .Default(true);

    RegisterParameter("enable_unrecognized_alert", EnableUnrecognizedAlert)
        .Default(true);

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

    RegisterParameter("operations_destroy_period", OperationsDestroyPeriod)
        .Default(TDuration::Seconds(1));

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

    RegisterParameter("operation_transaction_ping_period", OperationTransactionPingPeriod)
        .Default(TDuration::Seconds(30));

    RegisterParameter("pool_change_is_allowed", PoolChangeIsAllowed)
        .Default(true);

    RegisterParameter("skip_operations_with_malformed_spec_during_revival", SkipOperationsWithMalformedSpecDuringRevival)
        .Default(false);

    RegisterParameter("max_offline_node_age", MaxOfflineNodeAge)
        .Default(TDuration::Hours(12));

    RegisterParameter("max_node_unseen_period_to_abort_jobs", MaxNodeUnseenPeriodToAbortJobs)
        .Default(TDuration::Minutes(5));

    RegisterParameter("orchid_worker_thread_count", OrchidWorkerThreadCount)
        .Default(4)
        .GreaterThan(0);

    RegisterParameter("fair_share_update_thread_count", FairShareUpdateThreadCount)
        .Default(4)
        .GreaterThan(0);

    RegisterParameter("handle_node_id_changes_strictly", HandleNodeIdChangesStrictly)
        .Default(true);

    RegisterParameter("allowed_node_resources_overcommit_duration", AllowedNodeResourcesOvercommitDuration)
        .Default(TDuration::Seconds(15));

    RegisterParameter("pool_trees_root", PoolTreesRoot)
        .Default(PoolTreesRootCypressPath);

    RegisterParameter("validate_node_tags_period", ValidateNodeTagsPeriod)
        .Default(TDuration::Seconds(30));

    RegisterParameter("enable_job_abort_on_zero_user_slots", EnableJobAbortOnZeroUserSlots)
        .Default(true);

    RegisterParameter("fetch_operation_attributes_subbatch_size", FetchOperationAttributesSubbatchSize)
        .Default(1000);

    RegisterParameter("resource_metering", ResourceMetering)
        .DefaultNew();

    RegisterParameter("scheduling_segments_manage_period", SchedulingSegmentsManagePeriod)
        .Default(TDuration::Seconds(10));

    RegisterParameter("scheduling_segments_initialization_timeout", SchedulingSegmentsInitializationTimeout)
        .Default(TDuration::Minutes(5));

    RegisterParameter("parse_operation_attributes_batch_size", ParseOperationAttributesBatchSize)
        .Default(100);

    RegisterParameter("experiments", Experiments)
        .Default();

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

        ValidateExperiments(Experiments);
    });
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerBootstrapConfig::TSchedulerBootstrapConfig()
{
    RegisterParameter("cluster_connection", ClusterConnection);
    RegisterParameter("scheduler", Scheduler)
        .DefaultNew();
    RegisterParameter("response_keeper", ResponseKeeper)
        .DefaultNew();
    RegisterParameter("addresses", Addresses)
        .Default();
    RegisterParameter("cypress_annotations", CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
            ->AsMap());

    RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
        .Default(false);

    RegisterPreprocessor([&] () {
        ResponseKeeper->EnableWarmup = false;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
