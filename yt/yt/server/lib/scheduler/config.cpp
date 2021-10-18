#include "config.h"

#include "experiments.h"
#include "yt/yt/server/lib/scheduler/public.h"

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/server/lib/node_tracker_server/name_helpers.h>
#include <yt/yt/server/scheduler/private.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobResourcesConfigPtr GetDefaultMinSpareJobResourcesOnNode()
{
    auto config = New<TJobResourcesConfig>();
    config->UserSlots = 1;
    config->Cpu = 1;
    config->Memory = 256_MB;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TStrategyTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("delay_inside_fair_share_update", &TStrategyTestingOptions::DelayInsideFairShareUpdate)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategyControllerThrottling::Register(TRegistrar registrar)
{
    registrar.Parameter("schedule_job_start_backoff_time", &TFairShareStrategyControllerThrottling::ScheduleJobStartBackoffTime)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("schedule_job_max_backoff_time", &TFairShareStrategyControllerThrottling::ScheduleJobMaxBackoffTime)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("schedule_job_backoff_multiplier", &TFairShareStrategyControllerThrottling::ScheduleJobBackoffMultiplier)
        .Default(1.1);
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategyOperationControllerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_concurrent_controller_schedule_job_calls", &TFairShareStrategyOperationControllerConfig::MaxConcurrentControllerScheduleJobCalls)
        .Default(100)
        .GreaterThan(0);

    registrar.Parameter("concurrent_controller_schedule_job_calls_regularization", &TFairShareStrategyOperationControllerConfig::ConcurrentControllerScheduleJobCallsRegularization)
        .Default(2.0)
        .GreaterThanOrEqual(1.0);

    registrar.Parameter("schedule_job_time_limit", &TFairShareStrategyOperationControllerConfig::ScheduleJobTimeLimit)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("schedule_job_fail_backoff_time", &TFairShareStrategyOperationControllerConfig::ScheduleJobFailBackoffTime)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("controller_throttling", &TFairShareStrategyOperationControllerConfig::ControllerThrottling)
        .DefaultNew();

    registrar.Parameter("schedule_job_timeout_alert_reset_time", &TFairShareStrategyOperationControllerConfig::ScheduleJobTimeoutAlertResetTime)
        .Default(TDuration::Minutes(15));

    registrar.Parameter("schedule_jobs_timeout", &TFairShareStrategyOperationControllerConfig::ScheduleJobsTimeout)
        .Default(TDuration::Seconds(40));

    registrar.Parameter("long_schedule_job_logging_threshold", &TFairShareStrategyOperationControllerConfig::LongScheduleJobLoggingThreshold)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategySchedulingSegmentsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("mode", &TFairShareStrategySchedulingSegmentsConfig::Mode)
        .Default(ESegmentedSchedulingMode::Disabled);

    registrar.Parameter("satisfaction_margins", &TFairShareStrategySchedulingSegmentsConfig::SatisfactionMargins)
        .Default();

    registrar.Parameter("unsatisfied_segments_rebalancing_timeout", &TFairShareStrategySchedulingSegmentsConfig::UnsatisfiedSegmentsRebalancingTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("data_center_reconsideration_timeout", &TFairShareStrategySchedulingSegmentsConfig::DataCenterReconsiderationTimeout)
        .Default(TDuration::Minutes(20));

    registrar.Parameter("data_centers", &TFairShareStrategySchedulingSegmentsConfig::DataCenters)
        .Default();

    registrar.Parameter("data_center_assignment_heuristic", &TFairShareStrategySchedulingSegmentsConfig::DataCenterAssignmentHeuristic)
        .Default(ESchedulingSegmentDataCenterAssignmentHeuristic::MaxRemainingCapacity);

    registrar.Postprocessor([&] (TFairShareStrategySchedulingSegmentsConfig* config) {
        for (const auto& dataCenter : config->DataCenters) {
            ValidateDataCenterName(dataCenter);
        }
    });

    registrar.Postprocessor([&] (TFairShareStrategySchedulingSegmentsConfig* config) {
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            if (!IsDataCenterAwareSchedulingSegment(segment)) {
                continue;
            }

            for (const auto& dataCenter : config->SatisfactionMargins.At(segment).GetDataCenters()) {
                if (!dataCenter) {
                    // This could never happen but I'm afraid to put YT_VERIFY here.
                    THROW_ERROR_EXCEPTION("Satisfaction margin can be specified only for non-null data centers");
                }

                if (config->DataCenters.find(*dataCenter) == config->DataCenters.end()) {
                    THROW_ERROR_EXCEPTION("Satisfaction margin can be specified only for configured data centers")
                        << TErrorAttribute("configured_data_centers", config->DataCenters)
                        << TErrorAttribute("specified_data_center", dataCenter);
                }
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategyTreeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("nodes_filter", &TFairShareStrategyTreeConfig::NodesFilter)
        .Default();

    registrar.Parameter("fair_share_starvation_timeout", &TFairShareStrategyTreeConfig::FairShareStarvationTimeout)
        .Alias("fair_share_preemption_timeout")
        .Default(TDuration::Seconds(30));
    registrar.Parameter("fair_share_aggressive_starvation_timeout", &TFairShareStrategyTreeConfig::FairShareAggressiveStarvationTimeout)
        .Default(TDuration::Seconds(120));
    registrar.Parameter("fair_share_starvation_tolerance", &TFairShareStrategyTreeConfig::FairShareStarvationTolerance)
        .InRange(0.0, 1.0)
        .Default(0.8);

    registrar.Parameter("enable_aggressive_starvation", &TFairShareStrategyTreeConfig::EnableAggressiveStarvation)
        .Default(false);

    registrar.Parameter("max_unpreemptable_running_job_count", &TFairShareStrategyTreeConfig::MaxUnpreemptableRunningJobCount)
        .Default(10);

    registrar.Parameter("max_running_operation_count", &TFairShareStrategyTreeConfig::MaxRunningOperationCount)
        .Default(200)
        .GreaterThan(0);

    registrar.Parameter("max_running_operation_count_per_pool", &TFairShareStrategyTreeConfig::MaxRunningOperationCountPerPool)
        .Default(50)
        .GreaterThan(0);

    registrar.Parameter("max_operation_count_per_pool", &TFairShareStrategyTreeConfig::MaxOperationCountPerPool)
        .Default(50)
        .GreaterThan(0);

    registrar.Parameter("max_operation_count", &TFairShareStrategyTreeConfig::MaxOperationCount)
        .Default(50000)
        .GreaterThan(0);

    registrar.Parameter("enable_pool_starvation", &TFairShareStrategyTreeConfig::EnablePoolStarvation)
        .Default(true);

    registrar.Parameter("default_parent_pool", &TFairShareStrategyTreeConfig::DefaultParentPool)
        .Default(RootPoolName);

    registrar.Parameter("forbid_immediate_operations_in_root", &TFairShareStrategyTreeConfig::ForbidImmediateOperationsInRoot)
        .Default(true);

    registrar.Parameter("job_count_preemption_timeout_coefficient", &TFairShareStrategyTreeConfig::JobCountPreemptionTimeoutCoefficient)
        .Default(1.0)
        .GreaterThanOrEqual(1.0);

    registrar.Parameter("preemption_satisfaction_threshold", &TFairShareStrategyTreeConfig::PreemptionSatisfactionThreshold)
        .Default(1.0)
        .GreaterThan(0);

    registrar.Parameter("aggressive_preemption_satisfaction_threshold", &TFairShareStrategyTreeConfig::AggressivePreemptionSatisfactionThreshold)
        .Default(0.2)
        .GreaterThanOrEqual(0);

    registrar.Parameter("enable_scheduling_tags", &TFairShareStrategyTreeConfig::EnableSchedulingTags)
        .Default(true);

    registrar.Parameter("heartbeat_tree_scheduling_info_log_period", &TFairShareStrategyTreeConfig::HeartbeatTreeSchedulingInfoLogBackoff)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("max_ephemeral_pools_per_user", &TFairShareStrategyTreeConfig::MaxEphemeralPoolsPerUser)
        .GreaterThanOrEqual(1)
        .Default(1);

    registrar.Parameter("update_preemptable_list_duration_logging_threshold", &TFairShareStrategyTreeConfig::UpdatePreemptableListDurationLoggingThreshold)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("enable_operations_profiling", &TFairShareStrategyTreeConfig::EnableOperationsProfiling)
        .Default(true);

    registrar.Parameter("custom_profiling_tag_filter", &TFairShareStrategyTreeConfig::CustomProfilingTagFilter)
        .Default();

    registrar.Parameter("total_resource_limits_consider_delay", &TFairShareStrategyTreeConfig::TotalResourceLimitsConsiderDelay)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("preemptive_scheduling_backoff", &TFairShareStrategyTreeConfig::PreemptiveSchedulingBackoff)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("tentative_tree_saturation_deactivation_period", &TFairShareStrategyTreeConfig::TentativeTreeSaturationDeactivationPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("infer_weight_from_guarantees_share_multiplier", &TFairShareStrategyTreeConfig::InferWeightFromGuaranteesShareMultiplier)
        .Alias("infer_weight_from_strong_guarantee_share_multiplier")
        .Alias("infer_weight_from_min_share_ratio_multiplier")
        .Default()
        .GreaterThanOrEqual(1.0);

    registrar.Parameter("packing", &TFairShareStrategyTreeConfig::Packing)
        .DefaultNew();

    registrar.Parameter("non_tentative_operation_types", &TFairShareStrategyTreeConfig::NonTentativeOperationTypes)
        .Default(std::nullopt);

    registrar.Parameter("best_allocation_ratio_update_period", &TFairShareStrategyTreeConfig::BestAllocationRatioUpdatePeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("enable_by_user_profiling", &TFairShareStrategyTreeConfig::EnableByUserProfiling)
        .Default(true);

    registrar.Parameter("integral_guarantees", &TFairShareStrategyTreeConfig::IntegralGuarantees)
        .DefaultNew();

    registrar.Parameter("enable_resource_tree_structure_lock_profiling", &TFairShareStrategyTreeConfig::EnableResourceTreeStructureLockProfiling)
        .Default(true);

    registrar.Parameter("enable_resource_tree_usage_lock_profiling", &TFairShareStrategyTreeConfig::EnableResourceTreeUsageLockProfiling)
        .Default(true);

    registrar.Parameter("preemption_check_starvation", &TFairShareStrategyTreeConfig::PreemptionCheckStarvation)
        .Default(true);

    registrar.Parameter("preemption_check_satisfaction", &TFairShareStrategyTreeConfig::PreemptionCheckSatisfaction)
        .Default(true);

    registrar.Parameter("job_interrupt_timeout", &TFairShareStrategyTreeConfig::JobInterruptTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("job_graceful_interrupt_timeout", &TFairShareStrategyTreeConfig::JobGracefulInterruptTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("scheduling_segments", &TFairShareStrategyTreeConfig::SchedulingSegments)
        .DefaultNew();

    registrar.Parameter("enable_pools_vector_profiling", &TFairShareStrategyTreeConfig::EnablePoolsVectorProfiling)
        .Default(true);

    registrar.Parameter("enable_operations_vector_profiling", &TFairShareStrategyTreeConfig::EnableOperationsVectorProfiling)
        .Default(false);

    registrar.Parameter("enable_limiting_ancestor_check", &TFairShareStrategyTreeConfig::EnableLimitingAncestorCheck)
        .Default(true);

    registrar.Parameter("profiled_pool_resources", &TFairShareStrategyTreeConfig::ProfiledPoolResources)
        .Default({
            EJobResourceType::Cpu,
            EJobResourceType::Memory,
            EJobResourceType::UserSlots,
            EJobResourceType::Gpu,
            EJobResourceType::Network
        });

    registrar.Parameter("profiled_operation_resources", &TFairShareStrategyTreeConfig::ProfiledOperationResources)
        .Default({
            EJobResourceType::Cpu,
            EJobResourceType::Memory,
            EJobResourceType::UserSlots,
            EJobResourceType::Gpu,
            EJobResourceType::Network
        });

    registrar.Parameter("waiting_job_timeout", &TFairShareStrategyTreeConfig::WaitingJobTimeout)
        .Default();

    registrar.Parameter("min_child_heap_size", &TFairShareStrategyTreeConfig::MinChildHeapSize)
        .Default(16);

    registrar.Parameter("main_resource", &TFairShareStrategyTreeConfig::MainResource)
        .Default(EJobResourceType::Cpu);

    registrar.Parameter("metering_tags", &TFairShareStrategyTreeConfig::MeteringTags)
        .Default();
    
    registrar.Parameter("pool_config_presets", &TFairShareStrategyTreeConfig::PoolConfigPresets)
        .Default();

    registrar.Parameter("enable_fair_share_truncation_in_fifo_pool", &TFairShareStrategyTreeConfig::EnableFairShareTruncationInFifoPool)
        .Alias("truncate_fifo_pool_unsatisfied_child_fair_share")
        .Default(false);

    registrar.Parameter("enable_conditional_preemption", &TFairShareStrategyTreeConfig::EnableConditionalPreemption)
        .Default(false);
    
    registrar.Parameter("use_resource_usage_with_precommit", &TFairShareStrategyTreeConfig::UseResourceUsageWithPrecommit)
        .Default(false);

    registrar.Parameter("allowed_resource_usage_staleness", &TFairShareStrategyTreeConfig::AllowedResourceUsageStaleness)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("cached_job_preemption_statuses_update_period", &TFairShareStrategyTreeConfig::CachedJobPreemptionStatusesUpdatePeriod)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("should_distribute_free_volume_among_children", &TFairShareStrategyTreeConfig::ShouldDistributeFreeVolumeAmongChildren)
        // TODO(renadeen): temporarily disabled.
        .Default(false);

    registrar.Parameter("use_user_default_parent_pool_map", &TFairShareStrategyTreeConfig::UseUserDefaultParentPoolMap)
        .Default(false);
    
    registrar.Parameter("enable_resource_usage_snapshot", &TFairShareStrategyTreeConfig::EnableResourceUsageSnapshot)
        .Default(false);    

    registrar.Postprocessor([&] (TFairShareStrategyTreeConfig* config) {
        if (config->AggressivePreemptionSatisfactionThreshold > config->PreemptionSatisfactionThreshold) {
            THROW_ERROR_EXCEPTION("Aggressive starvation satisfaction threshold must be less than starvation satisfaction threshold")
                << TErrorAttribute("aggressive_threshold", config->AggressivePreemptionSatisfactionThreshold)
                << TErrorAttribute("threshold", config->PreemptionSatisfactionThreshold);
        }
        if (config->FairShareAggressiveStarvationTimeout < config->FairShareStarvationTimeout) {
            THROW_ERROR_EXCEPTION("Aggressive starvation timeout must be greater than starvation timeout")
                << TErrorAttribute("aggressive_timeout", config->FairShareAggressiveStarvationTimeout)
                << TErrorAttribute("timeout", config->FairShareStarvationTimeout);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TPoolTreesTemplateConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("priority", &TPoolTreesTemplateConfig::Priority);

    registrar.Parameter("filter", &TPoolTreesTemplateConfig::Filter);

    registrar.Parameter("config", &TPoolTreesTemplateConfig::Config);
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("fair_share_update_period", &TFairShareStrategyConfig::FairShareUpdatePeriod)
        .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
        .Default(TDuration::MilliSeconds(1000));

    registrar.Parameter("fair_share_profiling_period", &TFairShareStrategyConfig::FairShareProfilingPeriod)
        .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
        .Default(TDuration::MilliSeconds(5000));

    registrar.Parameter("fair_share_log_period", &TFairShareStrategyConfig::FairShareLogPeriod)
        .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
        .Default(TDuration::MilliSeconds(1000));

    registrar.Parameter("min_needed_resources_update_period", &TFairShareStrategyConfig::MinNeededResourcesUpdatePeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("resource_metering_period", &TFairShareStrategyConfig::ResourceMeteringPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("resource_usage_snapshot_update_period", &TFairShareStrategyConfig::ResourceUsageSnapshotUpdatePeriod)
        .Default(TDuration::MilliSeconds(20));

    registrar.Parameter("operation_hangup_check_period", &TFairShareStrategyConfig::OperationHangupCheckPeriod)
        .Alias("operation_unschedulable_check_period")
        .Default(TDuration::Minutes(1));

    registrar.Parameter("operation_hangup_safe_timeout", &TFairShareStrategyConfig::OperationHangupSafeTimeout)
        .Alias("operation_unschedulable_safe_timeout")
        .Default(TDuration::Minutes(60));

    registrar.Parameter("operation_hangup_min_schedule_job_attempts", &TFairShareStrategyConfig::OperationHangupMinScheduleJobAttempts)
        .Alias("operation_unschedulable_min_schedule_job_attempts")
        .Default(1000);

    registrar.Parameter("operation_hangup_deactivation_reasons", &TFairShareStrategyConfig::OperationHangupDeactivationReasons)
        .Alias("operation_unschedulable_deactivation_reasons")
        .Default({EDeactivationReason::ScheduleJobFailed, EDeactivationReason::MinNeededResourcesUnsatisfied});

    registrar.Parameter("operation_hangup_due_to_limiting_ancestor_safe_timeout", &TFairShareStrategyConfig::OperationHangupDueToLimitingAncestorSafeTimeout)
        .Alias("operation_unschedulable_due_to_limiting_ancestor_safe_timeout")
        .Default(TDuration::Minutes(5));

    registrar.Parameter("max_operation_count", &TFairShareStrategyConfig::MaxOperationCount)
        .Default(5000)
        .GreaterThan(0)
        // This value corresponds to the maximum possible number of memory tags.
        // It should be changed simultaneously with values of all `MaxTagValue`
        // across the code base.
        .LessThan(NYTAlloc::MaxMemoryTag);

    registrar.Parameter("operations_without_tentative_pool_trees", &TFairShareStrategyConfig::OperationsWithoutTentativePoolTrees)
        .Default({EOperationType::Sort, EOperationType::MapReduce, EOperationType::RemoteCopy});

    registrar.Parameter("default_tentative_pool_trees", &TFairShareStrategyConfig::DefaultTentativePoolTrees)
        .Default();

    registrar.Parameter("enable_schedule_in_single_tree", &TFairShareStrategyConfig::EnableScheduleInSingleTree)
        .Default(true);

    registrar.Parameter("strategy_testing_options", &TFairShareStrategyConfig::StrategyTestingOptions)
        .DefaultNew();

    registrar.Parameter("template_pool_tree_config_map", &TFairShareStrategyConfig::TemplatePoolTreeConfigMap)
        .Default();

    registrar.Postprocessor([&] (TFairShareStrategyConfig* config) {
        THashMap<int, TStringBuf> priorityToName;
        priorityToName.reserve(std::size(config->TemplatePoolTreeConfigMap));

        for (const auto& [name, value] : config->TemplatePoolTreeConfigMap) {
            if (const auto [it, inserted] = priorityToName.try_emplace(value->Priority, name); !inserted) {
                THROW_ERROR_EXCEPTION("\"template_pool_tree_config_map\" has equal priority for templates")
                    << TErrorAttribute("template_names", std::array{it->second, TStringBuf{name}});
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_random_master_disconnection", &TTestingOptions::EnableRandomMasterDisconnection)
        .Default(false);
    registrar.Parameter("random_master_disconnection_max_backoff", &TTestingOptions::RandomMasterDisconnectionMaxBackoff)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("master_disconnect_delay", &TTestingOptions::MasterDisconnectDelay)
        .Default();
    registrar.Parameter("handle_orphaned_operations_delay", &TTestingOptions::HandleOrphanedOperationsDelay)
        .Default();
    registrar.Parameter("finish_operation_transition_delay", &TTestingOptions::FinishOperationTransitionDelay)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TOperationsCleanerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TOperationsCleanerConfig::Enable)
        .Default(true);
    registrar.Parameter("enable_archivation", &TOperationsCleanerConfig::EnableArchivation)
        .Default(true);
    registrar.Parameter("clean_delay", &TOperationsCleanerConfig::CleanDelay)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("analysis_period", &TOperationsCleanerConfig::AnalysisPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("remove_batch_size", &TOperationsCleanerConfig::RemoveBatchSize)
        .Default(256);
    registrar.Parameter("remove_subbatch_size", &TOperationsCleanerConfig::RemoveSubbatchSize)
        .Default(64);
    registrar.Parameter("remove_batch_timeout", &TOperationsCleanerConfig::RemoveBatchTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("archive_batch_size", &TOperationsCleanerConfig::ArchiveBatchSize)
        .Default(100);
    registrar.Parameter("archive_batch_timeout", &TOperationsCleanerConfig::ArchiveBatchTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_operation_age", &TOperationsCleanerConfig::MaxOperationAge)
        .Default(TDuration::Hours(6));
    registrar.Parameter("max_operation_count_per_user", &TOperationsCleanerConfig::MaxOperationCountPerUser)
        .Default(200);
    registrar.Parameter("soft_retained_operation_count", &TOperationsCleanerConfig::SoftRetainedOperationCount)
        .Default(200);
    registrar.Parameter("hard_retained_operation_count", &TOperationsCleanerConfig::HardRetainedOperationCount)
        .Default(4000);
    registrar.Parameter("min_archivation_retry_sleep_delay", &TOperationsCleanerConfig::MinArchivationRetrySleepDelay)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("max_archivation_retry_sleep_delay", &TOperationsCleanerConfig::MaxArchivationRetrySleepDelay)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("max_operation_count_enqueued_for_archival", &TOperationsCleanerConfig::MaxOperationCountEnqueuedForArchival)
        .Default(20000);
    registrar.Parameter("archivation_enable_delay", &TOperationsCleanerConfig::ArchivationEnableDelay)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("max_removal_sleep_delay", &TOperationsCleanerConfig::MaxRemovalSleepDelay)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("min_operation_count_enqueued_for_alert", &TOperationsCleanerConfig::MinOperationCountEnqueuedForAlert)
        .Default(500);
    registrar.Parameter("finished_operations_archive_lookup_timeout", &TOperationsCleanerConfig::FinishedOperationsArchiveLookupTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("parse_operation_attributes_batch_size", &TOperationsCleanerConfig::ParseOperationAttributesBatchSize)
        .Default(100);

    registrar.Postprocessor([&] (TOperationsCleanerConfig* config) {
        if (config->MaxArchivationRetrySleepDelay <= config->MinArchivationRetrySleepDelay) {
            THROW_ERROR_EXCEPTION("\"max_archivation_retry_sleep_delay\" must be greater than "
                "\"min_archivation_retry_sleep_delay\"")
                << TErrorAttribute("min_archivation_retry_sleep_delay", config->MinArchivationRetrySleepDelay)
                << TErrorAttribute("max_archivation_retry_sleep_delay", config->MaxArchivationRetrySleepDelay);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSchedulerIntegralGuaranteesConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("smooth_period", &TSchedulerIntegralGuaranteesConfig::SmoothPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("pool_capacity_saturation_period", &TSchedulerIntegralGuaranteesConfig::PoolCapacitySaturationPeriod)
        .Default(TDuration::Days(1));

    registrar.Parameter("relaxed_share_multiplier_limit", &TSchedulerIntegralGuaranteesConfig::RelaxedShareMultiplierLimit)
        .Default(3);
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TAliveControllerAgentThresholds& thresholds, const NYTree::INodePtr& node)
{
    const auto& mapNode = node->AsMap();

    thresholds.Absolute = mapNode->GetChildOrThrow("absolute")->AsInt64()->GetValue();
    thresholds.Relative = mapNode->GetChildOrThrow("relative")->AsDouble()->GetValue();
}

void Serialize(const TAliveControllerAgentThresholds& thresholds, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("absolute").Value(thresholds.Absolute)
            .Item("relative").Value(thresholds.Relative)
        .EndMap();
}

void TControllerAgentTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("light_rpc_timeout", &TControllerAgentTrackerConfig::LightRpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("heavy_rpc_timeout", &TControllerAgentTrackerConfig::HeavyRpcTimeout)
        .Default(TDuration::Minutes(30));

    registrar.Parameter("heartbeat_timeout", &TControllerAgentTrackerConfig::HeartbeatTimeout)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("incarnation_transaction_timeout", &TControllerAgentTrackerConfig::IncarnationTransactionTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("incarnation_transaction_ping_period", &TControllerAgentTrackerConfig::IncarnationTransactionPingPeriod)
        .Default();

    registrar.Parameter("agent_pick_strategy", &TControllerAgentTrackerConfig::AgentPickStrategy)
        .Default(EControllerAgentPickStrategy::Random);

    registrar.Parameter("min_agent_available_memory", &TControllerAgentTrackerConfig::MinAgentAvailableMemory)
        .Default(1_GB);

    registrar.Parameter("min_agent_available_memory_fraction", &TControllerAgentTrackerConfig::MinAgentAvailableMemoryFraction)
        .InRange(0.0, 1.0)
        .Default(0.05);

    registrar.Parameter("memory_balanced_pick_strategy_score_power", &TControllerAgentTrackerConfig::MemoryBalancedPickStrategyScorePower)
        .Default(1.0);

    registrar.Parameter("min_agent_count", &TControllerAgentTrackerConfig::MinAgentCount)
        .Default(1);

    registrar.Parameter("tag_to_alive_controller_agent_thresholds", &TControllerAgentTrackerConfig::TagToAliveControllerAgentThresholds)
        .Default();

    registrar.Parameter("max_message_job_event_count", &TControllerAgentTrackerConfig::MaxMessageJobEventCount)
        .Default(10000)
        .GreaterThan(0);

    registrar.Postprocessor([&] (TControllerAgentTrackerConfig* config) {
        if (!config->TagToAliveControllerAgentThresholds.contains(DefaultOperationTag)) {
            config->TagToAliveControllerAgentThresholds[DefaultOperationTag] = {static_cast<i64>(config->MinAgentCount), 0.0};
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TResourceMeteringConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_new_abc_format", &TResourceMeteringConfig::EnableNewAbcFormat)
        .Default(true);

    registrar.Parameter("default_abc_id", &TResourceMeteringConfig::DefaultAbcId)
        .Default(-1);

    registrar.Parameter("default_cloud_id", &TResourceMeteringConfig::DefaultCloudId)
        .Default();

    registrar.Parameter("default_folder_id", &TResourceMeteringConfig::DefaultFolderId)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TSchedulerConfig::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);

    registrar.Parameter("node_shard_count", &TSchedulerConfig::NodeShardCount)
        .Default(4)
        .InRange(1, MaxNodeShardCount);

    registrar.Parameter("connect_retry_backoff_time", &TSchedulerConfig::ConnectRetryBackoffTime)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("node_heartbeat_timeout", &TSchedulerConfig::NodeHeartbeatTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("node_registration_timeout", &TSchedulerConfig::NodeRegistrationTimeout)
        .Default(TDuration::Seconds(600));

    registrar.Parameter("watchers_update_period", &TSchedulerConfig::WatchersUpdatePeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("nodes_attributes_update_period", &TSchedulerConfig::NodesAttributesUpdatePeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("profiling_update_period", &TSchedulerConfig::ProfilingUpdatePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("alerts_update_period", &TSchedulerConfig::AlertsUpdatePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("node_shard_submit_jobs_to_strategy_period", &TSchedulerConfig::NodeShardSubmitJobsToStrategyPeriod)
        .Default(TDuration::MilliSeconds(100));

    // NB: This setting is NOT synchronized with the Cypress while scheduler is connected to master.
    registrar.Parameter("lock_transaction_timeout", &TSchedulerConfig::LockTransactionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("pool_trees_lock_transaction_timeout", &TSchedulerConfig::PoolTreesLockTransactionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("pool_trees_lock_check_backoff", &TSchedulerConfig::PoolTreesLockCheckBackoff)
        .Default(TDuration::MilliSeconds(500));

    registrar.Parameter("job_prober_rpc_timeout", &TSchedulerConfig::JobProberRpcTimeout)
        .Default(TDuration::Seconds(300));

    registrar.Parameter("cluster_info_logging_period", &TSchedulerConfig::ClusterInfoLoggingPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("nodes_info_logging_period", &TSchedulerConfig::NodesInfoLoggingPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("exec_node_descriptors_update_period", &TSchedulerConfig::ExecNodeDescriptorsUpdatePeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("jobs_logging_period", &TSchedulerConfig::JobsLoggingPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("running_jobs_update_period", &TSchedulerConfig::RunningJobsUpdatePeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("running_job_statistics_update_period", &TSchedulerConfig::RunningJobStatisticsUpdatePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("missing_jobs_check_period", &TSchedulerConfig::MissingJobsCheckPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("transient_operation_queue_scan_period", &TSchedulerConfig::TransientOperationQueueScanPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("pending_by_pool_operation_scan_period", &TSchedulerConfig::PendingByPoolOperationScanPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("operation_to_agent_assignment_backoff", &TSchedulerConfig::OperationToAgentAssignmentBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("max_started_jobs_per_heartbeat", &TSchedulerConfig::MaxStartedJobsPerHeartbeat)
        .Default()
        .GreaterThan(0);

    registrar.Parameter("node_shard_exec_nodes_cache_update_period", &TSchedulerConfig::NodeShardExecNodesCacheUpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("heartbeat_process_backoff", &TSchedulerConfig::HeartbeatProcessBackoff)
        .Default(TDuration::MilliSeconds(5000));
    registrar.Parameter("soft_concurrent_heartbeat_limit", &TSchedulerConfig::SoftConcurrentHeartbeatLimit)
        .Default(50)
        .GreaterThanOrEqual(1);
    registrar.Parameter("hard_concurrent_heartbeat_limit", &TSchedulerConfig::HardConcurrentHeartbeatLimit)
        .Default(100)
        .GreaterThanOrEqual(1);

    registrar.Parameter("static_orchid_cache_update_period", &TSchedulerConfig::StaticOrchidCacheUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("orchid_keys_update_period", &TSchedulerConfig::OrchidKeysUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("enable_job_reporter", &TSchedulerConfig::EnableJobReporter)
        .Default(true);
    registrar.Parameter("enable_job_spec_reporter", &TSchedulerConfig::EnableJobSpecReporter)
        .Default(true);
    registrar.Parameter("enable_job_stderr_reporter", &TSchedulerConfig::EnableJobStderrReporter)
        .Default(true);
    registrar.Parameter("enable_job_profile_reporter", &TSchedulerConfig::EnableJobProfileReporter)
        .Default(true);
    registrar.Parameter("enable_job_fail_context_reporter", &TSchedulerConfig::EnableJobFailContextReporter)
        .Default(true);

    registrar.Parameter("enable_unrecognized_alert", &TSchedulerConfig::EnableUnrecognizedAlert)
        .Default(true);

    registrar.Parameter("job_revival_abort_timeout", &TSchedulerConfig::JobRevivalAbortTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("scheduling_tag_filter_expire_timeout", &TSchedulerConfig::SchedulingTagFilterExpireTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("operations_cleaner", &TSchedulerConfig::OperationsCleaner)
        .DefaultNew();

    registrar.Parameter("operations_update_period", &TSchedulerConfig::OperationsUpdatePeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("finished_job_storing_timeout", &TSchedulerConfig::FinishedJobStoringTimeout)
        .Default(TDuration::Minutes(30));

    registrar.Parameter("finished_operation_job_storing_timeout", &TSchedulerConfig::FinishedOperationJobStoringTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("operations_destroy_period", &TSchedulerConfig::OperationsDestroyPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("testing_options", &TSchedulerConfig::TestingOptions)
        .DefaultNew();

    registrar.Parameter("event_log", &TSchedulerConfig::EventLog)
        .DefaultNew();

    registrar.Parameter("spec_template", &TSchedulerConfig::SpecTemplate)
        .Default();

    registrar.Parameter("controller_agent_tracker", &TSchedulerConfig::ControllerAgentTracker)
        .DefaultNew();

    registrar.Parameter("job_reporter_issues_check_period", &TSchedulerConfig::JobReporterIssuesCheckPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("job_reporter_write_failures_alert_threshold", &TSchedulerConfig::JobReporterWriteFailuresAlertThreshold)
        .Default(1000);
    registrar.Parameter("job_reporter_queue_is_too_large_alert_threshold", &TSchedulerConfig::JobReporterQueueIsTooLargeAlertThreshold)
        .Default(10);

    registrar.Parameter("node_changes_count_threshold_to_update_cache", &TSchedulerConfig::NodeChangesCountThresholdToUpdateCache)
        .Default(5);

    registrar.Parameter("operation_transaction_ping_period", &TSchedulerConfig::OperationTransactionPingPeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("pool_change_is_allowed", &TSchedulerConfig::PoolChangeIsAllowed)
        .Default(true);

    registrar.Parameter("skip_operations_with_malformed_spec_during_revival", &TSchedulerConfig::SkipOperationsWithMalformedSpecDuringRevival)
        .Default(false);

    registrar.Parameter("max_offline_node_age", &TSchedulerConfig::MaxOfflineNodeAge)
        .Default(TDuration::Hours(12));

    registrar.Parameter("max_node_unseen_period_to_abort_jobs", &TSchedulerConfig::MaxNodeUnseenPeriodToAbortJobs)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("orchid_worker_thread_count", &TSchedulerConfig::OrchidWorkerThreadCount)
        .Default(4)
        .GreaterThan(0);

    registrar.Parameter("fair_share_update_thread_count", &TSchedulerConfig::FairShareUpdateThreadCount)
        .Default(4)
        .GreaterThan(0);

    registrar.Parameter("handle_node_id_changes_strictly", &TSchedulerConfig::HandleNodeIdChangesStrictly)
        .Default(true);

    registrar.Parameter("allowed_node_resources_overcommit_duration", &TSchedulerConfig::AllowedNodeResourcesOvercommitDuration)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("pool_trees_root", &TSchedulerConfig::PoolTreesRoot)
        .Default(PoolTreesRootCypressPath);

    registrar.Parameter("validate_node_tags_period", &TSchedulerConfig::ValidateNodeTagsPeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("enable_job_abort_on_zero_user_slots", &TSchedulerConfig::EnableJobAbortOnZeroUserSlots)
        .Default(true);

    registrar.Parameter("fetch_operation_attributes_subbatch_size", &TSchedulerConfig::FetchOperationAttributesSubbatchSize)
        .Default(1000);

    registrar.Parameter("resource_metering", &TSchedulerConfig::ResourceMetering)
        .DefaultNew();

    registrar.Parameter("scheduling_segments_manage_period", &TSchedulerConfig::SchedulingSegmentsManagePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("scheduling_segments_initialization_timeout", &TSchedulerConfig::SchedulingSegmentsInitializationTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("parse_operation_attributes_batch_size", &TSchedulerConfig::ParseOperationAttributesBatchSize)
        .Default(100);

    registrar.Parameter("experiments", &TSchedulerConfig::Experiments)
        .Default();

    registrar.Parameter("min_spare_job_resources_on_node", &TSchedulerConfig::MinSpareJobResourcesOnNode)
        .DefaultCtor(&GetDefaultMinSpareJobResourcesOnNode);
    
    registrar.Parameter("schedule_job_duration_logging_threshold", &TSchedulerConfig::ScheduleJobDurationLoggingThreshold)
        .Default(TDuration::MilliSeconds(500));

    registrar.Parameter("send_preemption_reason_in_node_heartbeat", &TSchedulerConfig::SendPreemptionReasonInNodeHeartbeat)
        .Default(true);
    
    registrar.Parameter("update_last_metering_log_time", &TSchedulerConfig::UpdateLastMeteringLogTime)
        .Default(true);

    registrar.Preprocessor([&] (TSchedulerConfig* config) {
        config->EventLog->MaxRowWeight = 128_MB;
        if (!config->EventLog->Path) {
            config->EventLog->Path = "//sys/scheduler/event_log";
        }
    });

    registrar.Postprocessor([&] (TSchedulerConfig* config) {
        if (config->SoftConcurrentHeartbeatLimit > config->HardConcurrentHeartbeatLimit) {
            THROW_ERROR_EXCEPTION("\"soft_limit\" must be less than or equal to \"hard_limit\"")
                << TErrorAttribute("soft_limit", config->SoftConcurrentHeartbeatLimit)
                << TErrorAttribute("hard_limit", config->HardConcurrentHeartbeatLimit);
        }

        ValidateExperiments(config->Experiments);
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
