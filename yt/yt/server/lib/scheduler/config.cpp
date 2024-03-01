#include "config.h"

#include "experiments.h"
#include "helpers.h"
#include "public.h"

#include <yt/yt/server/scheduler/private.h>

#include <yt/yt/server/lib/node_tracker_server/name_helpers.h>

#include <yt/yt/ytlib/event_log/config.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/core/misc/config.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TJobResourcesConfigPtr GetDefaultRequiredResourceLimitsForRemoteCopy()
{
    auto config = New<TJobResourcesConfig>();
    config->UserSlots = 1000;
    return config;
}

TJobResourcesConfigPtr GetDefaultMinSpareAllocationResourcesOnNode()
{
    auto config = New<TJobResourcesConfig>();
    config->UserSlots = 1;
    config->Cpu = 1;
    config->Memory = 256_MB;
    return config;
}

TJobResourcesConfigPtr GetDefaultLowPriorityFallbackMinSpareAllocationResources()
{
    auto config = New<TJobResourcesConfig>();
    config->UserSlots = 1;
    config->Cpu = 5;
    config->Memory = 1_GB;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

THistogramDigestConfigPtr GetDefaultPerPoolSatisfactionDigestConfig()
{
    auto config = New<THistogramDigestConfig>();
    config->LowerBound = 0.0;
    config->UpperBound = 2.0;
    config->AbsolutePrecision = 0.001;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TStrategyTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("delay_inside_fair_share_update", &TThis::DelayInsideFairShareUpdate)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategyControllerThrottling::Register(TRegistrar registrar)
{
    registrar.Parameter("schedule_allocation_start_backoff_time", &TThis::ScheduleAllocationStartBackoffTime)
        .Alias("schedule_job_start_backoff_time")
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("schedule_allocation_max_backoff_time", &TThis::ScheduleAllocationMaxBackoffTime)
        .Alias("schedule_job_max_backoff_time")
        .Default(TDuration::Seconds(10));
    registrar.Parameter("schedule_allocation_backoff_multiplier", &TThis::ScheduleAllocationBackoffMultiplier)
        .Alias("schedule_job_backoff_multiplier")
        .Default(1.1);
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategyOperationControllerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_concurrent_controller_schedule_allocation_calls", &TThis::MaxConcurrentControllerScheduleAllocationCalls)
        .Alias("max_concurrent_controller_schedule_job_calls")
        .Default(100)
        .GreaterThan(0);
    registrar.Parameter("max_concurrent_controller_schedule_allocation_exec_duration", &TThis::MaxConcurrentControllerScheduleAllocationExecDuration)
        .Alias("max_concurrent_controller_schedule_job_exec_duration")
        .Default(TDuration::Seconds(1))
        .GreaterThan(TDuration::Zero());

    registrar.Parameter("enable_concurrent_schedule_allocation_exec_duration_throttling", &TThis::EnableConcurrentScheduleAllocationExecDurationThrottling)
        .Alias("enable_concurrent_schedule_job_exec_duration_throttling")
        .Default(false);

    registrar.Parameter("concurrent_controller_schedule_allocation_calls_regularization", &TThis::ConcurrentControllerScheduleAllocationCallsRegularization)
        .Alias("concurrent_controller_schedule_job_calls_regularization")
        .Default(2.0)
        .GreaterThanOrEqual(1.0);

    registrar.Parameter("schedule_allocation_time_limit", &TThis::ScheduleAllocationTimeLimit)
        .Alias("schedule_job_time_limit")
        .Default(TDuration::Seconds(30));

    registrar.Parameter("schedule_allocation_fail_backoff_time", &TThis::ScheduleAllocationFailBackoffTime)
        .Alias("schedule_job_fail_backoff_time")
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("controller_throttling", &TThis::ControllerThrottling)
        .DefaultNew();

    registrar.Parameter("schedule_allocation_timeout_alert_reset_time", &TThis::ScheduleAllocationTimeoutAlertResetTime)
        .Alias("schedule_job_timeout_alert_reset_time")
        .Default(TDuration::Minutes(15));

    registrar.Parameter("schedule_allocations_timeout", &TThis::ScheduleAllocationsTimeout)
        .Alias("schedule_jobs_timeout")
        .Default(TDuration::Seconds(40));

    registrar.Parameter("long_schedule_allocation_logging_threshold", &TThis::LongScheduleAllocationLoggingThreshold)
        .Alias("long_schedule_job_logging_threshold")
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategySchedulingSegmentsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("mode", &TThis::Mode)
        .Default(ESegmentedSchedulingMode::Disabled);

    registrar.Parameter("reserve_fair_resource_amount", &TThis::ReserveFairResourceAmount)
        .Default();

    registrar.Parameter("initialization_timeout", &TThis::InitializationTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("manage_period", &TThis::ManagePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("unsatisfied_segments_rebalancing_timeout", &TThis::UnsatisfiedSegmentsRebalancingTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("module_reconsideration_timeout", &TThis::ModuleReconsiderationTimeout)
        .Alias("data_center_reconsideration_timeout")
        .Default(TDuration::Minutes(20));

    registrar.Parameter("data_centers", &TThis::DataCenters)
        .Default();

    registrar.Parameter("infiniband_clusters", &TThis::InfinibandClusters)
        .Default();

    // TODO(eshcherbin): Change default to MinRemainingFeasibleCapacity.
    registrar.Parameter("module_assignment_heuristic", &TThis::ModuleAssignmentHeuristic)
        .Alias("data_center_assignment_heuristic")
        .Default(ESchedulingSegmentModuleAssignmentHeuristic::MaxRemainingCapacity);

    registrar.Parameter("module_preemption_heuristic", &TThis::ModulePreemptionHeuristic)
        .Default(ESchedulingSegmentModulePreemptionHeuristic::Greedy);

    registrar.Parameter("module_type", &TThis::ModuleType)
        .Default(ESchedulingSegmentModuleType::DataCenter);

    registrar.Parameter("enable_infiniband_cluster_tag_validation", &TThis::EnableInfinibandClusterTagValidation)
        .Default(false);

    // TODO(eshcherbin): Change default to true.
    registrar.Parameter("allow_only_gang_operations_in_large_segment", &TThis::AllowOnlyGangOperationsInLargeSegment)
        .Default(false);

    registrar.Parameter("enable_detailed_logs", &TThis::EnableDetailedLogs)
        .Default(false);

    registrar.Parameter("enable_module_reset_on_zero_fair_share_and_usage", &TThis::EnableModuleResetOnZeroFairShareAndUsage)
        .Default(false);

    registrar.Parameter("priority_module_assignment_timeout", &TThis::PriorityModuleAssignmentTimeout)
        .Default(TDuration::Minutes(15));

    registrar.Postprocessor([&] (TFairShareStrategySchedulingSegmentsConfig* config) {
        for (const auto& schedulingSegmentModule : config->DataCenters) {
            ValidateDataCenterName(schedulingSegmentModule);
        }
        for (const auto& schedulingSegmentModule : config->InfinibandClusters) {
            ValidateInfinibandClusterName(schedulingSegmentModule);
        }
    });

    registrar.Postprocessor([&] (TFairShareStrategySchedulingSegmentsConfig* config) {
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            if (!IsModuleAwareSchedulingSegment(segment)) {
                auto value = config->ReserveFairResourceAmount.At(segment).GetOrDefault();
                if (value < 0.0) {
                    THROW_ERROR_EXCEPTION("Reserve fair resource amount must not be negative")
                        << TErrorAttribute("segment", segment)
                        << TErrorAttribute("value", value);
                }

                continue;
            }

            const auto& configuredModules = config->GetModules();
            const auto& valuesPerModule = config->ReserveFairResourceAmount.At(segment);
            for (const auto& schedulingSegmentModule : valuesPerModule.GetModules()) {
                if (!schedulingSegmentModule) {
                    // This could never happen but I'm afraid to put YT_VERIFY here.
                    THROW_ERROR_EXCEPTION("Reserve fair resource amount can be specified only for non-null modules");
                }

                if (!configuredModules.contains(*schedulingSegmentModule)) {
                    THROW_ERROR_EXCEPTION("Reserve fair resource amount can be specified only for configured modules")
                        << TErrorAttribute("configured_modules", configuredModules)
                        << TErrorAttribute("specified_module", schedulingSegmentModule);
                }

                auto value = valuesPerModule.GetOrDefaultAt(schedulingSegmentModule);
                if (value < 0.0) {
                    THROW_ERROR_EXCEPTION("Reserve fair resource amount must not be negative")
                        << TErrorAttribute("segment", segment)
                        << TErrorAttribute("module", schedulingSegmentModule)
                        << TErrorAttribute("value", value);
                }
            }
        }
    });
}

const THashSet<TString>& TFairShareStrategySchedulingSegmentsConfig::GetModules() const
{
    switch (ModuleType) {
        case ESchedulingSegmentModuleType::DataCenter:
            return DataCenters;
        case ESchedulingSegmentModuleType::InfinibandCluster:
            return InfinibandClusters;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategySsdPriorityPreemptionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    registrar.Parameter("node_tag_filter", &TThis::NodeTagFilter)
        .Default();

    registrar.Parameter("medium_names", &TThis::MediumNames)
        .Default();

    registrar.Postprocessor([&] (TFairShareStrategySsdPriorityPreemptionConfig* config) {
        if (config->Enable && config->NodeTagFilter == EmptySchedulingTagFilter) {
            THROW_ERROR_EXCEPTION("SSD node tag filter must be non-empty when SSD priority preemption is enabled");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TBatchOperationSchedulingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("batch_size", &TThis::BatchSize)
        // COMPAT(eshcherbin)
        .Alias("medium_priority_operation_count_limit")
        .GreaterThanOrEqual(0)
        .Default(1000);

    registrar.Parameter("fallback_min_spare_allocation_resources", &TThis::FallbackMinSpareAllocationResources)
        .Alias("fallback_min_spare_job_resources")
        // COMPAT(eshcherbin)
        .Alias("low_priority_fallback_min_spare_job_resources")
        .DefaultCtor(&GetDefaultLowPriorityFallbackMinSpareAllocationResources);
}

////////////////////////////////////////////////////////////////////////////////

void TTreeTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("delay_inside_fair_share_update", &TThis::DelayInsideFairShareUpdate)
        .Default();

    registrar.Parameter("delay_inside_resource_usage_initialization_in_tree", &TThis::DelayInsideResourceUsageInitializationInTree)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategyTreeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("nodes_filter", &TThis::NodesFilter)
        .Default();

    registrar.Parameter("fair_share_starvation_timeout", &TThis::FairShareStarvationTimeout)
        .Alias("fair_share_preemption_timeout")
        .Default(TDuration::Seconds(30));
    registrar.Parameter("fair_share_aggressive_starvation_timeout", &TThis::FairShareAggressiveStarvationTimeout)
        .Default(TDuration::Seconds(120));
    registrar.Parameter("fair_share_starvation_tolerance", &TThis::FairShareStarvationTolerance)
        .InRange(0.0, 1.0)
        .Default(0.8);

    registrar.Parameter("enable_aggressive_starvation", &TThis::EnableAggressiveStarvation)
        .Default(false);

    registrar.Parameter("max_unpreemptible_running_allocation_count", &TThis::MaxUnpreemptibleRunningAllocationCount)
        .Alias("max_unpreemptible_running_job_count")
        .Alias("max_unpreemptable_running_job_count")
        .Default();

    registrar.Parameter("non_preemptible_resource_usage_threshold", &TThis::NonPreemptibleResourceUsageThreshold)
        .DefaultNew();

    registrar.Parameter("max_running_operation_count", &TThis::MaxRunningOperationCount)
        .Default(2000)
        .GreaterThan(0);

    registrar.Parameter("max_running_operation_count_per_pool", &TThis::MaxRunningOperationCountPerPool)
        .Default(50)
        .GreaterThan(0);

    registrar.Parameter("max_operation_count_per_pool", &TThis::MaxOperationCountPerPool)
        .Default(50)
        .GreaterThan(0);

    registrar.Parameter("max_operation_count", &TThis::MaxOperationCount)
        .Default(50000)
        .GreaterThan(0);

    registrar.Parameter("enable_pool_starvation", &TThis::EnablePoolStarvation)
        .Default(true);

    registrar.Parameter("default_parent_pool", &TThis::DefaultParentPool)
        .Default(RootPoolName);

    registrar.Parameter("forbid_immediate_operations_in_root", &TThis::ForbidImmediateOperationsInRoot)
        .Default(true);

    registrar.Parameter("allocation_count_preemption_timeout_coefficient", &TThis::AllocationCountPreemptionTimeoutCoefficient)
        .Alias("job_count_preemption_timeout_coefficient")
        .Default(1.0)
        .GreaterThanOrEqual(1.0);

    registrar.Parameter("preemption_satisfaction_threshold", &TThis::PreemptionSatisfactionThreshold)
        .Default(1.0)
        .GreaterThan(0);

    registrar.Parameter("aggressive_preemption_satisfaction_threshold", &TThis::AggressivePreemptionSatisfactionThreshold)
        .Default(0.2)
        .GreaterThanOrEqual(0);

    registrar.Parameter("enable_scheduling_tags", &TThis::EnableSchedulingTags)
        .Default(true);

    registrar.Parameter("heartbeat_tree_scheduling_info_log_period", &TThis::HeartbeatTreeSchedulingInfoLogBackoff)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("max_ephemeral_pools_per_user", &TThis::MaxEphemeralPoolsPerUser)
        .GreaterThanOrEqual(1)
        .Default(1);

    registrar.Parameter("update_preemptible_list_duration_logging_threshold", &TThis::UpdatePreemptibleListDurationLoggingThreshold)
        .Alias("update_preemptable_list_duration_logging_threshold")
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("enable_operations_profiling", &TThis::EnableOperationsProfiling)
        .Default(true);

    registrar.Parameter("custom_profiling_tag_filter", &TThis::CustomProfilingTagFilter)
        .Default();

    registrar.Parameter("node_reconnection_timeout", &TThis::NodeReconnectionTimeout)
        .Alias("total_resource_limits_consider_delay")
        .Default(TDuration::Minutes(5));

    registrar.Parameter("preemptive_scheduling_backoff", &TThis::PreemptiveSchedulingBackoff)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("tentative_tree_saturation_deactivation_period", &TThis::TentativeTreeSaturationDeactivationPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("infer_weight_from_guarantees_share_multiplier", &TThis::InferWeightFromGuaranteesShareMultiplier)
        .Alias("infer_weight_from_strong_guarantee_share_multiplier")
        .Alias("infer_weight_from_min_share_ratio_multiplier")
        .Default()
        .GreaterThanOrEqual(1.0);

    registrar.Parameter("packing", &TThis::Packing)
        .DefaultNew();

    registrar.Parameter("non_tentative_operation_types", &TThis::NonTentativeOperationTypes)
        .Default(std::nullopt);

    registrar.Parameter("best_allocation_share_update_period", &TThis::BestAllocationShareUpdatePeriod)
        .Alias("best_allocation_ratio_update_period")
        .Default(TDuration::Minutes(1));

    registrar.Parameter("enable_by_user_profiling", &TThis::EnableByUserProfiling)
        .Default(true);

    registrar.Parameter("integral_guarantees", &TThis::IntegralGuarantees)
        .DefaultNew();

    registrar.Parameter("enable_resource_tree_structure_lock_profiling", &TThis::EnableResourceTreeStructureLockProfiling)
        .Default(true);

    registrar.Parameter("enable_resource_tree_usage_lock_profiling", &TThis::EnableResourceTreeUsageLockProfiling)
        .Default(true);

    registrar.Parameter("preemption_check_starvation", &TThis::PreemptionCheckStarvation)
        .Default(true);

    registrar.Parameter("preemption_check_satisfaction", &TThis::PreemptionCheckSatisfaction)
        .Default(true);

    registrar.Parameter("allocation_preemption_timeout", &TThis::AllocationPreemptionTimeout)
        .Alias("job_interrupt_timeout")
        .Default(TDuration::Seconds(10));

    registrar.Parameter("allocation_graceful_preemption_timeout", &TThis::AllocationGracefulPreemptionTimeout)
        .Alias("job_graceful_interrupt_timeout")
        .Default(TDuration::Seconds(60));

    registrar.Parameter("scheduling_segments", &TThis::SchedulingSegments)
        .DefaultNew();

    registrar.Parameter("enable_pools_vector_profiling", &TThis::EnablePoolsVectorProfiling)
        .Default(true);

    registrar.Parameter("enable_operations_vector_profiling", &TThis::EnableOperationsVectorProfiling)
        .Default(false);

    registrar.Parameter("sparsify_fair_share_profiling", &TThis::SparsifyFairShareProfiling)
        .Default(false);

    registrar.Parameter("enable_limiting_ancestor_check", &TThis::EnableLimitingAncestorCheck)
        .Default(true);

    registrar.Parameter("profiled_pool_resources", &TThis::ProfiledPoolResources)
        .Default({
            EJobResourceType::Cpu,
            EJobResourceType::Memory,
            EJobResourceType::UserSlots,
            EJobResourceType::Gpu,
            EJobResourceType::Network
        })
        .ResetOnLoad();

    registrar.Parameter("profiled_operation_resources", &TThis::ProfiledOperationResources)
        .Default({
            EJobResourceType::Cpu,
            EJobResourceType::Memory,
            EJobResourceType::UserSlots,
            EJobResourceType::Gpu,
            EJobResourceType::Network
        })
        .ResetOnLoad();

    registrar.Parameter("waiting_for_resources_on_node_timeout", &TThis::WaitingForResourcesOnNodeTimeout)
        .Alias("waiting_job_timeout")
        .Default();

    registrar.Parameter("min_child_heap_size", &TThis::MinChildHeapSize)
        .Default(16);

    registrar.Parameter("main_resource", &TThis::MainResource)
        .Default(EJobResourceType::Cpu);

    registrar.Parameter("metering_tags", &TThis::MeteringTags)
        .Default();

    registrar.Parameter("pool_config_presets", &TThis::PoolConfigPresets)
        .Default();

    registrar.Parameter("enable_fair_share_truncation_in_fifo_pool", &TThis::EnableFairShareTruncationInFifoPool)
        .Alias("truncate_fifo_pool_unsatisfied_child_fair_share")
        .Default(false);

    registrar.Parameter("enable_conditional_preemption", &TThis::EnableConditionalPreemption)
        .Default(true);

    registrar.Parameter("use_resource_usage_with_precommit", &TThis::UseResourceUsageWithPrecommit)
        .Default(true);

    registrar.Parameter("allowed_resource_usage_staleness", &TThis::AllowedResourceUsageStaleness)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("cached_allocation_preemption_statuses_update_period", &TThis::CachedAllocationPreemptionStatusesUpdatePeriod)
        .Alias("cached_job_preemption_statuses_update_period")
        .Default(TDuration::Seconds(15));

    registrar.Parameter("should_distribute_free_volume_among_children", &TThis::ShouldDistributeFreeVolumeAmongChildren)
        // TODO(renadeen): temporarily disabled.
        .Default(false);

    registrar.Parameter("use_user_default_parent_pool_map", &TThis::UseUserDefaultParentPoolMap)
        .Default(false);

    registrar.Parameter("enable_resource_usage_snapshot", &TThis::EnableResourceUsageSnapshot)
        .Default(true);

    registrar.Parameter("max_event_log_pool_batch_size", &TThis::MaxEventLogPoolBatchSize)
        .Default(1000);

    registrar.Parameter("max_event_log_operation_batch_size", &TThis::MaxEventLogOperationBatchSize)
        .Default(1000);

    registrar.Parameter("accumulated_resource_usage_update_period", &TThis::AccumulatedResourceUsageUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("allow_aggressive_preemption_for_gang_operations", &TThis::AllowAggressivePreemptionForGangOperations)
        .Default(true);

    registrar.Parameter("fail_remote_copy_on_missing_resource_limits", &TThis::FailRemoteCopyOnMissingResourceLimits)
        // TODO(egor-gutrov): set default to true
        .Default(false);

    registrar.Parameter("required_resource_limits_for_remote_copy", &TThis::RequiredResourceLimitsForRemoteCopy)
        .DefaultCtor(&GetDefaultRequiredResourceLimitsForRemoteCopy);

    registrar.Parameter("ssd_priority_preemption", &TThis::SsdPriorityPreemption)
        .DefaultNew();

    registrar.Parameter("enable_scheduled_and_preempted_resources_profiling", &TThis::EnableScheduledAndPreemptedResourcesProfiling)
        .Default(true);

    registrar.Parameter("max_schedulable_element_count_in_fifo_pool", &TThis::MaxSchedulableElementCountInFifoPool)
        .GreaterThan(0)
        .Default();

    registrar.Parameter("check_operation_for_liveness_in_preschedule", &TThis::CheckOperationForLivenessInPreschedule)
        .Default(true);

    registrar.Parameter("testing_options", &TThis::TestingOptions)
        .DefaultNew();

    registrar.Parameter("scheduling_preemption_priority_scope", &TThis::SchedulingPreemptionPriorityScope)
        .Default(EOperationPreemptionPriorityScope::OperationAndAncestors);

    registrar.Parameter("running_allocation_statistics_update_period", &TThis::RunningAllocationStatisticsUpdatePeriod)
        .Alias("running_job_statistics_update_period")
        .Default(TDuration::Seconds(1));

    registrar.Parameter("batch_operation_scheduling", &TThis::BatchOperationScheduling)
        // COMPAT(eshcherbin)
        .Alias("prioritized_regular_scheduling")
        .Default();

    registrar.Parameter("fifo_pool_scheduling_order", &TThis::FifoPoolSchedulingOrder)
        .Default(EFifoPoolSchedulingOrder::Satisfaction);

    registrar.Parameter("use_pool_satisfaction_for_scheduling", &TThis::UsePoolSatisfactionForScheduling)
        .Default(false);

    registrar.Parameter("per_pool_satisfaction_digest", &TThis::PerPoolSatisfactionDigest)
        .DefaultCtor(&GetDefaultPerPoolSatisfactionDigestConfig);

    registrar.Parameter("per_pool_satisfaction_profiling_quantiles", &TThis::PerPoolSatisfactionProfilingQuantiles)
        .Default({0.01, 0.05, 0.1, 0.25, 0.4, 0.5, 0.6, 0.75, 0.9, 0.95, 0.99});

    registrar.Parameter("enable_guarantee_priority_scheduling", &TThis::EnableGuaranteePriorityScheduling)
        .Default(false);

    registrar.Parameter("necessary_resources_for_operation", &TThis::NecessaryResourcesForOperation)
        .Default();

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

    registrar.Postprocessor([&] (TFairShareStrategyTreeConfig* config) {
        if (!config->NonPreemptibleResourceUsageThreshold) {
            THROW_ERROR_EXCEPTION("\"non_preemptible_resource_usage_threshold\" must not be null");
        }

        auto& nonPreemptibleUserSlotsUsage = config->NonPreemptibleResourceUsageThreshold->UserSlots;
        if (!nonPreemptibleUserSlotsUsage) {
            nonPreemptibleUserSlotsUsage = config->MaxUnpreemptibleRunningAllocationCount;
        }
    });

    registrar.Postprocessor([&] (TFairShareStrategyTreeConfig* config) {
        static const int MaxPerPoolProfilingQuantileCount = 20;

        int quantileCount = std::ssize(config->PerPoolSatisfactionProfilingQuantiles);
        if (quantileCount > MaxPerPoolProfilingQuantileCount) {
            THROW_ERROR_EXCEPTION("Too many per pool profiling quantiles specified")
                << TErrorAttribute("max_quantile_count", MaxPerPoolProfilingQuantileCount)
                << TErrorAttribute("quantile_count", quantileCount);
        }

        for (auto quantile : config->PerPoolSatisfactionProfilingQuantiles) {
            if (quantile < 0.0 || quantile > 1.0) {
                THROW_ERROR_EXCEPTION("Per pool satisfaction profiling quantiles must be from range [0.0, 1.0]")
                    << TErrorAttribute("out_of_range_quantile", quantile);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TPoolTreesTemplateConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("priority", &TThis::Priority);

    registrar.Parameter("filter", &TThis::Filter);

    registrar.Parameter("config", &TThis::Config);
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("fair_share_update_period", &TThis::FairShareUpdatePeriod)
        .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
        .Default(TDuration::MilliSeconds(200));

    registrar.Parameter("fair_share_profiling_period", &TThis::FairShareProfilingPeriod)
        .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
        .Default(TDuration::MilliSeconds(5000));

    registrar.Parameter("fair_share_log_period", &TThis::FairShareLogPeriod)
        .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
        .Default(TDuration::MilliSeconds(1000));

    registrar.Parameter("accumulated_usage_log_period", &TThis::AccumulatedUsageLogPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("min_needed_resources_update_period", &TThis::MinNeededResourcesUpdatePeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("resource_metering_period", &TThis::ResourceMeteringPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("resource_usage_snapshot_update_period", &TThis::ResourceUsageSnapshotUpdatePeriod)
        .Default(TDuration::MilliSeconds(20));

    registrar.Parameter("operation_hangup_check_period", &TThis::OperationHangupCheckPeriod)
        .Alias("operation_unschedulable_check_period")
        .Default(TDuration::Minutes(1));

    registrar.Parameter("operation_hangup_safe_timeout", &TThis::OperationHangupSafeTimeout)
        .Alias("operation_unschedulable_safe_timeout")
        .Default(TDuration::Minutes(60));

    registrar.Parameter("operation_hangup_min_schedule_allocation_attempts", &TThis::OperationHangupMinScheduleAllocationAttempts)
        .Alias("operation_hangup_min_schedule_job_attempts")
        .Alias("operation_unschedulable_min_schedule_job_attempts")
        .Default(1000);

    registrar.Parameter("operation_hangup_deactivation_reasons", &TThis::OperationHangupDeactivationReasons)
        .Alias("operation_unschedulable_deactivation_reasons")
        .Default({EDeactivationReason::ScheduleAllocationFailed, EDeactivationReason::MinNeededResourcesUnsatisfied})
        .ResetOnLoad();

    registrar.Parameter("operation_hangup_due_to_limiting_ancestor_safe_timeout", &TThis::OperationHangupDueToLimitingAncestorSafeTimeout)
        .Alias("operation_unschedulable_due_to_limiting_ancestor_safe_timeout")
        .Default(TDuration::Minutes(5));

    registrar.Parameter("max_operation_count", &TThis::MaxOperationCount)
        .Default(5000)
        .GreaterThan(0);

    registrar.Parameter("operations_without_tentative_pool_trees", &TThis::OperationsWithoutTentativePoolTrees)
        .Default({EOperationType::Sort, EOperationType::MapReduce, EOperationType::RemoteCopy})
        .ResetOnLoad();

    registrar.Parameter("default_tentative_pool_trees", &TThis::DefaultTentativePoolTrees)
        .Default();

    registrar.Parameter("enable_schedule_in_single_tree", &TThis::EnableScheduleInSingleTree)
        .Default(true);

    registrar.Parameter("strategy_testing_options", &TThis::StrategyTestingOptions)
        .DefaultNew();

    registrar.Parameter("template_pool_tree_config_map", &TThis::TemplatePoolTreeConfigMap)
        .Default();

    registrar.Parameter("enable_pool_trees_config_cache", &TThis::EnablePoolTreesConfigCache)
        .Default(true);

    registrar.Parameter("scheduler_tree_alerts_update_period", &TThis::SchedulerTreeAlertsUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("enable_optimized_operation_orchid", &TThis::EnableOptimizedOperationOrchid)
        .Default(false);

    registrar.Parameter("enable_async_operation_event_logging", &TThis::EnableAsyncOperationEventLogging)
        .Default(true);

    registrar.Parameter("ephemeral_pool_name_regex", &TThis::EphemeralPoolNameRegex)
        .Default("[-_a-z0-9:A-Z]+");

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
    registrar.Parameter("enable_random_master_disconnection", &TThis::EnableRandomMasterDisconnection)
        .Default(false);
    registrar.Parameter("random_master_disconnection_max_backoff", &TThis::RandomMasterDisconnectionMaxBackoff)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("master_disconnect_delay", &TThis::MasterDisconnectDelay)
        .Default();
    registrar.Parameter("handle_orphaned_operations_delay", &TThis::HandleOrphanedOperationsDelay)
        .Default();
    registrar.Parameter("finish_operation_transition_delay", &TThis::FinishOperationTransitionDelay)
        .Default();
    registrar.Parameter("node_heartbeat_processing_delay", &TThis::NodeHeartbeatProcessingDelay)
        .Default();
    registrar.Parameter("secure_vault_creation_delay", &TThis::SecureVaultCreationDelay)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TOperationsCleanerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("enable_operation_archivation", &TThis::EnableOperationArchivation)
        .Alias("enable_archivation")
        .Default(true);
    registrar.Parameter("clean_delay", &TThis::CleanDelay)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("analysis_period", &TThis::AnalysisPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("remove_batch_size", &TThis::RemoveBatchSize)
        .Default(256);
    registrar.Parameter("remove_subbatch_size", &TThis::RemoveSubbatchSize)
        .Default(64);
    registrar.Parameter("remove_batch_timeout", &TThis::RemoveBatchTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("archive_batch_size", &TThis::ArchiveBatchSize)
        .Default(100);
    registrar.Parameter("archive_batch_timeout", &TThis::ArchiveBatchTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("tablet_transaction_timeout", &TThis::TabletTransactionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("max_operation_age", &TThis::MaxOperationAge)
        .Default(TDuration::Hours(6));
    registrar.Parameter("max_operation_count_per_user", &TThis::MaxOperationCountPerUser)
        .Default(200);
    registrar.Parameter("soft_retained_operation_count", &TThis::SoftRetainedOperationCount)
        .Default(200);
    registrar.Parameter("hard_retained_operation_count", &TThis::HardRetainedOperationCount)
        .Default(4000);
    registrar.Parameter("min_archivation_retry_sleep_delay", &TThis::MinArchivationRetrySleepDelay)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("max_archivation_retry_sleep_delay", &TThis::MaxArchivationRetrySleepDelay)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("max_operation_count_enqueued_for_archival", &TThis::MaxOperationCountEnqueuedForArchival)
        .Default(20000);
    registrar.Parameter("archivation_enable_delay", &TThis::ArchivationEnableDelay)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("max_removal_sleep_delay", &TThis::MaxRemovalSleepDelay)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("min_operation_count_enqueued_for_alert", &TThis::MinOperationCountEnqueuedForAlert)
        .Default(500);
    registrar.Parameter("finished_operations_archive_lookup_timeout", &TThis::FinishedOperationsArchiveLookupTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("parse_operation_attributes_batch_size", &TThis::ParseOperationAttributesBatchSize)
        .Default(100);
    registrar.Parameter("enable_operation_alert_event_archivation", &TThis::EnableOperationAlertEventArchivation)
        .Default(true);
    registrar.Parameter("max_enqueued_operation_alert_event_count", &TThis::MaxEnqueuedOperationAlertEventCount)
        .Default(1000)
        .GreaterThanOrEqual(0);
    registrar.Parameter("max_alert_event_count_per_alert_type", &TThis::MaxAlertEventCountPerAlertType)
        .Default(4)
        .GreaterThanOrEqual(0);
    registrar.Parameter("operation_alert_event_send_period", &TThis::OperationAlertEventSendPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("operation_alert_sender_alert_threshold", &TThis::OperationAlertSenderAlertThreshold)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("locked_operation_wait_timeout", &TThis::LockedOperationWaitTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("disconnect_on_finished_operation_fetch_failure", &TThis::DisconnectOnFinishedOperationFetchFailure)
        .Default(true);

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
    registrar.Parameter("smooth_period", &TThis::SmoothPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("pool_capacity_saturation_period", &TThis::PoolCapacitySaturationPeriod)
        .Default(TDuration::Days(1));

    registrar.Parameter("relaxed_share_multiplier_limit", &TThis::RelaxedShareMultiplierLimit)
        .Default(3);
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TAliveControllerAgentThresholds& thresholds, const NYTree::INodePtr& node)
{
    const auto& mapNode = node->AsMap();

    thresholds.Absolute = mapNode->GetChildOrThrow("absolute")->AsInt64()->GetValue();
    thresholds.Relative = mapNode->GetChildOrThrow("relative")->AsDouble()->GetValue();
}

void Deserialize(TAliveControllerAgentThresholds& thresholds, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(thresholds, ExtractTo<NYTree::INodePtr>(cursor));
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
    registrar.Parameter("response_keeper", &TThis::ResponseKeeper)
        .DefaultNew();

    registrar.Parameter("light_rpc_timeout", &TThis::LightRpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("heavy_rpc_timeout", &TThis::HeavyRpcTimeout)
        .Default(TDuration::Minutes(30));

    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("incarnation_transaction_timeout", &TThis::IncarnationTransactionTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("incarnation_transaction_ping_period", &TThis::IncarnationTransactionPingPeriod)
        .Default();

    registrar.Parameter("agent_pick_strategy", &TThis::AgentPickStrategy)
        .Default(EControllerAgentPickStrategy::Random);

    registrar.Parameter("min_agent_available_memory", &TThis::MinAgentAvailableMemory)
        .Default(1_GB);

    registrar.Parameter("min_agent_available_memory_fraction", &TThis::MinAgentAvailableMemoryFraction)
        .InRange(0.0, 1.0)
        .Default(0.05);

    registrar.Parameter("memory_balanced_pick_strategy_score_power", &TThis::MemoryBalancedPickStrategyScorePower)
        .Default(1.0);

    registrar.Parameter("min_agent_count", &TThis::MinAgentCount)
        .Default(1);

    registrar.Parameter("tag_to_alive_controller_agent_thresholds", &TThis::TagToAliveControllerAgentThresholds)
        .Default();

    registrar.Parameter("max_message_allocation_event_count", &TThis::MaxMessageAllocationEventCount)
        .Alias("max_message_job_event_count")
        .Default(10000)
        .GreaterThan(0);

    registrar.Parameter("message_offload_thread_count", &TThis::MessageOffloadThreadCount)
        .Default(1)
        .GreaterThan(0);

    registrar.Parameter("enable_response_keeper", &TThis::EnableResponseKeeper)
        .Default(false);

    registrar.Preprocessor([&] (TControllerAgentTrackerConfig* config) {
        config->ResponseKeeper->EnableWarmup = false;
    });

    registrar.Postprocessor([&] (TControllerAgentTrackerConfig* config) {
        if (!config->TagToAliveControllerAgentThresholds.contains(DefaultOperationTag)) {
            config->TagToAliveControllerAgentThresholds[DefaultOperationTag] = {static_cast<i64>(config->MinAgentCount), 0.0};
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TResourceMeteringConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_new_abc_format", &TThis::EnableNewAbcFormat)
        .Default(true);

    registrar.Parameter("default_abc_id", &TThis::DefaultAbcId)
        .Default(-1);

    registrar.Parameter("default_cloud_id", &TThis::DefaultCloudId)
        .Default();

    registrar.Parameter("default_folder_id", &TThis::DefaultFolderId)
        .Default();

    registrar.Parameter("enable_separate_schema_for_allocation", &TThis::EnableSeparateSchemaForAllocation)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TSchedulerConfig::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);

    registrar.Parameter("operation_service_response_keeper", &TThis::OperationServiceResponseKeeper)
        .DefaultNew();

    registrar.Parameter("node_shard_count", &TThis::NodeShardCount)
        .Default(4)
        .InRange(1, MaxNodeShardCount);

    registrar.Parameter("connect_retry_backoff_time", &TThis::ConnectRetryBackoffTime)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("node_heartbeat_timeout", &TThis::NodeHeartbeatTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("node_registration_timeout", &TThis::NodeRegistrationTimeout)
        .Default(TDuration::Seconds(600));

    registrar.Parameter("watchers_update_period", &TThis::WatchersUpdatePeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("nodes_attributes_update_period", &TThis::NodesAttributesUpdatePeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("profiling_update_period", &TThis::ProfilingUpdatePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("alerts_update_period", &TThis::AlertsUpdatePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("node_shard_submit_allocations_to_strategy_period", &TThis::NodeShardSubmitAllocationsToStrategyPeriod)
        .Alias("node_shard_submit_jobs_to_strategy_period")
        .Default(TDuration::MilliSeconds(100));

    // NB: This setting is NOT synchronized with the Cypress while scheduler is connected to master.
    registrar.Parameter("lock_transaction_timeout", &TThis::LockTransactionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("pool_trees_lock_transaction_timeout", &TThis::PoolTreesLockTransactionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("pool_trees_lock_check_backoff", &TThis::PoolTreesLockCheckBackoff)
        .Default(TDuration::MilliSeconds(500));

    registrar.Parameter("cluster_info_logging_period", &TThis::ClusterInfoLoggingPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("nodes_info_logging_period", &TThis::NodesInfoLoggingPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("exec_node_descriptors_update_period", &TThis::ExecNodeDescriptorsUpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("always_send_controller_agent_descriptors", &TThis::AlwaysSendControllerAgentDescriptors)
        .Default(true);
    registrar.Parameter("send_full_controller_agent_descriptors_for_allocations", &TThis::SendFullControllerAgentDescriptorsForAllocations)
        .Alias("send_full_controller_agent_descriptors_for_jobs")
        .Default(true);
    registrar.Parameter("allocations_logging_period", &TThis::AllocationsLoggingPeriod)
        .Alias("jobs_logging_period")
        .Default(TDuration::Seconds(30));
    registrar.Parameter("running_allocations_update_period", &TThis::RunningAllocationsUpdatePeriod)
        .Alias("running_jobs_update_period")
        .Default(TDuration::Seconds(10));
    registrar.Parameter("missing_allocations_check_period", &TThis::MissingAllocationsCheckPeriod)
        .Alias("missing_jobs_check_period")
        .Default(TDuration::Seconds(10));
    registrar.Parameter("transient_operation_queue_scan_period", &TThis::TransientOperationQueueScanPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("pending_by_pool_operation_scan_period", &TThis::PendingByPoolOperationScanPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("operation_to_agent_assignment_backoff", &TThis::OperationToAgentAssignmentBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("max_started_allocations_per_heartbeat", &TThis::MaxStartedAllocationsPerHeartbeat)
        .Alias("max_started_jobs_per_heartbeat")
        .Default()
        .GreaterThan(0);

    registrar.Parameter("node_shard_exec_nodes_cache_update_period", &TThis::NodeShardExecNodesCacheUpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("heartbeat_process_backoff", &TThis::HeartbeatProcessBackoff)
        .Default(TDuration::MilliSeconds(5000));
    registrar.Parameter("soft_concurrent_heartbeat_limit", &TThis::SoftConcurrentHeartbeatLimit)
        .Default(50)
        .GreaterThanOrEqual(1);
    registrar.Parameter("hard_concurrent_heartbeat_limit", &TThis::HardConcurrentHeartbeatLimit)
        .Default(100)
        .GreaterThanOrEqual(1);

    registrar.Parameter("scheduling_heartbeat_complexity_limit", &TThis::SchedulingHeartbeatComplexityLimit)
        .Default(200000)
        .GreaterThanOrEqual(1);

    registrar.Parameter("use_heartbeat_scheduling_complexity_throttling", &TThis::UseHeartbeatSchedulingComplexityThrottling)
        .Default(false);

    registrar.Parameter("static_orchid_cache_update_period", &TThis::StaticOrchidCacheUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("orchid_keys_update_period", &TThis::OrchidKeysUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("enable_unrecognized_alert", &TThis::EnableUnrecognizedAlert)
        .Default(true);

    registrar.Parameter("scheduling_tag_filter_expire_timeout", &TThis::SchedulingTagFilterExpireTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("operations_cleaner", &TThis::OperationsCleaner)
        .DefaultNew();

    registrar.Parameter("operations_update_period", &TThis::OperationsUpdatePeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("finished_operation_allocation_storing_timeout", &TThis::FinishedOperationAllocationStoringTimeout)
        .Alias("finished_operation_job_storing_timeout")
        .Default(TDuration::Seconds(10));

    registrar.Parameter("testing_options", &TThis::TestingOptions)
        .DefaultNew();

    registrar.Parameter("event_log", &TThis::EventLog)
        .DefaultNew();

    registrar.Parameter("spec_template", &TThis::SpecTemplate)
        .Default();

    registrar.Parameter("controller_agent_tracker", &TThis::ControllerAgentTracker)
        .DefaultNew();

    registrar.Parameter("job_reporter_issues_check_period", &TThis::JobReporterIssuesCheckPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("job_reporter_write_failures_alert_threshold", &TThis::JobReporterWriteFailuresAlertThreshold)
        .Default(1000);
    registrar.Parameter("job_reporter_queue_is_too_large_alert_threshold", &TThis::JobReporterQueueIsTooLargeAlertThreshold)
        .Default(10);

    registrar.Parameter("node_changes_count_threshold_to_update_cache", &TThis::NodeChangesCountThresholdToUpdateCache)
        .Default(5);

    registrar.Parameter("operation_transaction_ping_period", &TThis::OperationTransactionPingPeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("pool_change_is_allowed", &TThis::PoolChangeIsAllowed)
        .Default(true);

    registrar.Parameter("skip_operations_with_malformed_spec_during_revival", &TThis::SkipOperationsWithMalformedSpecDuringRevival)
        .Default(false);

    registrar.Parameter("max_offline_node_age", &TThis::MaxOfflineNodeAge)
        .Default(TDuration::Hours(12));

    registrar.Parameter("max_node_unseen_period_to_abort_allocations", &TThis::MaxNodeUnseenPeriodToAbortAllocations)
        .Alias("max_node_unseen_period_to_abort_jobs")
        .Default(TDuration::Minutes(5));

    registrar.Parameter("orchid_worker_thread_count", &TThis::OrchidWorkerThreadCount)
        .Default(1)
        .GreaterThan(0);

    registrar.Parameter("fair_share_update_thread_count", &TThis::FairShareUpdateThreadCount)
        .Default(1)
        .GreaterThan(0);

    registrar.Parameter("background_thread_count", &TThis::BackgroundThreadCount)
        .Default(1)
        .GreaterThan(0);

    registrar.Parameter("allowed_node_resources_overcommit_duration", &TThis::AllowedNodeResourcesOvercommitDuration)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("pool_trees_root", &TThis::PoolTreesRoot)
        .Default(PoolTreesRootCypressPath);

    registrar.Parameter("max_event_log_node_batch_size", &TThis::MaxEventLogNodeBatchSize)
        .Default(100);

    registrar.Parameter("validate_node_tags_period", &TThis::ValidateNodeTagsPeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("enable_allocation_abort_on_zero_user_slots", &TThis::EnableAllocationAbortOnZeroUserSlots)
        .Alias("enable_job_abort_on_zero_user_slots")
        .Default(true);

    registrar.Parameter("fetch_operation_attributes_subbatch_size", &TThis::FetchOperationAttributesSubbatchSize)
        .Default(1000);

    registrar.Parameter("resource_metering", &TThis::ResourceMetering)
        .DefaultNew();

    registrar.Parameter("parse_operation_attributes_batch_size", &TThis::ParseOperationAttributesBatchSize)
        .Default(100);

    registrar.Parameter("experiments", &TThis::Experiments)
        .Default();

    registrar.Parameter("min_spare_allocation_resources_on_node", &TThis::MinSpareAllocationResourcesOnNode)
        .Alias("min_spare_job_resources_on_node")
        .DefaultCtor(&GetDefaultMinSpareAllocationResourcesOnNode)
        .ResetOnLoad();

    registrar.Parameter("schedule_allocation_duration_logging_threshold", &TThis::ScheduleAllocationDurationLoggingThreshold)
        .Alias("schedule_job_duration_logging_threshold")
        .Default(TDuration::MilliSeconds(500));

    registrar.Parameter("send_preemption_reason_in_node_heartbeat", &TThis::SendPreemptionReasonInNodeHeartbeat)
        .Default(true);

    registrar.Parameter("consider_disk_quota_in_preemptive_scheduling_discount", &TThis::ConsiderDiskQuotaInPreemptiveSchedulingDiscount)
        .Default(false);

    registrar.Parameter("update_last_metering_log_time", &TThis::UpdateLastMeteringLogTime)
        .Default(true);

    registrar.Parameter("enable_heavy_runtime_parameters", &TThis::EnableHeavyRuntimeParameters)
        .Default(false);

    registrar.Parameter("enable_operation_heavy_attributes_archivation", &TThis::EnableOperationHeavyAttributesArchivation)
        .Default(false);

    registrar.Parameter("operation_heavy_attributes_archivation_timeout", &TThis::OperationHeavyAttributesArchivationTimeout)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("schedule_allocation_entry_removal_timeout", &TThis::ScheduleAllocationEntryRemovalTimeout)
        .Alias("schedule_job_entry_removal_timeout")
        .Default(TDuration::Minutes(2));

    registrar.Parameter("schedule_allocation_entry_check_period", &TThis::ScheduleAllocationEntryCheckPeriod)
        .Alias("schedule_job_entry_check_period")
        .Default(TDuration::Minutes(1));

    registrar.Parameter("wait_for_agent_heartbeat_during_operation_unregistration_at_controller", &TThis::WaitForAgentHeartbeatDuringOperationUnregistrationAtController)
        .Default(true);

    registrar.Parameter("crash_on_allocation_heartbeat_processing_exception", &TThis::CrashOnAllocationHeartbeatProcessingException)
        .Alias("crash_on_job_heartbeat_processing_exception")
        .Default(false);

    registrar.Parameter("min_required_archive_version", &TThis::MinRequiredArchiveVersion)
        .Default(50);

    registrar.Parameter("rpc_server", &TThis::RpcServer)
        .DefaultNew();

    registrar.Preprocessor([&] (TSchedulerConfig* config) {
        config->EventLog->MaxRowWeight = 128_MB;
        if (!config->EventLog->Path) {
            config->EventLog->Path = "//sys/scheduler/event_log";
        }
        config->OperationServiceResponseKeeper->EnableWarmup = false;
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

void TSchedulerBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("scheduler", &TThis::Scheduler)
        .DefaultNew();
    registrar.Parameter("addresses", &TThis::Addresses)
        .Default();
    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
            ->AsMap());

    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
