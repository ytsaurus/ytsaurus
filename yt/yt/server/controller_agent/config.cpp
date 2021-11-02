#include "config.h"

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

TIntermediateChunkScraperConfig::TIntermediateChunkScraperConfig()
{
    RegisterParameter("restart_timeout", RestartTimeout)
        .Default(TDuration::Seconds(10));
}

void TTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_snapshot_cycle_after_materialization", &TTestingOptions::EnableSnapshotCycleAfterMaterialization)
        .Default(false);

    registrar.Parameter("rootfs_test_layers", &TTestingOptions::RootfsTestLayers)
        .Default();

    registrar.Parameter("delay_in_unregistration", &TTestingOptions::DelayInUnregistration)
        .Default();
}

void TOperationAlertsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tmpfs_alert_max_unused_space_ratio", &TOperationAlertsConfig::TmpfsAlertMaxUnusedSpaceRatio)
        .InRange(0.0, 1.0)
        .Default(0.2);

    registrar.Parameter("tmpfs_alert_min_unused_space_threshold", &TOperationAlertsConfig::TmpfsAlertMinUnusedSpaceThreshold)
        .Default(512_MB)
        .GreaterThan(0);

    registrar.Parameter("tmpfs_alert_memory_usage_mute_ratio", &TOperationAlertsConfig::TmpfsAlertMemoryUsageMuteRatio)
        .InRange(0.0, 1.0)
        .Default(0.8);

    registrar.Parameter("memory_usage_alert_max_unused_size", &TOperationAlertsConfig::MemoryUsageAlertMaxUnusedSize)
        .Default(8_GB)
        .GreaterThan(0);

    registrar.Parameter("memory_usage_alert_max_unused_ratio", &TOperationAlertsConfig::MemoryUsageAlertMaxUnusedRatio)
        .InRange(0.0, 1.0)
        .Default(0.2);

    registrar.Parameter("memory_usage_alert_max_job_count", &TOperationAlertsConfig::MemoryUsageAlertMaxJobCount)
        .Default()
        .GreaterThan(0);

    registrar.Parameter("aborted_jobs_alert_max_aborted_time", &TOperationAlertsConfig::AbortedJobsAlertMaxAbortedTime)
        .Default((i64) 10 * 60 * 1000)
        .GreaterThan(0);

    registrar.Parameter("aborted_jobs_alert_max_aborted_time_ratio", &TOperationAlertsConfig::AbortedJobsAlertMaxAbortedTimeRatio)
        .InRange(0.0, 1.0)
        .Default(0.25);

    registrar.Parameter("short_jobs_alert_min_job_duration", &TOperationAlertsConfig::ShortJobsAlertMinJobDuration)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("short_jobs_alert_min_job_count", &TOperationAlertsConfig::ShortJobsAlertMinJobCount)
        .Default(1000);

    registrar.Parameter("short_jobs_alert_min_allowed_operation_duration_to_max_job_duration_ratio", &TOperationAlertsConfig::ShortJobsAlertMinAllowedOperationDurationToMaxJobDurationRatio)
        .Default(2.0);

    registrar.Parameter("intermediate_data_skew_alert_min_partition_size", &TOperationAlertsConfig::IntermediateDataSkewAlertMinPartitionSize)
        .Default(10_GB)
        .GreaterThan(0);

    registrar.Parameter("intermediate_data_skew_alert_min_interquartile_range", &TOperationAlertsConfig::IntermediateDataSkewAlertMinInterquartileRange)
        .Default(1_GB)
        .GreaterThan(0);

    registrar.Parameter("job_spec_throttling_alert_activation_count_threshold", &TOperationAlertsConfig::JobSpecThrottlingAlertActivationCountThreshold)
        .Default(1000)
        .GreaterThan(0);

    registrar.Parameter("low_cpu_usage_alert_min_execution_time", &TOperationAlertsConfig::LowCpuUsageAlertMinExecTime)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("low_cpu_usage_alert_min_average_job_time", &TOperationAlertsConfig::LowCpuUsageAlertMinAverageJobTime)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("low_cpu_usage_alert_cpu_usage_threshold", &TOperationAlertsConfig::LowCpuUsageAlertCpuUsageThreshold)
        .Default(0.5)
        .GreaterThan(0);

    registrar.Parameter("low_cpu_usage_alert_statistics", &TOperationAlertsConfig::LowCpuUsageAlertStatistics)
        .Default({
            "/job_proxy/cpu/system",
            "/job_proxy/cpu/user",
            "/user_job/cpu/system",
            "/user_job/cpu/user"
        });

    registrar.Parameter("low_cpu_usage_alert_job_states", &TOperationAlertsConfig::LowCpuUsageAlertJobStates)
        .Default({
            "completed"
        });

    registrar.Parameter("operation_too_long_alert_min_wall_time", &TOperationAlertsConfig::OperationTooLongAlertMinWallTime)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("operation_too_long_alert_estimate_duration_threshold", &TOperationAlertsConfig::OperationTooLongAlertEstimateDurationThreshold)
        .Default(TDuration::Days(7));

    registrar.Parameter("low_gpu_usage_alert_min_duration", &TOperationAlertsConfig::LowGpuUsageAlertMinDuration)
        .Default(TDuration::Minutes(30));

    registrar.Parameter("low_gpu_usage_alert_gpu_usage_threshold", &TOperationAlertsConfig::LowGpuUsageAlertGpuUsageThreshold)
        .Default(0.5);

    registrar.Parameter("low_gpu_usage_alert_statistics", &TOperationAlertsConfig::LowGpuUsageAlertStatistics)
        .Default({
            "/user_job/gpu/utilization_gpu",
        });

    registrar.Parameter("low_gpu_usage_alert_job_states", &TOperationAlertsConfig::LowGpuUsageAlertJobStates)
        .Default({
            "completed",
            "running"
        });

    registrar.Parameter("queue_average_wait_time_threshold", &TOperationAlertsConfig::QueueAverageWaitTimeThreshold)
        .Default(TDuration::Minutes(1));
}

void TJobSplitterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("min_job_time", &TJobSplitterConfig::MinJobTime)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("exec_to_prepare_time_ratio", &TJobSplitterConfig::ExecToPrepareTimeRatio)
        .Default(20.0);

    registrar.Parameter("no_progress_job_time_to_average_prepare_time_ratio", &TJobSplitterConfig::NoProgressJobTimeToAveragePrepareTimeRatio)
        .Default(20.0);

    registrar.Parameter("min_total_data_weight", &TJobSplitterConfig::MinTotalDataWeight)
        .Alias("min_total_data_size")
        .Default(1_GB);

    registrar.Parameter("update_period", &TJobSplitterConfig::UpdatePeriod)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("candidate_percentile", &TJobSplitterConfig::CandidatePercentile)
        .GreaterThanOrEqual(0.5)
        .LessThanOrEqual(1.0)
        .Default(0.8);

    registrar.Parameter("late_jobs_percentile", &TJobSplitterConfig::LateJobsPercentile)
        .GreaterThanOrEqual(0.5)
        .LessThanOrEqual(1.0)
        .Default(0.95);

    registrar.Parameter("residual_job_factor", &TJobSplitterConfig::ResidualJobFactor)
        .GreaterThan(0)
        .LessThanOrEqual(1.0)
        .Default(0.8);

    registrar.Parameter("residual_job_count_min_threshold", &TJobSplitterConfig::ResidualJobCountMinThreshold)
        .GreaterThan(0)
        .Default(10);

    registrar.Parameter("max_jobs_per_split", &TJobSplitterConfig::MaxJobsPerSplit)
        .GreaterThan(0)
        .Default(5);

    registrar.Parameter("max_input_table_count", &TJobSplitterConfig::MaxInputTableCount)
        .GreaterThan(0)
        .Default(100);

    registrar.Parameter("split_timeout_before_speculate", &TJobSplitterConfig::SplitTimeoutBeforeSpeculate)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("job_logging_period", &TJobSplitterConfig::JobLoggingPeriod)
        .Default(TDuration::Minutes(3));

    registrar.Parameter("enable_job_splitting", &TJobSplitterConfig::EnableJobSplitting)
        .Default(true);

    registrar.Parameter("enable_job_speculation", &TJobSplitterConfig::EnableJobSpeculation)
        .Default(true);

    registrar.Parameter("show_running_jobs_in_progress", &TJobSplitterConfig::ShowRunningJobsInProgress)
        .Default(false);
}

void TSuspiciousJobsOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("inactivity_timeout", &TSuspiciousJobsOptions::InactivityTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("cpu_usage_threshold", &TSuspiciousJobsOptions::CpuUsageThreshold)
        .Default(300);
    registrar.Parameter("input_pipe_time_idle_fraction", &TSuspiciousJobsOptions::InputPipeIdleTimeFraction)
        .Default(0.95);
    registrar.Parameter("output_pipe_time_idle_fraction", &TSuspiciousJobsOptions::OutputPipeIdleTimeFraction)
        .Default(0.95);
    registrar.Parameter("update_period", &TSuspiciousJobsOptions::UpdatePeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_orchid_entry_count_per_type", &TSuspiciousJobsOptions::MaxOrchidEntryCountPerType)
        .Default(100);
}

void TDataBalancerOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("logging_min_consecutive_violation_count", &TDataBalancerOptions::LoggingMinConsecutiveViolationCount)
        .Default(1000);
    registrar.Parameter("logging_period", &TDataBalancerOptions::LoggingPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("tolerance", &TDataBalancerOptions::Tolerance)
        .Default(2.0);
}

void TUserJobOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_limit_multiplier", &TUserJobOptions::ThreadLimitMultiplier)
        .Default(10'000);
    registrar.Parameter("initial_thread_limit", &TUserJobOptions::InitialThreadLimit)
        .Default(10'000);
}

void TOperationOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("spec_template", &TOperationOptions::SpecTemplate)
        .Default()
        .MergeBy(NYTree::EMergeStrategy::Combine);

    registrar.Parameter("slice_data_weight_multiplier", &TOperationOptions::SliceDataWeightMultiplier)
        .Alias("slice_data_size_multiplier")
        .Default(0.51)
        .GreaterThan(0.0);

    registrar.Parameter("max_data_slices_per_job", &TOperationOptions::MaxDataSlicesPerJob)
        // This is a reasonable default for jobs with user code.
        // Defaults for system jobs are in Initializer.
        .Default(1000)
        .GreaterThan(0);

    registrar.Parameter("max_slice_data_weight", &TOperationOptions::MaxSliceDataWeight)
        .Alias("max_slice_data_size")
        .Default(1_GB)
        .GreaterThan(0);

    registrar.Parameter("min_slice_data_weight", &TOperationOptions::MinSliceDataWeight)
        .Alias("min_slice_data_size")
        .Default(1_MB)
        .GreaterThan(0);

    registrar.Parameter("max_input_table_count", &TOperationOptions::MaxInputTableCount)
        .Default(3000)
        .GreaterThan(0);

    registrar.Parameter("max_output_tables_times_jobs_count", &TOperationOptions::MaxOutputTablesTimesJobsCount)
        .Default(20 * 100000)
        .GreaterThanOrEqual(100000);

    registrar.Parameter("job_splitter", &TOperationOptions::JobSplitter)
        .DefaultNew();

    registrar.Parameter("max_build_retry_count", &TOperationOptions::MaxBuildRetryCount)
        .Default(5)
        .GreaterThanOrEqual(0);

    registrar.Parameter("data_weight_per_job_retry_factor", &TOperationOptions::DataWeightPerJobRetryFactor)
        .Default(2.0)
        .GreaterThan(1.0);

    registrar.Parameter("initial_cpu_limit_overcommit", &TOperationOptions::InitialCpuLimitOvercommit)
        .Default(2.0)
        .GreaterThanOrEqual(0);

    registrar.Parameter("cpu_limit_overcommit_multiplier", &TOperationOptions::CpuLimitOvercommitMultiplier)
        .Default(1.0)
        .GreaterThanOrEqual(1.0);

    registrar.Parameter("set_container_cpu_limit", &TOperationOptions::SetContainerCpuLimit)
        .Default(false);

    // NB: defaults for these values are actually in preprocessor of TControllerAgentConfig::OperationOptions.
    registrar.Parameter("controller_building_job_spec_count_limit", &TOperationOptions::ControllerBuildingJobSpecCountLimit)
        .Default();
    registrar.Parameter("controller_total_building_job_spec_slice_count_limit", &TOperationOptions::ControllerTotalBuildingJobSpecSliceCountLimit)
        .Default();

    registrar.Parameter("user_job_options", &TOperationOptions::UserJobOptions)
        .DefaultNew();

    registrar.Postprocessor([&] (TOperationOptions* options) {
        if (options->MaxSliceDataWeight < options->MinSliceDataWeight) {
            THROW_ERROR_EXCEPTION("Minimum slice data weight must be less than or equal to maximum slice data size")
                << TErrorAttribute("min_slice_data_weight", options->MinSliceDataWeight)
                << TErrorAttribute("max_slice_data_weight", options->MaxSliceDataWeight);
        }
    });
}

void TSimpleOperationOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("max_job_count", &TSimpleOperationOptions::MaxJobCount)
        .Default(100000);

    registrar.Parameter("data_weight_per_job", &TSimpleOperationOptions::DataWeightPerJob)
        .Alias("data_size_per_job")
        .Default(256_MB)
        .GreaterThan(0);
}

void TMapOperationOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("job_size_adjuster", &TMapOperationOptions::JobSizeAdjuster)
        .DefaultNew();

    registrar.Preprocessor([&] (TMapOperationOptions* options) {
        options->DataWeightPerJob = 128_MB;
    });
}

void TReduceOperationOptions::Register(TRegistrar registrar)
{
    registrar.Preprocessor([&] (TReduceOperationOptions* options) {
        options->DataWeightPerJob = 128_MB;
    });
}

void TSortOperationOptionsBase::Register(TRegistrar registrar)
{
    registrar.Parameter("max_partition_job_count", &TSortOperationOptionsBase::MaxPartitionJobCount)
        .Default(500000)
        .GreaterThan(0);

    registrar.Parameter("max_partition_count", &TSortOperationOptionsBase::MaxPartitionCount)
        .Default(200000)
        .GreaterThan(0);

    registrar.Parameter("max_new_partition_count", &TSortOperationOptionsBase::MaxNewPartitionCount)
        .Default(2'000'000)
        .GreaterThan(0);

    registrar.Parameter("max_partition_factor", &TSortOperationOptionsBase::MaxPartitionFactor)
        .Default(500)
        .GreaterThan(1);

    registrar.Parameter("max_sample_size", &TSortOperationOptionsBase::MaxSampleSize)
        .Default(10_KB)
        .GreaterThanOrEqual(1_KB)
            // NB(psushin): removing this validator may lead to weird errors in sorting.
        .LessThanOrEqual(NTableClient::MaxSampleSize);

    registrar.Parameter("compressed_block_size", &TSortOperationOptionsBase::CompressedBlockSize)
        .Default(1_MB)
        .GreaterThanOrEqual(1_KB);

    registrar.Parameter("min_partition_weight", &TSortOperationOptionsBase::MinPartitionWeight)
        .Alias("min_partition_size")
        .Default(256_MB)
        .GreaterThanOrEqual(1);

    // Minimum is 1 for tests.
    registrar.Parameter("min_uncompressed_block_size", &TSortOperationOptionsBase::MinUncompressedBlockSize)
        .Default(100_KB)
        .GreaterThanOrEqual(1);

    registrar.Parameter("max_value_count_per_simple_sort_job", &TSortOperationOptionsBase::MaxValueCountPerSimpleSortJob)
        .Default(10 * 1000 * 1000)
        .GreaterThanOrEqual(1);

    registrar.Parameter("partition_job_size_adjuster", &TSortOperationOptionsBase::PartitionJobSizeAdjuster)
        .DefaultNew();

    registrar.Parameter("data_balancer", &TSortOperationOptionsBase::DataBalancer)
        .DefaultNew();
}

void TUserJobMonitoringConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_monitored_user_jobs_per_operation", &TUserJobMonitoringConfig::MaxMonitoredUserJobsPerOperation)
        .Default(20)
        .GreaterThanOrEqual(0);

    registrar.Parameter("max_monitored_user_jobs_per_agent", &TUserJobMonitoringConfig::MaxMonitoredUserJobsPerAgent)
        .Default(1000)
        .GreaterThanOrEqual(0);
}

void TMemoryWatchdogConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("total_controller_memory_limit", &TMemoryWatchdogConfig::TotalControllerMemoryLimit)
        .Default();

    registrar.Parameter("operation_controller_memory_limit", &TMemoryWatchdogConfig::OperationControllerMemoryLimit)
        .Default(50_GB);
    registrar.Parameter("operation_controller_memory_overconsumption_threshold", &TMemoryWatchdogConfig::OperationControllerMemoryOverconsumptionThreshold)
        .Default(30_GB);

    registrar.Parameter("memory_usage_check_period", &TMemoryWatchdogConfig::MemoryUsageCheckPeriod)
        .Default(TDuration::Seconds(5));
}

void TControllerAgentConfig::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);

    registrar.Parameter("chunk_list_preallocation_count", &TControllerAgentConfig::ChunkListPreallocationCount)
        .Default(128)
        .GreaterThanOrEqual(0);
    registrar.Parameter("max_chunk_list_allocation_count", &TControllerAgentConfig::MaxChunkListAllocationCount)
        .Default(16384)
        .GreaterThanOrEqual(0);
    registrar.Parameter("chunk_list_watermark_count", &TControllerAgentConfig::ChunkListWatermarkCount)
        .Default(50)
        .GreaterThanOrEqual(0);
    registrar.Parameter("chunk_list_allocation_multiplier", &TControllerAgentConfig::ChunkListAllocationMultiplier)
        .Default(2.0)
        .GreaterThan(1.0);
    registrar.Parameter("desired_chunk_lists_per_release", &TControllerAgentConfig::DesiredChunkListsPerRelease)
        .Default(10 * 1000);
    registrar.Parameter("intermediate_output_master_cell_count", &TControllerAgentConfig::IntermediateOutputMasterCellCount)
        .GreaterThanOrEqual(1)
        .Default(4);

    registrar.Parameter("enable_snapshot_building", &TControllerAgentConfig::EnableSnapshotBuilding)
        .Default(true);
    registrar.Parameter("enable_snapshot_building_disabled_alert", &TControllerAgentConfig::EnableSnapshotBuildingDisabledAlert)
        .Default(true);
    registrar.Parameter("snapshot_period", &TControllerAgentConfig::SnapshotPeriod)
        .Default(TDuration::Seconds(300));
    registrar.Parameter("snapshot_timeout", &TControllerAgentConfig::SnapshotTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("snapshot_fork_timeout", &TControllerAgentConfig::SnapshotForkTimeout)
        .Default(TDuration::Minutes(2));
    registrar.Parameter("operation_controller_suspend_timeout", &TControllerAgentConfig::OperationControllerSuspendTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("parallel_snapshot_builder_count", &TControllerAgentConfig::ParallelSnapshotBuilderCount)
        .Default(4)
        .GreaterThan(0);
    registrar.Parameter("snapshot_writer", &TControllerAgentConfig::SnapshotWriter)
        .DefaultNew();

    registrar.Parameter("enable_snapshot_loading", &TControllerAgentConfig::EnableSnapshotLoading)
        .Default(false);
    registrar.Parameter("enable_snapshot_loading_disabled_alert", &TControllerAgentConfig::EnableSnapshotLoadingDisabledAlert)
        .Default(true);
    registrar.Parameter("snapshot_reader", &TControllerAgentConfig::SnapshotReader)
        .DefaultNew();

    registrar.Parameter("transactions_refresh_period", &TControllerAgentConfig::TransactionsRefreshPeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("operations_update_period", &TControllerAgentConfig::OperationsUpdatePeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("chunk_unstage_period", &TControllerAgentConfig::ChunkUnstagePeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("enable_unrecognized_alert", &TControllerAgentConfig::EnableUnrecognizedAlert)
        .Default(true);

    registrar.Parameter("max_children_per_attach_request", &TControllerAgentConfig::MaxChildrenPerAttachRequest)
        .Default(10000)
        .GreaterThan(0);

    registrar.Parameter("chunk_location_throttler", &TControllerAgentConfig::ChunkLocationThrottler)
        .DefaultNew();

    registrar.Parameter("event_log", &TControllerAgentConfig::EventLog)
        .DefaultNew();

    registrar.Parameter("scheduler_handshake_rpc_timeout", &TControllerAgentConfig::SchedulerHandshakeRpcTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("scheduler_handshake_failure_backoff", &TControllerAgentConfig::SchedulerHandshakeFailureBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("scheduler_heartbeat_rpc_timeout", &TControllerAgentConfig::SchedulerHeartbeatRpcTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("scheduler_heartbeat_failure_backoff", &TControllerAgentConfig::SchedulerHeartbeatFailureBackoff)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("scheduler_heartbeat_period", &TControllerAgentConfig::SchedulerHeartbeatPeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("config_update_period", &TControllerAgentConfig::ConfigUpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("exec_nodes_update_period", &TControllerAgentConfig::ExecNodesUpdatePeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("operations_push_period", &TControllerAgentConfig::OperationsPushPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("operation_job_metrics_push_period", &TControllerAgentConfig::OperationJobMetricsPushPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("operation_alerts_push_period", &TControllerAgentConfig::OperationAlertsPushPeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("suspicious_jobs_push_period", &TControllerAgentConfig::SuspiciousJobsPushPeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("controller_thread_count", &TControllerAgentConfig::ControllerThreadCount)
        .Default(16)
        .GreaterThan(0);

    registrar.Parameter("job_spec_build_thread_count", &TControllerAgentConfig::JobSpecBuildThreadCount)
        .Default(16)
        .GreaterThan(0);

    registrar.Parameter("controller_static_orchid_update_period", &TControllerAgentConfig::ControllerStaticOrchidUpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("controller_orchid_keys_update_period", &TControllerAgentConfig::ControllerOrchidKeysUpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("max_concurrent_safe_core_dumps", &TControllerAgentConfig::MaxConcurrentSafeCoreDumps)
        .Default(1)
        .GreaterThanOrEqual(0);

    registrar.Parameter("scheduling_tag_filter_expire_timeout", &TControllerAgentConfig::SchedulingTagFilterExpireTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("operation_time_limit", &TControllerAgentConfig::OperationTimeLimit)
        .Default();
    registrar.Parameter("operation_time_limit_check_period", &TControllerAgentConfig::OperationTimeLimitCheckPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("resource_demand_sanity_check_period", &TControllerAgentConfig::ResourceDemandSanityCheckPeriod)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("operation_initialization_timeout", &TControllerAgentConfig::OperationInitializationTimeout)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("operation_transaction_timeout", &TControllerAgentConfig::OperationTransactionTimeout)
        .Default(TDuration::Minutes(300));
    registrar.Parameter("operation_transaction_ping_period", &TControllerAgentConfig::OperationTransactionPingPeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("operation_progress_log_backoff", &TControllerAgentConfig::OperationLogProgressBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("task_update_period", &TControllerAgentConfig::TaskUpdatePeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("available_exec_nodes_check_period", &TControllerAgentConfig::AvailableExecNodesCheckPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("banned_exec_nodes_check_period", &TControllerAgentConfig::BannedExecNodesCheckPeriod)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("operation_progress_analysis_period", &TControllerAgentConfig::OperationProgressAnalysisPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("operation_build_progress_period", &TControllerAgentConfig::OperationBuildProgressPeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("check_tentative_tree_eligibility_period", &TControllerAgentConfig::CheckTentativeTreeEligibilityPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("zombie_operation_orchids", &TControllerAgentConfig::ZombieOperationOrchids)
        .DefaultNew();

    registrar.Parameter("max_retained_jobs_per_operation", &TControllerAgentConfig::MaxRetainedJobsPerOperation)
        .Alias("max_job_nodes_per_operation")
        .Default(200)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(1000);

    registrar.Parameter("max_archived_job_spec_count_per_operation", &TControllerAgentConfig::MaxArchivedJobSpecCountPerOperation)
        .Default(500)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(5000);

    registrar.Parameter("guaranteed_archived_job_spec_count_per_operation", &TControllerAgentConfig::GuaranteedArchivedJobSpecCountPerOperation)
        .Default(10)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(100);

    registrar.Parameter("min_job_duration_to_archive_job_spec", &TControllerAgentConfig::MinJobDurationToArchiveJobSpec)
        .Default(TDuration::Minutes(30))
        .GreaterThanOrEqual(TDuration::Minutes(5));

    registrar.Parameter("max_chunks_per_fetch", &TControllerAgentConfig::MaxChunksPerFetch)
        .Default(100000)
        .GreaterThan(0);

    registrar.Parameter("max_user_file_count", &TControllerAgentConfig::MaxUserFileCount)
        .Default(1000)
        .GreaterThan(0);
    registrar.Parameter("max_user_file_size", &TControllerAgentConfig::MaxUserFileSize)
        .Alias("max_file_size")
        .Default(10_GB);
    registrar.Parameter("max_user_file_table_data_weight", &TControllerAgentConfig::MaxUserFileTableDataWeight)
        .Default(10_GB);
    registrar.Parameter("max_user_file_chunk_count", &TControllerAgentConfig::MaxUserFileChunkCount)
        .Default(1000);

    registrar.Parameter("max_input_table_count", &TControllerAgentConfig::MaxInputTableCount)
        .Default(1000)
        .GreaterThan(0);

    registrar.Parameter("max_output_table_count", &TControllerAgentConfig::MaxOutputTableCount)
        .Default(1000)
        .GreaterThan(0);

    registrar.Parameter("max_ranges_on_table", &TControllerAgentConfig::MaxRangesOnTable)
        .Default(1000)
        .GreaterThan(0);

    registrar.Parameter("safe_online_node_count", &TControllerAgentConfig::SafeOnlineNodeCount)
        .GreaterThanOrEqual(0)
        .Default(1);

    registrar.Parameter("safe_scheduler_online_time", &TControllerAgentConfig::SafeSchedulerOnlineTime)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("controller_exec_node_info_update_period", &TControllerAgentConfig::ControllerExecNodeInfoUpdatePeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("max_chunks_per_locate_request", &TControllerAgentConfig::MaxChunksPerLocateRequest)
        .GreaterThan(0)
        .Default(10000);

    registrar.Parameter("operation_options", &TControllerAgentConfig::OperationOptions)
        .Default(NYTree::GetEphemeralNodeFactory()->CreateMap())
        .MergeBy(NYTree::EMergeStrategy::Combine);

    registrar.Parameter("map_operation_options", &TControllerAgentConfig::MapOperationOptions)
        .DefaultNew();
    registrar.Parameter("reduce_operation_options", &TControllerAgentConfig::ReduceOperationOptions)
        .DefaultNew();
    registrar.Parameter("join_reduce_operation_options", &TControllerAgentConfig::JoinReduceOperationOptions)
        .DefaultNew();
    registrar.Parameter("erase_operation_options", &TControllerAgentConfig::EraseOperationOptions)
        .DefaultNew();
    registrar.Parameter("ordered_merge_operation_options", &TControllerAgentConfig::OrderedMergeOperationOptions)
        .DefaultNew();
    registrar.Parameter("unordered_merge_operation_options", &TControllerAgentConfig::UnorderedMergeOperationOptions)
        .DefaultNew();
    registrar.Parameter("sorted_merge_operation_options", &TControllerAgentConfig::SortedMergeOperationOptions)
        .DefaultNew();
    registrar.Parameter("map_reduce_operation_options", &TControllerAgentConfig::MapReduceOperationOptions)
        .DefaultNew();
    registrar.Parameter("sort_operation_options", &TControllerAgentConfig::SortOperationOptions)
        .DefaultNew();
    registrar.Parameter("remote_copy_operation_options", &TControllerAgentConfig::RemoteCopyOperationOptions)
        .DefaultNew();
    registrar.Parameter("vanilla_operation_options", &TControllerAgentConfig::VanillaOperationOptions)
        .DefaultNew();

    registrar.Parameter("environment", &TControllerAgentConfig::Environment)
        .Default(THashMap<TString, TString>())
        .MergeBy(NYTree::EMergeStrategy::Combine);

    registrar.Parameter("enable_controller_failure_spec_option", &TControllerAgentConfig::EnableControllerFailureSpecOption)
        .Default(false);

    registrar.Parameter("enable_job_revival", &TControllerAgentConfig::EnableJobRevival)
        .Default(true);

    registrar.Parameter("enable_locality", &TControllerAgentConfig::EnableLocality)
        .Default(true);

    registrar.Parameter("fetcher", &TControllerAgentConfig::Fetcher)
        .DefaultNew();

    registrar.Parameter("chunk_slice_fetcher", &TControllerAgentConfig::ChunkSliceFetcher)
        .DefaultNew();

    registrar.Parameter("udf_registry_path", &TControllerAgentConfig::UdfRegistryPath)
        .Default();

    registrar.Parameter("enable_tmpfs", &TControllerAgentConfig::EnableTmpfs)
        .Default(true);
    registrar.Parameter("enable_map_job_size_adjustment", &TControllerAgentConfig::EnableMapJobSizeAdjustment)
        .Default(true);
    registrar.Parameter("enable_job_splitting", &TControllerAgentConfig::EnableJobSplitting)
        .Default(true);
    registrar.Parameter("enable_job_interrupts", &TControllerAgentConfig::EnableJobInterrupts)
        .Default(true);

    registrar.Parameter("heavy_job_spec_slice_count_threshold", &TControllerAgentConfig::HeavyJobSpecSliceCountThreshold)
        .Default(1000)
        .GreaterThan(0);

    //! By default we disable job size adjustment for partition maps,
    //! since it may lead to partition data skew between nodes.
    registrar.Parameter("enable_partition_map_job_size_adjustment", &TControllerAgentConfig::EnablePartitionMapJobSizeAdjustment)
        .Default(false);

    registrar.Parameter("user_job_memory_digest_precision", &TControllerAgentConfig::UserJobMemoryDigestPrecision)
        .Default(0.01)
        .GreaterThan(0);
    registrar.Parameter("user_job_memory_reserve_quantile", &TControllerAgentConfig::UserJobMemoryReserveQuantile)
        .InRange(0.0, 1.0)
        .Default(0.95);
    registrar.Parameter("job_proxy_memory_reserve_quantile", &TControllerAgentConfig::JobProxyMemoryReserveQuantile)
        .InRange(0.0, 1.0)
        .Default(0.95);
    registrar.Parameter("resource_overdraft_factor", &TControllerAgentConfig::ResourceOverdraftFactor)
        .InRange(1.0, 10.0)
        .Default(1.1);

    registrar.Parameter("iops_threshold", &TControllerAgentConfig::IopsThreshold)
        .Default();
    registrar.Parameter("iops_throttler_limit", &TControllerAgentConfig::IopsThrottlerLimit)
        .Default();

    registrar.Parameter("chunk_scraper", &TControllerAgentConfig::ChunkScraper)
        .DefaultNew();

    registrar.Parameter("max_total_slice_count", &TControllerAgentConfig::MaxTotalSliceCount)
        .Default((i64) 10 * 1000 * 1000)
        .GreaterThan(0);

    registrar.Parameter("operation_alerts", &TControllerAgentConfig::OperationAlerts)
        .DefaultNew();

    registrar.Parameter("controller_row_buffer_chunk_size", &TControllerAgentConfig::ControllerRowBufferChunkSize)
        .Default(64_KB)
        .GreaterThan(0);

    registrar.Parameter("testing_options", &TControllerAgentConfig::TestingOptions)
        .DefaultNew();

    registrar.Parameter("suspicious_jobs", &TControllerAgentConfig::SuspiciousJobs)
        .DefaultNew();

    registrar.Parameter("job_spec_codec", &TControllerAgentConfig::JobSpecCodec)
        .Default(NCompression::ECodec::Lz4);

    registrar.Parameter("job_metrics_report_period", &TControllerAgentConfig::JobMetricsReportPeriod)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("system_layer_path", &TControllerAgentConfig::SystemLayerPath)
        .Default();

    registrar.Parameter("default_layer_path", &TControllerAgentConfig::DefaultLayerPath)
        .Default();

    registrar.Parameter("cuda_toolkit_layer_directory_path", &TControllerAgentConfig::CudaToolkitLayerDirectoryPath)
        .Default();

    registrar.Parameter("gpu_check_layer_directory_path", &TControllerAgentConfig::GpuCheckLayerDirectoryPath)
        .Default();

    registrar.Parameter("schedule_job_statistics_log_backoff", &TControllerAgentConfig::ScheduleJobStatisticsLogBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("controller_throttling_log_backoff", &TControllerAgentConfig::ControllerThrottlingLogBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("job_spec_slice_throttler", &TControllerAgentConfig::JobSpecSliceThrottler)
        .Default(New<NConcurrency::TThroughputThrottlerConfig>(500000));

    registrar.Parameter("static_orchid_cache_update_period", &TControllerAgentConfig::StaticOrchidCacheUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("cached_running_jobs_update_period", &TControllerAgentConfig::CachedRunningJobsUpdatePeriod)
        .Default();

    registrar.Parameter("cached_unavailable_chunks_update_period", &TControllerAgentConfig::CachedUnavailableChunksUpdatePeriod)
        .Default();

    registrar.Parameter("tagged_memory_statistics_update_period", &TControllerAgentConfig::TaggedMemoryStatisticsUpdatePeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("alerts_update_period", &TControllerAgentConfig::AlertsUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("total_controller_memory_limit", &TControllerAgentConfig::TotalControllerMemoryLimit)
        .Default();

    registrar.Parameter("schedule_job_controller_queue", &TControllerAgentConfig::ScheduleJobControllerQueue)
        .Default(EOperationControllerQueue::Default);

    registrar.Parameter("job_events_controller_queue", &TControllerAgentConfig::JobEventsControllerQueue)
        .Default(EOperationControllerQueue::Default);

    registrar.Parameter("schedule_job_wait_time_threshold", &TControllerAgentConfig::ScheduleJobWaitTimeThreshold)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("allow_users_group_read_intermediate_data", &TControllerAgentConfig::AllowUsersGroupReadIntermediateData)
        .Default(false);

    registrar.Parameter("custom_job_metrics", &TControllerAgentConfig::CustomJobMetrics)
        .Default();

    registrar.Parameter("dynamic_table_lock_checking_attempt_count_limit", &TControllerAgentConfig::DynamicTableLockCheckingAttemptCountLimit)
        .Default(10);
    registrar.Parameter("dynamic_table_lock_checking_interval_scale", &TControllerAgentConfig::DynamicTableLockCheckingIntervalScale)
        .Default(1.5);
    registrar.Parameter("dynamic_table_lock_checking_interval_duration_min", &TControllerAgentConfig::DynamicTableLockCheckingIntervalDurationMin)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("dynamic_table_lock_checking_interval_duration_max", &TControllerAgentConfig::DynamicTableLockCheckingIntervalDurationMax)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("enable_operation_progress_archivation", &TControllerAgentConfig::EnableOperationProgressArchivation)
        .Default(true);
    registrar.Parameter("operation_progress_archivation_timeout", &TControllerAgentConfig::OperationProgressArchivationTimeout)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("legacy_live_preview_user_blacklist", &TControllerAgentConfig::LegacyLivePreviewUserBlacklist)
        .DefaultNew("robot-.*");

    registrar.Parameter("enable_bulk_insert_for_everyone", &TControllerAgentConfig::EnableBulkInsertForEveryone)
        .Default(false);
    registrar.Parameter("enable_versioned_remote_copy", &TControllerAgentConfig::EnableVersionedRemoteCopy)
        .Default(false);

    registrar.Parameter("default_enable_porto", &TControllerAgentConfig::DefaultEnablePorto)
        .Default(NScheduler::EEnablePorto::None);

    registrar.Parameter("job_reporter", &TControllerAgentConfig::JobReporter)
        .DefaultNew();

    registrar.Parameter("heavy_request_immediate_response_timeout", &TControllerAgentConfig::HeavyRequestImmediateResponseTimeout)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("memory_usage_profiling_period", &TControllerAgentConfig::MemoryUsageProfilingPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("enable_bypass_artifact_cache", &TControllerAgentConfig::EnableBypassArtifactCache)
        .Default(true);

    registrar.Parameter("enable_prerequisites_for_starting_completion_transactions", &TControllerAgentConfig::EnablePrerequisitesForStartingCompletionTransactions)
        .Default(true);

    registrar.Parameter("enable_eager_transaction_replication", &TControllerAgentConfig::EnableEagerTransactionReplication)
        .Default(true);

    // COMPAT(gritukan): This default is quite dangerous, change it when all controller agents will have fresh configs.
    registrar.Parameter("tags", &TControllerAgentConfig::Tags)
        .Default(std::vector<TString>({"default"}));

    registrar.Parameter("user_job_monitoring", &TControllerAgentConfig::UserJobMonitoring)
        .DefaultNew();

    registrar.Parameter("obligatory_account_mediums", &TControllerAgentConfig::ObligatoryAccountMediums)
        .Default();

    registrar.Parameter("enable_master_resource_usage_accounting", &TControllerAgentConfig::EnableMasterResourceUsageAccounting)
        .Default(true);

    registrar.Parameter("memory_watchdog", &TControllerAgentConfig::MemoryWatchdog)
        .DefaultNew();

    registrar.Parameter("secure_vault_length_limit", &TControllerAgentConfig::SecureVaultLengthLimit)
        .Default(64_MB);

    registrar.Parameter("full_job_info_wait_timeout", &TControllerAgentConfig::FullJobInfoWaitTimeout)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("enable_heartbeats_from_nodes", &TControllerAgentConfig::EnableHeartbeatsFromNodes)
        .Default(false);

    registrar.Parameter("chunk_teleporter", &TControllerAgentConfig::ChunkTeleporter)
        .DefaultNew();

    registrar.Preprocessor([&] (TControllerAgentConfig* config) {
        config->EventLog->MaxRowWeight = 128_MB;
        if (!config->EventLog->Path) {
            config->EventLog->Path = "//sys/scheduler/event_log";
        }

        config->ChunkLocationThrottler->Limit = 10000;

        // Value in options is an upper bound hint on uncompressed data size for merge jobs.
        config->OrderedMergeOperationOptions->DataWeightPerJob = 20_GB;
        config->OrderedMergeOperationOptions->MaxDataSlicesPerJob = 10000;

        config->SortedMergeOperationOptions->DataWeightPerJob = 20_GB;
        config->SortedMergeOperationOptions->MaxDataSlicesPerJob = 10000;

        config->UnorderedMergeOperationOptions->DataWeightPerJob = 20_GB;
        config->UnorderedMergeOperationOptions->MaxDataSlicesPerJob = 10000;

        config->OperationOptions->AsMap()->AddChild("controller_building_job_spec_count_limit", NYTree::ConvertToNode(100));
        config->OperationOptions->AsMap()->AddChild("controller_total_building_job_spec_slice_count_limit", NYTree::ConvertToNode(50'000));
    });

    registrar.Postprocessor([&] (TControllerAgentConfig* config) {
        UpdateOptions(&config->MapOperationOptions, config->OperationOptions);
        UpdateOptions(&config->ReduceOperationOptions, config->OperationOptions);
        UpdateOptions(&config->JoinReduceOperationOptions, config->OperationOptions);
        UpdateOptions(&config->EraseOperationOptions, config->OperationOptions);
        UpdateOptions(&config->OrderedMergeOperationOptions, config->OperationOptions);
        UpdateOptions(&config->UnorderedMergeOperationOptions, config->OperationOptions);
        UpdateOptions(&config->SortedMergeOperationOptions, config->OperationOptions);
        UpdateOptions(&config->MapReduceOperationOptions, config->OperationOptions);
        UpdateOptions(&config->SortOperationOptions, config->OperationOptions);
        UpdateOptions(&config->RemoteCopyOperationOptions, config->OperationOptions);
        UpdateOptions(&config->VanillaOperationOptions, config->OperationOptions);

        for (const auto& customJobMetricDescription : config->CustomJobMetrics) {
            for (auto metricName : TEnumTraits<NScheduler::EJobMetricName>::GetDomainValues()) {
                if (FormatEnum(metricName) == customJobMetricDescription.ProfilingName) {
                    THROW_ERROR_EXCEPTION("Metric with profiling name $Qv is already presented",
                         customJobMetricDescription.ProfilingName);
                }
            }
        }

        if (config->TotalControllerMemoryLimit) {
            config->MemoryWatchdog->TotalControllerMemoryLimit = config->TotalControllerMemoryLimit;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_DYNAMIC_PHOENIX_TYPE(TEraseOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TMapOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TMapReduceOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TReduceOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSimpleOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedMergeOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortOperationOptionsBase);
DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeOperationOptions);
DEFINE_DYNAMIC_PHOENIX_TYPE(TVanillaOperationOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
