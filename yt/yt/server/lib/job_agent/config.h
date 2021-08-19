#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    int UserSlots;
    double Cpu;
    int Gpu;
    int Network;
    i64 UserMemory;
    i64 SystemMemory;
    int ReplicationSlots;
    i64 ReplicationDataSize;
    i64 MergeDataSize;
    int RemovalSlots;
    int RepairSlots;
    i64 RepairDataSize;
    int SealSlots;
    int MergeSlots;

    TResourceLimitsConfig()
    {
        // These are some very low default limits.
        // Override for production use.
        RegisterParameter("user_slots", UserSlots)
            .GreaterThanOrEqual(0)
            .Default(1);
        RegisterParameter("cpu", Cpu)
            .GreaterThanOrEqual(0)
            .Default(1);
        RegisterParameter("gpu", Gpu)
            .GreaterThanOrEqual(0)
            .Default(0);
        RegisterParameter("network", Network)
            .GreaterThanOrEqual(0)
            .Default(100);
        RegisterParameter("user_memory", UserMemory)
            .Alias("memory")
            .GreaterThanOrEqual(0)
            .Default(std::numeric_limits<i64>::max());
        RegisterParameter("system_memory", SystemMemory)
            .GreaterThanOrEqual(0)
            .Default(std::numeric_limits<i64>::max());
        RegisterParameter("replication_slots", ReplicationSlots)
            .GreaterThanOrEqual(0)
            .Default(16);
        RegisterParameter("replication_data_size", ReplicationDataSize)
            .Default(10_GB)
            .GreaterThanOrEqual(0);
        RegisterParameter("merge_data_size", MergeDataSize)
            .Default(10_GB)
            .GreaterThanOrEqual(0);
        RegisterParameter("removal_slots", RemovalSlots)
            .GreaterThanOrEqual(0)
            .Default(16);
        RegisterParameter("repair_slots", RepairSlots)
            .GreaterThanOrEqual(0)
            .Default(4);
        RegisterParameter("repair_data_size", RepairDataSize)
            .Default(4_GB)
            .GreaterThanOrEqual(0);
        RegisterParameter("seal_slots", SealSlots)
            .GreaterThanOrEqual(0)
            .Default(16);
        RegisterParameter("merge_slots", MergeSlots)
            .GreaterThanOrEqual(0)
            .Default(4);
    }
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TGpuManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;

    TDuration HealthCheckTimeout;
    TDuration HealthCheckPeriod;

    TDuration HealthCheckFailureBackoff;

    std::optional<TShellCommandConfigPtr> JobSetupCommand;

    std::optional<NYPath::TYPath> DriverLayerDirectoryPath;
    std::optional<TString> DriverVersion;
    TDuration DriverLayerFetchPeriod;

    THashMap<TString, TString> CudaToolkitMinDriverVersion;

    //! This is a special testing option.
    //! Instead of normal gpu discovery, it forces the node to believe the number of GPUs passed in the config.
    bool TestResource;
    //! These options enable testing gpu layers and setup commands.
    bool TestLayers;
    bool TestSetupCommands;

    int TestGpuCount;

    TGpuManagerConfig()
    {
        RegisterParameter("enable", Enable)
            .Default(true);

        RegisterParameter("health_check_timeout", HealthCheckTimeout)
            .Default(TDuration::Minutes(5));
        RegisterParameter("health_check_period", HealthCheckPeriod)
            .Default(TDuration::Seconds(10));
        RegisterParameter("health_check_failure_backoff", HealthCheckFailureBackoff)
            .Default(TDuration::Minutes(10));

        RegisterParameter("job_setup_command", JobSetupCommand)
            .Default();

        RegisterParameter("driver_layer_directory_path", DriverLayerDirectoryPath)
            .Default();
        RegisterParameter("driver_version", DriverVersion)
            .Default();
        RegisterParameter("driver_layer_fetch_period", DriverLayerFetchPeriod)
            .Default(TDuration::Minutes(5));
        RegisterParameter("cuda_toolkit_min_driver_version", CudaToolkitMinDriverVersion)
            .Alias("toolkit_min_driver_version")
            .Default();

        RegisterParameter("test_resource", TestResource)
            .Default(false);
        RegisterParameter("test_layers", TestLayers)
            .Default(false);
        RegisterParameter("test_setup_commands", TestSetupCommands)
            .Default(false);
        RegisterParameter("test_gpu_count", TestGpuCount)
            .Default(0);

        RegisterPostprocessor([&] {
            if (TestLayers && !TestResource) {
                THROW_ERROR_EXCEPTION("You need to specify 'test_resource' option if 'test_layers' is specified");
            }
            if (TestGpuCount > 0 && !TestResource) {
                THROW_ERROR_EXCEPTION("You need to specify 'test_resource' option if 'test_gpu_count' is greater than zero");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TGpuManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TGpuManagerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<TDuration> HealthCheckTimeout;
    std::optional<TDuration> HealthCheckPeriod;
    std::optional<TDuration> HealthCheckFailureBackoff;

    std::optional<TDuration> DriverLayerFetchPeriod;

    std::optional<THashMap<TString, TString>> CudaToolkitMinDriverVersion;

    TGpuManagerDynamicConfig()
    {
        RegisterParameter("health_check_timeout", HealthCheckTimeout)
            .Default();
        RegisterParameter("health_check_period", HealthCheckPeriod)
            .Default();
        RegisterParameter("health_check_failure_backoff", HealthCheckFailureBackoff)
            .Default();

        RegisterParameter("driver_layer_fetch_period", DriverLayerFetchPeriod)
            .Default();
        RegisterParameter("cuda_toolkit_min_driver_version", CudaToolkitMinDriverVersion)
            .Alias("toolkit_min_driver_version")
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TGpuManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TShellCommandConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Path;
    std::vector<TString> Args;

    TShellCommandConfig()
    {
        RegisterParameter("path", Path)
            .NonEmpty();
        RegisterParameter("args", Args)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TShellCommandConfig)

////////////////////////////////////////////////////////////////////////////////

class TMappedMemoryControllerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration CheckPeriod;
    i64 ReservedMemory;

    TMappedMemoryControllerConfig()
    {
        RegisterParameter("check_period", CheckPeriod)
            .Default(TDuration::Seconds(30));
        RegisterParameter("reserved_memory", ReservedMemory)
            .Default(10_GB);
    }
};

DEFINE_REFCOUNTED_TYPE(TMappedMemoryControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobControllerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<TDuration> GetJobSpecsTimeout;
    std::optional<TDuration> TotalConfirmationPeriod;

    std::optional<TDuration> CpuOverdraftTimeout;
    std::optional<TDuration> MemoryOverdraftTimeout;
    
    std::optional<TDuration> ResourceAdjustmentPeriod;
    
    std::optional<TDuration> RecentlyRemovedJobsCleanPeriod;
    std::optional<TDuration> RecentlyRemovedJobsStoreTimeout;

    std::optional<bool> DisableJobProxyProfiling;
    
    TGpuManagerDynamicConfigPtr GpuManager;

    NJobProxy::TJobProxyDynamicConfigPtr JobProxy;

    TJobControllerDynamicConfig()
    {
        RegisterParameter("get_job_specs_timeout", GetJobSpecsTimeout)
            .Default();
        
        RegisterParameter("total_confirmation_period", TotalConfirmationPeriod)
            .Default();
        
        RegisterParameter("cpu_overdraft_timeout", CpuOverdraftTimeout)
            .Default();
        
        RegisterParameter("memory_overdraft_timeout", MemoryOverdraftTimeout)
            .Default();
        
        RegisterParameter("resource_adjustment_period", ResourceAdjustmentPeriod)
            .Default();

        RegisterParameter("recently_removed_jobs_clean_period", RecentlyRemovedJobsCleanPeriod)
            .Default();

        RegisterParameter("recently_removed_jobs_store_timeout", RecentlyRemovedJobsStoreTimeout)
            .Default();
        
        RegisterParameter("gpu_manager", GpuManager)
            .DefaultNew();
        
        RegisterParameter("disable_job_proxy_profiling", DisableJobProxyProfiling)
            .Default();
        
        RegisterParameter("job_proxy", JobProxy)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TJobControllerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobControllerConfig
    : public NYTree::TYsonSerializable
{
public:
    TResourceLimitsConfigPtr ResourceLimits;
    NConcurrency::TThroughputThrottlerConfigPtr StatisticsThrottler;
    TDuration WaitingJobsTimeout;
    TDuration GetJobSpecsTimeout;
    TDuration TotalConfirmationPeriod;

    TDuration CpuOverdraftTimeout;
    TDuration MemoryOverdraftTimeout;

    TDuration ResourceAdjustmentPeriod;

    TDuration RecentlyRemovedJobsCleanPeriod;
    TDuration RecentlyRemovedJobsStoreTimeout;

    i64 FreeMemoryWatermark;

    double CpuPerTabletSlot;

    //! Port set has higher priority than StartPort ans PortCount if it is specified.
    int StartPort;
    int PortCount;
    std::optional<THashSet<int>> PortSet;

    TGpuManagerConfigPtr GpuManager;

    TMappedMemoryControllerConfigPtr MappedMemoryController;

    std::optional<TShellCommandConfigPtr> JobSetupCommand;
    TString SetupCommandUser;

    TDuration JobProxyBuildInfoUpdatePeriod;

    bool DisableJobProxyProfiling;

    TJobControllerConfig()
    {
        RegisterParameter("resource_limits", ResourceLimits)
            .DefaultNew();
        RegisterParameter("statistics_throttler", StatisticsThrottler)
            .DefaultNew();

        // Make it greater than interrupt preemption timeout.
        RegisterParameter("waiting_jobs_timeout", WaitingJobsTimeout)
            .Default(TDuration::Seconds(30));

        RegisterParameter("get_job_specs_timeout", GetJobSpecsTimeout)
            .Default(TDuration::Seconds(5));

        RegisterParameter("total_confirmation_period", TotalConfirmationPeriod)
            .Default(TDuration::Minutes(10));

        RegisterParameter("memory_overdraft_timeout", MemoryOverdraftTimeout)
            .Default(TDuration::Minutes(5));

        RegisterParameter("cpu_overdraft_timeout", CpuOverdraftTimeout)
            .Default(TDuration::Minutes(10));

        RegisterParameter("resource_adjustment_period", ResourceAdjustmentPeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("recently_removed_jobs_clean_period", RecentlyRemovedJobsCleanPeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("recently_removed_jobs_store_timeout", RecentlyRemovedJobsStoreTimeout)
            .Default(TDuration::Seconds(60));

        RegisterParameter("cpu_per_tablet_slot", CpuPerTabletSlot)
            .Default(1.0);

        RegisterParameter("start_port", StartPort)
            .Default(20000);

        RegisterParameter("port_count", PortCount)
            .Default(10000);

        RegisterParameter("port_set", PortSet)
            .Default();

        RegisterParameter("gpu_manager", GpuManager)
            .DefaultNew();

        RegisterParameter("mapped_memory_controller", MappedMemoryController)
            .Default(nullptr);

        RegisterParameter("free_memory_watermark", FreeMemoryWatermark)
            .Default(0)
            .GreaterThanOrEqual(0);

        RegisterParameter("job_setup_command", JobSetupCommand)
            .Default();

        RegisterParameter("setup_command_user", SetupCommandUser)
            .Default("root");

        RegisterParameter("job_proxy_build_info_update_period", JobProxyBuildInfoUpdatePeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("disable_job_proxy_profiling", DisableJobProxyProfiling)
            .Default(false);

        RegisterPreprocessor([&] () {
            // 100 kB/sec * 1000 [nodes] = 100 MB/sec that corresponds to
            // approximate incoming bandwidth of 1Gbit/sec of the scheduler.
            StatisticsThrottler->Limit = 100_KB;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TJobControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobReporterConfig
    : public TArchiveReporterConfig
{
public:
    TArchiveHandlerConfigPtr JobHandler;
    TArchiveHandlerConfigPtr OperationIdHandler;
    TArchiveHandlerConfigPtr JobSpecHandler;
    TArchiveHandlerConfigPtr JobStderrHandler;
    TArchiveHandlerConfigPtr JobFailContextHandler;
    TArchiveHandlerConfigPtr JobProfileHandler;

    TString User;
    bool ReportStatisticsLz4;

    // COMPAT(dakovalkov): Delete these when all job reporter configs are in new format.
    std::optional<int> MaxInProgressJobDataSize;
    std::optional<int> MaxInProgressOperationIdDataSize;
    std::optional<int> MaxInProgressJobSpecDataSize;
    std::optional<int> MaxInProgressJobStderrDataSize;
    std::optional<int> MaxInProgressJobFailContextDataSize;

    TJobReporterConfig()
    {
        RegisterParameter("job_handler", JobHandler)
            .DefaultNew();
        RegisterParameter("operation_id_handler", OperationIdHandler)
            .DefaultNew();
        RegisterParameter("job_spec_handler", JobSpecHandler)
            .DefaultNew();
        RegisterParameter("job_stderr_handler", JobStderrHandler)
            .DefaultNew();
        RegisterParameter("job_fail_context_handler", JobFailContextHandler)
            .DefaultNew();
        RegisterParameter("job_profile_handler", JobProfileHandler)
            .DefaultNew();

        RegisterParameter("user", User)
            .Default(NRpc::RootUserName);
        RegisterParameter("report_statistics_lz4", ReportStatisticsLz4)
            .Default(false);

        RegisterParameter("max_in_progress_job_data_size", MaxInProgressJobDataSize)
            .Default();
        RegisterParameter("max_in_progress_operation_id_data_size", MaxInProgressOperationIdDataSize)
            .Default();
        RegisterParameter("max_in_progress_job_spec_data_size", MaxInProgressJobSpecDataSize)
            .Default();
        RegisterParameter("max_in_progress_job_stderr_data_size", MaxInProgressJobStderrDataSize)
            .Default();
        RegisterParameter("max_in_progress_job_fail_context_data_size", MaxInProgressJobFailContextDataSize)
            .Default();

        RegisterPreprocessor([&] {
            OperationIdHandler->MaxInProgressDataSize = 10_MB;

            JobHandler->Path = NScheduler::GetOperationsArchiveJobsPath();
            OperationIdHandler->Path = NScheduler::GetOperationsArchiveOperationIdsPath();
            JobSpecHandler->Path = NScheduler::GetOperationsArchiveJobSpecsPath();
            JobStderrHandler->Path = NScheduler::GetOperationsArchiveJobStderrsPath();
            JobFailContextHandler->Path = NScheduler::GetOperationsArchiveJobFailContextsPath();
            JobProfileHandler->Path = NScheduler::GetOperationsArchiveJobProfilesPath();
        });

        RegisterPostprocessor([&] {
            if (MaxInProgressJobDataSize) {
                JobHandler->MaxInProgressDataSize = *MaxInProgressJobDataSize;
            }
            if (MaxInProgressOperationIdDataSize) {
                OperationIdHandler->MaxInProgressDataSize = *MaxInProgressOperationIdDataSize;
            }
            if (MaxInProgressJobSpecDataSize) {
                JobSpecHandler->MaxInProgressDataSize = *MaxInProgressJobSpecDataSize;
            }
            if (MaxInProgressJobStderrDataSize) {
                JobStderrHandler->MaxInProgressDataSize = *MaxInProgressJobStderrDataSize;
            }
            if (MaxInProgressJobFailContextDataSize) {
                JobFailContextHandler->MaxInProgressDataSize = *MaxInProgressJobFailContextDataSize;
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TJobReporterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
