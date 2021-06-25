#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/server/lib/containers/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/ytree/node.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration of a single environment.
class TJobEnvironmentConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    EJobEnvironmentType Type;

    //! When job control is enabled, system runs user jobs under fake
    //! uids in range [StartUid, StartUid + SlotCount - 1].
    int StartUid;

    TDuration MemoryWatchdogPeriod;

    TJobEnvironmentConfig()
    {
        RegisterParameter("type", Type)
            .Default(EJobEnvironmentType::Simple);

        RegisterParameter("start_uid", StartUid)
            .Default(10000);

        RegisterParameter("memory_watchdog_period", MemoryWatchdogPeriod)
            .Default(TDuration::Seconds(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobEnvironmentConfig
    : public TJobEnvironmentConfig
{
public:
    TSimpleJobEnvironmentConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TSimpleJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

class TPortoJobEnvironmentConfig
    : public TJobEnvironmentConfig
{
public:
    NContainers::TPortoExecutorConfigPtr PortoExecutor;

    TDuration BlockIOWatchdogPeriod;

    THashMap<TString, TString> ExternalBinds;

    double JobsIOWeight;
    double NodeDedicatedCpu;

    bool UseShortContainerNames;

    // COMPAT(psushin): this is compatibility option between different versions of ytcfgen and yt_node.
    //! Used by ytcfgen, when it creates "yt_daemon" subcontainer inside iss_hook_start.
    bool UseDaemonSubcontainer;

    //! For testing purposes only.
    bool UseExecFromLayer;

    TPortoJobEnvironmentConfig()
    {
        RegisterParameter("porto_executor", PortoExecutor)
            .DefaultNew();

        RegisterParameter("block_io_watchdog_period", BlockIOWatchdogPeriod)
            .Default(TDuration::Seconds(60));

        RegisterParameter("external_binds", ExternalBinds)
            .Default();

        RegisterParameter("jobs_io_weight", JobsIOWeight)
            .Default(0.05);
        RegisterParameter("node_dedicated_cpu", NodeDedicatedCpu)
            .GreaterThanOrEqual(0)
            .Default(2);

        RegisterParameter("use_short_container_names", UseShortContainerNames)
            .Default(false);

        RegisterParameter("use_daemon_subcontainer", UseDaemonSubcontainer)
            .Default(false);

        RegisterParameter("use_exec_from_layer", UseExecFromLayer)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TPortoJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlotLocationConfig
    : public TDiskLocationConfig
{
public:
    std::optional<i64> DiskQuota;
    i64 DiskUsageWatermark;

    TString MediumName;

    TSlotLocationConfig()
    {
        RegisterParameter("disk_quota", DiskQuota)
            .Default()
            .GreaterThan(0);
        RegisterParameter("disk_usage_watermark", DiskUsageWatermark)
            .Default(10_GB)
            .GreaterThanOrEqual(0);

        RegisterParameter("medium_name", MediumName)
            .Default(NChunkClient::DefaultSlotsMediumName);
    }
};

DEFINE_REFCOUNTED_TYPE(TSlotLocationConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlotManagerTestingConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! If set, slot manager does not report JobProxyUnavailableAlert
    //! allowing scheduler to schedule jobs to current node. Such jobs are
    //! going to be aborted instead of failing; that is exactly what we test
    //! using this switch.
    bool SkipJobProxyUnavailableAlert;

    TSlotManagerTestingConfig()
    {
        RegisterParameter("skip_job_proxy_unavailable_alert", SkipJobProxyUnavailableAlert)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TSlotManagerTestingConfig)

class TSlotManagerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Root path for slot directories.
    std::vector<TSlotLocationConfigPtr> Locations;

    //! Enable using tmpfs on the node.
    bool EnableTmpfs;

    //! Use MNT_DETACH when tmpfs umount called. When option enabled the "Device is busy" error is impossible,
    //! because actual umount will be performed by Linux core asynchronously.
    bool DetachedTmpfsUmount;

    //! Polymorphic job environment configuration.
    NYTree::INodePtr JobEnvironment;

    //! Chunk size used for copying chunks if #copy_chunks is set to %true in operation spec.
    i64 FileCopyChunkSize;

    TDuration DiskResourcesUpdatePeriod;

    TDuration SlotLocationStatisticsUpdatePeriod;

    int MaxConsecutiveAborts;

    TDuration DisableJobsTimeout;

    //! Default medium used to run jobs without disk requests.
    TString DefaultMediumName;

    bool DisableJobsOnGpuCheckFailure;

    TSlotManagerTestingConfigPtr Testing;

    TSlotManagerConfig()
    {
        RegisterParameter("locations", Locations);
        RegisterParameter("enable_tmpfs", EnableTmpfs)
            .Default(true);
        RegisterParameter("detached_tmpfs_umount", DetachedTmpfsUmount)
            .Default(true);
        RegisterParameter("job_environment", JobEnvironment)
            .Default(ConvertToNode(New<TSimpleJobEnvironmentConfig>()));
        RegisterParameter("file_copy_chunk_size", FileCopyChunkSize)
            .GreaterThanOrEqual(1_KB)
            .Default(10_MB);

        RegisterParameter("disk_resources_update_period", DiskResourcesUpdatePeriod)
            .Alias("disk_info_update_period")
            .Default(TDuration::Seconds(5));
        RegisterParameter("slot_location_statistics_update_period", SlotLocationStatisticsUpdatePeriod)
            .Default(TDuration::Seconds(30));

        RegisterParameter("max_consecutive_aborts", MaxConsecutiveAborts)
            .Default(500);
        RegisterParameter("disable_jobs_timeout", DisableJobsTimeout)
            .Default(TDuration::Minutes(10));

        RegisterParameter("default_medium_name", DefaultMediumName)
            .Default(NChunkClient::DefaultSlotsMediumName);

        RegisterParameter("disable_jobs_on_gpu_check_failure", DisableJobsOnGpuCheckFailure)
            .Default(true);

        RegisterParameter("testing", Testing)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TSlotManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConnectorConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent heartbeats.
    TDuration HeartbeatPeriod;

    //! Random delay before first heartbeat.
    TDuration HeartbeatSplay;

    //! Start backoff for sending the next heartbeat after a failure.
    TDuration FailedHeartbeatBackoffStartTime;

    //! Maximum backoff for sending the next heartbeat after a failure.
    TDuration FailedHeartbeatBackoffMaxTime;

    //! Backoff mulitplier for sending the next heartbeat after a failure.
    double FailedHeartbeatBackoffMultiplier;

    TSchedulerConnectorConfig()
    {
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("heartbeat_splay", HeartbeatSplay)
            .Default(TDuration::Seconds(1));
        RegisterParameter("failed_heartbeat_backoff_start_time", FailedHeartbeatBackoffStartTime)
            .GreaterThan(TDuration::Zero())
            .Default(TDuration::Seconds(5));
        RegisterParameter("failed_heartbeat_backoff_max_time", FailedHeartbeatBackoffMaxTime)
            .GreaterThan(TDuration::Zero())
            .Default(TDuration::Seconds(60));
        RegisterParameter("failed_heartbeat_backoff_multiplier", FailedHeartbeatBackoffMultiplier)
            .GreaterThanOrEqual(1.0)
            .Default(2.0);
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent exec node heartbeats.
    TDuration HeartbeatPeriod;

    //! Splay for exec node heartbeats.
    TDuration HeartbeatPeriodSplay;

    //! Timeout of the exec node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    TMasterConnectorConfig()
    {
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(30));
        RegisterParameter("heartbeat_period_splay", HeartbeatPeriodSplay)
            .Default(TDuration::Seconds(1));
        RegisterParameter("heartbeat_timeout", HeartbeatTimeout)
            .Default(TDuration::Seconds(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TBindConfig
    : public NYTree::TYsonSerializable
{
public:
    TString ExternalPath;
    TString InternalPath;
    bool ReadOnly;

    TBindConfig()
    {
        RegisterParameter("external_path", ExternalPath);
        RegisterParameter("internal_path", InternalPath);
        RegisterParameter("read_only", ReadOnly)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TBindConfig)

////////////////////////////////////////////////////////////////////////////////

class TUserJobSensor
    : public NYTree::TYsonSerializable
{
public:
    NProfiling::EMetricType Type;

    TUserJobSensor()
    {
        RegisterParameter("type", Type);
    }
};

DECLARE_REFCOUNTED_CLASS(TUserJobSensor)
DEFINE_REFCOUNTED_TYPE(TUserJobSensor)

////////////////////////////////////////////////////////////////////////////////

class TUserJobMonitoringConfig
    : public NYTree::TYsonSerializable
{
public:
    THashMap<TString, TUserJobSensorPtr> Sensors;

    TUserJobMonitoringConfig()
    {
        RegisterParameter("sensors", Sensors)
            .Default(GetDefaultSensors());
    }

private:
    static const THashMap<TString, TUserJobSensorPtr>& GetDefaultSensors();
};

DECLARE_REFCOUNTED_CLASS(TUserJobMonitoringConfig)
DEFINE_REFCOUNTED_TYPE(TUserJobMonitoringConfig)

////////////////////////////////////////////////////////////////////////////////

class TExecAgentConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TSlotManagerConfigPtr SlotManager;
    NJobAgent::TJobControllerConfigPtr JobController;
    NJobAgent::TJobReporterConfigPtr JobReporter;
    TSchedulerConnectorConfigPtr SchedulerConnector;

    NLogging::TLogManagerConfigPtr JobProxyLogging;
    NTracing::TJaegerTracerConfigPtr JobProxyJaeger;
    std::optional<TString> JobProxyStderrPath;

    TDuration SupervisorRpcTimeout;
    TDuration JobProberRpcTimeout;

    TDuration JobProxyHeartbeatPeriod;

    //! This is a special testing option.
    //! Instead of actually setting root fs, it just provides special environment variable.
    bool TestRootFS;

    std::vector<TBindConfigPtr> RootFSBinds;

    int NodeDirectoryPrepareRetryCount;
    TDuration NodeDirectoryPrepareBackoffTime;

    TDuration JobProxyPreparationTimeout;

    i64 MinRequiredDiskSpace;

    TDuration JobAbortionTimeout;

    //! This option is used for testing purposes only.
    //! Adds inner errors for failed jobs.
    bool TestJobErrorTruncation;

    NJobProxy::TCoreWatcherConfigPtr CoreWatcher;

    //! This option is used for testing purposes only.
    //! It runs job shell under root user instead of slot user.
    bool TestPollJobShell;

    //! If set, user job will not receive uid.
    //! For testing purposes only.
    bool DoNotSetUserId;

    TDuration MemoryTrackerCachePeriod;
    TDuration SMapsMemoryTrackerCachePeriod;

    TUserJobMonitoringConfigPtr UserJobMonitoring;

    TMasterConnectorConfigPtr MasterConnector;

    NProfiling::TSolomonExporterConfigPtr JobProxySolomonExporter;
    TDuration SensorDumpTimeout;

    TExecAgentConfig()
    {
        RegisterParameter("slot_manager", SlotManager)
            .DefaultNew();
        RegisterParameter("job_controller", JobController)
            .DefaultNew();
        RegisterParameter("job_reporter", JobReporter)
            .Alias("statistics_reporter")
            .DefaultNew();
        RegisterParameter("scheduler_connector", SchedulerConnector)
            .DefaultNew();

        RegisterParameter("job_proxy_logging", JobProxyLogging)
            .DefaultNew();
        RegisterParameter("job_proxy_jaeger", JobProxyJaeger)
            .DefaultNew();
        RegisterParameter("job_proxy_stderr_path", JobProxyStderrPath)
            .Default();

        RegisterParameter("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("job_prober_rpc_timeout", JobProberRpcTimeout)
            .Default(TDuration::Seconds(300));

        RegisterParameter("job_proxy_heartbeat_period", JobProxyHeartbeatPeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("test_root_fs", TestRootFS)
            .Default(false);

        RegisterParameter("root_fs_binds", RootFSBinds)
            .Default();

        RegisterParameter("node_directory_prepare_retry_count", NodeDirectoryPrepareRetryCount)
            .Default(10);
        RegisterParameter("node_directory_prepare_backoff_time", NodeDirectoryPrepareBackoffTime)
            .Default(TDuration::Seconds(3));

        RegisterParameter("job_proxy_preparation_timeout", JobProxyPreparationTimeout)
            .Default(TDuration::Minutes(3));

        RegisterParameter("min_required_disk_space", MinRequiredDiskSpace)
            .Default(100_MB);
        RegisterParameter("job_abortion_timeout", JobAbortionTimeout)
            .Default(TDuration::Minutes(15));

        RegisterParameter("test_job_error_truncation", TestJobErrorTruncation)
            .Default(false);

        RegisterParameter("core_watcher", CoreWatcher)
            .DefaultNew();

        RegisterParameter("test_poll_job_shell", TestPollJobShell)
            .Default(false);

        RegisterParameter("do_not_set_user_id", DoNotSetUserId)
            .Default(false);

        RegisterParameter("memory_tracker_cache_period", MemoryTrackerCachePeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("smaps_memory_tracker_cache_period", SMapsMemoryTrackerCachePeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("user_job_monitoring", UserJobMonitoring)
            .DefaultNew();

        RegisterParameter("master_connector", MasterConnector)
            .DefaultNew();

        RegisterParameter("job_proxy_solomon_exporter", JobProxySolomonExporter)
            .DefaultNew();
        RegisterParameter("sensor_dump_timeout", SensorDumpTimeout)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TExecAgentConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent exec node heartbeats.
    std::optional<TDuration> HeartbeatPeriod;

    //! Splay for exec node heartbeats.
    std::optional<TDuration> HeartbeatPeriodSplay;

    TMasterConnectorDynamicConfig()
    {
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default();
        RegisterParameter("heartbeat_period_splay", HeartbeatPeriodSplay)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlotManagerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<bool> DisableJobsOnGpuCheckFailure;

    TSlotManagerDynamicConfig()
    {
        RegisterParameter("disable_jobs_on_gpu_check_failure", DisableJobsOnGpuCheckFailure)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSlotManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TExecAgentDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    TMasterConnectorDynamicConfigPtr MasterConnector;

    TSlotManagerDynamicConfigPtr SlotManager;
    
    NJobAgent::TJobControllerDynamicConfigPtr JobController;

    TExecAgentDynamicConfig()
    {
        RegisterParameter("master_connector", MasterConnector)
            .DefaultNew();

        RegisterParameter("slot_manager", SlotManager)
            .DefaultNew();
        
        RegisterParameter("job_controller", JobController)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TExecAgentDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
