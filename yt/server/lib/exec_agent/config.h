#pragma once

#include "public.h"

#include <yt/server/lib/job_agent/config.h>

#include <yt/server/lib/job_proxy/config.h>

#include <yt/server/lib/containers/config.h>

#include <yt/server/lib/misc/config.h>

#include <yt/ytlib/cgroup/config.h>

#include <yt/core/ytree/node.h>

#include <yt/core/ytree/yson_serializable.h>

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
    //! When set to |true|, job proxies are run under per-slot pseudousers.
    //! This option requires node server process to have root privileges.
    bool EnforceJobControl;

    TSimpleJobEnvironmentConfig()
    {
        RegisterParameter("enforce_job_control", EnforceJobControl)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TSimpleJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

class TCGroupJobEnvironmentConfig
    : public TJobEnvironmentConfig
    , public NCGroup::TCGroupConfig
{
public:
    TDuration BlockIOWatchdogPeriod;

    TCGroupJobEnvironmentConfig()
    {
        RegisterParameter("block_io_watchdog_period", BlockIOWatchdogPeriod)
            .Default(TDuration::Seconds(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TCGroupJobEnvironmentConfig)

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

    TSlotLocationConfig()
    {
        RegisterParameter("disk_quota", DiskQuota)
            .Default()
            .GreaterThan(0);
        RegisterParameter("disk_usage_watermark", DiskUsageWatermark)
            .Default(10_GB)
            .GreaterThanOrEqual(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TSlotLocationConfig)

////////////////////////////////////////////////////////////////////////////////

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

    //! Fail node if some error occurred during slot cleanup.
    bool SlotInitializationFailureIsFatal;

    //! Chunk size used for copying chunks if #copy_chunks is set to %true in operation spec.
    i64 FileCopyChunkSize;

    TDuration DiskResourcesUpdatePeriod;

    int MaxConsecutiveAborts;

    TDuration DisableJobsTimeout;

    TSlotManagerConfig()
    {
        RegisterParameter("locations", Locations);
        RegisterParameter("enable_tmpfs", EnableTmpfs)
            .Default(true);
        RegisterParameter("detached_tmpfs_umount", DetachedTmpfsUmount)
            .Default(true);
        RegisterParameter("job_environment", JobEnvironment)
            .Default(ConvertToNode(New<TSimpleJobEnvironmentConfig>()));
        RegisterParameter("slot_initialization_failure_is_fatal", SlotInitializationFailureIsFatal)
            .Default(false);
        RegisterParameter("file_copy_chunk_size", FileCopyChunkSize)
            .GreaterThanOrEqual(1_KB)
            .Default(10_MB);

        RegisterParameter("disk_resources_update_period", DiskResourcesUpdatePeriod)
            .Alias("disk_info_update_period")
            .Default(TDuration::Seconds(5));

        RegisterParameter("max_consecutive_aborts", MaxConsecutiveAborts)
            .Default(500);
        RegisterParameter("disable_jobs_timeout", DisableJobsTimeout)
            .Default(TDuration::Minutes(10));
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
            .Alias("unsuccess_heartbeat_backoff_time")
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

class TExecAgentConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TSlotManagerConfigPtr SlotManager;
    NJobAgent::TJobControllerConfigPtr JobController;
    NJobAgent::TJobReporterConfigPtr JobReporter;
    TSchedulerConnectorConfigPtr SchedulerConnector;

    NLogging::TLogManagerConfigPtr JobProxyLogging;
    NTracing::TTraceManagerConfigPtr JobProxyTracing;

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
    //! Do not create L3 virtual interface in porto container.
    bool TestNetwork;

    //! This option is used for testing purposes only.
    //! Adds inner errors for failed jobs.
    bool TestJobErrorTruncation;

    NJobProxy::TCoreWatcherConfigPtr CoreWatcher;

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
        RegisterParameter("job_proxy_tracing", JobProxyTracing)
            .DefaultNew();

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

        RegisterParameter("test_network", TestNetwork)
            .Default(false);

        RegisterParameter("test_job_error_truncation", TestJobErrorTruncation)
            .Default(false);

        RegisterParameter("core_watcher", CoreWatcher)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TExecAgentConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
