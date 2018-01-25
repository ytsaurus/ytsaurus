#pragma once

#include "public.h"

#include <yt/server/job_agent/config.h>

#include <yt/server/job_proxy/config.h>

#include <yt/server/misc/config.h>

#include <yt/ytlib/cgroup/config.h>

#include <yt/core/ytree/node.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NExecAgent {

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
    TDuration PortoWaitTime;
    TDuration PortoPollPeriod;
    TDuration BlockIOWatchdogPeriod;
    bool UseResourceLimits;

    TNullable<TDuration> ResourceLimitsUpdatePeriod;
    TNullable<TString> ExternalJobContainer;
    TNullable<TString> ExternalJobRootVolume;
    yhash<TString, TString> ExternalBinds;

    double JobsIOWeight;

    TPortoJobEnvironmentConfig()
    {
        RegisterParameter("porto_wait_time", PortoWaitTime)
            .Default(TDuration::Seconds(10));
        RegisterParameter("porto_poll_period", PortoPollPeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("block_io_watchdog_period", BlockIOWatchdogPeriod)
            .Default(TDuration::Seconds(60));
        RegisterParameter("use_resource_limits", UseResourceLimits)
            .Default(false);

        RegisterParameter("resource_limits_update_period", ResourceLimitsUpdatePeriod)
            .Default(Null);
        RegisterParameter("external_job_container", ExternalJobContainer)
            .Default(Null);
        RegisterParameter("external_job_root_volume", ExternalJobRootVolume)
            .Default(Null);
        RegisterParameter("external_binds", ExternalBinds)
            .Default();

        RegisterParameter("jobs_io_weight", JobsIOWeight)
            .Default(0.05);
    }
};

DEFINE_REFCOUNTED_TYPE(TPortoJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlotLocationConfig
    : public TDiskLocationConfig
{
public:
    TNullable<i64> DiskQuota;
    i64 DiskUsageWatermark;

    TSlotLocationConfig()
    {
        RegisterParameter("disk_quota", DiskQuota)
            .Default(Null)
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

    //! A directory that contains files defining the correspondence between slot user id
    //! and its job proxy RPC Unix Domain Socket name.
    TNullable<TString> JobProxySocketNameDirectory;

    TDuration DiskInfoUpdatePeriod;

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

        RegisterParameter("job_proxy_socket_name_directory", JobProxySocketNameDirectory)
            .Default(Null);

        RegisterParameter("disk_info_update_period", DiskInfoUpdatePeriod)
            .Default(TDuration::Seconds(5));
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

    //! Backoff for sending the next heartbeat after failure or skip.
    TDuration UnsuccessHeartbeatBackoffTime;

    TSchedulerConnectorConfig()
    {
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("heartbeat_splay", HeartbeatSplay)
            .Default(TDuration::Seconds(1));
        RegisterParameter("unsuccess_heartbeat_backoff_time", UnsuccessHeartbeatBackoffTime)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TExecAgentConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TSlotManagerConfigPtr SlotManager;
    NJobAgent::TJobControllerConfigPtr JobController;
    NJobAgent::TStatisticsReporterConfigPtr StatisticsReporter;
    TSchedulerConnectorConfigPtr SchedulerConnector;

    NLogging::TLogConfigPtr JobProxyLogging;
    NTracing::TTraceManagerConfigPtr JobProxyTracing;

    TDuration SupervisorRpcTimeout;
    TDuration JobProberRpcTimeout;

    TDuration JobProxyHeartbeatPeriod;

    //! This is a special testing option.
    //! Instead of actually setting root fs, it just provides special environment variable.
    bool TestRootFS;

    int NodeDirectoryPrepareRetryCount;
    TDuration NodeDirectoryPrepareBackoffTime;

    TDuration CoreForwarderTimeout;

    i64 MinRequiredDiskSpace;

    TExecAgentConfig()
    {
        RegisterParameter("slot_manager", SlotManager)
            .DefaultNew();
        RegisterParameter("job_controller", JobController)
            .DefaultNew();
        RegisterParameter("statistics_reporter", StatisticsReporter)
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

        RegisterParameter("node_directory_prepare_retry_count", NodeDirectoryPrepareRetryCount)
            .Default(10);
        RegisterParameter("node_directory_prepare_backoff_time", NodeDirectoryPrepareBackoffTime)
            .Default(TDuration::Seconds(3));

        RegisterParameter("core_forwarder_timeout", CoreForwarderTimeout)
            .Default(TDuration::Seconds(60))
            .GreaterThan(TDuration::Zero());

        RegisterParameter("min_required_disk_space", MinRequiredDiskSpace)
            .Default(100_MB);
    }
};

DEFINE_REFCOUNTED_TYPE(TExecAgentConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
