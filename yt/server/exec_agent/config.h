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
        // Type-dependent configuration is stored as options.
        SetKeepOptions(true);

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
    //! CPU share in cpu cgroup dedicated for slots.
    double CGroupCpuShare;

    TDuration BlockIOWatchdogPeriod;

    TCGroupJobEnvironmentConfig()
    {
        // NB: Default value is minimum possible share in cgroup.
        // http://git.kernel.org/cgit/linux/kernel/git/tip/tip.git/tree/kernel/sched/sched.h#n279
        RegisterParameter("cgroup_cpu_share", CGroupCpuShare)
            .GreaterThanOrEqual(0.0)
            .Default(2.0 / 1024.0);

        RegisterParameter("block_io_watchdog_period", BlockIOWatchdogPeriod)
            .Default(TDuration::Seconds(60));
    }
};


DEFINE_REFCOUNTED_TYPE(TCGroupJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlotLocationConfig
    : public TDiskLocationConfig
{ };

DEFINE_REFCOUNTED_TYPE(TSlotLocationConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlotManagerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Root path for slot directories.
    std::vector<TSlotLocationConfigPtr> Locations;

    //! Polymorphic job environment configuration.
    NYTree::INodePtr JobEnvironment;

    //! Fail node if slot manager didn't initialize properly.
    bool SlotInitializationFailureIsFatal;

    TSlotManagerConfig()
    {
        RegisterParameter("locations", Locations);

        RegisterParameter("job_environment", JobEnvironment)
            .Default(ConvertToNode(New<TSimpleJobEnvironmentConfig>()));

        RegisterParameter("slot_initialization_failure_is_fatal", SlotInitializationFailureIsFatal)
            .Default(false);
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
    TSchedulerConnectorConfigPtr SchedulerConnector;

    NYTree::INodePtr JobProxyLogging;
    NYTree::INodePtr JobProxyTracing;

    TDuration SupervisorRpcTimeout;
    TDuration JobProberRpcTimeout;

    TDuration JobProxyHeartbeatPeriod;

    TExecAgentConfig()
    {
        RegisterParameter("slot_manager", SlotManager)
            .DefaultNew();
        RegisterParameter("job_controller", JobController)
            .DefaultNew();
        RegisterParameter("scheduler_connector", SchedulerConnector)
            .DefaultNew();

        RegisterParameter("job_proxy_logging", JobProxyLogging)
            .Default();
        RegisterParameter("job_proxy_tracing", JobProxyTracing)
            .Default();

        RegisterParameter("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("job_prober_rpc_timeout", JobProberRpcTimeout)
            .Default(TDuration::Seconds(300));

        RegisterParameter("job_proxy_heartbeat_period", JobProxyHeartbeatPeriod)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TExecAgentConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
