#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <server/job_agent/config.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration of a single environment.
class TEnvironmentConfig
    : public NYTree::TYsonSerializable
{
public:
    Stroka Type;

    // Type-dependent configuration is stored as options.

    TEnvironmentConfig()
    {
        SetKeepOptions(true);
        RegisterParameter("type", Type)
            .NonEmpty();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration for a collection of named environments.
class TEnvironmentManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TEnvironmentManagerConfig()
    {
        RegisterParameter("environments", Environments);
    }

    TEnvironmentConfigPtr FindEnvironment(const Stroka& name)
    {
        auto it = Environments.find(name);
        if (it == Environments.end()) {
            THROW_ERROR_EXCEPTION("No such environment %Qv", name);
        }
        return it->second;
    }

    yhash_map<Stroka, TEnvironmentConfigPtr> Environments;

};

class TSlotManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Root path for slot directories.
    Stroka Path;

    //! When set to |true|, job proxies are run under per-slot pseudousers.
    //! This option requires node server process to have root privileges.
    bool EnforceJobControl;

    //! When job control is enabled, system runs user jobs under fake
    //! uids in range [StartUid, StartUid + SlotCount - 1].
    int StartUid;

    //! When set to |true| slot spawns job proxies into separate
    //! freezer cgroup. It uses this to kill all job proxies descendant
    //! This option requires node server process to have permission
    //! to create freezer subcgroups
    bool EnableCGroups;

    TSlotManagerConfig()
    {
        RegisterParameter("path", Path)
            .NonEmpty();
        RegisterParameter("enforce_job_control", EnforceJobControl)
            .Default(false);
        RegisterParameter("start_uid", StartUid)
            .Default(10000);
        RegisterParameter("enable_cgroups", EnableCGroups)
            .Default(true);
    }
};

class TSchedulerConnectorConfig
    : public NScheduler::TSchedulerConnectionConfig
{
public:
    //! Period between consequent heartbeats.
    TDuration HeartbeatPeriod;

    //! Random delay before first heartbeat.
    TDuration HeartbeatSplay;

    TSchedulerConnectorConfig()
    {
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("heartbeat_splay", HeartbeatSplay)
            .Default(TDuration::Seconds(1));
    }
};

class TExecAgentConfig
    : public NYTree::TYsonSerializable
{
public:
    TSlotManagerConfigPtr SlotManager;
    NJobAgent::TJobControllerConfigPtr JobController;
    TEnvironmentManagerConfigPtr EnvironmentManager;
    TSchedulerConnectorConfigPtr SchedulerConnector;

    NYTree::INodePtr JobProxyLogging;
    NYTree::INodePtr JobProxyTracing;

    TDuration SupervisorRpcTimeout;
    TDuration MemoryWatchdogPeriod;

    double MemoryLimitMultiplier;
    bool ForceEnableAccounting;
    bool EnableCGroupMemoryHierarchy;

    TExecAgentConfig()
    {
        RegisterParameter("slot_manager", SlotManager)
            .DefaultNew();
        RegisterParameter("job_controller", JobController)
            .DefaultNew();
        RegisterParameter("environment_manager", EnvironmentManager)
            .DefaultNew();
        RegisterParameter("scheduler_connector", SchedulerConnector)
            .DefaultNew();

        RegisterParameter("job_proxy_logging", JobProxyLogging)
            .Default();
        RegisterParameter("job_proxy_tracing", JobProxyTracing)
            .Default();

        RegisterParameter("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("memory_watchdog_period", MemoryWatchdogPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("memory_limit_multiplier", MemoryLimitMultiplier)
            .Default(2.0);
        RegisterParameter("force_enable_accounting", ForceEnableAccounting)
            .Default(false);
        RegisterParameter("enable_cgroup_memory_hierarchy", EnableCGroupMemoryHierarchy)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
