#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <ytlib/cgroup/config.h>

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
    std::vector<Stroka> Paths;

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

    std::vector<Stroka> SupportedCGroups;

    //! Thread pool for job startup initialization.
    int PoolSize;

    TSlotManagerConfig()
    {
        RegisterParameter("paths", Paths)
            .NonEmpty();
        RegisterParameter("enforce_job_control", EnforceJobControl)
            .Default(false);
        RegisterParameter("start_uid", StartUid)
            .Default(10000);
        RegisterParameter("enable_cgroups", EnableCGroups)
            .Default(true);
        RegisterParameter("supported_cgroups", SupportedCGroups)
            .Default();
        RegisterParameter("pool_size", PoolSize)
            .GreaterThanOrEqual(1)
            .Default(3);
    }
};

class TSchedulerConnectorConfig
    : public NYTree::TYsonSerializable
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
    : public NCGroup::TCGroupConfig
{
public:
    TSlotManagerConfigPtr SlotManager;
    NJobAgent::TJobControllerConfigPtr JobController;
    TEnvironmentManagerConfigPtr EnvironmentManager;
    TSchedulerConnectorConfigPtr SchedulerConnector;

    NYTree::INodePtr JobProxyLogging;
    NYTree::INodePtr JobProxyTracing;

    TDuration SupervisorRpcTimeout;
    TDuration JobProberRpcTimeout;
    TDuration MemoryWatchdogPeriod;
    TDuration BlockIOWatchdogPeriod;

    bool EnableIopsThrottling;

    double MemoryLimitMultiplier;

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
        RegisterParameter("job_prober_rpc_timeout", JobProberRpcTimeout)
            .Default(TDuration::Seconds(300));
        RegisterParameter("memory_watchdog_period", MemoryWatchdogPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("block_io_watchdog_period", BlockIOWatchdogPeriod)
            .Default(TDuration::Seconds(60));

        RegisterParameter("memory_limit_multiplier", MemoryLimitMultiplier)
            .Default(2.0);

        RegisterParameter("enable_iops_throttling", EnableIopsThrottling)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
