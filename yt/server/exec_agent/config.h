#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration of a single environment.
class TEnvironmentConfig
    : public TYsonSerializable
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
    : public TYsonSerializable
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
            THROW_ERROR_EXCEPTION("No such environment %s", ~name);
        }
        return it->second;
    }

    yhash_map<Stroka, TEnvironmentConfigPtr> Environments;

};

class TResourceLimitsConfig
    : public TYsonSerializable
{
public:
    int Slots;
    int Cpu;
    int Network;

    TResourceLimitsConfig()
    {
        // These are some very low default limits.
        // Override for production use.
        RegisterParameter("slots", Slots)
            .Default(2);
        RegisterParameter("cpu", Cpu)
            .Default(2);
        RegisterParameter("network", Network)
            .Default(100);
    }
};

class TJobManagerConfig
    : public TYsonSerializable
{
public:
    TResourceLimitsConfigPtr ResourceLimits;
    Stroka SlotLocation;

    // User IDs from StartUid to StartUid + SlotLocation are used
    // to run user jobs if job control is enabled.
    int StartUserId;

    TJobManagerConfig()
    {
        RegisterParameter("resource_limits", ResourceLimits)
            .DefaultNew();
        RegisterParameter("slot_location", SlotLocation)
            .NonEmpty();
        RegisterParameter("start_user_id", StartUserId)
            .Default(10000);
    }
};

class TSchedulerConnectorConfig
    : public TYsonSerializable
{
public:
    //! Timeout for RPC requests to scheduler.
    TDuration RpcTimeout;

    //! Period between consequent heartbeats.
    TDuration HeartbeatPeriod;

    //! Random delay before first heartbeat.
    TDuration HeartbeatSplay;

    TSchedulerConnectorConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(60));
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("heartbeat_splay", HeartbeatSplay)
            .Default(TDuration::Seconds(1));
    }
};

class TExecAgentConfig
    : public TYsonSerializable
{
public:
    TJobManagerConfigPtr JobManager;
    TEnvironmentManagerConfigPtr EnvironmentManager;
    TSchedulerConnectorConfigPtr SchedulerConnector;

    NYTree::INodePtr JobProxyLogging;
    TDuration SupervisorRpcTimeout;
    TDuration MemoryWatchdogPeriod;

    // When set, exec agent doesn't start if it doesn't have
    // root privileges which allow calling setuid and enforcing
    // pseudouser based restrictions on job control.
    bool EnforceJobControl;

    double MemoryLimitMultiplier;

    TExecAgentConfig()
    {
        RegisterParameter("job_manager", JobManager)
            .DefaultNew();
        RegisterParameter("environment_manager", EnvironmentManager)
            .DefaultNew();
        RegisterParameter("scheduler_connector", SchedulerConnector)
            .DefaultNew();
        RegisterParameter("job_proxy_logging", JobProxyLogging)
            .Default(NULL);
        RegisterParameter("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(60));
        RegisterParameter("memory_watchdog_period", MemoryWatchdogPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("enforce_job_control", EnforceJobControl)
            .Default(false);
        RegisterParameter("memory_limit_multiplier", MemoryLimitMultiplier)
            .Default(2.0);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
