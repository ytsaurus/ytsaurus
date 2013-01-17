#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration of a single environment.
struct TEnvironmentConfig
    : public TYsonSerializable
{
    Stroka Type;

    // Type-dependent configuration is stored as options.

    TEnvironmentConfig()
    {
        SetKeepOptions(true);
        Register("type", Type)
            .NonEmpty();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration for a collection of named environments.
struct TEnvironmentManagerConfig
    : public TYsonSerializable
{
public:
    TEnvironmentManagerConfig()
    {
        Register("environments", Environments);
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

struct TResourceLimitsConfig
    : public TYsonSerializable
{
    int Slots;
    int Cpu;
    int Network;

    TResourceLimitsConfig()
    {
        // These are some very low default limits.
        // Override for production use.
        Register("slots", Slots)
            .Default(2);
        Register("cpu", Cpu)
            .Default(2);
        Register("network", Network)
            .Default(100);
    }
};

struct TJobManagerConfig
    : public TYsonSerializable
{
    TResourceLimitsConfigPtr ResourceLimits;
    Stroka SlotLocation;

    // User IDs from StartUid to StartUid + SlotLocation are used
    // to run user jobs if job control is enabled.
    int StartUserId;

    TJobManagerConfig()
    {
        Register("resource_limits", ResourceLimits)
            .DefaultNew();
        Register("slot_location", SlotLocation)
            .NonEmpty();
        Register("start_user_id", StartUserId)
            .Default(10000);
    }
};

struct TSchedulerConnectorConfig
    : public TYsonSerializable
{
    //! Timeout for RPC requests to scheduler.
    TDuration RpcTimeout;

    //! Period between consequent heartbeats.
    TDuration HeartbeatPeriod;

    //! Random delay before first heartbeat.
    TDuration HeartbeatSplay;

    TSchedulerConnectorConfig()
    {
        Register("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(60));
        Register("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        Register("heartbeat_splay", HeartbeatSplay)
            .Default(TDuration::Seconds(1));
    }
};

struct TExecAgentConfig
    : public TYsonSerializable
{
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

    TExecAgentConfig()
    {
        Register("job_manager", JobManager)
            .DefaultNew();
        Register("environment_manager", EnvironmentManager)
            .DefaultNew();
        Register("scheduler_connector", SchedulerConnector)
            .DefaultNew();
        Register("job_proxy_logging", JobProxyLogging)
            .Default(NULL);
        Register("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(60));
        Register("memory_watchdog_period", MemoryWatchdogPeriod)
            .Default(TDuration::Seconds(1));
        Register("enforce_job_control", EnforceJobControl)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
