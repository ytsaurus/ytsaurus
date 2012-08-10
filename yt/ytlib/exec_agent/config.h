#pragma once

#include "public.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/ytree/yson_serializable.h>

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
            ythrow yexception() << Sprintf("No such environment %s", ~name);
        }
        return it->second;
    }

    std::unordered_map<Stroka, TEnvironmentConfigPtr> Environments;

};

struct TResourceLimitsConfig
    : public TYsonSerializable
{
    int Slots;
    int Cores;
    i64 Memory;
    int Network;

    TResourceLimitsConfig()
    {
        // These are some very low default limits.
        // Override for production use.
        Register("slots", Slots)
            .Default(2);
        Register("cores", Cores)
            .Default(2);
        Register("memory", Memory)
            .Default((i64) 4 * 1024 * 1024 * 1024);
        Register("network", Network)
            .Default(100);
    }
};

typedef TIntrusivePtr<TResourceLimitsConfig> TResourceLimitsConfigPtr;

struct TJobManagerConfig
    : public TYsonSerializable
{
    TResourceLimitsConfigPtr ResourceLimits;
    Stroka SlotLocation;

    TJobManagerConfig()
    {
        Register("resource_limits", ResourceLimits)
            .DefaultNew();
        Register("slot_location", SlotLocation)
            .NonEmpty();
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
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
