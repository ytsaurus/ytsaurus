#pragma once

#include "public.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration of a single environment.
struct TEnvironmentConfig
    : public TConfigurable
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
    : public TConfigurable
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

    yhash_map<Stroka, TEnvironmentConfigPtr> Environments;

};

struct TJobManagerConfig
    : public TConfigurable
{
    int  SlotCount;
    Stroka SlotLocation;

    TJobManagerConfig()
    {
        Register("slot_count", SlotCount)
            .Default(8);
        Register("slot_location", SlotLocation)
            .NonEmpty();
    }
};

struct TSchedulerConnectorConfig
    : public TConfigurable
{
    //! Period between consequent heartbeats.
    TDuration HeartbeatPeriod;

    //! Random delay before first heartbeat
    TDuration HeartbeatSplay;

    TSchedulerConnectorConfig()
    {
        Register("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        Register("heartbeat_splay", HeartbeatSplay)
            .Default(TDuration::Seconds(1));
    }
};

struct TExecAgentConfig
    : public TConfigurable
{
    TJobManagerConfigPtr JobManager;
    TEnvironmentManagerConfigPtr EnvironmentManager;
    TSchedulerConnectorConfigPtr SchedulerConnector;

    NYTree::INodePtr JobProxyLogging;
    TDuration SupervisorTimeout;

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
        Register("supervisor_timeout", SupervisorTimeout)
            .Default(TDuration::Seconds(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
