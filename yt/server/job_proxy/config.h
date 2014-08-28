#pragma once

#include "public.h"

#include <ytlib/table_client/config.h>
#include <ytlib/file_client/config.h>
#include <ytlib/meta_state/config.h>
#include <core/ytree/node.h>
#include <core/ytree/yson_serializable.h>
#include <core/bus/config.h>
#include <ytlib/scheduler/config.h>
#include <core/misc/address.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxyConfig
    : public NYTree::TYsonSerializable
{
public:
    // Filled by exec agent.
    NBus::TTcpBusClientConfigPtr SupervisorConnection;
    TDuration SupervisorRpcTimeout;

    Stroka SandboxName;

    TDuration HeartbeatPeriod;

    TDuration MemoryWatchdogPeriod;
    
    TAddressResolverConfigPtr AddressResolver;

    double MemoryLimitMultiplier;

    bool ForceEnableAccounting;

    int UserId;

    NScheduler::TJobIOConfigPtr JobIO;

    NYTree::INodePtr Logging;

    TJobProxyConfig()
    {
        RegisterParameter("supervisor_connection", SupervisorConnection);
        RegisterParameter("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(30));

        RegisterParameter("sandbox_name", SandboxName)
            .NonEmpty();
        
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        
        RegisterParameter("memory_watchdog_period", MemoryWatchdogPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("memory_limit_multiplier", MemoryLimitMultiplier)
            .Default(2.0);
        RegisterParameter("force_enable_accounting", ForceEnableAccounting)
            .Default(false);
        
        RegisterParameter("user_id", UserId).
            Default(-1);
        
        RegisterParameter("job_io", JobIO)
            .DefaultNew();
        
        RegisterParameter("logging", Logging)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
