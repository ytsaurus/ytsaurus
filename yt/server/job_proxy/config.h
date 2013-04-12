#pragma once

#include "public.h"

#include <ytlib/table_client/config.h>
#include <ytlib/file_client/config.h>
#include <ytlib/meta_state/config.h>
#include <ytlib/ytree/node.h>
#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/bus/config.h>
#include <ytlib/scheduler/config.h>
#include <ytlib/misc/address.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxyConfig
    : public TYsonSerializable
{
public:
    // Filled by exec agent.
    NBus::TTcpBusClientConfigPtr SupervisorConnection;
    TDuration SupervisorRpcTimeout;
    TDuration MasterRpcTimeout;

    Stroka SandboxName;

    TDuration HeartbeatPeriod;

    TDuration MemoryWatchdogPeriod;
    
    TAddressResolverConfigPtr AddressResolver;

    double MemoryLimitMultiplier;

    int UserId;

    NScheduler::TJobIOConfigPtr JobIO;

    NYTree::INodePtr Logging;

    TJobProxyConfig()
    {
        Register("supervisor_connection", SupervisorConnection);
        Register("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(30));
        Register("master_rpc_timeout", MasterRpcTimeout)
            .Default(TDuration::Seconds(30));

        Register("sandbox_name", SandboxName)
            .NonEmpty();
        
        Register("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        
        Register("memory_watchdog_period", MemoryWatchdogPeriod)
            .Default(TDuration::Seconds(1));
        Register("address_resolver", AddressResolver)
            .Default();
        Register("memory_limit_multiplier", MemoryLimitMultiplier)
            .Default(2.0);
        
        Register("user_id", UserId).
            Default(-1);
        
        Register("job_io", JobIO)
            .DefaultNew();
        
        Register("logging", Logging)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
