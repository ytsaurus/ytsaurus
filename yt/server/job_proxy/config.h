#pragma once

#include "public.h"

#include <core/misc/address.h>

#include <core/ytree/node.h>
#include <core/ytree/yson_serializable.h>

#include <core/bus/config.h>

#include <ytlib/cgroup/config.h>

#include <ytlib/file_client/config.h>

#include <ytlib/hydra/config.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxyConfig
    : public NCGroup::TCGroupConfig
{
public:
    // Filled by exec agent.
    NApi::TConnectionConfigPtr ClusterConnection;

    NBus::TTcpBusServerConfigPtr RpcServer;
    NBus::TTcpBusClientConfigPtr SupervisorConnection;
    TDuration SupervisorRpcTimeout;

    TDuration HeartbeatPeriod;

    TDuration MemoryWatchdogPeriod;

    TDuration BlockIOWatchdogPeriod;

    TAddressResolverConfigPtr AddressResolver;

    double MemoryLimitMultiplier;

    TNullable<int> UserId;

    bool EnableIopsThrottling;

    NScheduler::TJobIOConfigPtr JobIO;

    NYTree::INodePtr Logging;
    NYTree::INodePtr Tracing;

    TJobProxyConfig()
    {
        RegisterParameter("rpc_server", RpcServer)
            .DefaultNew();
        RegisterParameter("cluster_connection", ClusterConnection);
        RegisterParameter("supervisor_connection", SupervisorConnection);
        RegisterParameter("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(30));

        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("memory_watchdog_period", MemoryWatchdogPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("block_io_watchdog_period", BlockIOWatchdogPeriod)
            .Default(TDuration::Seconds(60));

        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("memory_limit_multiplier", MemoryLimitMultiplier)
            .Default(2.0);

        RegisterParameter("user_id", UserId)
            .Default();

        RegisterParameter("enable_iops_throttling", EnableIopsThrottling)
            .Default(false);

        RegisterParameter("job_io", JobIO)
            .DefaultNew();

        RegisterParameter("logging", Logging)
            .Default();
        RegisterParameter("tracing", Tracing)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TJobProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
