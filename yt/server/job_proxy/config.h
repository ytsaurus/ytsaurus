#pragma once

#include "public.h"

#include <yt/server/exec_agent/config.h>

#include <yt/ytlib/cgroup/config.h>

#include <yt/ytlib/file_client/config.h>

#include <yt/ytlib/hydra/config.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/core/bus/config.h>

#include <yt/core/misc/address.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxyConfig
    : public NYTree::TYsonSerializable
{
public:
    // Job-specific parameters.
    NBus::TTcpBusServerConfigPtr RpcServer;

    int SlotIndex;

    TNullable<Stroka> TmpfsPath;

    NScheduler::TJobIOConfigPtr JobIO;

    // Job-independent parameters.
    NApi::TConnectionConfigPtr ClusterConnection;

    NBus::TTcpBusClientConfigPtr SupervisorConnection;
    TDuration SupervisorRpcTimeout;

    TAddressResolverConfigPtr AddressResolver;

    TDuration HeartbeatPeriod;

    NYTree::INodePtr JobEnvironment;

    //! Addresses derived from node local descriptor to leverage locality.
    NNodeTrackerClient::TAddressMap Addresses;
    TNullable<Stroka> Rack;

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

        RegisterParameter("job_environment", JobEnvironment);

        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();

        RegisterParameter("slot_index", SlotIndex);

        RegisterParameter("tmpfs_path", TmpfsPath)
            .Default();

        RegisterParameter("job_io", JobIO)
            .DefaultNew();

        RegisterParameter("addresses", Addresses)
            .Default();

        RegisterParameter("rack", Rack)
            .Default(Null);

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
