#pragma once

#include "public.h"

#include <yt/server/exec_agent/config.h>

#include <yt/server/misc/config.h>

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
    : public TServerConfig
{
public:
    // Job-specific parameters.
    int SlotIndex;

    TNullable<TString> TmpfsPath;

    // Job-independent parameters.
    NApi::TNativeConnectionConfigPtr ClusterConnection;

    NBus::TTcpBusClientConfigPtr SupervisorConnection;
    TDuration SupervisorRpcTimeout;

    TDuration HeartbeatPeriod;
    TDuration InputPipeBlinkerPeriod;

    NYTree::INodePtr JobEnvironment;

    //! Addresses derived from node local descriptor to leverage locality.
    NNodeTrackerClient::TAddressMap Addresses;
    TNullable<TString> Rack;
    TNullable<TString> DataCenter;

    TDuration CoreForwarderTimeout;

    i64 AheadMemoryReserve;

    TJobProxyConfig()
    {
        RegisterParameter("slot_index", SlotIndex);

        RegisterParameter("tmpfs_path", TmpfsPath)
            .Default();

        RegisterParameter("cluster_connection", ClusterConnection);

        RegisterParameter("supervisor_connection", SupervisorConnection);

        RegisterParameter("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(30));

        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("input_pipe_blinker_period", InputPipeBlinkerPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("job_environment", JobEnvironment);

        RegisterParameter("addresses", Addresses)
            .Default();

        RegisterParameter("rack", Rack)
            .Default(Null);

        RegisterParameter("data_center", DataCenter)
            .Default(Null);

        RegisterParameter("core_forwarder_timeout", CoreForwarderTimeout)
            .Default();

        RegisterParameter("ahead_memory_reserve", AheadMemoryReserve)
            .Default(200 * MB);
    }
};

DEFINE_REFCOUNTED_TYPE(TJobProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
