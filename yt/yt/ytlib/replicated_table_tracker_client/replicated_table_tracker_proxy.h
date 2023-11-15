#pragma once

#include <yt/yt/ytlib/replicated_table_tracker_client/proto/replicated_table_tracker_client.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NReplicatedTableTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TReplicatedTableTrackerServiceProxy, ReplicatedTableTrackerService);

    DEFINE_RPC_PROXY_METHOD(NProto, GetTrackerStateUpdates);
    DEFINE_RPC_PROXY_METHOD(NProto, ApplyChangeReplicaModeCommands);
    DEFINE_RPC_PROXY_METHOD(NProto, ComputeReplicaLagTimes);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTrackerClient
