#pragma once

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>
#include <yt/yt/ytlib/chaos_client/proto/coordinator_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TCoordinatorServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TCoordinatorServiceProxy, CoordinatorService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, SuspendCoordinator);
    DEFINE_RPC_PROXY_METHOD(NProto, ResumeCoordinator);
    DEFINE_RPC_PROXY_METHOD(NProto, RegisterTransactionActions);
    DEFINE_RPC_PROXY_METHOD(NProto, GetReplicationCardEra);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

