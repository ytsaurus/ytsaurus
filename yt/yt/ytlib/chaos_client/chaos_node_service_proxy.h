#pragma once

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TChaosServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TChaosServiceProxy, ChaosService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, CreateReplicationCard);
    DEFINE_RPC_PROXY_METHOD(NProto, GetReplicationCard);
    DEFINE_RPC_PROXY_METHOD(NProto, CreateTableReplica);
    DEFINE_RPC_PROXY_METHOD(NProto, RemoveTableReplica);
    DEFINE_RPC_PROXY_METHOD(NProto, AlterTableReplica);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdateReplicationProgress);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

