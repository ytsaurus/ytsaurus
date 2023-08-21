#pragma once

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TChaosNodeServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TChaosNodeServiceProxy, ChaosNodeService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, GenerateReplicationCardId);
    DEFINE_RPC_PROXY_METHOD(NProto, CreateReplicationCard);
    DEFINE_RPC_PROXY_METHOD(NProto, RemoveReplicationCard);
    DEFINE_RPC_PROXY_METHOD(NProto, GetReplicationCard);
    DEFINE_RPC_PROXY_METHOD(NProto, FindReplicationCard);
    DEFINE_RPC_PROXY_METHOD(NProto, AlterReplicationCard);
    DEFINE_RPC_PROXY_METHOD(NProto, CreateTableReplica);
    DEFINE_RPC_PROXY_METHOD(NProto, RemoveTableReplica);
    DEFINE_RPC_PROXY_METHOD(NProto, AlterTableReplica);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdateTableReplicaProgress);
    DEFINE_RPC_PROXY_METHOD(NProto, MigrateReplicationCards);
    DEFINE_RPC_PROXY_METHOD(NProto, ResumeChaosCell);
    DEFINE_RPC_PROXY_METHOD(NProto, CreateReplicationCardCollocation);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

