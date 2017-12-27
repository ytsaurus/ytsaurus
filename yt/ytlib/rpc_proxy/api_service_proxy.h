#pragma once

#include "public.h"

#include <yt/ytlib/rpc_proxy/proto/api_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TApiServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TApiServiceProxy, ApiService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, GenerateTimestamps);

    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, StartTransaction);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, PingTransaction);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, CommitTransaction);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, AbortTransaction);

    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, ExistsNode);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, GetNode);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, SetNode);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, RemoveNode);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, ListNode);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, CreateNode);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, LockNode);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, CopyNode);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, MoveNode);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, LinkNode);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, ConcatenateNodes);

    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, MountTable);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, UnmountTable);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, RemountTable);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, FreezeTable);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, UnfreezeTable);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, ReshardTable);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, TrimTable);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, AlterTable);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, AlterTableReplica);

    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, LookupRows);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, VersionedLookupRows);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, SelectRows);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, GetInSyncReplicas);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, GetTabletInfos);

    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, ModifyRows);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
