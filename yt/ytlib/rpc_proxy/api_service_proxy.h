#pragma once

#include "public.h"

#include "protocol_version.h"

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
        .SetProtocolVersion(NRpc::TProtocolVersion{1, 0}));

    // Transaction server
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, GenerateTimestamps);

    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, StartTransaction);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, PingTransaction);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, CommitTransaction);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, AbortTransaction);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, AttachTransaction);

    // Cypress server
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

    // Tablet server
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

    // File chaching
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, GetFileFromCache);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, PutFileToCache);

    // Object server
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, CreateObject);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, GetTableMountInfo);

    // IAdmin
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, BuildSnapshot);
    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, GCCollect);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
