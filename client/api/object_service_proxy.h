#pragma once

#include <yp/client/api/proto/object_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYP::NClient::NApi {

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceProxy
    : public NYT::NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TObjectServiceProxy, ObjectService,
        .SetNamespace("NYP.NClient.NApi.NProto"));

    DEFINE_RPC_PROXY_METHOD(NProto, GenerateTimestamp);
    DEFINE_RPC_PROXY_METHOD(NProto, StartTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, CommitTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, AbortTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, CreateObject);
    DEFINE_RPC_PROXY_METHOD(NProto, RemoveObject);
    DEFINE_RPC_PROXY_METHOD(NProto, RemoveObjects);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdateObject);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdateObjects);
    DEFINE_RPC_PROXY_METHOD(NProto, GetObject);
    DEFINE_RPC_PROXY_METHOD(NProto, SelectObjects);
    DEFINE_RPC_PROXY_METHOD(NProto, CheckObjectPermissions);
    DEFINE_RPC_PROXY_METHOD(NProto, GetObjectAccessAllowedFor);
    DEFINE_RPC_PROXY_METHOD(NProto, GetUserAccessAllowedTo);
    DEFINE_RPC_PROXY_METHOD(NProto, WatchObjects);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi
