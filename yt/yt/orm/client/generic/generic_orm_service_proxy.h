#pragma once

#include "public.h"

#include <yt/yt_proto/yt/orm/client/proto/generic_object_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NOrm::NClient::NGeneric {

////////////////////////////////////////////////////////////////////////////////

class TGenericServiceProxy
    : public NRpc::TProxyBase
{
public:
    TGenericServiceProxy(NRpc::IChannelPtr channel, const NRpc::TServiceDescriptor& descriptor);

    DEFINE_RPC_PROXY_METHOD(NProto, CreateObject);
    DEFINE_RPC_PROXY_METHOD(NProto, RemoveObject);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdateObject);
    DEFINE_RPC_PROXY_METHOD(NProto, GetObjects);
    DEFINE_RPC_PROXY_METHOD(NProto, SelectObjects);
};

////////////////////////////////////////////////////////////////////////////////

} // NYT::NOrm::NClient::NGeneric
