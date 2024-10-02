// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include "public.h"

#include <yt/yt/orm/example/client/proto/api/discovery_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NOrm::NExample::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceProxy
    : public NYT::NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDiscoveryServiceProxy, DiscoveryService,
        .SetNamespace("NYT.NOrm.NExample.NClient.NProto"));

    DEFINE_RPC_PROXY_METHOD(NYT::NOrm::NExample::NClient::NProto, GetMasters);
};

////////////////////////////////////////////////////////////////////////////////

} // NYT::NOrm::NExample::NClient::NNative
