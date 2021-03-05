#pragma once

#include "public.h"

#include <yt/yt/ytlib/cypress_client/proto/cypress_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NCypressClient {

////////////////////////////////////////////////////////////////////////////////

class TCypressServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TCypressServiceProxy, CypressService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, TouchNodes);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressClient
