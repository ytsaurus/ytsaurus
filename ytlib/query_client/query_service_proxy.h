#pragma once

#include "public.h"

#include <yt/ytlib/query_client/query_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TQueryServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TQueryServiceProxy, QueryService,
        .SetProtocolVersion(33));

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);
    DEFINE_RPC_PROXY_METHOD(NProto, Read);
    DEFINE_RPC_PROXY_METHOD(NProto, GetTabletInfo);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
