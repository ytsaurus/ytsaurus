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
    DEFINE_RPC_PROXY(TQueryServiceProxy, RPC_PROXY_DESC(QueryService)
        .SetProtocolVersion(31));

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);
    DEFINE_RPC_PROXY_METHOD(NProto, Read);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
