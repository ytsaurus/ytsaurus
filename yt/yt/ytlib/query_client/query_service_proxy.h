#pragma once

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/library/query/proto/query_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TQueryServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TQueryServiceProxy, QueryService,
        .SetProtocolVersion(33));

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);
    DEFINE_RPC_PROXY_METHOD(NProto, Multiread);
    DEFINE_RPC_PROXY_METHOD(NProto, PullRows);
    DEFINE_RPC_PROXY_METHOD(NProto, GetTabletInfo);
    DEFINE_RPC_PROXY_METHOD(NProto, ReadDynamicStore,
        .SetStreamingEnabled(true));
    DEFINE_RPC_PROXY_METHOD(NProto, FetchTabletStores);
    DEFINE_RPC_PROXY_METHOD(NProto, FetchTableRows);
    DEFINE_RPC_PROXY_METHOD(NProto, GetOrderedTabletSafeTrimRowCount);

    DEFINE_RPC_PROXY_METHOD(NProto, CreateDistributedSession);
    DEFINE_RPC_PROXY_METHOD(NProto, PingDistributedSession);
    DEFINE_RPC_PROXY_METHOD(NProto, CloseDistributedSession);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
