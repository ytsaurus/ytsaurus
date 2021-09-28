#pragma once

#include <yt/yt/ytlib/arbitrage_service/proto/arbitrage_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NArbitrageClient {

////////////////////////////////////////////////////////////////////////////////

class TArbitrageServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TArbitrageServiceProxy, ArbitrageService);

    DEFINE_RPC_PROXY_METHOD(NProto, GetStatus);
    DEFINE_RPC_PROXY_METHOD(NProto, SetTarget);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArbitrageClient
