#pragma once

#include <yt/chyt/client/protos/query_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TQueryServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TQueryServiceProxy, QueryService);

    DEFINE_RPC_PROXY_METHOD(NProto, ExecuteQuery);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
