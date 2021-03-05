#pragma once

#include <yt/yt/server/clickhouse_server/protos/clickhouse_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TClickHouseServiceProxy, ClickHouseService);

    DEFINE_RPC_PROXY_METHOD(NProto, ProcessGossip);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
