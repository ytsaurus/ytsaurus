#pragma once

#include <yt/yt/ytlib/query_tracker_client/proto/query_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NQueryTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TQueryTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TQueryTrackerServiceProxy, TQueryTrackerService,
        .SetProtocolVersion(NRpc::TProtocolVersion{0, 0}));

    DEFINE_RPC_PROXY_METHOD(NProto, StartQuery);
    DEFINE_RPC_PROXY_METHOD(NProto, AbortQuery);
    DEFINE_RPC_PROXY_METHOD(NProto, GetQueryResult);
    DEFINE_RPC_PROXY_METHOD(NProto, ReadQueryResult);
    DEFINE_RPC_PROXY_METHOD(NProto, GetQuery);
    DEFINE_RPC_PROXY_METHOD(NProto, ListQueries);
    DEFINE_RPC_PROXY_METHOD(NProto, AlterQuery);
    DEFINE_RPC_PROXY_METHOD(NProto, GetQueryTrackerInfo);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlClient
