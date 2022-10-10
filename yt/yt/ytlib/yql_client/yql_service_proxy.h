#pragma once

#include <yt/yt/ytlib/yql_client/proto/yql_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NYqlClient {

////////////////////////////////////////////////////////////////////////////////

class TYqlServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TYqlServiceProxy, TYqlService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NProto, StartQuery);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlClient
