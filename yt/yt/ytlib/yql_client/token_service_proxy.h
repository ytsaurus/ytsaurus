#pragma once

#include <yt/yt/ytlib/yql_client/proto/token_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NYqlClient {

////////////////////////////////////////////////////////////////////////////////

class TTokenServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(
        TTokenServiceProxy,
        TTokenService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NProto, IssueQueryTemporaryToken);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlClient
