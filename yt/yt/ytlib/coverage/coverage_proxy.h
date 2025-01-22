#pragma once

#include <yt/yt/ytlib/coverage/proto/coverage.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NCoverage {

////////////////////////////////////////////////////////////////////////////////

class TCoverageProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TCoverageProxy, Coverage);

    DEFINE_RPC_PROXY_METHOD(NProto, Collect);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoverage
