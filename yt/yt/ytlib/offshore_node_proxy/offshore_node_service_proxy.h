#pragma once

#include <yt/yt/core/rpc/client.h>

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

class TOffshoreNodeServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TOffshoreNodeServiceProxy, OffshoreNodeService,
        .SetProtocolVersion(0));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
