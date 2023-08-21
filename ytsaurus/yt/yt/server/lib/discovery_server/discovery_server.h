#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

struct IDiscoveryServer
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;
    virtual void Finalize() = 0;

    virtual NYTree::IYPathServicePtr GetYPathService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDiscoveryServer)

IDiscoveryServerPtr CreateDiscoveryServer(
    NRpc::IServerPtr rpcServer,
    TString selfAddress,
    TDiscoveryServerConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr serverInvoker,
    IInvokerPtr gossipInvoker,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
