#pragma once

#include "public.h"

#include <yt/core/rpc/public.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServer
    : public TRefCounted
{
public:
    TDiscoveryServer(
        NRpc::IServerPtr rpcServer,
        TString selfAddress,
        TDiscoveryServerConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory,
        IInvokerPtr serverInvoker,
        IInvokerPtr gossipInvoker);
    ~TDiscoveryServer();

    void Initialize();
    void Finalize();

    NYTree::IYPathServicePtr GetYPathService();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
