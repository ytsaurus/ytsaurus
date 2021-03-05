#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TDynamicChannelPool
    : public TRefCounted
{
public:
    TDynamicChannelPool(
        TDynamicChannelPoolConfigPtr config,
        IChannelFactoryPtr channelFactory,
        TString endpointDescription,
        NYTree::IAttributeDictionaryPtr endpointAttributes,
        TString serviceName,
        TDiscoverRequestHook discoverRequestHook = {});
    ~TDynamicChannelPool();

    TFuture<IChannelPtr> GetRandomChannel();
    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request);

    void SetPeers(const std::vector<TString>& addresses);
    void SetPeerDiscoveryError(const TError& error);

    void Terminate(const TError& error);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TDynamicChannelPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
