#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/client/election/public.h>

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

class TPeerChannel
    : public NRpc::IChannel
{
public:
    const TString& GetEndpointDescription() const override;

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;

    NRpc::IClientRequestControlPtr Send(
        NRpc::IClientRequestPtr request,
        NRpc::IClientResponseHandlerPtr responseHandler,
        const NRpc::TSendOptions& options) override;

    void Terminate(const TError& error) override;

    void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override;

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override;

    void SetUnderlying(NRpc::IChannelPtr underlying);

    void SetBroken(bool broken);

    int GetInflightRequestCount() override;

    const IMemoryUsageTrackerPtr& GetChannelMemoryTracker() override;

private:
    NRpc::IChannelPtr Underlying_;
    std::atomic<bool> Broken_ = false;
};

DEFINE_REFCOUNTED_TYPE(TPeerChannel)

//////////////////////////////////////////////////////////////////////////////////

class TChannelManager
    : public TRefCounted
{
public:
    explicit TChannelManager(int peerCount);

    NRpc::TStaticChannelFactoryPtr GetChannelFactory(int peerId);

    void SetUnderlying(NRpc::IChannelPtr channel, int peerId);

    void SetBroken(int from, int to, bool broken);

private:
    std::vector<std::vector<TPeerChannelPtr>> Channels_;
    std::vector<NRpc::TStaticChannelFactoryPtr> ChannelFactories_;
};

DEFINE_REFCOUNTED_TYPE(TChannelManager)

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
