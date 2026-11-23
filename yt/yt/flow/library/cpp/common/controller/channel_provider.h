#pragma once

#include <yt/yt/core/rpc/roaming_channel.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <functional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TControllerChannelProvider
    : public NRpc::IRoamingChannelProvider
{
public:
    TControllerChannelProvider() = default;
    explicit TControllerChannelProvider(std::function<NRpc::IChannelPtr()> getChannel);

    void SetChannel(NRpc::IChannelPtr channel);

private:
    const std::function<NRpc::IChannelPtr()> GetChannelCallback_;
    TAtomicIntrusivePtr<NRpc::IChannel> Channel_;

    const std::string& GetEndpointDescription() const override;
    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;
    TFuture<NRpc::IChannelPtr> GetChannel() override;
    TFuture<NRpc::IChannelPtr> GetChannel(const NRpc::IClientRequestPtr& request) override;
    TFuture<NRpc::IChannelPtr> GetChannel(std::string serviceName) override;
    void Terminate(const TError& error) override;
};

DEFINE_REFCOUNTED_TYPE(TControllerChannelProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
