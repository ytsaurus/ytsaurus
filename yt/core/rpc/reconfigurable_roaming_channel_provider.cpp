#include "reconfigurable_roaming_channel_provider.h"
#include "channel.h"

#include <yt/core/misc/atomic_object.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

namespace NYT::NRpc {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TReconfigurableRoamingChannelProvider
    : public IReconfigurableRoamingChannelProvider
{
public:
     TReconfigurableRoamingChannelProvider(
        IChannelPtr underlyingChannel,
        const TString& endpointDescription,
        const IAttributeDictionary& endpointAttributes,
        const TNetworkId& networkId)
        : EndpointDescription_(endpointDescription)
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Items(endpointAttributes)
            .EndMap()))
        , NetworkId_(networkId)
        , UnderlyingChannel_(std::move(underlyingChannel))
    { }

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual TNetworkId GetNetworkId() const override
    {
        return NetworkId_;
    }

    virtual TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /* request */) override
    {
        return MakeFuture(UnderlyingChannel_.Load());
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return UnderlyingChannel_.Load()->Terminate(error);
    }

    virtual void SetUnderlyingChannel(IChannelPtr channel) override
    {
        YT_VERIFY(channel);
        UnderlyingChannel_.Store(std::move(channel));
    }

private:
    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;
    const TNetworkId NetworkId_;

    TAtomicObject<IChannelPtr> UnderlyingChannel_;
};

IReconfigurableRoamingChannelProviderPtr CreateReconfigurableRoamingChannelProvider(
    IChannelPtr underlyingChannel,
    const TString& endpointDescription,
    const IAttributeDictionary& endpointAttributes,
    const TNetworkId& networkId)
{
    YT_VERIFY(underlyingChannel);
    return New<TReconfigurableRoamingChannelProvider>(
        underlyingChannel,
        endpointDescription,
        endpointAttributes,
        networkId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
