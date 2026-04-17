#include "tablet_balancer_channel.h"

#include "config.h"

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/rpc/channel.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <library/cpp/yt/error/error.h>

namespace NYT::NTabletBalancerClient {

using namespace NRpc;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerChannelProvider
    : public IRoamingChannelProvider
{
public:
    TTabletBalancerChannelProvider(
        TTabletBalancerChannelConfigPtr config,
        IChannelFactoryPtr channelFactory,
        IChannelPtr masterChannel,
        const TNetworkPreferenceList& networks)
        : Config_(std::move(config))
        , ChannelFactory_(std::move(channelFactory))
        , MasterChannel_(std::move(masterChannel))
        , Networks_(networks)
        , EndpointDescription_(Format("TabletBalancer@%v",
            MasterChannel_->GetEndpointDescription()))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("tablet_balancer").Value(true)
                .Items(MasterChannel_->GetEndpointAttributes())
            .EndMap()))
    { }

    const std::string& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        {
            auto guard = Guard(SpinLock_);
            if (CachedChannel_) {
                return MakeFuture(CachedChannel_);
            }
        }

        return TObjectServiceProxy::FromDirectMasterChannel(MasterChannel_)
            .Execute(TYPathProxy::Get("//sys/tablet_balancer/@addresses"))
            .Apply(BIND([this, this_ = MakeStrong(this)] (const TYPathProxy::TErrorOrRspGetPtr& rspOrError) -> IChannelPtr
            {
                if (rspOrError.FindMatching(NYT::NYTree::EErrorCode::ResolveError)) {
                    THROW_ERROR_EXCEPTION("No tablet balancer is configured");
                }

                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Cannot determine tablet balancer address");

                auto addresses = ConvertTo<TAddressMap>(TYsonString(rspOrError.Value()->value()));

                auto channel = CreateFailureDetectingChannel(
                    ChannelFactory_->CreateChannel(GetAddressOrThrow(addresses, Networks_)),
                    Config_->RpcAcknowledgementTimeout,
                    BIND(&TTabletBalancerChannelProvider::OnChannelFailed, MakeWeak(this)));

                {
                    auto guard = Guard(SpinLock_);
                    CachedChannel_ = channel;
                }

                return channel;
            }));
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    void Terminate(const TError& error) override
    {
        auto guard = Guard(SpinLock_);
        if (CachedChannel_) {
            CachedChannel_->Terminate(error);
        }
    }

private:
    const TTabletBalancerChannelConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const IChannelPtr MasterChannel_;
    const TNetworkPreferenceList Networks_;

    const std::string EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    IChannelPtr CachedChannel_;

    void OnChannelFailed(const IChannelPtr& channel, const TError& /*error*/)
    {
        auto guard = Guard(SpinLock_);
        if (CachedChannel_ == channel) {
            CachedChannel_.Reset();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateTabletBalancerChannel(
    TTabletBalancerChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    IChannelPtr masterChannel,
    const TNetworkPreferenceList& networks)
{
    auto channelProvider = New<TTabletBalancerChannelProvider>(
        config,
        std::move(channelFactory),
        std::move(masterChannel),
        networks);

    return CreateDefaultTimeoutChannel(
        CreateRetryingChannel(
            config,
            CreateRoamingChannel(std::move(channelProvider))),
        config->RpcTimeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancerClient
