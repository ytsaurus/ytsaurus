#include "scheduler_channel.h"
#include "config.h"

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NScheduler {

using namespace NBus;
using namespace NRpc;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TSchedulerChannelProvider
    : public IRoamingChannelProvider
{
public:
    TSchedulerChannelProvider(
        TSchedulerConnectionConfigPtr config,
        IChannelFactoryPtr channelFactory,
        IChannelPtr masterChannel,
        const TNetworkPreferenceList& networks)
        : Config_(std::move(config))
        , ChannelFactory_(std::move(channelFactory))
        , MasterChannel_(std::move(masterChannel))
        , Networks_(networks)
        , EndpointDescription_(Format("Scheduler@%v",
            MasterChannel_->GetEndpointDescription()))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("scheduler").Value(true)
                .Items(MasterChannel_->GetEndpointAttributes())
            .EndMap()))
    { }

    const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
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

        // TODO(gritukan): Pass Cypress Proxy here?
        auto proxy = TObjectServiceProxy::FromDirectMasterChannel(MasterChannel_);
        auto batchReq = proxy.ExecuteBatch();
        batchReq->AddRequest(TYPathProxy::Get("//sys/scheduler/@addresses"));
        return batchReq->Invoke()
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) -> IChannelPtr {
                auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(0);
                if (rsp.FindMatching(NYT::NYTree::EErrorCode::ResolveError)) {
                    THROW_ERROR_EXCEPTION("No scheduler is configured");
                }

                THROW_ERROR_EXCEPTION_IF_FAILED(rsp, "Cannot determine scheduler address");

                auto addresses = ConvertTo<TAddressMap>(TYsonString(rsp.Value()->value()));

                auto channel = ChannelFactory_->CreateChannel(GetAddressOrThrow(addresses, Networks_));
                channel = CreateFailureDetectingChannel(
                    channel,
                    Config_->RpcAcknowledgementTimeout,
                    BIND(&TSchedulerChannelProvider::OnChannelFailed, MakeWeak(this)));

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

    TFuture<IChannelPtr> GetChannel(const TString& /*serviceName*/) override
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
    const TSchedulerConnectionConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const IChannelPtr MasterChannel_;
    const TNetworkPreferenceList Networks_;

    const TString EndpointDescription_;
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

IChannelPtr CreateSchedulerChannel(
    TSchedulerConnectionConfigPtr config,
    IChannelFactoryPtr channelFactory,
    IChannelPtr masterChannel,
    const TNetworkPreferenceList& networks)
{
    YT_VERIFY(config);
    YT_VERIFY(channelFactory);
    YT_VERIFY(masterChannel);

    auto channelProvider = New<TSchedulerChannelProvider>(config, channelFactory, masterChannel, networks);
    auto channel = CreateRoamingChannel(channelProvider);

    channel = CreateRetryingChannel(config, channel);

    channel = CreateDefaultTimeoutChannel(channel, config->RpcTimeout);

    return channel;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
