#include "connection.h"

#include "config.h"
#include "discovery_service_proxy.h"
#include "private.h"

#include <yt/core/rpc/channel.h>
#include <yt/core/rpc/grpc/channel.h>
#include <yt/core/rpc/retrying_channel.h>
#include <yt/core/rpc/roaming_channel.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/ytree/fluent.h>

#include <util/random/random.h>

namespace NYP::NClient::NApi::NNative {

using namespace NYT;

using namespace NYT::NConcurrency;
using namespace NYT::NRpc;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

template <class TRequestPtr>
void SetupRequestAuthentication(
    const TRequestPtr& request,
    const TAuthenticationConfigPtr& authentication)
{
    request->SetUser(authentication->User);
    auto* credentialsExtension = request->Header().MutableExtension(
        NYT::NRpc::NProto::TCredentialsExt::credentials_ext);
    credentialsExtension->set_token(authentication->Token);
}

////////////////////////////////////////////////////////////////////////////////

struct TMasterInfo
{
    TString Fqdn;
    TString GrpcAddress;
    TString HttpAddress;
    int InstanceTag;
    bool Alive;
    bool Leading;
};

struct TGetMastersResult
{
    std::vector<TMasterInfo> MasterInfos;
    int ClusterTag;
};

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClient
    : public TRefCounted
{
public:
    TDiscoveryClient(
        IChannelPtr discoveryChannel,
        TAuthenticationConfigPtr authentication,
        TDuration timeout)
        : DiscoveryChannel_(std::move(discoveryChannel))
        , Authentication_(std::move(authentication))
        , Timeout_(timeout)
    { }

    const TString& GetEndpointDescription() const
    {
        return DiscoveryChannel_->GetEndpointDescription();
    }

    const IAttributeDictionary& GetEndpointAttributes() const
    {
        return DiscoveryChannel_->GetEndpointAttributes();
    }

    TNetworkId GetNetworkId() const
    {
        return DiscoveryChannel_->GetNetworkId();
    }

    TFuture<TGetMastersResult> GetMasters()
    {
        TDiscoveryServiceProxy proxy(DiscoveryChannel_);
        auto req = proxy.GetMasters();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request (Method: %v)",
            req->GetMethod());

        return req->Invoke().Apply(BIND(&TDiscoveryClient::ParseGetMastersResult));
    }

private:
    const IChannelPtr DiscoveryChannel_;
    const TAuthenticationConfigPtr Authentication_;
    const TDuration Timeout_;


    template <class TRequestPtr>
    void SetupRequest(const TRequestPtr& request) const
    {
        SetupRequestAuthentication(request, Authentication_);
        request->SetTimeout(Timeout_);
    }


    static TGetMastersResult ParseGetMastersResult(
        const TErrorOr<TDiscoveryServiceProxy::TRspGetMastersPtr>& rspOrError)
    {
        const auto& rsp = rspOrError.ValueOrThrow();

        TGetMastersResult result;

        result.MasterInfos.reserve(rsp->master_infos_size());
        for (auto& protoMasterInfo : *rsp->mutable_master_infos()) {
            auto& masterInfo = result.MasterInfos.emplace_back();
            masterInfo.Fqdn = std::move(*protoMasterInfo.mutable_fqdn());
            masterInfo.GrpcAddress = std::move(*protoMasterInfo.mutable_grpc_address());
            masterInfo.HttpAddress = std::move(*protoMasterInfo.mutable_http_address());
            masterInfo.InstanceTag = protoMasterInfo.instance_tag();
            masterInfo.Alive = protoMasterInfo.alive();
            masterInfo.Leading = protoMasterInfo.leading();
        }

        result.ClusterTag = rsp->cluster_tag();

        return result;
    }
};

using TDiscoveryClientPtr = TIntrusivePtr<TDiscoveryClient>;

////////////////////////////////////////////////////////////////////////////////

class TChannelPool
    : public TRefCounted
{
public:
    explicit TChannelPool(IChannelFactoryPtr channelFactory)
        : ChannelFactory_(std::move(channelFactory))
    { }

    TErrorOr<IChannelPtr> GetRandom()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(ChannelSlotLock_);
        if (!ChannelSlotsOrError_.IsOK()) {
            return TError(ChannelSlotsOrError_);
        }
        const auto& channelSlots = ChannelSlotsOrError_.Value();
        if (channelSlots.empty()) {
            return TError(NYT::NRpc::EErrorCode::Unavailable, "There is no alive channel");
        }
        int index = RandomNumber(channelSlots.size());
        return channelSlots[index].Channel;
    }

    //! Reuses existing channels and terminates idle channels.
    void Setup(TErrorOr<THashSet<TString>> addressesOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (addressesOrError.IsOK()) {
            SetupAddresses(addressesOrError.Value());
        } else {
            SetupError(std::move(addressesOrError));
        }
    }

private:
    struct TChannelSlot
    {
        TString Address;
        IChannelPtr Channel;

        TChannelSlot() = default;

        TChannelSlot(TString address, IChannelPtr channel)
            : Address(std::move(address))
            , Channel(std::move(channel))
        { }
    };

    using TChannelSlotsContainer = std::vector<TChannelSlot>;

    const IChannelFactoryPtr ChannelFactory_;

    TReaderWriterSpinLock ChannelSlotLock_;
    TErrorOr<TChannelSlotsContainer> ChannelSlotsOrError_;

    TSpinLock ChannelSlotReleaseLock_;
    TChannelSlotsContainer ChannelSlotsToRelease_;


    void SetupAddresses(const THashSet<TString>& addresses)
    {
        YT_LOG_DEBUG("Setting up channel pool addresses (Addresses: %v)",
            addresses);

        TChannelSlotsContainer newChannelSlotsToRelease;
        THashSet<TString> newAddresses = addresses;
        {
            TWriterGuard guard(ChannelSlotLock_);

            if (ChannelSlotsOrError_.IsOK()) {
                auto& channelSlots = ChannelSlotsOrError_.Value();

                auto itBegin = std::partition(
                    channelSlots.begin(),
                    channelSlots.end(),
                    [&] (const TChannelSlot& channelSlot) {
                        return addresses.find(channelSlot.Address) != addresses.end();
                    });

                newChannelSlotsToRelease.reserve(std::distance(itBegin, channelSlots.end()));
                for (auto it = itBegin; it != channelSlots.end(); ++it) {
                    YT_LOG_DEBUG("Removing old channel from pool (Address: %v)",
                        it->Address);
                    newChannelSlotsToRelease.push_back(std::move(*it));
                }
                channelSlots.erase(itBegin, channelSlots.end());

                for (const auto& channelSlot : channelSlots) {
                    newAddresses.erase(channelSlot.Address);
                }
            }
        }

        TChannelSlotsContainer newChannelSlots;
        newChannelSlots.reserve(newAddresses.size());
        for (auto& address : newAddresses) {
            YT_LOG_DEBUG("Adding new channel to pool (Address: %v)",
                address);
            auto channel = ChannelFactory_->CreateChannel(address);
            newChannelSlots.emplace_back(std::move(address), std::move(channel));
        }

        if (!newChannelSlots.empty()) {
            TWriterGuard guard(ChannelSlotLock_);
            if (!ChannelSlotsOrError_.IsOK()) {
                ChannelSlotsOrError_ = TChannelSlotsContainer();
            }
            auto& channelSlots = ChannelSlotsOrError_.Value();
            channelSlots.insert(
                channelSlots.end(),
                std::make_move_iterator(newChannelSlots.begin()),
                std::make_move_iterator(newChannelSlots.end()));
        }

        ReleaseChannelSlots(std::move(newChannelSlotsToRelease));
    }

    void SetupError(TError error)
    {
        YT_LOG_DEBUG("Setting up channel pool error (Error: %v)",
            error);

        TChannelSlotsContainer newChannelSlotsToRelease;
        {
            TWriterGuard guard(ChannelSlotLock_);
            if (ChannelSlotsOrError_.IsOK()) {
                newChannelSlotsToRelease = std::move(ChannelSlotsOrError_).Value();
            }
            ChannelSlotsOrError_ = std::move(error);
        }

        ReleaseChannelSlots(std::move(newChannelSlotsToRelease));
    }

    void ReleaseChannelSlots(TChannelSlotsContainer newChannelSlotsToRelease)
    {
        auto guard = Guard(ChannelSlotReleaseLock_);

        ChannelSlotsToRelease_.insert(
            ChannelSlotsToRelease_.end(),
            std::make_move_iterator(newChannelSlotsToRelease.begin()),
            std::make_move_iterator(newChannelSlotsToRelease.end()));

        auto itBegin = std::partition(
            ChannelSlotsToRelease_.begin(),
            ChannelSlotsToRelease_.end(),
            [&] (const TChannelSlot& channelSlot) {
                return channelSlot.Channel->GetRefCount() > 1;
            });

        for (auto it = itBegin; it != ChannelSlotsToRelease_.end(); ++it) {
            YT_LOG_DEBUG("Terminating pool channel (Address: %v)",
                it->Address);
            it->Channel->Terminate(TError("Channel is not already in use"));
        }

        ChannelSlotsToRelease_.erase(
            itBegin,
            ChannelSlotsToRelease_.end());
    }
};

using TChannelPoolPtr = TIntrusivePtr<TChannelPool>;

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryChannelProvider
    : public IRoamingChannelProvider
{
public:
    TDiscoveryChannelProvider(
        TDiscoveryClientPtr discoveryClient,
        IChannelFactoryPtr channelFactory,
        TDuration discoveryPeriod)
        : DiscoveryClient_(std::move(discoveryClient))
        , ChannelFactory_(std::move(channelFactory))
        , DiscoveryPeriod_(discoveryPeriod)
        , EndpointDescription_(Format("Discovery(%v,%v)",
            DiscoveryClient_->GetEndpointDescription(),
            DiscoveryPeriod_))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("discovery_client").Value(DiscoveryClient_->GetEndpointAttributes())
                .Item("discovery_period").Value(DiscoveryPeriod_)
            .EndMap()))
        , ChannelPool_(New<TChannelPool>(ChannelFactory_))
        , DiscoveryExecutor_(New<TPeriodicExecutor>(
            DiscoveryQueue_->GetInvoker(),
            BIND(&TDiscoveryChannelProvider::Discover, MakeWeak(this)),
            DiscoveryPeriod_))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            DiscoveryQueue_->GetInvoker(),
            DiscoveryThread);

        DiscoveryExecutor_->Start();
    }

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
        return DiscoveryClient_->GetNetworkId();
    }

    virtual TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return MakeFuture(ChannelPool_->GetRandom());
    }

    virtual TFuture<void> Terminate(const TError& /*error*/) override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const TDiscoveryClientPtr DiscoveryClient_;
    const IChannelFactoryPtr ChannelFactory_;
    const TDuration DiscoveryPeriod_;

    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;

    const TChannelPoolPtr ChannelPool_;

    DECLARE_THREAD_AFFINITY_SLOT(DiscoveryThread);
    const TActionQueuePtr DiscoveryQueue_ = New<TActionQueue>("YPApiClientDiscovery");
    const TPeriodicExecutorPtr DiscoveryExecutor_;


    void Discover()
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            DiscoveryQueue_->GetInvoker(),
            DiscoveryThread);

        try {
            auto getMastersResultOrError = WaitFor(DiscoveryClient_->GetMasters());

            if (getMastersResultOrError.IsOK()) {
                const auto& getMastersResult = getMastersResultOrError.Value();

                THashSet<TString> addresses;
                addresses.reserve(getMastersResult.MasterInfos.size());
                for (const auto& masterInfo : getMastersResult.MasterInfos) {
                    if (masterInfo.Alive) {
                        if (!addresses.insert(masterInfo.GrpcAddress).second) {
                            YT_LOG_WARNING("Discovered duplicate master (GrpcAddress: %v)",
                                masterInfo.GrpcAddress);
                        }
                    }
                }

                YT_LOG_DEBUG("Discovered alive masters (GrpcAddresses: %v)",
                    addresses);

                ChannelPool_->Setup(std::move(addresses));
            } else {
                YT_LOG_DEBUG("Error discoverying masters (Error: %v)",
                    getMastersResultOrError);

                ChannelPool_->Setup(TError(std::move(getMastersResultOrError)));
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error discoverying masters");
        }
    }
};

using TDiscoveryChannelProviderPtr = TIntrusivePtr<TDiscoveryChannelProvider>;

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public IConnection
{
public:
    explicit TConnection(TConnectionConfigPtr config)
        : Config_(std::move(config))
        , DiscoveryClient_(New<TDiscoveryClient>(
            CreateDiscoveryChannel(),
            Config_->Authentication,
            Config_->DiscoveryTimeout))
        , DiscoveryChannelProvider_(New<TDiscoveryChannelProvider>(
            DiscoveryClient_,
            New<TDiscoveryChannelFactory>(Config_->GrpcChannel),
            Config_->DiscoveryPeriod))
    { }

    virtual IChannelPtr GetChannel() override
    {
        return CreateRetryingChannel(CreateRoamingChannel(DiscoveryChannelProvider_));
    }

    virtual void SetupRequestAuthentication(const TIntrusivePtr<TClientRequest>& request) override
    {
        NNative::SetupRequestAuthentication(request, Config_->Authentication);
    }

private:
    class TDiscoveryChannelFactory
        : public IChannelFactory
    {
    public:
        explicit TDiscoveryChannelFactory(NGrpc::TChannelConfigPtr grpcChannelConfig)
            : GrpcChannelConfig_(std::move(grpcChannelConfig))
        { }

        virtual IChannelPtr CreateChannel(const TString& address) override
        {
            return CreateChannelImpl(address);
        }

        virtual IChannelPtr CreateChannel(const TAddressWithNetwork& addressWithNetwork) override
        {
            return CreateChannelImpl(addressWithNetwork.Address);
        }

    private:
        const NGrpc::TChannelConfigPtr GrpcChannelConfig_;


        NGrpc::TChannelConfigPtr GenerateGrpcChannelConfig(TString address)
        {
            auto grpcChannelConfig = CloneYsonSerializable(GrpcChannelConfig_);
            grpcChannelConfig->Address = std::move(address);
            return grpcChannelConfig;
        }

        IChannelPtr CreateChannelImpl(NGrpc::TChannelConfigPtr grpcChannelConfig)
        {
            return NGrpc::CreateGrpcChannel(std::move(grpcChannelConfig));
        }

        IChannelPtr CreateChannelImpl(TString address)
        {
            auto grpcChannelConfig = GenerateGrpcChannelConfig(std::move(address));
            return CreateChannelImpl(std::move(grpcChannelConfig));
        }
    };


    const TConnectionConfigPtr Config_;
    const TDiscoveryClientPtr DiscoveryClient_;
    const TDiscoveryChannelProviderPtr DiscoveryChannelProvider_;


    IChannelPtr CreateRetryingChannel(IChannelPtr underlyingChannel) const
    {
        return NYT::NRpc::CreateRetryingChannel(
            Config_,
            std::move(underlyingChannel));
    }

    IChannelPtr CreateDiscoveryChannel() const
    {
        return CreateRetryingChannel(NGrpc::CreateGrpcChannel(Config_->GrpcChannel));
    }
};

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr CreateConnection(TConnectionConfigPtr config)
{
    return New<TConnection>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
