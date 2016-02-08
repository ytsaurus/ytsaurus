#include "balancing_channel.h"
#include "private.h"
#include "caching_channel_factory.h"
#include "client.h"
#include "config.h"
#include "roaming_channel.h"

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/string.h>
#include <yt/core/misc/variant.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <util/random/random.h>

namespace NYT {
namespace NRpc {

using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBalancingChannelSubprovider)
DECLARE_REFCOUNTED_CLASS(TBalancingChannelProvider)

class TBalancingChannelSubprovider
    : public TRefCounted
{
public:
    explicit TBalancingChannelSubprovider(
        TBalancingChannelConfigPtr config,
        IChannelFactoryPtr channelFactory,
        const Stroka& endpointDescription,
        const IAttributeDictionary& endpointAttributes,
        const Stroka& serviceName,
        TDiscoverRequestHook discoverRequestHook)
        : Config_(config)
        , ChannelFactory_(channelFactory)
        , EndpointDescription_(endpointDescription)
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Items(endpointAttributes)
                .Item("service").Value(serviceName)
            .EndMap()))
        , ServiceName_(serviceName)
        , DiscoverRequestHook_(discoverRequestHook)
    {
        AddPeers(Config_->Addresses);

        Logger = RpcClientLogger;
        Logger.AddTag("Endpoint: %v, Service: %v",
            EndpointDescription_,
            ServiceName_);
    }

    TFuture<IChannelPtr> GetChannel()
    {
        auto channel = PickViableChannel();
        return channel ? MakeFuture(std::move(channel)) : RunDiscoverySession();
    }

    TFuture<void> Terminate(const TError& error)
    {
        std::vector<std::pair<Stroka, IChannelPtr>> viableChannels;
        {
            TWriterGuard guard(SpinLock_);
            Terminated_ = true;
            TerminationError_ = error;
            ViableChannels_.swap(viableChannels);
        }

        std::vector<TFuture<void>> asyncResults;
        for (const auto& pair : viableChannels) {
            asyncResults.push_back(pair.second->Terminate(error));
        }

        return Combine(asyncResults);
    }

private:
    const TBalancingChannelConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const Stroka EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;
    const Stroka ServiceName_;
    const TDiscoverRequestHook DiscoverRequestHook_;

    mutable TReaderWriterSpinLock SpinLock_;
    bool Terminated_ = false;
    TFuture<IChannelPtr> CurrentDiscoverySessionResult_;
    TError TerminationError_;
    yhash_set<Stroka> ActiveAddresses_;
    yhash_set<Stroka> BannedAddresses_;
    std::vector<std::pair<Stroka, IChannelPtr>> ViableChannels_;

    NLogging::TLogger Logger;


    struct TTooManyConcurrentRequests { };
    struct TNoMorePeers { };

    using TPickPeerResult = TVariant<
        Stroka,
        TTooManyConcurrentRequests,
        TNoMorePeers>;

    class TDiscoverySession
        : public TRefCounted
    {
    public:
        explicit TDiscoverySession(TBalancingChannelSubproviderPtr owner)
            : Owner_(std::move(owner))
            , Logger(Owner_->Logger)
        { }

        TFuture<IChannelPtr> GetResult()
        {
            return Promise_;
        }

        void Run()
        {
            LOG_DEBUG("Starting peer discovery");
            DoRun();
        }

    private:
        const TBalancingChannelSubproviderPtr Owner_;

        TPromise<IChannelPtr> Promise_ = NewPromise<IChannelPtr>();

        TSpinLock SpinLock_;
        yhash_set<Stroka> RequestedAddresses_;
        yhash_set<Stroka> RequestingAddresses_;
        std::vector<TError> InnerErrors_;

        NLogging::TLogger Logger;


        void DoRun()
        {
            while (true) {
                auto pickResult = PickPeer();

                if (pickResult.Is<TTooManyConcurrentRequests>()) {
                    break;
                }

                if (pickResult.Is<TNoMorePeers>()) {
                    OnFinished();
                    break;
                }

                QueryPeer(pickResult.As<Stroka>());
            }
        }

        void QueryPeer(const Stroka& address)
        {
            LOG_DEBUG("Querying peer (Address: %v)", address);

            auto channel = Owner_->ChannelFactory_->CreateChannel(address);

            TGenericProxy proxy(channel, Owner_->ServiceName_);
            proxy.SetDefaultTimeout(Owner_->Config_->DiscoverTimeout);

            auto req = proxy.Discover();
            if (Owner_->DiscoverRequestHook_) {
                Owner_->DiscoverRequestHook_.Run(req.Get());
            }

            auto asyncResult = req->Invoke();

            asyncResult.Subscribe(BIND(
                &TDiscoverySession::OnResponse,
                MakeStrong(this),
                address,
                channel));

            Promise_.OnCanceled(BIND([=] () mutable {
                asyncResult.Cancel();
            }));
        }

        void OnResponse(
            const Stroka& address,
            IChannelPtr channel,
            const TGenericProxy::TErrorOrRspDiscoverPtr& rspOrError)
        {
            if (Promise_.IsCanceled()) {
                TrySetResult(TError(NYT::EErrorCode::Canceled, "Discovery session has been canceled"));
                return;
            }
 
            OnPeerQueried(address);

            if (rspOrError.IsOK()) {
                const auto& rsp = rspOrError.Value();
                bool up = rsp->up();
                auto suggestedAddresses = FromProto<std::vector<Stroka>>(rsp->suggested_addresses());

                if (!suggestedAddresses.empty()) {
                    LOG_DEBUG("Peers suggested (SuggestorAddress: %v, SuggestedAddresses: %v)",
                        address,
                        suggestedAddresses);
                    Owner_->AddPeers(suggestedAddresses);
                }

                if (up) {
                    AddViablePeer(address, channel);
                } else {
                    LOG_DEBUG("Peer is down (Address: %v)", address);
                    auto error = TError("Peer %v is down", address)
                         << *Owner_->EndpointAttributes_;
                    BanPeer(address, error, Owner_->Config_->SoftBackoffTime);
                }
            } else {
                auto error = TError("Discovery request failed for peer %v", address)
                    << *Owner_->EndpointAttributes_
                    << rspOrError;
                LOG_WARNING(error);
                BanPeer(address, error, Owner_->Config_->HardBackoffTime);
            }

            DoRun();
        }

        TPickPeerResult PickPeer()
        {
            TGuard<TSpinLock> guard(SpinLock_);
            return Owner_->PickPeer(&RequestingAddresses_, &RequestedAddresses_);
        }

        void OnPeerQueried(const Stroka& address)
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YCHECK(RequestingAddresses_.erase(address) == 1);
        }

        void BanPeer(const Stroka& address, const TError& error, TDuration backoffTime)
        {
            {
                TGuard<TSpinLock> guard(SpinLock_);
                YCHECK(RequestedAddresses_.erase(address) == 1);
                InnerErrors_.push_back(error);
            }

            Owner_->BanPeer(address, backoffTime);
        }

        void AddViablePeer(const Stroka& address, IChannelPtr channel)
        {
            auto wrappedChannel = Owner_->AddViablePeer(address, channel);
            TrySetResult(wrappedChannel);
        }

        void OnFinished()
        {
            TError result;
            {
                TGuard<TSpinLock> guard(SpinLock_);
                result = TError(NRpc::EErrorCode::Unavailable, "No alive peers left")
                    << *Owner_->EndpointAttributes_
                    << InnerErrors_;
            }

            TrySetResult(result);
        }

        void TrySetResult(const TErrorOr<IChannelPtr>& result)
        {
            if (Promise_.TrySet(result)) {
                Owner_->OnDiscoverySessionFinished();
            }
        }
    };


    IChannelPtr PickViableChannel()
    {
        TReaderGuard guard(SpinLock_);
        if (ViableChannels_.empty()) {
            return nullptr;
        }
        int index = RandomNumber(ViableChannels_.size());
        return ViableChannels_[index].second;
    }


    TFuture<IChannelPtr> RunDiscoverySession()
    {
        TIntrusivePtr<TDiscoverySession> session;
        {
            TWriterGuard guard(SpinLock_);

            if (Terminated_) {
                return MakeFuture<IChannelPtr>(TError(
                    NRpc::EErrorCode::TransportError,
                    "Channel terminated")
                    << *EndpointAttributes_
                    << TerminationError_);
            }

            if (CurrentDiscoverySessionResult_) {
                return CurrentDiscoverySessionResult_;
            }

            session = New<TDiscoverySession>(this);
            CurrentDiscoverySessionResult_ = session->GetResult();
        }

        session->Run();
        return session->GetResult();
    }

    void OnDiscoverySessionFinished()
    {
        TWriterGuard guard(SpinLock_);

        YCHECK(CurrentDiscoverySessionResult_);
        CurrentDiscoverySessionResult_.Reset();

        TDelayedExecutor::Submit(
            BIND(IgnoreResult(&TBalancingChannelSubprovider::RunDiscoverySession), MakeWeak(this)),
            Config_->RediscoverPeriod);
    }


    void AddPeers(const std::vector<Stroka>& addresses)
    {
        TWriterGuard guard(SpinLock_);
        for (const auto& address : addresses) {
            if (!ActiveAddresses_.insert(address).second)
                continue;
            if (BannedAddresses_.find(address) != BannedAddresses_.end())
                continue;

            ActiveAddresses_.insert(address);
            LOG_DEBUG("Peer added (Address: %v)", address);
        }
    }

    TPickPeerResult PickPeer(
        yhash_set<Stroka>* requestingAddresses,
        yhash_set<Stroka>* requestedAddresses)
    {
        TReaderGuard guard(SpinLock_);

        if (requestingAddresses->size() >= Config_->MaxConcurrentDiscoverRequests) {
            return TTooManyConcurrentRequests();
        }

        std::vector<Stroka> candidates;
        candidates.reserve(ActiveAddresses_.size());

        for (const auto& address : ActiveAddresses_) {
            if (requestedAddresses->find(address) == requestedAddresses->end()) {
                candidates.push_back(address);
            }
        }

        if (candidates.empty()) {
            if (requestedAddresses->empty()) {
                return TNoMorePeers();
            } else {
                return TTooManyConcurrentRequests();
            }
        }

        const auto& result = candidates[RandomNumber(candidates.size())];
        YCHECK(requestedAddresses->insert(result).second);
        YCHECK(requestingAddresses->insert(result).second);
        return result;
    }

    void BanPeer(const Stroka& address, TDuration backoffTime)
    {
        {
            TWriterGuard guard(SpinLock_);
            if (ActiveAddresses_.erase(address) != 1)
                return;
            BannedAddresses_.insert(address);
        }

        LOG_DEBUG("Peer banned (Address: %v, BackoffTime: %v)",
            address,
            backoffTime);

        TDelayedExecutor::Submit(
            BIND(&TBalancingChannelSubprovider::OnPeerBanTimeout, MakeWeak(this), address),
            backoffTime);
    }

    void OnPeerBanTimeout(const Stroka& address)
    {
        {
            TWriterGuard guard(SpinLock_);
            if (BannedAddresses_.erase(address) != 1)
                return;
            ActiveAddresses_.insert(address);
        }

        LOG_DEBUG("Peer unbanned (Address: %v)", address);
    }

    IChannelPtr AddViablePeer(const Stroka& address, IChannelPtr channel)
    {
        auto wrappedChannel = CreateFailureDetectingChannel(
            channel,
            BIND(&TBalancingChannelSubprovider::OnChannelFailed, MakeWeak(this), address));

        {
            TWriterGuard guard(SpinLock_);
            bool found = false;
            for (auto& pair : ViableChannels_) {
                if (pair.first == address) {
                    pair.second = wrappedChannel;
                    found = true;
                    break;
                }
            }
            if (!found) {
                ViableChannels_.push_back(std::make_pair(address, wrappedChannel));
            }
        }

        LOG_DEBUG("Peer is up (Address: %v)", address);
        return wrappedChannel;
    }

    void OnChannelFailed(const Stroka& address, IChannelPtr channel)
    {
        {
            TWriterGuard guard(SpinLock_);
            for (int index = 0; index < ViableChannels_.size(); ++index) {
                const auto& pair = ViableChannels_[index];
                if (pair.first == address && pair.second == channel) {
                    std::swap(ViableChannels_[index], ViableChannels_[ViableChannels_.size() - 1]);
                    ViableChannels_.resize(ViableChannels_.size() - 1);
                    break;
                }
            }
        }

        LOG_DEBUG("Peer failed (Address: %v)", address);
    }
};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelSubprovider)

class TBalancingChannelProvider
    : public IRoamingChannelProvider
{
public:
    TBalancingChannelProvider(
        TBalancingChannelConfigPtr config,
        IChannelFactoryPtr channelFactory,
        const Stroka& endpointDescription,
        const IAttributeDictionary& endpointAttributes,
        TDiscoverRequestHook discoverRequestHook)
        : Config_(config)
        , ChannelFactory_(CreateCachingChannelFactory(channelFactory))
        , DiscoverRequestHook_(discoverRequestHook)
        , EndpointDescription_(Format("%v%v",
            endpointDescription,
            Config_->Addresses))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("addresses").Value(Config_->Addresses)
                .Items(endpointAttributes)
            .EndMap()))
    { }

    virtual const Stroka& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual TFuture<IChannelPtr> GetChannel(const Stroka& serviceName) override
    {
        if (Config_->Addresses.size() == 1) {
            // Disable discovery and balancing when just one address is given.
            // This is vital for jobs since node's redirector is incapable of handling
            // Discover requests properly.
            return MakeFuture(ChannelFactory_->CreateChannel(Config_->Addresses[0]));
        } else {
            return GetSubprovider(serviceName)->GetChannel();
        }
    }

    virtual TFuture<void> Terminate(const TError& error)
    {
        std::vector<TBalancingChannelSubproviderPtr> subproviders;
        {
            TReaderGuard guard(SpinLock_);
            for (const auto& pair : SubproviderMap_) {
                subproviders.push_back(pair.second);
            }
        }

        std::vector<TFuture<void>> asyncResults;
        for (const auto& subprovider : subproviders) {
            asyncResults.push_back(subprovider->Terminate(error));
        }

        return Combine(asyncResults);
    }

private:
    const TBalancingChannelConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const TDiscoverRequestHook DiscoverRequestHook_;

    const Stroka EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;

    mutable TReaderWriterSpinLock SpinLock_;
    yhash_map<Stroka, TBalancingChannelSubproviderPtr> SubproviderMap_;


    TBalancingChannelSubproviderPtr GetSubprovider(const Stroka& serviceName)
    {
        {
            TReaderGuard guard(SpinLock_);
            auto it = SubproviderMap_.find(serviceName);
            if (it != SubproviderMap_.end()) {
                return it->second;
            }
        }

        {
            TWriterGuard guard(SpinLock_);
            auto it = SubproviderMap_.find(serviceName);
            if (it != SubproviderMap_.end()) {
                return it->second;
            }

            auto subprovider = New<TBalancingChannelSubprovider>(
                Config_,
                ChannelFactory_,
                EndpointDescription_,
                *EndpointAttributes_,
                serviceName,
                DiscoverRequestHook_);
            YCHECK(SubproviderMap_.insert(std::make_pair(serviceName, subprovider)).second);
            return subprovider;
        }
    }

};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelProvider)

IChannelPtr CreateBalancingChannel(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    const Stroka& endpointDescription,
    const IAttributeDictionary& endpointAttributes,
    TDiscoverRequestHook discoverRequestHook)
{
    YCHECK(config);
    YCHECK(channelFactory);

    auto channelProvider = New<TBalancingChannelProvider>(
        config,
        channelFactory,
        endpointDescription,
        endpointAttributes,
        discoverRequestHook);
    return CreateRoamingChannel(channelProvider);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
