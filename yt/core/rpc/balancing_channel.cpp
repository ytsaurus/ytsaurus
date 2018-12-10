#include "balancing_channel.h"
#include "private.h"
#include "caching_channel_factory.h"
#include "client.h"
#include "config.h"
#include "roaming_channel.h"
#include "message.h"

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/variant.h>
#include <yt/core/misc/random.h>

#include <yt/core/utilex/random.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <util/random/random.h>

namespace NYT::NRpc {

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
        const TString& endpointDescription,
        const IAttributeDictionary& endpointAttributes,
        const TString& serviceName,
        TDiscoverRequestHook discoverRequestHook)
        : Config_(std::move(config))
        , ChannelFactory_(std::move(channelFactory))
        , EndpointDescription_(endpointDescription)
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Items(endpointAttributes)
                .Item("service").Value(serviceName)
            .EndMap()))
        , ServiceName_(serviceName)
        , DiscoverRequestHook_(std::move(discoverRequestHook))
    {
        AddPeers(Config_->Addresses);

        Logger = RpcClientLogger;
        Logger.AddTag("ChannelId: %v, Endpoint: %v, Service: %v",
            TGuid::Create(),
            EndpointDescription_,
            ServiceName_);
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request)
    {
        auto channel = PickViableChannel(request);
        return channel ? MakeFuture(std::move(channel)) : RunDiscoverySession();
    }

    TFuture<void> Terminate(const TError& error)
    {
        decltype(AddressToIndex_) addressToIndex;
        decltype(ViablePeers_) viablePeers;
        decltype(HashToViableChannel_) hashToViableChannel;
        {
            TWriterGuard guard(SpinLock_);
            Terminated_ = true;
            TerminationError_ = error;
            AddressToIndex_.swap(addressToIndex);
            ViablePeers_.swap(viablePeers);
            HashToViableChannel_.swap(hashToViableChannel);
        }

        std::vector<TFuture<void>> asyncResults;
        for (const auto& peer : viablePeers) {
            asyncResults.push_back(peer.Channel->Terminate(error));
        }

        return Combine(asyncResults);
    }

private:
    const TBalancingChannelConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;
    const TString ServiceName_;
    const TDiscoverRequestHook DiscoverRequestHook_;

    mutable TReaderWriterSpinLock SpinLock_;
    bool Terminated_ = false;
    TFuture<IChannelPtr> CurrentDiscoverySessionResult_;
    TDelayedExecutorCookie RediscoveryCookie_;
    TError TerminationError_;

    THashSet<TString> ActiveAddresses_;
    THashSet<TString> BannedAddresses_;

    struct TViablePeer
    {
        TString Address;
        IChannelPtr Channel;
    };

    THashMap<TString, int> AddressToIndex_;
    std::vector<TViablePeer> ViablePeers_;
    std::map<std::pair<size_t, TString>, IChannelPtr> HashToViableChannel_;

    NLogging::TLogger Logger;


    struct TTooManyConcurrentRequests { };
    struct TNoMorePeers { };

    using TPickPeerResult = TVariant<
        TString,
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
        THashSet<TString> RequestedAddresses_;
        THashSet<TString> RequestingAddresses_;
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

                QueryPeer(pickResult.As<TString>());
            }
        }

        void QueryPeer(const TString& address)
        {
            LOG_DEBUG("Querying peer (Address: %v)", address);

            auto channel = Owner_->ChannelFactory_->CreateChannel(address);

            TGenericProxy proxy(
                channel,
                TServiceDescriptor(Owner_->ServiceName_)
                    .SetProtocolVersion(GenericProtocolVersion));
            proxy.SetDefaultTimeout(Owner_->Config_->DiscoverTimeout);

            auto req = proxy.Discover();
            if (Owner_->DiscoverRequestHook_) {
                Owner_->DiscoverRequestHook_.Run(req.Get());
            }

            req->Invoke().Subscribe(BIND(
                &TDiscoverySession::OnResponse,
                MakeStrong(this),
                address,
                channel));
        }

        void OnResponse(
            const TString& address,
            const IChannelPtr& channel,
            const TGenericProxy::TErrorOrRspDiscoverPtr& rspOrError)
        {
            OnPeerQueried(address);

            if (rspOrError.IsOK()) {
                const auto& rsp = rspOrError.Value();
                bool up = rsp->up();
                auto suggestedAddresses = FromProto<std::vector<TString>>(rsp->suggested_addresses());

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
                    InvalidatePeer(address);
                }
            } else {
                auto error = TError("Discovery request failed for peer %v", address)
                    << *Owner_->EndpointAttributes_
                    << rspOrError;
                LOG_DEBUG(error);
                BanPeer(address, error, Owner_->Config_->HardBackoffTime);
                InvalidatePeer(address);
            }

            DoRun();
        }

        TPickPeerResult PickPeer()
        {
            TGuard<TSpinLock> guard(SpinLock_);
            return Owner_->PickPeer(&RequestingAddresses_, &RequestedAddresses_);
        }

        void OnPeerQueried(const TString& address)
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YCHECK(RequestingAddresses_.erase(address) == 1);
        }

        void BanPeer(const TString& address, const TError& error, TDuration backoffTime)
        {
            {
                TGuard<TSpinLock> guard(SpinLock_);
                YCHECK(RequestedAddresses_.erase(address) == 1);
                InnerErrors_.push_back(error);
            }

            Owner_->BanPeer(address, backoffTime);
        }

        void AddViablePeer(const TString& address, const IChannelPtr& channel)
        {
            auto wrappedChannel = Owner_->AddViablePeer(address, channel);
            TrySetResult(wrappedChannel);
        }

        void InvalidatePeer(const TString& address)
        {
            Owner_->InvalidatePeer(address);
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


    IChannelPtr PickViableChannel(const IClientRequestPtr& request)
    {
        const auto& balancingExt = request->Header().GetExtension(NProto::TBalancingExt::balancing_ext);
        return balancingExt.enable_stickness()
            ? PickStickyViableChannel(request, balancingExt.sticky_group_size())
            : PickRandomViableChannel(request);
    }

    IChannelPtr PickStickyViableChannel(const IClientRequestPtr& request, int stickyGroupSize)
    {
        auto hash = request->GetHash();
        auto randomIndex = RandomNumber<size_t>(stickyGroupSize);

        TReaderGuard guard(SpinLock_);

        if (HashToViableChannel_.empty()) {
            return nullptr;
        }

        auto it = HashToViableChannel_.lower_bound(std::make_pair(hash, TString()));
        for (int index = 0; index < randomIndex; ++index) {
            if (it == HashToViableChannel_.end()) {
                it = HashToViableChannel_.begin();
            }
            ++it;
        }

        if (it == HashToViableChannel_.end()) {
            it = HashToViableChannel_.begin();
        }

        LOG_DEBUG("Sticky peer selected (RequestId: %v, RequestHash: %x, RandomIndex: %v/%v, Address: %v)",
            request->GetRequestId(),
            hash,
            randomIndex,
            stickyGroupSize,
            it->first.second);

        return it->second;
    }

    IChannelPtr PickRandomViableChannel(const IClientRequestPtr& request)
    {
        TReaderGuard guard(SpinLock_);

        if (ViablePeers_.empty()) {
            return nullptr;
        }

        auto index = RandomNumber<size_t>(ViablePeers_.size());
        const auto& peer = ViablePeers_[index];

        LOG_DEBUG("Random peer selected (RequestId: %v, Address: %v)",
            request->GetRequestId(),
            peer.Address);

        return peer.Channel;
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

        TDelayedExecutor::CancelAndClear(RediscoveryCookie_);
        RediscoveryCookie_ = TDelayedExecutor::Submit(
            BIND(&TBalancingChannelSubprovider::OnRediscovery, MakeWeak(this)),
            Config_->RediscoverPeriod + RandomDuration(Config_->RediscoverSplay));
    }

    void OnRediscovery(bool aborted)
    {
        if (aborted) {
            return;
        }

        RunDiscoverySession();
    }

    void AddPeers(const std::vector<TString>& addresses)
    {
        TWriterGuard guard(SpinLock_);
        for (const auto& address : addresses) {
            if (BannedAddresses_.find(address) != BannedAddresses_.end()) {
                continue;
            }

            if (!ActiveAddresses_.insert(address).second) {
                continue;
            }

            LOG_DEBUG("Peer added (Address: %v)", address);
        }
    }

    TPickPeerResult PickPeer(
        THashSet<TString>* requestingAddresses,
        THashSet<TString>* requestedAddresses)
    {
        TReaderGuard guard(SpinLock_);

        if (requestingAddresses->size() >= Config_->MaxConcurrentDiscoverRequests) {
            return TTooManyConcurrentRequests();
        }

        std::vector<TString> candidates;
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

    void BanPeer(const TString& address, TDuration backoffTime)
    {
        {
            TWriterGuard guard(SpinLock_);
            if (ActiveAddresses_.erase(address) != 1) {
                return;
            }
            BannedAddresses_.insert(address);
        }

        LOG_DEBUG("Peer banned (Address: %v, BackoffTime: %v)",
            address,
            backoffTime);

        TDelayedExecutor::Submit(
            BIND(&TBalancingChannelSubprovider::OnPeerBanTimeout, MakeWeak(this), address),
            backoffTime);
    }

    void OnPeerBanTimeout(const TString& address, bool aborted)
    {
        if (aborted) {
            // If we are terminating -- do not unban anyone to prevent infinite retries.
            return;
        }

        {
            TWriterGuard guard(SpinLock_);
            if (BannedAddresses_.erase(address) != 1) {
                return;
            }
            ActiveAddresses_.insert(address);
        }

        LOG_DEBUG("Peer unbanned (Address: %v)", address);
    }

    template <class F>
    void GeneratePeerHashes(const TString& address, F f)
    {
        TRandomGenerator generator(address.hash());
        for (int index = 0; index < Config_->HashesPerPeer; ++index) {
            f(generator.Generate<size_t>());
        }
    }


    IChannelPtr AddViablePeer(const TString& address, const IChannelPtr& channel)
    {
        auto wrappedChannel = CreateFailureDetectingChannel(
            channel,
            BIND(&TBalancingChannelSubprovider::OnChannelFailed, MakeWeak(this), address));

        bool updated;
        {
            TWriterGuard guard(SpinLock_);
            updated = RegisterViablePeer(address, wrappedChannel);
        }

        LOG_DEBUG("Peer is viable (Address: %v, Updated: %v)",
            address,
            updated);

        return wrappedChannel;
    }

    void InvalidatePeer(const TString& address)
    {
        TWriterGuard guard(SpinLock_);
        auto it = AddressToIndex_.find(address);
        if (it != AddressToIndex_.end()) {
            UnregisterViablePeer(it);
        }
    }

    void OnChannelFailed(const TString& address, const IChannelPtr& channel)
    {
        bool evicted = false;
        {
            TWriterGuard guard(SpinLock_);
            auto it = AddressToIndex_.find(address);
            if (it != AddressToIndex_.end() && ViablePeers_[it->second].Channel == channel) {
                evicted = true;
                UnregisterViablePeer(it);
            }
        }

        LOG_DEBUG("Peer is no longer viable (Address: %v, Evicted: %v)",
            address,
            evicted);
    }


    bool RegisterViablePeer(const TString& address, const IChannelPtr& channel)
    {
        GeneratePeerHashes(address, [&] (size_t hash) {
            HashToViableChannel_[std::make_pair(hash, address)] = channel;
        });

        bool updated = false;
        auto it = AddressToIndex_.find(address);
        if (it == AddressToIndex_.end()) {
            int index = static_cast<int>(ViablePeers_.size());
            ViablePeers_.push_back(TViablePeer{address, channel});
            YCHECK(AddressToIndex_.emplace(address, index).second);
        } else {
            int index = it->second;
            ViablePeers_[index].Channel = channel;
            updated = true;
        }
        return updated;
    }

    void UnregisterViablePeer(THashMap<TString, int>::iterator it)
    {
        const auto& address = it->first;
        GeneratePeerHashes(address, [&] (size_t hash) {
            HashToViableChannel_.erase(std::make_pair(hash, address));
        });

        int index1 = it->second;
        int index2 = static_cast<int>(ViablePeers_.size() - 1);
        if (index1 != index2) {
            std::swap(ViablePeers_[index1], ViablePeers_[index2]);
            AddressToIndex_[ViablePeers_[index1].Address] = index1;
        }
        ViablePeers_.pop_back();
        AddressToIndex_.erase(it);
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
        const TString& endpointDescription,
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

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request) override
    {
        if (Config_->Addresses.size() == 1) {
            // Disable discovery and balancing when just one address is given.
            // This is vital for jobs since node's redirector is incapable of handling
            // Discover requests properly.
            return MakeFuture(ChannelFactory_->CreateChannel(Config_->Addresses[0]));
        } else {
            return GetSubprovider(request->GetService())->GetChannel(request);
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

    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;

    mutable TReaderWriterSpinLock SpinLock_;
    THashMap<TString, TBalancingChannelSubproviderPtr> SubproviderMap_;


    TBalancingChannelSubproviderPtr GetSubprovider(const TString& serviceName)
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
    const TString& endpointDescription,
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

} // namespace NYT::NRpc
