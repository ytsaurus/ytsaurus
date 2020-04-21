#include "balancing_channel.h"
#include "private.h"
#include "caching_channel_factory.h"
#include "client.h"
#include "config.h"
#include "dispatcher.h"
#include "roaming_channel.h"
#include "message.h"

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/variant.h>
#include <yt/core/misc/random.h>
#include <yt/core/misc/small_set.h>

#include <yt/core/utilex/random.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <util/random/random.h>

namespace NYT::NRpc {

using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

using NYT::FromProto;
using NYT::ToProto;

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
        , Logger(NLogging::TLogger(RpcClientLogger)
            .AddTag("ChannelId: %v, Endpoint: %v, Service: %v",
                TGuid::Create(),
                EndpointDescription_,
                ServiceName_))
    {
        AddPeers(Config_->Addresses);
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request)
    {
        auto channel = PickViableChannel(request);
        if (channel) {
            return MakeFuture(channel);
        }

        auto sessionOrError = RunDiscoverySession();
        if (!sessionOrError.IsOK()) {
            return MakeFuture<IChannelPtr>(TError(sessionOrError));
        }

        const auto& session = sessionOrError.Value();
        const auto& balancingExt = request->Header().GetExtension(NProto::TBalancingExt::balancing_ext);
        auto future = balancingExt.enable_stickness()
            ? session->GetFinished()
            : session->GetFirstPeerDiscovered();
        return future.Apply(
            BIND(&TBalancingChannelSubprovider::GetChannelAfterDiscovery, MakeStrong(this), request));
    }

    void Terminate(const TError& error)
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

        for (const auto& peer : viablePeers) {
            peer.Channel->Terminate(error);
        }
    }

private:
    class TDiscoverySession;
    using TDiscoverySessionPtr = TIntrusivePtr<TDiscoverySession>;

    const TBalancingChannelConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;
    const TString ServiceName_;
    const TDiscoverRequestHook DiscoverRequestHook_;

    const NLogging::TLogger Logger;

    mutable TReaderWriterSpinLock SpinLock_;
    bool Terminated_ = false;
    TDiscoverySessionPtr CurrentDiscoverySession_;
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


    struct TTooManyConcurrentRequests { };
    struct TNoMorePeers { };

    using TPickPeerResult = std::variant<
        TString,
        TTooManyConcurrentRequests,
        TNoMorePeers>;

    class TDiscoverySession
        : public TRefCounted
    {
    public:
        explicit TDiscoverySession(TBalancingChannelSubprovider* owner)
            : Owner_(owner)
            , Logger(owner->Logger)
        { }

        TFuture<void> GetFirstPeerDiscovered()
        {
            return FirstPeerDiscoveredPromise_;
        }

        TFuture<void> GetFinished()
        {
            return FinishedPromise_;
        }

        void Run()
        {
            YT_LOG_DEBUG("Starting peer discovery");
            DoRun();
        }

    private:
        const TWeakPtr<TBalancingChannelSubprovider> Owner_;
        const NLogging::TLogger Logger;

        TPromise<void> FirstPeerDiscoveredPromise_ = NewPromise<void>();
        TPromise<void> FinishedPromise_ = NewPromise<void>();
        std::atomic_flag Finished_ = ATOMIC_FLAG_INIT;
        std::atomic<bool> Success_ = {false};

        TSpinLock SpinLock_;
        THashSet<TString> RequestedAddresses_;
        THashSet<TString> RequestingAddresses_;
        std::vector<TError> DiscoveryErrors_;

        void DoRun()
        {
            while (true) {
                auto mustBreak = false;
                auto pickResult = PickPeer();
                Visit(pickResult,
                    [&] (TTooManyConcurrentRequests) {
                        mustBreak = true;
                    },
                    [&] (TNoMorePeers) {
                        if (!HasOutstandingQueries()) {
                            OnFinished();
                        }
                        mustBreak = true;
                    },
                    [&] (const TString& address) {
                        QueryPeer(address);
                    });

                if (mustBreak) {
                    break;
                }
            }
        }

        void QueryPeer(const TString& address)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            YT_LOG_DEBUG("Querying peer (Address: %v)", address);

            auto channel = owner->ChannelFactory_->CreateChannel(address);

            TGenericProxy proxy(
                channel,
                TServiceDescriptor(owner->ServiceName_)
                    .SetProtocolVersion(GenericProtocolVersion));
            proxy.SetDefaultTimeout(owner->Config_->DiscoverTimeout);

            auto req = proxy.Discover();
            if (owner->DiscoverRequestHook_) {
                owner->DiscoverRequestHook_.Run(req.Get());
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
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            if (rspOrError.IsOK()) {
                const auto& rsp = rspOrError.Value();
                bool up = rsp->up();
                auto suggestedAddresses = FromProto<std::vector<TString>>(rsp->suggested_addresses());

                if (!suggestedAddresses.empty()) {
                    YT_LOG_DEBUG("Peers suggested (SuggestorAddress: %v, SuggestedAddresses: %v)",
                        address,
                        suggestedAddresses);
                    owner->AddPeers(suggestedAddresses);
                }

                if (up) {
                    AddViablePeer(address, channel);
                    Success_.store(true);
                    FirstPeerDiscoveredPromise_.TrySet();
                } else {
                    YT_LOG_DEBUG("Peer is down (Address: %v)", address);
                    auto error = owner->MakePeerDownError(address);
                    BanPeer(address, error, owner->Config_->SoftBackoffTime);
                    InvalidatePeer(address);
                }
            } else {
                YT_LOG_DEBUG(rspOrError, "Peer discovery request failed (Address: %v)", address);
                auto error = owner->MakePeerDiscoveryFailedError(address, rspOrError);
                BanPeer(address, error, owner->Config_->HardBackoffTime);
                InvalidatePeer(address);
            }

            OnPeerQueried(address);
            DoRun();
        }

        TPickPeerResult PickPeer()
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return TNoMorePeers();
            }

            TGuard<TSpinLock> guard(SpinLock_);
            return owner->PickPeer(&RequestingAddresses_, &RequestedAddresses_);
        }

        void OnPeerQueried(const TString& address)
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YT_VERIFY(RequestingAddresses_.erase(address) == 1);
        }

        bool HasOutstandingQueries()
        {
            TGuard<TSpinLock> guard(SpinLock_);
            return !RequestingAddresses_.empty();
        }

        void BanPeer(const TString& address, const TError& error, TDuration backoffTime)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            {
                TGuard<TSpinLock> guard(SpinLock_);
                YT_VERIFY(RequestedAddresses_.erase(address) == 1);
                DiscoveryErrors_.push_back(error);
            }

            owner->BanPeer(address, backoffTime);
        }

        std::vector<TError> GetDiscoveryErrors()
        {
            TGuard<TSpinLock> guard(SpinLock_);
            return DiscoveryErrors_;
        }

        void AddViablePeer(const TString& address, const IChannelPtr& channel)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            owner->AddViablePeer(address, channel);
        }

        void InvalidatePeer(const TString& address)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            owner->InvalidatePeer(address);
        }

        void OnFinished()
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            if (Finished_.test_and_set()) {
                return;
            }

            if (Success_.load()) {
                YT_ASSERT(FirstPeerDiscoveredPromise_.IsSet());
                FinishedPromise_.Set();
            } else {
                auto error = owner->MakeNoAlivePeersError()
                    << GetDiscoveryErrors();
                FirstPeerDiscoveredPromise_.Set(error);
                FinishedPromise_.Set(error);
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

        if (ViablePeers_.empty()) {
            return nullptr;
        }

        auto it = HashToViableChannel_.lower_bound(std::make_pair(hash, TString()));
        auto rebaseIt = [&] {
            if (it == HashToViableChannel_.end()) {
                it = HashToViableChannel_.begin();
            }
        };

        SmallSet<TStringBuf, 16> seenAddresses;
        auto currentRandomIndex = randomIndex % ViablePeers_.size();
        while (true) {
            rebaseIt();
            const auto& address = it->first.second;
            if (seenAddresses.count(address) == 0) {
                if (currentRandomIndex == 0) {
                    break;
                }
                seenAddresses.insert(address);
                --currentRandomIndex;
            } else {
                ++it;
            }
        }

        YT_LOG_DEBUG("Sticky peer selected (RequestId: %v, RequestHash: %llx, RandomIndex: %v/%v, Address: %v)",
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

        YT_LOG_DEBUG("Random peer selected (RequestId: %v, Address: %v)",
            request->GetRequestId(),
            peer.Address);

        return peer.Channel;
    }


    TErrorOr<TDiscoverySessionPtr> RunDiscoverySession()
    {
        TWriterGuard guard(SpinLock_);

        if (Terminated_) {
            return TError(
                NRpc::EErrorCode::TransportError,
                "Channel terminated")
                << *EndpointAttributes_
                << TerminationError_;
        }

        if (CurrentDiscoverySession_) {
            return CurrentDiscoverySession_;
        }

        auto session = CurrentDiscoverySession_ = New<TDiscoverySession>(this);
        session->GetFinished().Subscribe(
            BIND(&TBalancingChannelSubprovider::OnDiscoverySessionFinished, MakeWeak(this)));

        guard.Release();

        session->Run();
        return session;
    }

    TError MakeNoAlivePeersError()
    {
        return TError(NRpc::EErrorCode::Unavailable, "No alive peers found")
            << *EndpointAttributes_;
    }

    TError MakePeerDownError(const TString& address)
    {
        return TError("Peer %v is down", address)
            << *EndpointAttributes_;
    }

    TError MakePeerDiscoveryFailedError(const TString& address, const TError&  error)
    {
        return TError("Discovery request failed for peer %v", address)
            << *EndpointAttributes_
            << error;
    }

    IChannelPtr GetChannelAfterDiscovery(const IClientRequestPtr& request)
    {
        auto channel = PickViableChannel(request);
        if (!channel) {
            // Not very likely but possible in theory.
            THROW_ERROR MakeNoAlivePeersError();
        }
        return channel;
    }

    void OnDiscoverySessionFinished(const TError& /*error*/)
    {
        NTracing::TNullTraceContextGuard nullTraceContext;
        TWriterGuard guard(SpinLock_);

        YT_VERIFY(CurrentDiscoverySession_);
        CurrentDiscoverySession_.Reset();

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

        Y_UNUSED(RunDiscoverySession());
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

            YT_LOG_DEBUG("Peer added (Address: %v)", address);
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
            if (requestingAddresses->find(address) == requestingAddresses->end() &&
                requestedAddresses->find(address) == requestedAddresses->end())
            {
                candidates.push_back(address);
            }
        }

        if (candidates.empty()) {
            return TNoMorePeers();
        }

        const auto& result = candidates[RandomNumber(candidates.size())];
        YT_VERIFY(requestedAddresses->insert(result).second);
        YT_VERIFY(requestingAddresses->insert(result).second);
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

        YT_LOG_DEBUG("Peer banned (Address: %v, BackoffTime: %v)",
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

        YT_LOG_DEBUG("Peer unbanned (Address: %v)", address);
    }

    template <class F>
    void GeneratePeerHashes(const TString& address, F f)
    {
        TRandomGenerator generator(address.hash());
        for (int index = 0; index < Config_->HashesPerPeer; ++index) {
            f(generator.Generate<size_t>());
        }
    }


    void AddViablePeer(const TString& address, const IChannelPtr& channel)
    {
        auto wrappedChannel = CreateFailureDetectingChannel(
            channel,
            Config_->AcknowledgementTimeout,
            BIND(&TBalancingChannelSubprovider::OnChannelFailed, MakeWeak(this), address));

        bool updated;
        {
            TWriterGuard guard(SpinLock_);
            updated = RegisterViablePeer(address, wrappedChannel);
        }

        YT_LOG_DEBUG("Peer is viable (Address: %v, Updated: %v)",
            address,
            updated);
    }

    void InvalidatePeer(const TString& address)
    {
        TWriterGuard guard(SpinLock_);
        auto it = AddressToIndex_.find(address);
        if (it != AddressToIndex_.end()) {
            UnregisterViablePeer(it);
        }
    }

    void OnChannelFailed(const TString& address, const IChannelPtr& channel, const TError& error)
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

        YT_LOG_DEBUG(error, "Peer is no longer viable (Address: %v, Evicted: %v)",
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
            YT_VERIFY(AddressToIndex_.emplace(address, index).second);
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

    virtual TNetworkId GetNetworkId() const override
    {
        // NB(psushin): Assume that balanced channels always use default network.
        // This is important for setting ToS level.
        return DefaultNetworkId;
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

    virtual void Terminate(const TError& error)
    {
        std::vector<TBalancingChannelSubproviderPtr> subproviders;
        {
            TReaderGuard guard(SpinLock_);
            for (const auto& pair : SubproviderMap_) {
                subproviders.push_back(pair.second);
            }
        }

        for (const auto& subprovider : subproviders) {
            subprovider->Terminate(error);
        }
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
            YT_VERIFY(SubproviderMap_.insert(std::make_pair(serviceName, subprovider)).second);
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
    YT_VERIFY(config);
    YT_VERIFY(channelFactory);

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
