#include "stdafx.h"
#include "balancing_channel.h"
#include "roaming_channel.h"
#include "config.h"
#include "client.h"
#include "private.h"

#include <core/concurrency/delayed_executor.h>

#include <core/misc/string.h>
#include <core/misc/variant.h>

#include <core/ytree/convert.h>

#include <core/logging/log.h>

#include <util/random/random.h>

namespace NYT {
namespace NRpc {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBalancingChannelProvider)

class TBalancingChannelProvider
    : public IRoamingChannelProvider
{
public:
    explicit TBalancingChannelProvider(
        TBalancingChannelConfigPtr config,
        IChannelFactoryPtr channelFactory)
        : Config_(config)
        , ChannelFactory_(channelFactory)
        , Logger(RpcClientLogger)
    {
        Logger.AddTag("Channel: %v", this);

        AddPeers(Config_->Addresses);
    }

    virtual TYsonString GetEndpointDescription() const override
    {
        return ConvertToYsonString(GetAllAddresses());
    }

    virtual TFuture<TErrorOr<IChannelPtr>> DiscoverChannel(IClientRequestPtr request) override
    {
        return New<TSession>(this, request)->Run();
    }

private:
    TBalancingChannelConfigPtr Config_;
    IChannelFactoryPtr ChannelFactory_;

    mutable TSpinLock SpinLock_;
    yhash_set<Stroka> ActiveAddresses_;
    yhash_set<Stroka> BannedAddresses_;
    yhash_set<Stroka> RequestedAddresses_;

    NLog::TLogger Logger;


    class TSession
        : public TRefCounted
    {
    public:
        TSession(
            TBalancingChannelProviderPtr owner,
            IClientRequestPtr request)
            : Owner_(owner)
            , Request_(request)
            , Promise_(NewPromise<TErrorOr<IChannelPtr>>())
            , Logger(Owner_->Logger)
        {
            Logger.AddTag("Service: %v", Request_->GetService());
        }

        TFuture<TErrorOr<IChannelPtr>> Run()
        {
            LOG_DEBUG("Starting peer discovery");
            DoRun();
            return Promise_;
        }

    private:
        TBalancingChannelProviderPtr Owner_;
        IClientRequestPtr Request_;

        TPromise<TErrorOr<IChannelPtr>> Promise_;
        
        NLog::TLogger Logger;


        void DoRun()
        {
            if (Promise_.IsSet())
                return;

            while (true) {
                auto pickResult = Owner_->PickPeer();
                
                if (pickResult.Is<TNoAlivePeersLeft>()) {
                    ReportError(TError(NRpc::EErrorCode::Unavailable, "No alive peers left"));
                    return;
                }

                if (pickResult.Is<TTooManyConcurrentRequests>()) {
                    break;
                }
                
                auto address = pickResult.As<Stroka>();

                LOG_DEBUG("Quering peer %v", address);

                auto channel = Owner_->ChannelFactory_->CreateChannel(address);
       
                TGenericProxy proxy(channel, Request_->GetService());
                proxy.SetDefaultTimeout(Owner_->Config_->DiscoverTimeout);

                auto req = proxy.Discover();
                req->Invoke().Subscribe(BIND(
                    &TSession::OnResponse,
                    MakeStrong(this),
                    address,
                    channel));
            }
        }

        void OnResponse(
            const Stroka& address,
            IChannelPtr channel,
            TProxyBase::TRspDiscoverPtr rsp)
        {
            if (rsp->IsOK()) {
                bool up = rsp->up();
                auto suggestedAddresses = FromProto<Stroka>(rsp->suggested_addresses());

                LOG_DEBUG("Peer %v is %v (SuggestedAddresses: [%v])",
                    address,
                    up ? "up" : "down",
                    JoinToString(suggestedAddresses));

                Owner_->AddPeers(suggestedAddresses);

                if (up) {
                    Promise_.TrySet(channel);
                } else {
                    Owner_->BanPeer(address, Owner_->Config_->SoftBackoffTime);
                }
            } else {
                LOG_WARNING(*rsp, "Peer %v has failed to respond", address);
                Owner_->BanPeer(address, Owner_->Config_->HardBackoffTime);
            }

            DoRun();
        }

        void ReportError(const TError& error)
        {
            auto detailedError = error
                << TErrorAttribute("endpoint", Owner_->GetEndpointDescription());
            Promise_.Set(detailedError);
        }
    };


    struct TNoAlivePeersLeft { };
    struct TTooManyConcurrentRequests { };

    TVariant<Stroka, TNoAlivePeersLeft, TTooManyConcurrentRequests> PickPeer()
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (RequestedAddresses_.size() >= Config_->MaxConcurrentDiscoverRequests) {
            return TTooManyConcurrentRequests();
        }

        std::vector<Stroka> candidates;
        candidates.reserve(ActiveAddresses_.size());

        for (const auto& address : ActiveAddresses_) {
            if (RequestedAddresses_.find(address) == RequestedAddresses_.end()) {
                candidates.push_back(address);
            }
        }

        if (candidates.empty()) {
            if (RequestedAddresses_.empty()) {
                return TNoAlivePeersLeft();
            } else {
                return TTooManyConcurrentRequests();
            }
        }

        const auto& result = candidates[RandomNumber(candidates.size())];
        YCHECK(RequestedAddresses_.insert(result).second);
        return result;
    }

    void ReleasePeer(const Stroka& address)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        YCHECK(RequestedAddresses_.erase(address) == 1);
    }

    std::vector<Stroka> GetAllAddresses() const
    {
        std::vector<Stroka> result;
        TGuard<TSpinLock> guard(SpinLock_);
        result.insert(result.end(), ActiveAddresses_.begin(), ActiveAddresses_.end());
        result.insert(result.end(), BannedAddresses_.begin(), BannedAddresses_.end());
        return result;
    }

    void AddPeers(const std::vector<Stroka>& addresses)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        for (const auto& address : addresses) {
            if (!ActiveAddresses_.insert(address).second)
                continue;
            if (BannedAddresses_.find(address) != BannedAddresses_.end())
                continue;

            ActiveAddresses_.insert(address);
            LOG_DEBUG("Added peer %v", address);
        }
    }

    void BanPeer(const Stroka& address, TDuration backoffTime)
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YCHECK(RequestedAddresses_.erase(address) == 1);
            if (ActiveAddresses_.erase(address) != 1)
                return;
            BannedAddresses_.insert(address);
        }

        LOG_DEBUG("Peer %v banned (BackoffTime: %v)",
            address,
            backoffTime);

        TDelayedExecutor::Submit(
            BIND(&TBalancingChannelProvider::UnbanPeer, MakeWeak(this), address),
            backoffTime);
    }

    void UnbanPeer(const Stroka& address)
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (BannedAddresses_.erase(address) != 1)
                return;
            ActiveAddresses_.insert(address);
        }

        LOG_DEBUG("Peer %v unbanned", address);
    }

};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelProvider)

IChannelPtr CreateBalancingChannel(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory)
{
    YCHECK(config);
    YCHECK(channelFactory);

    auto channelProvider = New<TBalancingChannelProvider>(config, channelFactory);
    return CreateRoamingChannel(channelProvider);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
