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

using namespace NYson;
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
        IChannelFactoryPtr channelFactory,
        TDiscoverRequestHook discoverRequestHook)
        : Config_(config)
        , ChannelFactory_(channelFactory)
        , DiscoverRequestHook_(discoverRequestHook)
        , Logger(RpcClientLogger)
    {
        Logger.AddTag("Channel: %v", this);

        AddPeers(Config_->Addresses);
    }

    virtual Stroka GetEndpointTextDescription() const override
    {
        return "[" + JoinToString(GetAllAddresses()) + "]";
    }

    virtual TYsonString GetEndpointYsonDescription() const override
    {
        return ConvertToYsonString(GetAllAddresses());
    }

    virtual TFuture<IChannelPtr> DiscoverChannel(IClientRequestPtr request) override
    {
        return New<TSession>(this, request)->Run();
    }

private:
    const TBalancingChannelConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const TDiscoverRequestHook DiscoverRequestHook_;

    mutable TSpinLock SpinLock_;
    yhash_set<Stroka> ActiveAddresses_;
    yhash_set<Stroka> BannedAddresses_;

    NLogging::TLogger Logger;


    class TSession
        : public TRefCounted
    {
    public:
        TSession(
            TBalancingChannelProviderPtr owner,
            IClientRequestPtr request)
            : Owner_(owner)
            , Request_(request)
            , Logger(Owner_->Logger)
        {
            Logger.AddTag("Service: %v", Request_->GetService());
        }

        TFuture<IChannelPtr> Run()
        {
            LOG_DEBUG("Starting peer discovery");
            DoRun();
            return Promise_;
        }

    private:
        const TBalancingChannelProviderPtr Owner_;
        const IClientRequestPtr Request_;

        TPromise<IChannelPtr> Promise_ = NewPromise<IChannelPtr>();

        TSpinLock SpinLock_;
        yhash_set<Stroka> RequestedAddresses_;
        std::vector<TError> InnerErrors_;

        NLogging::TLogger Logger;


        void DoRun()
        {
            if (Promise_.IsSet())
                return;

            while (true) {
                auto pickResult = PickPeer();
                
                if (auto* error = pickResult.TryAs<TError>()) {
                    Promise_.TrySet(
                        *error
                        << TErrorAttribute("endpoint", Owner_->GetEndpointYsonDescription()));
                    return;
                }

                if (pickResult.Is<TTooManyConcurrentRequests>()) {
                    break;
                }
                
                auto address = pickResult.As<Stroka>();

                LOG_DEBUG("Querying peer %v", address);

                auto channel = Owner_->ChannelFactory_->CreateChannel(address);
       
                TGenericProxy proxy(channel, Request_->GetService());
                proxy.SetDefaultTimeout(Owner_->Config_->DiscoverTimeout);

                auto req = proxy.Discover();
                if (Owner_->DiscoverRequestHook_) {
                    Owner_->DiscoverRequestHook_.Run(req.Get());
                }
                
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
            const TGenericProxy::TErrorOrRspDiscoverPtr& rspOrError)
        {
            if (rspOrError.IsOK()) {
                const auto& rsp = rspOrError.Value();
                bool up = rsp->up();
                auto suggestedAddresses = FromProto<Stroka>(rsp->suggested_addresses());

                LOG_DEBUG("Peer %v is %v (SuggestedAddresses: [%v])",
                    address,
                    up ? "up" : "down",
                    JoinToString(suggestedAddresses));

                Owner_->AddPeers(suggestedAddresses);

                if (up) {
                    Promise_.TrySet(channel);
                    return;
                }

                auto error = TError("Peer %v is down", address);
                BanPeer(address, error, Owner_->Config_->SoftBackoffTime);
            } else {
                auto error = TError("Discovery request failed for peer %v", address)
                    << rspOrError;
                LOG_WARNING(error);
                BanPeer(address, error, Owner_->Config_->HardBackoffTime);
            }

            DoRun();
        }

        struct TTooManyConcurrentRequests { };

        TVariant<Stroka, TError, TTooManyConcurrentRequests> PickPeer()
        {
            TGuard<TSpinLock> thisGuard(SpinLock_);
            TGuard<TSpinLock> ownerGuard(Owner_->SpinLock_);

            if (RequestedAddresses_.size() >= Owner_->Config_->MaxConcurrentDiscoverRequests) {
                return TTooManyConcurrentRequests();
            }

            std::vector<Stroka> candidates;
            candidates.reserve(Owner_->ActiveAddresses_.size());

            for (const auto& address : Owner_->ActiveAddresses_) {
                if (RequestedAddresses_.find(address) == RequestedAddresses_.end()) {
                    candidates.push_back(address);
                }
            }

            if (candidates.empty()) {
                if (RequestedAddresses_.empty()) {
                    return TError(NRpc::EErrorCode::Unavailable, "No alive peers left")
                         << InnerErrors_;
                } else {
                    return TTooManyConcurrentRequests();
                }
            }

            const auto& result = candidates[RandomNumber(candidates.size())];
            YCHECK(RequestedAddresses_.insert(result).second);
            return result;
        }

       void BanPeer(const Stroka& address, const TError& error, TDuration backoffTime)
       {
            {
                TGuard<TSpinLock> thisGuard(SpinLock_);
                TGuard<TSpinLock> ownerGuard(Owner_->SpinLock_);
                YCHECK(RequestedAddresses_.erase(address) == 1);
                InnerErrors_.push_back(error);
                if (Owner_->ActiveAddresses_.erase(address) != 1)
                    return;
                Owner_->BannedAddresses_.insert(address);
            }

            LOG_DEBUG("Peer %v banned (BackoffTime: %v)",
                address,
                backoffTime);

            TDelayedExecutor::Submit(
                BIND(&TBalancingChannelProvider::OnPeerBanTimeout, Owner_, address),
                backoffTime);
        }

    };


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

    void OnPeerBanTimeout(const Stroka& address)
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
    IChannelFactoryPtr channelFactory,
    TDiscoverRequestHook discoverRequestHook)
{
    YCHECK(config);
    YCHECK(channelFactory);

    if (config->Addresses.size() == 1) {
        // Shortcut: don't run any discovery if just one address is given.
        return channelFactory->CreateChannel(config->Addresses[0]);
    } else {
        auto channelProvider = New<TBalancingChannelProvider>(
            config,
            channelFactory,
            discoverRequestHook);
        return CreateRoamingChannel(channelProvider);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
