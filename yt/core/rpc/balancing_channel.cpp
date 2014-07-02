#include "stdafx.h"
#include "balancing_channel.h"
#include "roaming_channel.h"
#include "config.h"
#include "client.h"
#include "private.h"

#include <core/logging/tagged_logger.h>

#include <core/concurrency/delayed_executor.h>

#include <core/misc/string.h>

#include <util/random/random.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBalancingDiscovery)

class TBalancingDiscovery
    : public TRefCounted
{
public:
    explicit TBalancingDiscovery(
        TBalancingChannelConfigPtr config,
        IChannelFactoryPtr channelFactory)
        : Config_(config)
        , ChannelFactory_(channelFactory)
        , Logger(RpcClientLogger)
    {
        YCHECK(Config_);
        YCHECK(ChannelFactory_);

        Logger.AddTag(Sprintf("Channel: %p", this));

        AddPeers(Config_->Addresses);
    }

    TFuture<TErrorOr<IChannelPtr>> Discover(IClientRequestPtr request)
    {
        return New<TSession>(this, request)->Run();
    }

private:
    TBalancingChannelConfigPtr Config_;
    IChannelFactoryPtr ChannelFactory_;

    TSpinLock SpinLock_;
    yhash_set<Stroka> ActiveAddresses_;
    yhash_set<Stroka> BannedAddresses_;

    NLog::TTaggedLogger Logger;


    class TSession
        : public TRefCounted
    {
    public:
        TSession(
            TBalancingDiscoveryPtr owner,
            IClientRequestPtr request)
            : Owner_(owner)
            , Request_(request)
            , Promise_(NewPromise<TErrorOr<IChannelPtr>>())
            , Logger(Owner_->Logger)
        {
            Logger.AddTag(Sprintf("Service: %s", ~Request_->GetService()));
        }

        TFuture<TErrorOr<IChannelPtr>> Run()
        {
            LOG_DEBUG("Starting peer discovery");
            DoRun();
            return Promise_;
        }

    private:
        TBalancingDiscoveryPtr Owner_;
        IClientRequestPtr Request_;

        TPromise<TErrorOr<IChannelPtr>> Promise_;
        
        Stroka CurrentAddress_;
        IChannelPtr CurrentChannel_;

        NLog::TTaggedLogger Logger;


        void DoRun()
        {
            auto maybeAddress = Owner_->PickPeer();
            if (!maybeAddress) {
                Promise_.Set(TError(
                    NRpc::EErrorCode::Unavailable,
                    "No alive peers left")
                    << TErrorAttribute("addresses", Owner_->GetAllAddresses()));
                return;
            }
            CurrentAddress_ = *maybeAddress;

            LOG_DEBUG("Checking peer %s",
                ~CurrentAddress_);

            CurrentChannel_ = Owner_->ChannelFactory_->CreateChannel(CurrentAddress_);
        
            TGenericProxy proxy(CurrentChannel_, Request_->GetService());
            proxy.SetDefaultTimeout(Owner_->Config_->DiscoverTimeout);

            auto req = proxy.Discover();
            req->Invoke().Subscribe(BIND(
                &TSession::OnDiscovered,
                MakeStrong(this)));
        }

        void OnDiscovered(TProxyBase::TRspDiscoverPtr rsp)
        {
            if (rsp->IsOK()) {
                bool up = rsp->up();
                auto suggestedAddresses = FromProto<Stroka>(rsp->suggested_addresses());

                LOG_DEBUG("Peer %s is %s (SuggestedAddresses: [%s])",
                    ~CurrentAddress_,
                    up ? "up" : "down",
                    ~JoinToString(suggestedAddresses));

                Owner_->AddPeers(suggestedAddresses);

                if (up) {
                    Promise_.Set(CurrentChannel_);
                } else {
                    Owner_->BanPeer(CurrentAddress_, Owner_->Config_->SoftBackoffTime);
                    DoRun();
                }
            } else {
                LOG_WARNING(*rsp, "Peer %s has failed to respond",
                    ~CurrentAddress_);
                Owner_->BanPeer(CurrentAddress_, Owner_->Config_->HardBackoffTime);
                DoRun();
            }
        }

    };


    TNullable<Stroka> PickPeer()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (ActiveAddresses_.empty()) {
            return Null;
        }
        std::vector<Stroka> addresses(ActiveAddresses_.begin(), ActiveAddresses_.end());
        return addresses[RandomNumber(addresses.size())];
    }

    std::vector<Stroka> GetAllAddresses()
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

            LOG_DEBUG("Added peer %s",
                ~address);
        }
    }

    void BanPeer(const Stroka& address, TDuration backoffTime)
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (ActiveAddresses_.erase(address) != 1)
                return;
            BannedAddresses_.insert(address);
        }

        LOG_DEBUG("Peer %s banned (BackoffTime: %s)",
            ~address,
            ~ToString(backoffTime));

        TDelayedExecutor::Submit(
            BIND(&TBalancingDiscovery::UnbanPeer, MakeWeak(this), address),
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

        LOG_DEBUG("Peer %s unbanned",
            ~address);
    }

};

DEFINE_REFCOUNTED_TYPE(TBalancingDiscovery)

IChannelPtr CreateBalancingChannel(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory)
{
    auto discovery = New<TBalancingDiscovery>(
        config,
        channelFactory);
    auto producer = BIND(&TBalancingDiscovery::Discover, discovery);
    return CreateRoamingChannel(producer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
