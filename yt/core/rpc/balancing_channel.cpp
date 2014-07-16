#include "stdafx.h"
#include "balancing_channel.h"
#include "roaming_channel.h"
#include "config.h"
#include "client.h"
#include "private.h"

#include <core/logging/tagged_logger.h>

#include <core/concurrency/delayed_executor.h>

#include <core/misc/string.h>

#include <core/ytree/convert.h>

#include <util/random/random.h>

namespace NYT {
namespace NRpc {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcClientLogger;

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

    NLog::TTaggedLogger Logger;


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
        
        Stroka CurrentAddress_;
        IChannelPtr CurrentChannel_;

        NLog::TTaggedLogger Logger;


        void DoRun()
        {
            auto maybeAddress = Owner_->PickPeer();
            if (!maybeAddress) {
                ReportError(TError(NRpc::EErrorCode::Unavailable, "No alive peers left"));
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

        void ReportError(const TError& error)
        {
            auto detailedError = error
                << TErrorAttribute("endpoint", Owner_->GetEndpointDescription());
            Promise_.Set(detailedError);
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

        LOG_DEBUG("Peer %s unbanned",
            ~address);
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
