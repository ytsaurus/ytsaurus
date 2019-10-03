#include "proxy_coordinator.h"

#include <yt/ytlib/security_client/public.h>

#include <yt/core/rpc/public.h>
#include <yt/core/rpc/service.h>

#include <yt/core/tracing/sampler.h>

#include <yt/core/ytree/ypath_proxy.h>

#include <atomic>

namespace NYT::NRpcProxy {

using namespace NRpc;
using namespace NConcurrency;
using namespace NTracing;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TProxyCoordinator
    : public IProxyCoordinator
{
public:
    virtual bool SetBannedState(bool banned) override;
    virtual bool GetBannedState() const override;

    virtual void SetBanMessage(const TString& message) override;
    virtual TString GetBanMessage() const override;

    virtual bool SetAvailableState(bool available) override;
    virtual bool GetAvailableState() const override;

    virtual void ValidateOperable() const override;

    virtual void SetDynamicConfig(TDynamicConfigPtr config) override;
    virtual TDynamicConfigPtr GetDynamicConfig() const override;
    virtual TSampler* GetTraceSampler() override;

    virtual NYTree::IYPathServicePtr CreateOrchidService() override;

private:
    std::atomic<bool> IsBanned_ = {false};
    std::atomic<bool> IsAvailable_ = {false};

    TSpinLock BanSpinLock_;
    TString BanMessage_;

    NTracing::TSampler Sampler_;

    TReaderWriterSpinLock ConfigLock_;
    TDynamicConfigPtr Config_ = New<TDynamicConfig>();

    void BuildOrchid(IYsonConsumer* consumer);
};

bool TProxyCoordinator::SetBannedState(bool banned)
{
    return IsBanned_.exchange(banned, std::memory_order_relaxed) != banned;
}

bool TProxyCoordinator::GetBannedState() const
{
    return IsBanned_.load(std::memory_order_relaxed);
}

void TProxyCoordinator::SetBanMessage(const TString& message)
{
    auto guard = Guard(BanSpinLock_);
    BanMessage_ = message;
}

TString TProxyCoordinator::GetBanMessage() const
{
    auto guard = Guard(BanSpinLock_);
    return BanMessage_;
}

bool TProxyCoordinator::SetAvailableState(bool available)
{
    return IsAvailable_.exchange(available, std::memory_order_relaxed) != available;
}

bool TProxyCoordinator::GetAvailableState() const
{
    return IsAvailable_.load(std::memory_order_relaxed);
}

void TProxyCoordinator::ValidateOperable() const
{
    if (!GetAvailableState()) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Proxy cannot synchronize with cluster");
    }
    if (GetBannedState()) {
        THROW_ERROR_EXCEPTION(NApi::NRpcProxy::EErrorCode::ProxyBanned, "Proxy has been banned")
            << TErrorAttribute("message", GetBanMessage());
    }
}

void TProxyCoordinator::SetDynamicConfig(TDynamicConfigPtr config)
{
    if (config->Tracing) {
        Sampler_.UpdateConfig(config->Tracing);
        Sampler_.ResetPerUserLimits();
    }

    TWriterGuard guard(ConfigLock_);
    std::swap(Config_, config);
}

TDynamicConfigPtr TProxyCoordinator::GetDynamicConfig() const
{
    TReaderGuard guard(ConfigLock_);
    return Config_;
}

TSampler* TProxyCoordinator::GetTraceSampler()
{
    return &Sampler_;
}

NYTree::IYPathServicePtr TProxyCoordinator::CreateOrchidService()
{
   return IYPathService::FromProducer(BIND(&TProxyCoordinator::BuildOrchid, MakeStrong(this)));
}

void TProxyCoordinator::BuildOrchid(IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("dynamic_config").Value(GetDynamicConfig())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

IProxyCoordinatorPtr CreateProxyCoordinator()
{
    return New<TProxyCoordinator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
