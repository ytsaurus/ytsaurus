#include "proxy_coordinator.h"

#include <yt/ytlib/security_client/public.h>

#include <yt/core/rpc/public.h>
#include <yt/core/rpc/service.h>

#include <yt/core/tracing/sampler.h>

#include <yt/core/ytree/ypath_proxy.h>

#include <yt/core/misc/atomic_object.h>

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

    virtual void SetDynamicConfig(TDynamicProxyConfigPtr config) override;
    virtual TDynamicProxyConfigPtr GetDynamicConfig() const override;
    virtual TSampler* GetTraceSampler() override;

    virtual NYTree::IYPathServicePtr CreateOrchidService() override;

private:
    std::atomic<bool> Banned_ = false;
    std::atomic<bool> Available_ = false;

    TSpinLock BanSpinLock_;
    TString BanMessage_;

    NTracing::TSampler Sampler_;

    TAtomicObject<TDynamicProxyConfigPtr> Config_{New<TDynamicProxyConfig>()};

    void BuildOrchid(IYsonConsumer* consumer);
};

bool TProxyCoordinator::SetBannedState(bool banned)
{
    return Banned_.exchange(banned, std::memory_order_relaxed) != banned;
}

bool TProxyCoordinator::GetBannedState() const
{
    return Banned_.load(std::memory_order_relaxed);
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
    return Available_.exchange(available, std::memory_order_relaxed) != available;
}

bool TProxyCoordinator::GetAvailableState() const
{
    return Available_.load(std::memory_order_relaxed);
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

void TProxyCoordinator::SetDynamicConfig(TDynamicProxyConfigPtr config)
{
    if (config->Tracing) {
        Sampler_.UpdateConfig(config->Tracing);
        Sampler_.ResetPerUserLimits();
    }

    Config_.Store(std::move(config));
}

TDynamicProxyConfigPtr TProxyCoordinator::GetDynamicConfig() const
{
    return Config_.Load();
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
