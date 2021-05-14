#include "proxy_coordinator.h"

#include "bootstrap.h"
#include "dynamic_config_manager.h"

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

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
    explicit TProxyCoordinator(TBootstrap* bootstrap);

    virtual void Initialize() override;

    virtual bool SetBannedState(bool banned) override;
    virtual bool GetBannedState() const override;

    virtual void SetBanMessage(const TString& message) override;
    virtual TString GetBanMessage() const override;

    virtual void SetProxyRole(const std::optional<TString>& role) override;
    virtual std::optional<TString> GetProxyRole() const override;

    virtual bool SetAvailableState(bool available) override;
    virtual bool GetAvailableState() const override;

    virtual bool GetOperableState() const override;
    virtual void ValidateOperable() const override;

    virtual TSampler* GetTraceSampler() override;

public:
    DEFINE_SIGNAL(void(const std::optional<TString>&), OnProxyRoleChanged);

private:
    TBootstrap* Bootstrap_;

    std::atomic<bool> Banned_ = false;
    std::atomic<bool> Available_ = false;

    TAtomicObject<TString> BanMessage_;

    TAtomicObject<std::optional<TString>> ProxyRole_;

    NTracing::TSampler Sampler_;

    void BuildOrchid(IYsonConsumer* consumer);

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig);
};

TProxyCoordinator::TProxyCoordinator(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

void TProxyCoordinator::Initialize()
{
    const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
    dynamicConfigManager->SubscribeConfigChanged(BIND(&TProxyCoordinator::OnDynamicConfigChanged, MakeWeak(this)));
}

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
    BanMessage_.Store(message);
}

TString TProxyCoordinator::GetBanMessage() const
{
    return BanMessage_.Load();
}

void TProxyCoordinator::SetProxyRole(const std::optional<TString>& role)
{
    ProxyRole_.Store(role);
    OnProxyRoleChanged_.Fire(role);
}

std::optional<TString> TProxyCoordinator::GetProxyRole() const
{
    return ProxyRole_.Load();
}

bool TProxyCoordinator::SetAvailableState(bool available)
{
    return Available_.exchange(available, std::memory_order_relaxed) != available;
}

bool TProxyCoordinator::GetAvailableState() const
{
    return Available_.load(std::memory_order_relaxed);
}

bool TProxyCoordinator::GetOperableState() const
{
    return GetAvailableState() && !GetBannedState();
}

void TProxyCoordinator::ValidateOperable() const
{
    if (!GetAvailableState()) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Proxy is unable to synchronize with the cluster");
    }
    if (GetBannedState()) {
        THROW_ERROR_EXCEPTION(NApi::NRpcProxy::EErrorCode::ProxyBanned, "Proxy is banned")
            << TErrorAttribute("message", GetBanMessage());
    }
}

TSampler* TProxyCoordinator::GetTraceSampler()
{
    return &Sampler_;
}

void TProxyCoordinator::OnDynamicConfigChanged(
    const TProxyDynamicConfigPtr& /*oldConfig*/,
    const TProxyDynamicConfigPtr& newConfig)
{
    Sampler_.UpdateConfig(newConfig->Tracing);
}

////////////////////////////////////////////////////////////////////////////////

IProxyCoordinatorPtr CreateProxyCoordinator(TBootstrap* bootstrap)
{
    return New<TProxyCoordinator>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
