#include "proxy_coordinator.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <atomic>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TProxyCoordinator
    : public IProxyCoordinator
{
public:
    bool SetBannedState(bool banned) override;
    bool GetBannedState() const override;

    void SetBanMessage(const TString& message) override;
    TString GetBanMessage() const override;

    void SetProxyRole(const std::optional<std::string>& role) override;
    std::optional<std::string> GetProxyRole() const override;

    bool SetAvailableState(bool available) override;
    bool GetAvailableState() const override;

    bool GetOperableState() const override;
    void ValidateOperable() const override;

    DEFINE_SIGNAL_OVERRIDE(void(const std::optional<std::string>&), OnProxyRoleChanged);

private:
    std::atomic<bool> Banned_ = false;
    std::atomic<bool> Available_ = false;

    NThreading::TAtomicObject<TString> BanMessage_;

    NThreading::TAtomicObject<std::optional<std::string>> ProxyRole_;
};

////////////////////////////////////////////////////////////////////////////////

bool TProxyCoordinator::SetBannedState(bool banned)
{
    return Banned_.exchange(banned, std::memory_order::relaxed) != banned;
}

bool TProxyCoordinator::GetBannedState() const
{
    return Banned_.load(std::memory_order::relaxed);
}

void TProxyCoordinator::SetBanMessage(const TString& message)
{
    BanMessage_.Store(message);
}

TString TProxyCoordinator::GetBanMessage() const
{
    return BanMessage_.Load();
}

void TProxyCoordinator::SetProxyRole(const std::optional<std::string>& role)
{
    ProxyRole_.Store(role);
    OnProxyRoleChanged_.Fire(role);
}

std::optional<std::string> TProxyCoordinator::GetProxyRole() const
{
    return ProxyRole_.Load();
}

bool TProxyCoordinator::SetAvailableState(bool available)
{
    return Available_.exchange(available, std::memory_order::relaxed) != available;
}

bool TProxyCoordinator::GetAvailableState() const
{
    return Available_.load(std::memory_order::relaxed);
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

////////////////////////////////////////////////////////////////////////////////

IProxyCoordinatorPtr CreateProxyCoordinator()
{
    return New<TProxyCoordinator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
