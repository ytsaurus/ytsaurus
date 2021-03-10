#pragma once

#include "public.h"

#include "config.h"

#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct IProxyCoordinator
    : public virtual TRefCounted
{
    virtual bool SetBannedState(bool banned) = 0;
    virtual bool GetBannedState() const = 0;

    virtual void SetBanMessage(const TString& message) = 0;
    virtual TString GetBanMessage() const = 0;

    virtual void SetProxyRole(const std::optional<TString>& role) = 0;
    virtual std::optional<TString> GetProxyRole() const = 0;

    virtual bool SetAvailableState(bool available) = 0;
    virtual bool GetAvailableState() const = 0;

    virtual bool GetOperableState() const = 0;
    virtual void ValidateOperable() const = 0;

    virtual void SetDynamicConfig(TDynamicProxyConfigPtr config) = 0;
    virtual TDynamicProxyConfigPtr GetDynamicConfig() const = 0;
    virtual NTracing::TSampler* GetTraceSampler() = 0;

    virtual NYTree::IYPathServicePtr CreateOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IProxyCoordinator)

////////////////////////////////////////////////////////////////////////////////

IProxyCoordinatorPtr CreateProxyCoordinator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
