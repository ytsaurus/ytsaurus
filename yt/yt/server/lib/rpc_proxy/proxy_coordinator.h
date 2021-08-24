#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

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

    DECLARE_INTERFACE_SIGNAL(void(const std::optional<TString>& newRole), OnProxyRoleChanged);
};

DEFINE_REFCOUNTED_TYPE(IProxyCoordinator)

////////////////////////////////////////////////////////////////////////////////

IProxyCoordinatorPtr CreateProxyCoordinator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
