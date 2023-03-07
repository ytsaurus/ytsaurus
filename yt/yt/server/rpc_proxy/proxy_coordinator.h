#pragma once

#include "public.h"

#include "config.h"

#include <yt/client/api/rpc_proxy/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct IProxyCoordinator
    : public virtual TRefCounted
{
    //! Sets banned state.
    /*!
     *  Returns true on changed banned state.
     */
    virtual bool SetBannedState(bool banned) = 0;

    //! Gets banned state.
    /*!
     *  Lightweight call.
     */
    virtual bool GetBannedState() const = 0;

    virtual void SetBanMessage(const TString& message) = 0;
    virtual TString GetBanMessage() const = 0;

    //! Sets available state.
    /*!
     *  Returns true on changed available state.
     */
    virtual bool SetAvailableState(bool available) = 0;

    //! Gets available state.
    /*!
     *  Lightweight call.
     */
    virtual bool GetAvailableState() const = 0;

    virtual void ValidateOperable() const = 0;

    virtual void SetDynamicConfig(TDynamicConfigPtr config) = 0;
    virtual TDynamicConfigPtr GetDynamicConfig() const = 0;
    virtual NTracing::TSampler* GetTraceSampler() = 0;

    virtual NYTree::IYPathServicePtr CreateOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IProxyCoordinator)

////////////////////////////////////////////////////////////////////////////////

IProxyCoordinatorPtr CreateProxyCoordinator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
