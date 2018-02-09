#pragma once

#include "public.h"

#include <yt/server/cell_proxy/public.h>

#include <yt/ytlib/rpc_proxy/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Thread affinity: any
 */
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

    virtual bool IsOperable(const NRpc::IServiceContextPtr& context) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IProxyCoordinator)

////////////////////////////////////////////////////////////////////////////////

IProxyCoordinatorPtr CreateProxyCoordinator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
