#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

//! A server handling requests via the NBD protocol.
struct INbdServer
    : public TRefCounted
{
    //! Registers a new #device with a given #name.
    //! Throws if #name is already in use.
    virtual void RegisterDevice(
        const TString& name,
        IBlockDevicePtr device) = 0;

    //! Tries to unregister the device with a given #name.
    //! Returns |true| upon success and |false| if no such device is registered.
    virtual bool TryUnregisterDevice(const TString& name) = 0;
};

DEFINE_REFCOUNTED_TYPE(INbdServer)

////////////////////////////////////////////////////////////////////////////////

INbdServerPtr CreateNbdServer(
    TNbdServerConfigPtr config,
    NConcurrency::IPollerPtr poller,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
