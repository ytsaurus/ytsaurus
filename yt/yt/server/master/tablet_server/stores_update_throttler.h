#pragma once

#include "public.h"

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

//! Not thread-safe. All methods should be called from StoresUpdateThrottlerInvoker.
//! Moreover, this invoker must be serialized.
struct IStoresUpdateThrottler
    : public TRefCounted
{
    virtual void Acquire(
        const std::string& bundleName,
        NTabletClient::ETabletStoresUpdateReason updateReason,
        int value) = 0;
    virtual bool TryAcquire(
        const std::string& bundleName,
        NTabletClient::ETabletStoresUpdateReason updateReason,
        int value) = 0;

    virtual void Reconfigure(const TStoresUpdateThrottlerConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IStoresUpdateThrottler)

////////////////////////////////////////////////////////////////////////////////

IStoresUpdateThrottlerPtr CreateStoresUpdateThrottler(TStoresUpdateThrottlerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
