#pragma once

#include "public.h"
#include "chaos_lease.h"

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct IChaosLeaseCache
    : public virtual TRefCounted
{
    virtual TFuture<TChaosLeasePtr> GetChaosLease(TChaosLeaseId chaosLeaseId) = 0;
    virtual void Clear() = 0;
    virtual void Reconfigure(const TChaosLeaseCacheConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosLeaseCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
