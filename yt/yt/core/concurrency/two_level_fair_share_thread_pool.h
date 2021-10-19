#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct ITwoLevelFairShareThreadPool
    : public virtual TRefCounted
{
    virtual void Configure(int threadCount) = 0;

    virtual IInvokerPtr GetInvoker(
        const TString& poolName,
        double weight,
        const TFairShareThreadPoolTag& tag) = 0;

    virtual void Shutdown() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITwoLevelFairShareThreadPool)

ITwoLevelFairShareThreadPoolPtr CreateTwoLevelFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

