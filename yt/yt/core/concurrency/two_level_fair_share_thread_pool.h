#pragma once

#include "public.h"

#include <yt/yt/core/misc/shutdownable.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct ITwoLevelFairShareThreadPool
    : public virtual TRefCounted
    , public IShutdownable
{
    virtual void Configure(int threadCount) = 0;

    virtual IInvokerPtr GetInvoker(
        const TString& poolName,
        double weight,
        const TFairShareThreadPoolTag& tag) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITwoLevelFairShareThreadPool)

ITwoLevelFairShareThreadPoolPtr CreateTwoLevelFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

