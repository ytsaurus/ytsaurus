#pragma once

#include "public.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct ITwoLevelFairShareThreadPool
    : public virtual TRefCounted
    , public IShutdownable
{
    virtual IInvokerPtr GetInvoker(
        const TString& poolName,
        double weight,
        const TFairShareThreadPoolTag& tag) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITwoLevelFairShareThreadPool);

ITwoLevelFairShareThreadPoolPtr CreateTwoLevelFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    bool enableLogging = true,
    bool enableProfiling = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

