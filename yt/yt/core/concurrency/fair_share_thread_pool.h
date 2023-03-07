#pragma once

#include "public.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IFairShareThreadPool
    : public virtual TRefCounted
    , public IShutdownable
{
    virtual IInvokerPtr GetInvoker(const TFairShareThreadPoolTag& tag) = 0;

    virtual void Configure(int threadCount) = 0;
};

IFairShareThreadPoolPtr CreateFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    bool enableLogging = true,
    bool enableProfiling = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

