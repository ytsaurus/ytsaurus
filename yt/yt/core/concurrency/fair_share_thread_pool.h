#pragma once

#include "public.h"

#include <yt/yt/core/misc/shutdownable.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IFairShareThreadPool
    : public virtual TRefCounted
    , public IShutdownable
{
    virtual IInvokerPtr GetInvoker(const TFairShareThreadPoolTag& tag) = 0;

    virtual void Configure(int threadCount) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareThreadPool)

IFairShareThreadPoolPtr CreateFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

