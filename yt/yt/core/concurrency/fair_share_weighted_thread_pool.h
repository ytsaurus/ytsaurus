#pragma once

#include "public.h"

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IInvokerWithExpectedSize
    : public virtual IInvoker
{
    virtual void InvokeWithExpectedSize(TClosure callback, i64 expectedSize) = 0;
    virtual void InvokeWithExpectedSize(TMutableRange<std::pair<TClosure, i64>> callbacks) = 0;
};

DECLARE_REFCOUNTED_STRUCT(IInvokerWithExpectedSize)
DEFINE_REFCOUNTED_TYPE(IInvokerWithExpectedSize)

////////////////////////////////////////////////////////////////////////////////

// A copy of ITwoLevelFairShareThreadPool with GetInvokerWithExpectedSize instead of GetInvoker.
struct IFairShareWeightedThreadPool
    : public virtual TRefCounted
{
    virtual int GetThreadCount() = 0;
    virtual void Configure(int threadCount) = 0;
    virtual void Configure(TDuration pollingPeriod) = 0;

    virtual IInvokerWithExpectedSizePtr GetInvokerWithExpectedSize(
        const TString& poolName,
        const TFairShareThreadPoolTag& tag,
        double bucketWeight = 1.0) = 0;

    virtual void Shutdown() = 0;

    using TWaitTimeObserver = std::function<void(TDuration)>;
    virtual void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver) = 0;
};
DEFINE_REFCOUNTED_TYPE(IFairShareWeightedThreadPool)

////////////////////////////////////////////////////////////////////////////////

struct TFairShareWeightedThreadPoolOptions
{
    IPoolWeightProviderPtr PoolWeightProvider = nullptr;
    bool VerboseLogging = false;
    TDuration PollingPeriod = TDuration::MilliSeconds(10);
    TDuration PoolRetentionTime = TDuration::Seconds(30);
};

IFairShareWeightedThreadPoolPtr CreateFairShareWeightedThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    const TFairShareWeightedThreadPoolOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
