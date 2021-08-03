#pragma once

#include "private.h"
#include "spinlock.h"

#include <yt/yt/core/misc/shutdownable.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TBucketDescription
{
    std::vector<NProfiling::TTagSet> QueueTagSets;
    NProfiling::TTagSet BucketTagSet;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareInvokerQueue
    : public TRefCounted
    , public IShutdownable
{
public:
    TFairShareInvokerQueue(
        TIntrusivePtr<TEventCount> callbackEventCount,
        const std::vector<TBucketDescription>& bucketDescriptions);

    ~TFairShareInvokerQueue();

    void SetThreadId(TThreadId threadId);

    const IInvokerPtr& GetInvoker(int bucketIndex, int queueIndex) const;

    virtual void Shutdown() override;

    void Drain();

    bool IsRunning() const;

    TClosure BeginExecute(TEnqueuedAction* action);
    void EndExecute(TEnqueuedAction* action);

    void Reconfigure(std::vector<double> weights);

private:
    constexpr static i64 UnitWeight = 1000;

    struct TBucket
    {
        TMpscInvokerQueuePtr Queue;
        std::vector<IInvokerPtr> Invokers;
        NProfiling::TCpuDuration ExcessTime = 0;

        // ceil(UnitWeight / weight).
        int64_t InversedWeight = UnitWeight;
    };

    std::vector<TBucket> Buckets_;

    std::atomic<bool> NeedToReconfigure_ = false;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, WeightsLock_);
    std::vector<double> Weights_;

    TBucket* CurrentBucket_ = nullptr;

    TBucket* GetStarvingBucket();
};

DEFINE_REFCOUNTED_TYPE(TFairShareInvokerQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

