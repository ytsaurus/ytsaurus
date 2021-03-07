#pragma once

#include "private.h"

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
        std::shared_ptr<TEventCount> callbackEventCount,
        const std::vector<TBucketDescription>& bucketDescriptions);

    ~TFairShareInvokerQueue();

    void SetThreadId(TThreadId threadId);

    const IInvokerPtr& GetInvoker(int bucketIndex, int queueIndex) const;

    virtual void Shutdown() override;

    void Drain();

    bool IsRunning() const;

    TClosure BeginExecute(TEnqueuedAction* action);
    void EndExecute(TEnqueuedAction* action);

private:
    struct TBucket
    {
        TMpscInvokerQueuePtr Queue;
        std::vector<IInvokerPtr> Invokers;
        NProfiling::TCpuDuration ExcessTime = 0;
    };

    std::vector<TBucket> Buckets_;

    TBucket* CurrentBucket_ = nullptr;

    TBucket* GetStarvingBucket();
};

DEFINE_REFCOUNTED_TYPE(TFairShareInvokerQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

