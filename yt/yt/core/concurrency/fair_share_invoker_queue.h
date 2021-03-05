#pragma once

#include "private.h"

#include <yt/yt/core/misc/shutdownable.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFairShareInvokerQueue
    : public TRefCounted
    , public IShutdownable
{
public:
    TFairShareInvokerQueue(
        std::shared_ptr<TEventCount> callbackEventCount,
        const std::vector<NProfiling::TTagSet>& bucketsTags);

    ~TFairShareInvokerQueue();

    void SetThreadId(TThreadId threadId);

    const IInvokerPtr& GetInvoker(int index);

    virtual void Shutdown() override;

    void Drain();

    bool IsRunning() const;

    TClosure BeginExecute(TEnqueuedAction* action);
    void EndExecute(TEnqueuedAction* action);

private:
    struct TBucket
    {
        TMpscInvokerQueuePtr Queue;
        IInvokerPtr Invoker;
        NProfiling::TCpuDuration ExcessTime = 0;
    };

    std::vector<TBucket> Buckets_;

    TBucket* CurrentBucket_ = nullptr;

    TBucket* GetStarvingBucket();
};

DEFINE_REFCOUNTED_TYPE(TFairShareInvokerQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

