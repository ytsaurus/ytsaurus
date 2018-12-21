#pragma once

#include "scheduler_thread.h"
#include "fair_share_invoker_queue.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFairShareQueueSchedulerThread;
typedef TIntrusivePtr<TFairShareQueueSchedulerThread> TFairShareQueueSchedulerThreadPtr;

////////////////////////////////////////////////////////////////////////////////

class TFairShareQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TFairShareQueueSchedulerThread(
        TFairShareInvokerQueuePtr queue,
        std::shared_ptr<TEventCount> callbackEventCount,
        const TString& threadName,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    ~TFairShareQueueSchedulerThread();

    IInvokerPtr GetInvoker(int index);

protected:
    const TFairShareInvokerQueuePtr Queue_;

    TEnqueuedAction CurrentAction_;

    virtual EBeginExecuteResult BeginExecute() override;
    virtual void EndExecute() override;
};

DEFINE_REFCOUNTED_TYPE(TFairShareQueueSchedulerThread)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
