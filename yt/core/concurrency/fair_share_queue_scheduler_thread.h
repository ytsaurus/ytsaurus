#pragma once

#include "scheduler_thread.h"
#include "fair_share_invoker_queue.h"

namespace NYT {
namespace NConcurrency {

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
        TEventCount* callbackEventCount,
        const Stroka& threadName,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    ~TFairShareQueueSchedulerThread();

    IInvokerPtr GetInvoker(int index);

protected:
    TFairShareInvokerQueuePtr Queue;

    TEnqueuedAction CurrentAction;

    virtual EBeginExecuteResult BeginExecute() override;
    virtual void EndExecute() override;
};

DEFINE_REFCOUNTED_TYPE(TFairShareQueueSchedulerThread)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
