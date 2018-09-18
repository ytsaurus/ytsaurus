#pragma once

#include "private.h"
#include "scheduler_thread.h"
#include "invoker_queue.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSingleQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TSingleQueueSchedulerThread(
        TInvokerQueuePtr queue,
        std::shared_ptr<TEventCount> callbackEventCount,
        const TString& threadName,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling,
        int index = 0);

    ~TSingleQueueSchedulerThread();

protected:
    const TInvokerQueuePtr Queue;
    const int Index;

    TEnqueuedAction CurrentAction;

    virtual EBeginExecuteResult BeginExecute() override;
    virtual void EndExecute() override;
};

DEFINE_REFCOUNTED_TYPE(TSingleQueueSchedulerThread)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
