#pragma once

#include "scheduler_thread.h"
#include "invoker_queue.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSingleQueueSchedulerThread;
typedef TIntrusivePtr<TSingleQueueSchedulerThread> TSingleQueueSchedulerThreadPtr;

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

    IInvokerPtr GetInvoker();

protected:
    TInvokerQueuePtr Queue;

    TEnqueuedAction CurrentAction;
    int Index;

    virtual EBeginExecuteResult BeginExecute() override;
    virtual void EndExecute() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
