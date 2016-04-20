#pragma once

#include "scheduler_thread.h"

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
        TEventCount* callbackEventCount,
        const Stroka& threadName,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    ~TSingleQueueSchedulerThread();

    IInvokerPtr GetInvoker();

protected:
    TInvokerQueuePtr Queue;

    TEnqueuedAction CurrentAction;

    virtual EBeginExecuteResult BeginExecute() override;
    virtual void EndExecute() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
