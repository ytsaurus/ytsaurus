#pragma once

#include "private.h"
#include "scheduler_thread.h"
#include "invoker_queue.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
class TSingleQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TSingleQueueSchedulerThread(
        TInvokerQueuePtr<TQueueImpl> queue,
        TIntrusivePtr<TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName);

protected:
    const TInvokerQueuePtr<TQueueImpl> Queue_;

    TEnqueuedAction CurrentAction_;

    virtual TClosure BeginExecute() override;
    virtual void EndExecute() override;

    virtual void OnStart() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
