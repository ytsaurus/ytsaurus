#pragma once

#include "scheduler_thread.h"
#include "fair_share_invoker_queue.h"
#include "invoker_queue.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFairShareQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TFairShareQueueSchedulerThread(
        TFairShareInvokerQueuePtr queue,
        std::shared_ptr<TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName);

protected:
    const TFairShareInvokerQueuePtr Queue_;

    TEnqueuedAction CurrentAction_;

    virtual TClosure BeginExecute() override;
    virtual void EndExecute() override;

    virtual void OnStart() override;
};

DEFINE_REFCOUNTED_TYPE(TFairShareQueueSchedulerThread)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
