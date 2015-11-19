#include "stdafx.h"
#include "fair_share_queue_scheduler_thread.h"

namespace NYT {
namespace NConcurrency {

///////////////////////////////////////////////////////////////////////////////

TFairShareQueueSchedulerThread::TFairShareQueueSchedulerThread(
    TFairShareInvokerQueuePtr queue,
    TEventCount* callbackEventCount,
    const Stroka& threadName,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling)
    : TSchedulerThread(
        callbackEventCount,
        threadName,
        tagIds,
        enableLogging,
        enableProfiling)
    , Queue(std::move(queue))
{ }

TFairShareQueueSchedulerThread::~TFairShareQueueSchedulerThread()
{ }

IInvokerPtr TFairShareQueueSchedulerThread::GetInvoker(int index)
{
    return Queue->GetInvoker(index);
}

EBeginExecuteResult TFairShareQueueSchedulerThread::BeginExecute()
{
    return Queue->BeginExecute(&CurrentAction);
}

void TFairShareQueueSchedulerThread::EndExecute()
{
    Queue->EndExecute(&CurrentAction);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

