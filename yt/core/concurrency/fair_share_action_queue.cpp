#include "stdafx.h"
#include "fair_share_action_queue.h"
#include "fair_share_queue_scheduler_thread.h"
#include "profiler_utils.h"

#include <core/actions/invoker_detail.h>

#include <core/ypath/token.h>

#include <core/profiling/profile_manager.h>

namespace NYT {
namespace NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

class TFairShareActionQueue::TImpl
    : public TRefCounted
{
public:
    TImpl(
        const Stroka& threadName,
        const std::vector<Stroka>& bucketNames,
        bool enableLogging,
        bool enableProfiling)
        : Queue_(New<TFairShareInvokerQueue>(
            &CallbackEventCount_,
            GetBucketsTagIds(enableProfiling, threadName, bucketNames),
            enableLogging,
            enableProfiling))
        , Thread_(New<TFairShareQueueSchedulerThread>(
            Queue_,
            &CallbackEventCount_,
            threadName,
            GetThreadTagIds(enableProfiling, threadName),
            enableLogging,
            enableProfiling))
    { }

    ~TImpl()
    {
        Shutdown();
    }

    void Start()
    {
        Thread_->Start();
        // XXX(sandello): Racy! Fix me by moving this into OnThreadStart().
        Queue_->SetThreadId(Thread_->GetId());
    }

    void Shutdown()
    {
        Queue_->Shutdown();
        Thread_->Shutdown();
    }

    bool IsStarted() const
    {
        return Thread_->IsStarted();
    }

    IInvokerPtr GetInvoker(int index)
    {
        if (Y_UNLIKELY(!IsStarted())) {
            Start();
        }
        return Queue_->GetInvoker(index);
    }

private:
    TEventCount CallbackEventCount_;
    TFairShareInvokerQueuePtr Queue_;
    TFairShareQueueSchedulerThreadPtr Thread_;
};

TFairShareActionQueue::TFairShareActionQueue(
    const Stroka& threadName,
    const std::vector<Stroka>& bucketNames,
    bool enableLogging,
    bool enableProfiling)
    : Impl_(New<TImpl>(threadName, bucketNames, enableLogging, enableProfiling))
{ }

TFairShareActionQueue::~TFairShareActionQueue()
{ }

IInvokerPtr TFairShareActionQueue::GetInvoker(int index)
{
    return Impl_->GetInvoker(index);
}

void TFairShareActionQueue::Shutdown()
{
    return Impl_->Shutdown();
}

TCallback<TFairShareActionQueuePtr()> TFairShareActionQueue::CreateFactory(
    const Stroka& threadName,
    const std::vector<Stroka>& bucketNames,
    bool enableLogging,
    bool enableProfiling)
{
    return BIND(&New<TFairShareActionQueue, const Stroka&, const std::vector<Stroka>&, const bool&, const bool&>,
        threadName,
        bucketNames,
        enableLogging,
        enableProfiling);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

