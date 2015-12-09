#include "fair_share_action_queue.h"
#include "fair_share_queue_scheduler_thread.h"
#include "profiler_utils.h"

#include <yt/core/actions/invoker_detail.h>

#include <yt/core/ypath/token.h>

#include <yt/core/profiling/profile_manager.h>

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
            CallbackEventCount_,
            GetBucketsTagIds(enableProfiling, threadName, bucketNames),
            enableLogging,
            enableProfiling))
        , Thread_(New<TFairShareQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
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

        GetFinalizerInvoker()->Invoke(BIND([thread = Thread_] {
            thread->Shutdown();
        }));
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
    std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const TFairShareInvokerQueuePtr Queue_;
    const TFairShareQueueSchedulerThreadPtr Thread_;
};

TFairShareActionQueue::TFairShareActionQueue(
    const Stroka& threadName,
    const std::vector<Stroka>& bucketNames,
    bool enableLogging,
    bool enableProfiling)
    : Impl_(New<TImpl>(
        threadName,
        bucketNames,
        enableLogging,
        enableProfiling))
{ }

TFairShareActionQueue::~TFairShareActionQueue() = default;

IInvokerPtr TFairShareActionQueue::GetInvoker(int index)
{
    return Impl_->GetInvoker(index);
}

void TFairShareActionQueue::Shutdown()
{
    return Impl_->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

