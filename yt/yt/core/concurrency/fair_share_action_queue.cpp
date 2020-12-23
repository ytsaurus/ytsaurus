#include "fair_share_action_queue.h"
#include "fair_share_queue_scheduler_thread.h"
#include "profiling_helpers.h"

#include <yt/core/actions/invoker_detail.h>

#include <yt/core/ypath/token.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TFairShareActionQueue::TImpl
    : public TRefCounted
{
public:
    TImpl(
        const TString& threadName,
        const std::vector<TString>& bucketNames)
        : Queue_(New<TFairShareInvokerQueue>(
            CallbackEventCount_,
            GetBucketsTags(threadName, bucketNames)))
        , Thread_(New<TFairShareQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            threadName))
    { }

    ~TImpl()
    {
        Shutdown();
    }

    void Shutdown()
    {
        bool expected = false;
        if (!ShutdownFlag_.compare_exchange_strong(expected, true)) {
            return;
        }

        Queue_->Shutdown();

        FinalizerInvoker_->Invoke(BIND([thread = Thread_, queue = Queue_] {
            thread->Shutdown();
            queue->Drain();
        }));
        FinalizerInvoker_.Reset();
    }

    const IInvokerPtr& GetInvoker(int index)
    {
        EnsuredStarted();
        return Queue_->GetInvoker(index);
    }

private:
    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const TFairShareInvokerQueuePtr Queue_;
    const TFairShareQueueSchedulerThreadPtr Thread_;

    std::atomic<bool> StartFlag_ = false;
    std::atomic<bool> ShutdownFlag_ = false;

    IInvokerPtr FinalizerInvoker_ = GetFinalizerInvoker();

    void EnsuredStarted()
    {
        bool expected = false;
        if (!StartFlag_.compare_exchange_strong(expected, true)) {
            return;
        }

        Thread_->Start();
    }
};

////////////////////////////////////////////////////////////////////////////////

TFairShareActionQueue::TFairShareActionQueue(
    const TString& threadName,
    const std::vector<TString>& bucketNames)
    : Impl_(New<TImpl>(
        threadName,
        bucketNames))
{ }

namespace {

std::vector<TString> ToVector(TRange<TStringBuf> range)
{
    std::vector<TString> result;
    for (auto item : range) {
        result.emplace_back(item);
    }
    return result;
}

} // namespace

TFairShareActionQueue::TFairShareActionQueue(
    const TString& threadName,
    TRange<TStringBuf> bucketNames)
    : TFairShareActionQueue(
        threadName,
        ToVector(bucketNames))
{ }

TFairShareActionQueue::~TFairShareActionQueue() = default;

const IInvokerPtr& TFairShareActionQueue::GetInvoker(int index)
{
    return Impl_->GetInvoker(index);
}

void TFairShareActionQueue::Shutdown()
{
    return Impl_->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

