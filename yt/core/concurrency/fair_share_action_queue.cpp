#include "fair_share_action_queue.h"
#include "fair_share_queue_scheduler_thread.h"
#include "profiling_helpers.h"

#include <yt/core/actions/invoker_detail.h>

#include <yt/core/ypath/token.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT {
namespace NConcurrency {

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
        const std::vector<TString>& bucketNames,
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
        bool expected = false;
        if (StartFlag_.compare_exchange_strong(expected, true)) {
            DoStart();
        }
    }

    void DoStart()
    {
        Thread_->Start();
        // XXX(sandello): Racy! Fix me by moving this into OnThreadStart().
        Queue_->SetThreadId(Thread_->GetId());
    }

    void Shutdown()
    {
        bool expected = false;
        if (ShutdownFlag_.compare_exchange_strong(expected, true)) {
            DoShutdown();
        }
    }

    void DoShutdown()
    {
        StartFlag_ = true;

        Queue_->Shutdown();

        FinalizerInvoker_->Invoke(BIND([thread = Thread_, queue = Queue_] {
            thread->Shutdown();
            queue->Drain();
        }));
        FinalizerInvoker_.Reset();
    }

    bool IsStarted() const
    {
        return Thread_->IsStarted();
    }

    const IInvokerPtr& GetInvoker(int index)
    {
        if (Y_UNLIKELY(!StartFlag_.load(std::memory_order_relaxed))) {
            Start();
        }
        return Queue_->GetInvoker(index);
    }

private:
    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const TFairShareInvokerQueuePtr Queue_;
    const TFairShareQueueSchedulerThreadPtr Thread_;

    std::atomic<bool> StartFlag_ = {false};
    std::atomic<bool> ShutdownFlag_ = {false};

    IInvokerPtr FinalizerInvoker_ = GetFinalizerInvoker();
};

////////////////////////////////////////////////////////////////////////////////

TFairShareActionQueue::TFairShareActionQueue(
    const TString& threadName,
    const std::vector<TString>& bucketNames,
    bool enableLogging,
    bool enableProfiling)
    : Impl_(New<TImpl>(
        threadName,
        bucketNames,
        enableLogging,
        enableProfiling))
{ }

namespace {

std::vector<TString> ToVector(TRange<TStringBuf> range)
{
    std::vector<TString> result;
    for (auto item : range) {
        result.push_back(TString(item));
    }
    return result;
}

} // namespace

TFairShareActionQueue::TFairShareActionQueue(
    const TString& threadName,
    TRange<TStringBuf> bucketNames,
    bool enableLogging,
    bool enableProfiling)
    : TFairShareActionQueue(
        threadName,
        ToVector(bucketNames),
        enableLogging,
        enableProfiling)
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

} // namespace NConcurrency
} // namespace NYT

