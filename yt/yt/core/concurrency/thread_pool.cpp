#include "thread_pool.h"
#include "single_queue_scheduler_thread.h"
#include "private.h"
#include "profiling_helpers.h"

#include <yt/core/actions/invoker_detail.h>

#include <yt/core/ypath/token.h>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TThreadPool::TImpl
    : public TRefCounted
{
public:
    TImpl(
        int threadCount,
        const TString& threadNamePrefix,
        bool enableLogging,
        bool enableProfiling,
        EInvokerQueueType queueType)
        : ThreadNamePrefix_(threadNamePrefix)
        , EnableLogging_(enableLogging)
        , EnableProfiling_(enableProfiling)
        , Queue_(New<TInvokerQueue>(
            CallbackEventCount_,
            GetThreadTags(enableProfiling, threadNamePrefix),
            enableLogging,
            enableProfiling,
            queueType))
        , Invoker_(Queue_)
    {
        Configure(threadCount);
    }

    ~TImpl()
    {
        Shutdown();
    }

    void Configure(int threadCount)
    {
        YT_VERIFY(threadCount > 0);

        decltype(Threads_) threadsToStart;
        decltype(Threads_) threadsToShutdown;
        {
            TGuard<TAdaptiveLock> guard(SpinLock_);

            while (static_cast<int>(Threads_.size()) < threadCount) {
                auto thread = SpawnThread(static_cast<int>(Threads_.size()));
                threadsToStart.push_back(thread);
                Threads_.push_back(thread);
            }

            while (static_cast<int>(Threads_.size()) > threadCount) {
                threadsToShutdown.push_back(Threads_.back());
                Threads_.pop_back();
            }
        }

        for (const auto& thread : threadsToStart) {
            thread->Start();
        }
        for (const auto& thread : threadsToShutdown) {
            thread->Shutdown();
        }
    }

    void Shutdown()
    {
        bool expected = false;
        if (!ShutdownFlag_.compare_exchange_strong(expected, true)) {
            return;
        }

        StartFlag_ = true;

        Queue_->Shutdown();

        decltype(Threads_) threads;
        {
            TGuard<TAdaptiveLock> guard(SpinLock_);
            std::swap(threads, Threads_);
        }

        FinalizerInvoker_->Invoke(BIND([threads = std::move(threads), queue = Queue_] () {
            for (auto& thread : threads) {
                thread->Shutdown();
            }
            queue->Drain();
        }));
        FinalizerInvoker_.Reset();
    }

    const IInvokerPtr& GetInvoker()
    {
        EnsureStarted();
        return Invoker_;
    }

private:
    const TString ThreadNamePrefix_;
    const bool EnableLogging_;
    const bool EnableProfiling_;

    std::atomic<bool> StartFlag_ = false;
    std::atomic<bool> ShutdownFlag_ = false;

    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const TInvokerQueuePtr Queue_;
    const IInvokerPtr Invoker_;

    IInvokerPtr FinalizerInvoker_ = GetFinalizerInvoker();

    TAdaptiveLock SpinLock_;
    std::vector<TSchedulerThreadPtr> Threads_;

    void EnsureStarted()
    {
        bool expected = false;
        if (!StartFlag_.compare_exchange_strong(expected, true)) {
            return;
        }

        decltype(Threads_) threads;
        {
            TGuard<TAdaptiveLock> guard(SpinLock_);
            threads = Threads_;
        }

        for (const auto& thread : threads) {
            thread->Start();
        }
    }

    TSchedulerThreadPtr SpawnThread(int index)
    {
        return New<TSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            Format("%v:%v", ThreadNamePrefix_, index),
            GetThreadTags(EnableProfiling_, ThreadNamePrefix_),
            EnableLogging_,
            EnableProfiling_);
    }
};

TThreadPool::TThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    bool enableLogging,
    bool enableProfiling,
    EInvokerQueueType queueType)
    : Impl_(New<TImpl>(
        threadCount,
        threadNamePrefix,
        enableLogging,
        enableProfiling,
        queueType))
{ }

TThreadPool::~TThreadPool() = default;

void TThreadPool::Shutdown()
{
    return Impl_->Shutdown();
}

void TThreadPool::Configure(int threadCount)
{
    return Impl_->Configure(threadCount);
}

const IInvokerPtr& TThreadPool::GetInvoker()
{
    return Impl_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

