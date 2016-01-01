#include "thread_pool.h"
#include "single_queue_scheduler_thread.h"
#include "private.h"
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

class TThreadPool::TImpl
    : public TRefCounted
{
public:
    TImpl(
        int threadCount,
        const Stroka& threadNamePrefix,
        bool enableLogging,
        bool enableProfiling)
        : ThreadNamePrefix_(threadNamePrefix)
        , EnableLogging_(enableLogging)
        , EnableProfiling_(enableProfiling)
        , Queue_(New<TInvokerQueue>(
            CallbackEventCount_,
            GetThreadTagIds(enableProfiling, threadNamePrefix),
            enableLogging,
            enableProfiling))
    {
        Configure(threadCount);
    }

    ~TImpl()
    {
        Shutdown();
    }

    void Configure(int threadCount)
    {
        YCHECK(threadCount > 0);

        {
            TGuard<TSpinLock> guard(SpinLock_);

            for (int i = Threads_.size(); i < threadCount; ++i) {
                Threads_.emplace_back(SpawnThread(i));
            }

            for (int i = threadCount; i < Threads_.size(); ++i) {
                Threads_.back()->Shutdown();
                Threads_.back().Reset();
                Threads_.pop_back();
            }
        }

        if (Started_.load(std::memory_order_relaxed)) {
            Start();
        }
    }

    void Start()
    {
        decltype(Threads_) threads;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            threads = Threads_;
        }

        for (auto& thread : threads) {
            thread->Start();
        }
    }

    void Shutdown()
    {
        Queue_->Shutdown();

        decltype(Threads_) threads;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            std::swap(threads, Threads_);
        }

        GetFinalizerInvoker()->Invoke(BIND([threads = std::move(threads)] () {
            for (auto& thread : threads) {
                thread->Shutdown();
            }
        }));
    }

    IInvokerPtr GetInvoker()
    {
        if (Y_UNLIKELY(!Started_.load(std::memory_order_relaxed))) {
            // Concurrent calls to Start() are okay.
            Started_.store(true, std::memory_order_relaxed);
            Start();
        }
        return Queue_;
    }

private:
    const Stroka ThreadNamePrefix_;
    const bool EnableLogging_;
    const bool EnableProfiling_;

    std::atomic<bool> Started_ = {false};
    TSpinLock SpinLock_;

    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const TInvokerQueuePtr Queue_;
    std::vector<TSchedulerThreadPtr> Threads_;

    TSchedulerThreadPtr SpawnThread(int index)
    {
        return New<TSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            Format("%v:%v", ThreadNamePrefix_, index),
            GetThreadTagIds(EnableProfiling_, ThreadNamePrefix_),
            EnableLogging_,
            EnableProfiling_);
    }
};

TThreadPool::TThreadPool(
    int threadCount,
    const Stroka& threadNamePrefix,
    bool enableLogging,
    bool enableProfiling)
    : Impl_(New<TImpl>(
        threadCount,
        threadNamePrefix,
        enableLogging,
        enableProfiling))
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

IInvokerPtr TThreadPool::GetInvoker()
{
    return Impl_->GetInvoker();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

