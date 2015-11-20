#include "stdafx.h"
#include "thread_pool.h"
#include "single_queue_scheduler_thread.h"
#include "private.h"
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
            &CallbackEventCount_,
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

        // XXX(sandello): This is racy with other methods. Fix me.
        for (int i = Threads_.size(); i < threadCount; ++i) {
            Threads_.emplace_back(SpawnThread(i));
        }
        for (int i = threadCount; i < Threads_.size(); ++i) {
            Threads_.back()->Shutdown();
            Threads_.back().Reset();
            Threads_.pop_back();
        }
        if (Started_.load(std::memory_order_relaxed)) {
            Start();
        }
    }

    void Start()
    {
        for (auto& thread : Threads_) {
            thread->Start();
        }
    }

    void Shutdown()
    {
        Queue_->Shutdown();
        for (auto& thread : Threads_) {
            thread->Shutdown();
        }
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
    Stroka ThreadNamePrefix_;
    bool EnableLogging_;
    bool EnableProfiling_;

    std::atomic<bool> Started_ = {false};
    TEventCount CallbackEventCount_;
    TInvokerQueuePtr Queue_;
    std::vector<TSchedulerThreadPtr> Threads_;

    TSchedulerThreadPtr SpawnThread(int index)
    {
        return New<TSingleQueueSchedulerThread>(
            Queue_,
            &CallbackEventCount_,
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
    : Impl(New<TImpl>(threadCount, threadNamePrefix, enableLogging, enableProfiling))
{ }

TThreadPool::~TThreadPool()
{ }

void TThreadPool::Shutdown()
{
    return Impl->Shutdown();
}

void TThreadPool::Configure(int threadCount)
{
    return Impl->Configure(threadCount);
}

IInvokerPtr TThreadPool::GetInvoker()
{
    return Impl->GetInvoker();
}

TCallback<TThreadPoolPtr()> TThreadPool::CreateFactory(
    int threadCount,
    const Stroka& threadName,
    bool enableLogging,
    bool enableProfiling)
{
    return BIND(&New<TThreadPool, const int&, const Stroka&, const bool&, const bool&>,
        threadCount,
        threadName,
        enableLogging,
        enableProfiling);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

