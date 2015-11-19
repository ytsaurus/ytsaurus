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
        : Queue_(New<TInvokerQueue>(
            &CallbackEventCount_,
            GetThreadTagIds(enableProfiling, threadNamePrefix),
            enableLogging,
            enableProfiling))
        , Threads_(threadCount)
    {
        for (int i = 0; i < threadCount; ++i) {
            Threads_[i] = New<TSingleQueueSchedulerThread>(
                Queue_,
                &CallbackEventCount_,
                Format("%v:%v", threadNamePrefix, i),
                GetThreadTagIds(enableProfiling, threadNamePrefix),
                enableLogging,
                enableProfiling);
        }
    }

    ~TImpl()
    {
        Shutdown();
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
            Start();
            Started_.store(true, std::memory_order_relaxed);
        }
        return Queue_;
    }

private:
    std::atomic<bool> Started_ = {false};
    TEventCount CallbackEventCount_;
    TInvokerQueuePtr Queue_;
    std::vector<TSchedulerThreadPtr> Threads_;
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

