#include "thread_pool.h"
#include "single_queue_scheduler_thread.h"
#include "private.h"
#include "profiling_helpers.h"
#include "thread_pool_detail.h"

#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/ypath/token.h>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TThreadPool::TImpl
    : public TThreadPoolBase
{
public:
    TImpl(
        int threadCount,
        const TString& threadNamePrefix,
        bool startThreads)
        : TThreadPoolBase(threadNamePrefix)
        , Queue_(New<TMpmcInvokerQueue>(
            CallbackEventCount_,
            GetThreadTags(ThreadNamePrefix_)))
        , Invoker_(Queue_)
    {
        Configure(threadCount);
        if (startThreads) {
            EnsureStarted();
        }
    }

    ~TImpl()
    {
        Shutdown();
    }

    const IInvokerPtr& GetInvoker()
    {
        EnsureStarted();
        return Invoker_;
    }

private:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_ = New<NThreading::TEventCount>();
    const TMpmcInvokerQueuePtr Queue_;
    const IInvokerPtr Invoker_;


    void DoShutdown() override
    {
        Queue_->Shutdown();
        TThreadPoolBase::DoShutdown();
    }

    TClosure MakeFinalizerCallback() override
    {
        return BIND([queue = Queue_, callback = TThreadPoolBase::MakeFinalizerCallback()] {
            callback();
            queue->DrainConsumer();
        });
    }

    TSchedulerThreadBasePtr SpawnThread(int index) override
    {
        return New<TMpmcSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            ThreadNamePrefix_,
            MakeThreadName(index));
    }
};

////////////////////////////////////////////////////////////////////////////////

TThreadPool::TThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    bool startThreads)
    : Impl_(New<TImpl>(
        threadCount,
        threadNamePrefix,
        startThreads))
{ }

TThreadPool::~TThreadPool() = default;

void TThreadPool::Shutdown()
{
    return Impl_->Shutdown();
}

int TThreadPool::GetThreadCount()
{
    return Impl_->GetThreadCount();
}

void TThreadPool::Configure(int threadCount)
{
    return Impl_->Configure(threadCount);
}

void TThreadPool::EnsureStarted()
{
    return Impl_->EnsureStarted();
}

const IInvokerPtr& TThreadPool::GetInvoker()
{
    return Impl_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
