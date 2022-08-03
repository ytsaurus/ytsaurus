#include "thread_pool.h"
#include "notify_manager.h"
#include "single_queue_scheduler_thread.h"
#include "private.h"
#include "profiling_helpers.h"
#include "thread_pool_detail.h"

#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ypath/token.h>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// This routines could be placed in TThreadPool class but it causes a circular ref.
class TInvokerQueueAdapter
    : public TMpmcInvokerQueue
    , public TNotifyManager
{
public:
    TInvokerQueueAdapter(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TTagSet& counterTagSet)
        : TMpmcInvokerQueue(callbackEventCount, counterTagSet)
        , TNotifyManager(callbackEventCount)
    { }

    int GetQueueSize() const override
    {
        return TMpmcInvokerQueue::GetSize();
    }

    TClosure OnExecute(TEnqueuedAction* action, bool fetchNext, std::function<bool()> isStopping)
    {
        TMpmcInvokerQueue::EndExecute(action);

        if (!fetchNext) {
            return {};
        }

        IncrementWaiters();
        auto finally = Finally([this] {
            DecrementWaiters();
        });

        while (true) {
            auto cookie = GetEventCount()->PrepareWait();

            auto minEnqueuedAt = GetMinEnqueuedAt();
            auto callback = TMpmcInvokerQueue::BeginExecute(action);

            if (callback || isStopping()) {
                CancelWait();

                if (callback) {
                    YT_ASSERT(action->EnqueuedAt > 0);

                    // Increment minEnqueuedAt to prevent minEnqueuedAt to be resetted.
                    minEnqueuedAt = std::max(minEnqueuedAt + 1, action->EnqueuedAt);

                    auto cpuInstant = GetCpuInstant();
                    NotifyAfterFetch(cpuInstant, minEnqueuedAt);
                }

                return callback;
            }

            ResetMinEnqueuedAtIfEqual(minEnqueuedAt);

            Wait(cookie, isStopping);
            TMpmcInvokerQueue::EndExecute(action);
        }
    }

    void Invoke(TClosure callback) override
    {
        auto cpuInstant = TMpmcInvokerQueue::EnqueueCallback(
            std::move(callback),
            /*profilingTag*/ 0,
            /*profilerTag*/ nullptr);

        NotifyFromInvoke(cpuInstant);
    }

    void Configure(int threadCount)
    {
        ThreadCount_.store(threadCount);
    }

private:
    std::atomic<int> ThreadCount_ = 0;
};

class TThreadPoolThread
    : public TSchedulerThread
{
public:
    TThreadPoolThread(
        TIntrusivePtr<TInvokerQueueAdapter> queue,
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName,
        int shutdownPriority = 0)
        : TSchedulerThread(
            callbackEventCount,
            threadGroupName,
            threadName,
            shutdownPriority)
        , Queue_(std::move(queue))
    { }

protected:
    const TIntrusivePtr<TInvokerQueueAdapter> Queue_;
    TEnqueuedAction CurrentAction_;

    TClosure OnExecute() override
    {
        bool fetchNext = !TSchedulerThread::IsStopping() || TSchedulerThread::GracefulStop_;

        return Queue_->OnExecute(&CurrentAction_, fetchNext, [&] {
            return TSchedulerThread::IsStopping();
        });
    }

    TClosure BeginExecute() override
    {
        Y_UNREACHABLE();
    }

    void EndExecute() override
    {
        Y_UNREACHABLE();
    }
};

class TThreadPool::TImpl
    : public TThreadPoolBase
{
public:
    TImpl(
        int threadCount,
        const TString& threadNamePrefix,
        bool startThreads)
        : TThreadPoolBase(threadNamePrefix)
        , Queue_(New<TInvokerQueueAdapter>(
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
    const TIntrusivePtr<TInvokerQueueAdapter> Queue_;
    const IInvokerPtr Invoker_;


    void DoConfigure(int threadCount) override
    {
        Queue_->Configure(threadCount);
        TThreadPoolBase::DoConfigure(threadCount);
    }

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
        return New<TThreadPoolThread>(
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
