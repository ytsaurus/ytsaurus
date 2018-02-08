#include "action_queue.h"
#include "single_queue_scheduler_thread.h"
#include "fair_share_queue_scheduler_thread.h"
#include "private.h"
#include "profiling_helpers.h"

#include <yt/core/actions/invoker_detail.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/ypath/token.h>

namespace NYT {
namespace NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TActionQueue::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(
        const TString& threadName,
        bool enableLogging,
        bool enableProfiling)
        : Queue_(New<TInvokerQueue>(
            CallbackEventCount_,
            GetThreadTagIds(enableProfiling, threadName),
            enableLogging,
            enableProfiling))
        , Invoker_(Queue_)
        , Thread_(New<TSingleQueueSchedulerThread>(
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

    const IInvokerPtr& GetInvoker()
    {
        if (Y_UNLIKELY(!StartFlag_.load(std::memory_order_relaxed))) {
            Start();
        }
        return Invoker_;
    }

private:
    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const TInvokerQueuePtr Queue_;
    const IInvokerPtr Invoker_;
    const TSingleQueueSchedulerThreadPtr Thread_;

    std::atomic<bool> StartFlag_ = {false};
    std::atomic<bool> ShutdownFlag_ = {false};

    IInvokerPtr FinalizerInvoker_ = GetFinalizerInvoker();
};

TActionQueue::TActionQueue(
    const TString& threadName,
    bool enableLogging,
    bool enableProfiling)
    : Impl_(New<TImpl>(threadName, enableLogging, enableProfiling))
{ }

TActionQueue::~TActionQueue() = default;

void TActionQueue::Shutdown()
{
    return Impl_->Shutdown();
}

const IInvokerPtr& TActionQueue::GetInvoker()
{
    return Impl_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

class TSerializedInvoker
    : public TInvokerWrapper
{
public:
    explicit TSerializedInvoker(IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
    {
        Lock_.clear();
    }

    virtual void Invoke(TClosure callback) override
    {
        Queue_.Enqueue(std::move(callback));
        TrySchedule();
    }

private:
    TLockFreeQueue<TClosure> Queue_;
    std::atomic_flag Lock_;


    class TInvocationGuard
    {
    public:
        explicit TInvocationGuard(TIntrusivePtr<TSerializedInvoker> owner)
            : Owner_(std::move(owner))
        { }

        TInvocationGuard(TInvocationGuard&& other) = default;
        TInvocationGuard(const TInvocationGuard& other) = delete;

        void Activate()
        {
            Y_ASSERT(!Activated_);
            Activated_ = true;
        }

        void Reset()
        {
            Owner_.Reset();
        }

        ~TInvocationGuard()
        {
            if (Owner_) {
                Owner_->OnFinished(Activated_);
            }
        }

    private:
        TIntrusivePtr<TSerializedInvoker> Owner_;
        bool Activated_ = false;

    };

    void TrySchedule()
    {
        if (Queue_.IsEmpty()) {
            return;
        }

        if (!Lock_.test_and_set(std::memory_order_acquire)) {
            UnderlyingInvoker_->Invoke(BIND(
                &TSerializedInvoker::RunCallback,
                MakeStrong(this),
                Passed(TInvocationGuard(this))));
        }
    }

    void RunCallback(TInvocationGuard invocationGuard)
    {
        invocationGuard.Activate();

        TCurrentInvokerGuard currentInvokerGuard(this);
        TOneShotContextSwitchGuard contextSwitchGuard([&] {
            invocationGuard.Reset();
            OnFinished(true);
        });

        TClosure callback;
        if (Queue_.Dequeue(&callback)) {
            callback.Run();
        }
    }

    void OnFinished(bool scheduleMore)
    {
        Lock_.clear(std::memory_order_release);
        if (scheduleMore) {
            TrySchedule();
        }
    }

};

IInvokerPtr CreateSerializedInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TSerializedInvoker>(underlyingInvoker);
}

////////////////////////////////////////////////////////////////////////////////

class TPrioritizedInvoker
    : public TInvokerWrapper
    , public virtual IPrioritizedInvoker
{
public:
    explicit TPrioritizedInvoker(IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
    { }

    using TInvokerWrapper::Invoke;

    virtual void Invoke(TClosure callback, i64 priority) override
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            TEntry entry;
            entry.Callback = std::move(callback);
            entry.Priority = priority;
            Heap_.emplace_back(std::move(entry));
            std::push_heap(Heap_.begin(), Heap_.end());
        }
        UnderlyingInvoker_->Invoke(BIND(&TPrioritizedInvoker::DoExecute, MakeStrong(this)));
    }

private:
    struct TEntry
    {
        TClosure Callback;
        i64 Priority;

        bool operator < (const TEntry& other) const
        {
            return Priority < other.Priority;
        }
    };

    TSpinLock SpinLock_;
    std::vector<TEntry> Heap_;

    void DoExecute()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        std::pop_heap(Heap_.begin(), Heap_.end());
        auto callback = std::move(Heap_.back().Callback);
        Heap_.pop_back();
        guard.Release();
        callback.Run();
    }

};

IPrioritizedInvokerPtr CreatePrioritizedInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TPrioritizedInvoker>(std::move(underlyingInvoker));
}

////////////////////////////////////////////////////////////////////////////////

class TFakePrioritizedInvoker
    : public TInvokerWrapper
    , public virtual IPrioritizedInvoker
{
public:
    explicit TFakePrioritizedInvoker(IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
    { }

    using TInvokerWrapper::Invoke;

    virtual void Invoke(TClosure callback, i64 /*priority*/) override
    {
        return UnderlyingInvoker_->Invoke(std::move(callback));
    }
};

IPrioritizedInvokerPtr CreateFakePrioritizedInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TFakePrioritizedInvoker>(std::move(underlyingInvoker));
}

////////////////////////////////////////////////////////////////////////////////

class TFixedPriorityInvoker
    : public TInvokerWrapper
{
public:
    TFixedPriorityInvoker(
        IPrioritizedInvokerPtr underlyingInvoker,
        i64 priority)
        : TInvokerWrapper(underlyingInvoker)
        , UnderlyingInvoker_(std::move(underlyingInvoker))
        , Priority_(priority)
    { }

    using TInvokerWrapper::Invoke;

    virtual void Invoke(TClosure callback) override
    {
        return UnderlyingInvoker_->Invoke(std::move(callback), Priority_);
    }

private:
    const IPrioritizedInvokerPtr UnderlyingInvoker_;
    const i64 Priority_;

};

IInvokerPtr CreateFixedPriorityInvoker(
    IPrioritizedInvokerPtr underlyingInvoker,
    i64 priority)
{
    return New<TFixedPriorityInvoker>(
        std::move(underlyingInvoker),
        priority);
}

////////////////////////////////////////////////////////////////////////////////

class TBoundedConcurrencyInvoker
    : public TInvokerWrapper
{
public:
    TBoundedConcurrencyInvoker(
        IInvokerPtr underlyingInvoker,
        int maxConcurrentInvocations,
        const NProfiling::TTagIdList& tagIds)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , MaxConcurrentInvocations_(maxConcurrentInvocations)
        , Semaphore_(0)
        , Profiler("/bounded_concurrency_invoker")
        , SemaphoreCounter_("/semaphore", tagIds)
    { }

    virtual void Invoke(TClosure callback) override
    {
        Queue_.Enqueue(std::move(callback));
        ScheduleMore();
    }

private:
    int MaxConcurrentInvocations_;

    std::atomic<int> Semaphore_;
    TLockFreeQueue<TClosure> Queue_;

    static PER_THREAD TBoundedConcurrencyInvoker* CurrentSchedulingInvoker_;

    NProfiling::TProfiler Profiler;
    NProfiling::TSimpleCounter SemaphoreCounter_;

    class TInvocationGuard
    {
    public:
        explicit TInvocationGuard(TIntrusivePtr<TBoundedConcurrencyInvoker> owner)
            : Owner_(std::move(owner))
        { }

        TInvocationGuard(TInvocationGuard&& other) = default;
        TInvocationGuard(const TInvocationGuard& other) = delete;

        ~TInvocationGuard()
        {
            if (Owner_) {
                Owner_->OnFinished();
            }
        }

    private:
        TIntrusivePtr<TBoundedConcurrencyInvoker> Owner_;

    };


    void RunCallback(TClosure callback, TInvocationGuard /*invocationGuard*/)
    {
        TCurrentInvokerGuard guard(UnderlyingInvoker_); // sic!
        callback.Run();
    }

    void OnFinished()
    {
        ReleaseSemaphore();
        ScheduleMore();
    }

    void ScheduleMore()
    {
        // Prevent reenterant invocations.
        if (CurrentSchedulingInvoker_ == this)
            return;

        while (true) {
            if (!TryAcquireSemaphore())
                break;

            TClosure callback;
            if (!Queue_.Dequeue(&callback)) {
                ReleaseSemaphore();
                break;
            }

            // If UnderlyingInvoker_ is already terminated, Invoke may drop the guard right away.
            // Protect by setting CurrentSchedulingInvoker_ and checking it on entering ScheduleMore.
            CurrentSchedulingInvoker_ = this;

            UnderlyingInvoker_->Invoke(BIND(
                &TBoundedConcurrencyInvoker::RunCallback,
                MakeStrong(this),
                Passed(std::move(callback)),
                Passed(TInvocationGuard(this))));

            // Don't leave a dangling pointer behind.
            CurrentSchedulingInvoker_ = nullptr;
        }
    }

    bool TryAcquireSemaphore()
    {
        if (++Semaphore_ <= MaxConcurrentInvocations_) {
            Profiler.Increment(SemaphoreCounter_, 1);
            return true;
        } else {
            --Semaphore_;
            return false;
        }
    }

    void ReleaseSemaphore()
    {
        YCHECK(--Semaphore_ >= 0);
        Profiler.Increment(SemaphoreCounter_, -1);
    }
};

PER_THREAD TBoundedConcurrencyInvoker* TBoundedConcurrencyInvoker::CurrentSchedulingInvoker_ = nullptr;

IInvokerPtr CreateBoundedConcurrencyInvoker(
    IInvokerPtr underlyingInvoker,
    int maxConcurrentInvocations,
    const TString& invokerName)
{
    return New<TBoundedConcurrencyInvoker>(
        underlyingInvoker,
        maxConcurrentInvocations,
        GetInvokerTagIds(invokerName));
}

////////////////////////////////////////////////////////////////////////////////

class TSuspendableInvoker
    : public TInvokerWrapper
    , public virtual ISuspendableInvoker
{
public:
    explicit TSuspendableInvoker(IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
    { }

    virtual void Invoke(TClosure callback) override
    {
        Queue_.Enqueue(std::move(callback));
        ScheduleMore();
    }

    TFuture<void> Suspend() override
    {
        YCHECK(!Suspended_.exchange(true));
        {
            TGuard<TSpinLock> guard(SpinLock_);
            FreeEvent_ = NewPromise<void>();
            if (ActiveInvocationCount_ == 0) {
                FreeEvent_.Set();
            }
        }
        return FreeEvent_;
    }

    void Resume() override
    {
        YCHECK(Suspended_.exchange(false));
        {
            TGuard<TSpinLock> guard(SpinLock_);
            FreeEvent_.Reset();
        }
        ScheduleMore();
    }

    bool IsSuspended() override
    {
        return Suspended_;
    }

private:
    std::atomic<bool> Suspended_ = {false};
    std::atomic<bool> SchedulingMore_ = {false};
    std::atomic<int> ActiveInvocationCount_ = {0};

    TSpinLock SpinLock_;

    TLockFreeQueue<TClosure> Queue_;

    TPromise<void> FreeEvent_;

    // TODO(acid): Think how to merge this class with implementation in other invokers.
    class TInvocationGuard
    {
    public:
        explicit TInvocationGuard(TIntrusivePtr<TSuspendableInvoker> owner)
            : Owner_(std::move(owner))
        { }

        TInvocationGuard(TInvocationGuard&& other) = default;
        TInvocationGuard(const TInvocationGuard& other) = delete;

        void Reset()
        {
            Owner_.Reset();
        }

        ~TInvocationGuard()
        {
            if (Owner_) {
                Owner_->OnFinished();
            }
        }

    private:
        TIntrusivePtr<TSuspendableInvoker> Owner_;

    };


    void RunCallback(TClosure callback, TInvocationGuard invocationGuard)
    {
        TCurrentInvokerGuard currentInvokerGuard(this);
        TOneShotContextSwitchGuard contextSwitchGuard([&] {
            invocationGuard.Reset();
            OnFinished();
        });
        callback.Run();
    }

    void OnFinished()
    {
        YCHECK(ActiveInvocationCount_ > 0);

        if (--ActiveInvocationCount_ == 0 && Suspended_) {
            TGuard<TSpinLock> guard(SpinLock_);
            if (FreeEvent_ && !FreeEvent_.IsSet()) {
                auto freeEvent = FreeEvent_;
                guard.Release();
                freeEvent.Set();
            }
        }
    }

    void ScheduleMore()
    {
        if (Suspended_ || SchedulingMore_.exchange(true)) {
            return;
        }

        while (!Suspended_) {
            ++ActiveInvocationCount_;
            TInvocationGuard guard(this);

            TClosure callback;
            if (Suspended_ || !Queue_.Dequeue(&callback)) {
                break;
            }

            UnderlyingInvoker_->Invoke(BIND(
               &TSuspendableInvoker::RunCallback,
               MakeStrong(this),
               Passed(std::move(callback)),
               Passed(std::move(guard))));
        }

        SchedulingMore_ = false;
        if (!Queue_.IsEmpty()) {
            ScheduleMore();
        }
    }
};

ISuspendableInvokerPtr CreateSuspendableInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TSuspendableInvoker>(underlyingInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

