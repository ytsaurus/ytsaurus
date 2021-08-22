#include "action_queue.h"
#include "single_queue_scheduler_thread.h"
#include "fair_share_queue_scheduler_thread.h"
#include "private.h"
#include "profiling_helpers.h"

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <util/thread/lfqueue.h>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;
using namespace NYTAlloc;

////////////////////////////////////////////////////////////////////////////////

class TActionQueue::TImpl
    : public TRefCounted
{
public:
    TImpl(
        const TString& threadName)
        : Queue_(New<TMpscInvokerQueue>(
            CallbackEventCount_,
            GetThreadTags(threadName)))
        , Invoker_(Queue_)
        , Thread_(New<TMpscSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            threadName,
            threadName))
    { }

    ~TImpl()
    {
        Shutdown();
    }

    void Shutdown()
    {
        bool expected = false;
        if (!StopFlag_.compare_exchange_strong(expected, true)) {
            return;
        }

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
        EnsureStarted();
        return Invoker_;
    }

private:
    const TIntrusivePtr<TEventCount> CallbackEventCount_ = New<TEventCount>();
    const TMpscInvokerQueuePtr Queue_;
    const IInvokerPtr Invoker_;
    const TMpscSingleQueueSchedulerThreadPtr Thread_;

    std::atomic<bool> StartFlag_ = false;
    std::atomic<bool> StopFlag_ = false;

    IInvokerPtr FinalizerInvoker_ = GetFinalizerInvoker();

    void EnsureStarted()
    {
        bool expected = false;
        if (!StartFlag_.compare_exchange_strong(expected, true)) {
            return;
        }

        Thread_->Start();
    }
};

TActionQueue::TActionQueue(
    const TString& threadName)
    : Impl_(New<TImpl>(threadName))
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
    { }

    virtual void Invoke(TClosure callback) override
    {
        Queue_.Enqueue(std::move(callback));
        TrySchedule();
    }

private:
    TLockFreeQueue<TClosure> Queue_;
    std::atomic_flag Lock_ = ATOMIC_FLAG_INIT;


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
            YT_ASSERT(!Activated_);
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
        if (!Lock_.test_and_set(std::memory_order_acquire)) {
            UnderlyingInvoker_->Invoke(BIND(
                &TSerializedInvoker::RunCallback,
                MakeStrong(this),
                Passed(TInvocationGuard(this))));
        }
    }

    void DrainQueue()
    {
        TClosure callback;
        while (Queue_.Dequeue(&callback)) {
            callback.Reset();
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

    void OnFinished(bool activated)
    {
        Lock_.clear(std::memory_order_release);
        if (activated) {
            if (!Queue_.IsEmpty()) {
                TrySchedule();
            }
        } else {
            DrainQueue();
        }
    }

};

IInvokerPtr CreateSerializedInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TSerializedInvoker>(std::move(underlyingInvoker));
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
            auto guard = Guard(SpinLock_);
            TEntry entry;
            entry.Callback = std::move(callback);
            entry.Priority = priority;
            Heap_.push_back(std::move(entry));
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

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    std::vector<TEntry> Heap_;

    void DoExecute()
    {
        auto guard = Guard(SpinLock_);
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
        int maxConcurrentInvocations)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , MaxConcurrentInvocations_(maxConcurrentInvocations)
    { }

    virtual void Invoke(TClosure callback) override
    {
        auto guard = Guard(SpinLock_);
        if (Semaphore_ < MaxConcurrentInvocations_) {
            YT_VERIFY(Queue_.empty());
            IncrementSemaphore(+1);
            guard.Release();
            RunCallback(std::move(callback));
        } else {
            Queue_.push(std::move(callback));
        }
    }

private:
    const int MaxConcurrentInvocations_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    TRingQueue<TClosure> Queue_;
    int Semaphore_ = 0;

    static PER_THREAD TBoundedConcurrencyInvoker* CurrentSchedulingInvoker_;

private:
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

    void IncrementSemaphore(int delta)
    {
        Semaphore_ += delta;
        YT_ASSERT(Semaphore_ >= 0 && Semaphore_ <= MaxConcurrentInvocations_);
    }

    void RunCallback(TClosure callback)
    {
        // If UnderlyingInvoker_ is already terminated, Invoke may drop the guard right away.
        // Protect by setting CurrentSchedulingInvoker_ and checking it on entering ScheduleMore.
        CurrentSchedulingInvoker_ = this;

        UnderlyingInvoker_->Invoke(BIND(
            &TBoundedConcurrencyInvoker::DoRunCallback,
            MakeStrong(this),
            std::move(callback),
            Passed(TInvocationGuard(this))));

        // Don't leave a dangling pointer behind.
        CurrentSchedulingInvoker_ = nullptr;
    }

    void DoRunCallback(const TClosure& callback, TInvocationGuard /*invocationGuard*/)
    {
        TCurrentInvokerGuard guard(UnderlyingInvoker_); // sic!
        callback.Run();
    }

    void OnFinished()
    {
        auto guard = Guard(SpinLock_);
        // See RunCallback.
        if (Queue_.empty() || CurrentSchedulingInvoker_ == this) {
            IncrementSemaphore(-1);
        } else {
            auto callback = std::move(Queue_.front());
            Queue_.pop();
            guard.Release();
            RunCallback(std::move(callback));
        }
    }
};

PER_THREAD TBoundedConcurrencyInvoker* TBoundedConcurrencyInvoker::CurrentSchedulingInvoker_ = nullptr;

IInvokerPtr CreateBoundedConcurrencyInvoker(
    IInvokerPtr underlyingInvoker,
    int maxConcurrentInvocations)
{
    return New<TBoundedConcurrencyInvoker>(
        std::move(underlyingInvoker),
        maxConcurrentInvocations);
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
        YT_VERIFY(!Suspended_.exchange(true));
        {
            auto guard = Guard(SpinLock_);
            FreeEvent_ = NewPromise<void>();
            if (ActiveInvocationCount_ == 0) {
                FreeEvent_.Set();
            }
        }
        return FreeEvent_;
    }

    void Resume() override
    {
        YT_VERIFY(Suspended_.exchange(false));
        {
            auto guard = Guard(SpinLock_);
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

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);

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
        YT_VERIFY(ActiveInvocationCount_ > 0);

        if (--ActiveInvocationCount_ == 0 && Suspended_) {
            auto guard = Guard(SpinLock_);
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
    return New<TSuspendableInvoker>(std::move(underlyingInvoker));
}

////////////////////////////////////////////////////////////////////////////////

class TMemoryTaggingInvoker
    : public TInvokerWrapper
{
public:
    TMemoryTaggingInvoker(IInvokerPtr invoker, TMemoryTag memoryTag)
        : TInvokerWrapper(std::move(invoker))
        , MemoryTag_(memoryTag)
    { }

    virtual void Invoke(TClosure callback) override
    {
        UnderlyingInvoker_->Invoke(BIND(
            &TMemoryTaggingInvoker::RunCallback,
            MakeStrong(this),
            Passed(std::move(callback))));
    }

private:
    TMemoryTag MemoryTag_;

    void RunCallback(TClosure callback)
    {
        TCurrentInvokerGuard currentInvokerGuard(this);
        TMemoryTagGuard memoryTagGuard(MemoryTag_);
        callback.Run();
    }
};

IInvokerPtr CreateMemoryTaggingInvoker(IInvokerPtr underlyingInvoker, TMemoryTag tag)
{
    return New<TMemoryTaggingInvoker>(std::move(underlyingInvoker), tag);
}

////////////////////////////////////////////////////////////////////////////////

class TCodicilGuardedInvoker
    : public TInvokerWrapper
{
public:
    TCodicilGuardedInvoker(IInvokerPtr invoker, TString codicil)
        : TInvokerWrapper(std::move(invoker))
        , Codicil_(std::move(codicil))
    { }

    virtual void Invoke(TClosure callback) override
    {
        UnderlyingInvoker_->Invoke(BIND(
            &TCodicilGuardedInvoker::RunCallback,
            MakeStrong(this),
            Passed(std::move(callback))));
    }

private:
    const TString Codicil_;

    void RunCallback(TClosure callback)
    {
        TCurrentInvokerGuard currentInvokerGuard(this);
        TCodicilGuard codicilGuard(Codicil_);
        callback.Run();
    }
};

IInvokerPtr CreateCodicilGuardedInvoker(IInvokerPtr underlyingInvoker, TString codicil)
{
    return New<TCodicilGuardedInvoker>(std::move(underlyingInvoker), std::move(codicil));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
