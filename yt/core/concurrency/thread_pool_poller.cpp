#include "thread_pool_poller.h"
#include "poller.h"
#include "count_down_latch.h"
#include "private.h"

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/proc.h>

#include <yt/core/concurrency/notification_handle.h>
#include <yt/core/concurrency/scheduler_thread.h>

#include <util/system/thread.h>

#include <util/thread/lfqueue.h>

#include <util/network/pollerimpl.h>

#include <array>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ConcurrencyLogger;

static constexpr auto PollerThreadQuantum = TDuration::MilliSeconds(100);
static constexpr int MaxEventsPerPoll = 16;

////////////////////////////////////////////////////////////////////////////////

namespace {

EContPoll ToImplControl(EPollControl control)
{
    int implControl = CONT_POLL_ONE_SHOT;
    if (Any(control & EPollControl::Read)) {
        implControl |= CONT_POLL_READ;
    }
    if (Any(control & EPollControl::Write)) {
        implControl |= CONT_POLL_WRITE;
    }
    return EContPoll(implControl);
}

EPollControl FromImplControl(int implControl)
{
    auto control = EPollControl::None;
    if (implControl & CONT_POLL_READ) {
        control |= EPollControl::Read;
    }
    if (implControl & CONT_POLL_WRITE) {
        control |= EPollControl::Write;
    }
    return control;
}

} // namespace

class TThreadPoolPoller
    : public IPoller
{
public:
    TThreadPoolPoller(int threadCount, const TString& threadNamePrefix)
        : ThreadCount_(threadCount)
        , ThreadNamePrefix_(threadNamePrefix)
        , Threads_(ThreadCount_)
        , StartLatch_(ThreadCount_)
        , Invoker_(New<TInvoker>(this))
    {
        for (int index = 0; index < ThreadCount_; ++index) {
            Threads_[index] = New<TThread>(Invoker_->GetCallbackEventCount(), this, index);
        }
    }

    void Start()
    {
        for (const auto& thread : Threads_) {
            thread->Start();
        }
        StartLatch_.Wait();
        Invoker_->Start();
    }

    ~TThreadPoolPoller()
    {
        Shutdown();
        YCHECK(Pollables_.empty());
    }

    // IPoller implementation.
    virtual void Shutdown() override
    {
        ShutdownRequested_.store(true);
        for (const auto& thread : Threads_) {
            thread->Shutdown();
        }

        Invoker_->DrainQueue();
        DrainUnregister();
    }

    virtual void Register(const IPollablePtr& pollable) override
    {
        LOG_DEBUG("Pollable registered (%v)", pollable->GetLoggingId());
        {
            auto guard = Guard(SpinLock_);
            YCHECK(Pollables_.insert(pollable).second);
        }
    }

    virtual TFuture<void> Unregister(const IPollablePtr& pollable) override
    {
        LOG_DEBUG("Requesting pollable unregistration (%v)", pollable->GetLoggingId());
        auto entry = New<TUnregisterEntry>();
        entry->Pollable = pollable;
        if (!ShutdownRequested_.load()) {
            for (const auto& thread : Threads_) {
                thread->ScheduleUnregister(entry);
            }
        } else {
            ShutdownUnregisterEntries_.Enqueue(entry);
        }
        return entry->Promise;
    }

    virtual void Arm(int fd, const IPollablePtr& pollable, EPollControl control) override
    {
        Impl_.Set(pollable.Get(), fd, ToImplControl(control));
    }

    virtual void Unarm(int fd) override
    {
        Impl_.Remove(fd);
    }

    virtual IInvokerPtr GetInvoker() const override
    {
        return Invoker_;
    }

    TString GenerateThreadName(int index)
    {
        return ThreadCount_ == 1
            ? ThreadNamePrefix_
            : Format("%v:%v", ThreadNamePrefix_, index);
    }

private:
    const int ThreadCount_;
    const TString ThreadNamePrefix_;

    struct TUnregisterEntry
        : public TIntrinsicRefCounted
    {
        IPollablePtr Pollable;
        std::atomic<int> SeenBy = {0};
        TPromise<void> Promise = NewPromise<void>();
    };

    using TUnregisterEntryPtr = TIntrusivePtr<TUnregisterEntry>;

    class TThread
        : public TSchedulerThread
    {
    public:
        TThread(
            std::shared_ptr<TEventCount> callbackEventCount,
            TThreadPoolPoller* poller,
            int index)
            : TSchedulerThread(
                std::move(callbackEventCount),
                poller->GenerateThreadName(index),
                {},
                true,
                false)
            , Poller_(poller)
        { }

        void ScheduleUnregister(const TUnregisterEntryPtr& entry)
        {
            UnregisterEntries_.Enqueue(entry);
        }

    protected:
        virtual void OnStart() override
        {
            Poller_->StartLatch_.CountDown();
        }

        virtual EBeginExecuteResult BeginExecute() override
        {
            CallbackEventCount_->CancelWait();

            if (ExecutingCallbacks_) {
                auto result = Poller_->Invoker_->ExecuteCallbacks();
                if (result != EBeginExecuteResult::QueueEmpty) {
                    return result;
                }

                ExecutingCallbacks_ = false;
                Poller_->Invoker_->ArmPoller();
            }

            HandleEvents();
            HandleUnregister();

            return EBeginExecuteResult::Success;
        }

        virtual void EndExecute() override
        { }

        virtual void BeforeShutdown() override
        { }

        virtual void AfterShutdown() override
        {
            HandleUnregister();
        }

    private:
        TThreadPoolPoller* const Poller_;
        bool ExecutingCallbacks_ = false;

        TMultipleProducerSingleConsumerLockFreeStack<TUnregisterEntryPtr> UnregisterEntries_;

        void HandleEvents()
        {
            std::array<decltype(Poller_->Impl_)::TEvent, MaxEventsPerPoll> events;
            int eventCount = Poller_->Impl_.Wait(events.data(), MaxEventsPerPoll, PollerThreadQuantum.MicroSeconds());
            if (eventCount == 0) {
                return;
            }

            for (int index = 0; index < eventCount; ++index) {
                const auto& event = events[index];
                auto control = FromImplControl(Poller_->Impl_.ExtractFilter(&event));
                auto* pollable = static_cast<IPollable*>(Poller_->Impl_.ExtractEvent(&event));
                if (pollable) {
                    pollable->OnEvent(control);
                } else {
                    ExecutingCallbacks_ = true;
                    Poller_->Invoker_->ClearWakeupHandle();
                }
            }
        }

        void HandleUnregister()
        {
            auto entries = UnregisterEntries_.DequeueAll();
            std::vector<TUnregisterEntryPtr> deadEntries;
            for (const auto& entry : entries) {
                if (++entry->SeenBy == Poller_->ThreadCount_) {
                    deadEntries.push_back(entry);
                }
            }

            for (const auto& entry : deadEntries) {
                entry->Pollable->OnShutdown();
                LOG_DEBUG("Pollable unregistered (%v)", entry->Pollable->GetLoggingId());
                entry->Promise.Set();
            }

            if (!deadEntries.empty()) {
                auto guard = Guard(Poller_->SpinLock_);
                for (const auto& entry : deadEntries) {
                    YCHECK(Poller_->Pollables_.erase(entry->Pollable) == 1);
                }
            }
        }
    };

    using TThreadPtr = TIntrusivePtr<TThread>;

    std::vector<TThreadPtr> Threads_;

    TCountDownLatch StartLatch_;
    std::atomic<bool> ShutdownRequested_ = {false};

    TSpinLock SpinLock_;
    THashSet<IPollablePtr> Pollables_;

    TMultipleProducerSingleConsumerLockFreeStack<TUnregisterEntryPtr> ShutdownUnregisterEntries_;

    class TInvoker
        : public IInvoker
    {
    public:
        explicit TInvoker(TThreadPoolPoller* owner)
            : Owner_(owner)
            , CallbackEventCount_(std::make_shared<TEventCount>())
        { }

        void Start()
        {
            ArmPoller();
        }

        std::shared_ptr<TEventCount> GetCallbackEventCount()
        {
            return CallbackEventCount_;
        }

        // IInvoker implementation.
        virtual void Invoke(TClosure callback) override
        {
            if (Owner_->ShutdownRequested_.load()) {
                return;
            }

            Callbacks_.Enqueue(std::move(callback));
            WakeupHandle_.Raise();
        }

        EBeginExecuteResult ExecuteCallbacks()
        {
            TCurrentInvokerGuard guard(this);

            TClosure callback;
            if (Callbacks_.Dequeue(&callback)) {
                if (Owner_->ShutdownRequested_.load()) {
                    return EBeginExecuteResult::Terminated;
                }

                try {
                    callback.Run();
                    return EBeginExecuteResult::Success;
                } catch (const TFiberCanceledException&) {
                    return EBeginExecuteResult::Terminated;
                }
            }

            return EBeginExecuteResult::QueueEmpty;
        }

        void DrainQueue()
        {
            TClosure callback;
            while (Callbacks_.Dequeue(&callback)) {
                callback.Reset();
            }

            YCHECK(Callbacks_.IsEmpty()); // As a side effect, this releases free lists.
        }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        virtual NConcurrency::TThreadId GetThreadId() const override
        {
            return InvalidThreadId;
        }

        virtual bool CheckAffinity(const IInvokerPtr& /*invoker*/) const
        {
            return true;
        }
#endif
        void ArmPoller()
        {
            Owner_->Impl_.Set(nullptr, WakeupHandle_.GetFD(), CONT_POLL_READ|CONT_POLL_ONE_SHOT);
        }

        void ClearWakeupHandle()
        {
            WakeupHandle_.Clear();
        }

    private:
        TThreadPoolPoller* const Owner_;

        std::shared_ptr<TEventCount> CallbackEventCount_;
        TLockFreeQueue<TClosure> Callbacks_;
        TNotificationHandle WakeupHandle_;
    };

    const TIntrusivePtr<TInvoker> Invoker_;

    // Only makes sense for "select" backend.
    struct TMutexLocking
    {
        using TMyMutex = TMutex;
    };

    TPollerImpl<TMutexLocking> Impl_;

    void DrainUnregister()
    {
        while (!ShutdownUnregisterEntries_.IsEmpty()) {
            TUnregisterEntryPtr entry;
            YCHECK(ShutdownUnregisterEntries_.Dequeue(&entry));
            entry->Pollable->OnShutdown();
            LOG_DEBUG("Pollable unregistered (%v)", entry->Pollable->GetLoggingId());
            entry->Promise.Set();

            auto guard = Guard(SpinLock_);
            YCHECK(Pollables_.erase(entry->Pollable) == 1);
        }
    }
};

IPollerPtr CreateThreadPoolPoller(
    int threadCount,
    const TString& threadNamePrefix)
{
    auto poller = New<TThreadPoolPoller>(
        threadCount,
        threadNamePrefix);
    poller->Start();
    return poller;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
