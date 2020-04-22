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

static constexpr auto PollerThreadQuantum = TDuration::MilliSeconds(100);
static constexpr int MaxEventsPerPoll = 16;

////////////////////////////////////////////////////////////////////////////////

namespace {

EContPoll ToImplControl(EPollControl control)
{
    int implControl = CONT_POLL_ONE_SHOT;
    if (Any(control & EPollControl::EdgeTriggered)) {
            implControl = CONT_POLL_EDGE_TRIGGERED;
    }
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
        , Logger(NLogging::TLogger(ConcurrencyLogger)
            .AddTag("ThreadNamePrefix: %v", ThreadNamePrefix_))
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
        YT_LOG_INFO("Thread pool poller started");
    }

    ~TThreadPoolPoller()
    {
        Shutdown();
    }

    // IPoller implementation.
    virtual void Shutdown() override
    {
        std::vector<IPollablePtr> pollables;
        {
            auto guard = Guard(SpinLock_);
            if (ShutdownStarted_.load()) {
                return;
            }
            ShutdownStarted_.store(true);
            for (const auto& [pollable, entry]: Pollables_) {
                pollables.push_back(pollable);
            }
        }

        YT_LOG_INFO("Thread pool poller is waiting for pollables to shut down (PollableCount: %v)",
            pollables.size());

        std::vector<TFuture<void>> shutdownResults;
        for (const auto& pollable : pollables) {
            shutdownResults.push_back(Unregister(pollable));
        }

        Combine(shutdownResults)
            .Get();

        {
            IPollable* pollable;
            while (Retry_.Dequeue(&pollable)) {
            }
        }

        YT_LOG_INFO("Shutting down poller threads");

        for (const auto& thread : Threads_) {
            thread->Shutdown();
        }

        Invoker_->DrainQueue();

        {
            auto guard = Guard(SpinLock_);
            YT_VERIFY(Pollables_.empty());
            YT_VERIFY(Retry_.IsEmpty());
            ShutdownFinished_.store(true);
        }

        YT_LOG_INFO("Thread pool poller finished");
    }

    virtual void Register(const IPollablePtr& pollable) override
    {
        auto entry = New<TPollableEntry>(pollable);
        {
            auto guard = Guard(SpinLock_);
            if (ShutdownStarted_.load()) {
                return;
            }
            YT_VERIFY(Pollables_.emplace(pollable, std::move(entry)).second);
        }
        YT_LOG_DEBUG("Pollable registered (%v)",
            pollable->GetLoggingId());
    }

    virtual TFuture<void> Unregister(const IPollablePtr& pollable) override
    {
        TFuture<void> future;
        bool firstTime = false;
        {
            auto guard = Guard(SpinLock_);

            auto it = Pollables_.find(pollable);
            if (it == Pollables_.end()) {
                guard.Release();
                YT_LOG_DEBUG("Pollable is already unregistered (%v)",
                    pollable->GetLoggingId());
                return VoidFuture;
            }

            const auto& entry = it->second;
            future = entry->UnregisterPromise.ToFuture();

            YT_VERIFY(!ShutdownFinished_.load());

            if (entry->TryLockUnregister()) {
                for (const auto& thread : Threads_) {
                    thread->ScheduleUnregister(entry);
                }
                firstTime = true;
            }
        }

        YT_LOG_DEBUG("Requesting pollable unregistration (%v, FirstTime: %v)",
            pollable->GetLoggingId(),
            firstTime);
        return future;
    }

    virtual void Arm(int fd, const IPollablePtr& pollable, EPollControl control) override
    {
        Impl_.Set(pollable.Get(), fd, ToImplControl(control));
    }

    virtual void Unarm(int fd) override
    {
        Impl_.Remove(fd);
    }

    virtual void Retry(const IPollablePtr& pollable, bool wakeup) override
    {
        // Pollable is registered - skip grabbing reference
        Retry_.Enqueue(pollable.Get());
        if (wakeup) {
            Invoker_->RaiseWakeupHandle();
        }
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

    const NLogging::TLogger Logger;

    struct TPollableEntry
        : public TIntrinsicRefCounted
    {
        explicit TPollableEntry(IPollablePtr pollable)
            : Pollable(std::move(pollable))
        { }

        bool TryLockUnregister()
        {
            return !UnregisterLock.test_and_set();
        }

        const IPollablePtr Pollable;
        std::atomic<int> UnregisterSeenBy = {0};
        std::atomic_flag UnregisterLock = ATOMIC_FLAG_INIT;
        TPromise<void> UnregisterPromise = NewPromise<void>();
    };

    using TPollableEntryPtr = TIntrusivePtr<TPollableEntry>;

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
            , Logger(NLogging::TLogger(Poller_->Logger)
                .AddTag("ThreadIndex: %v", index))
            , ExecuteCallback_(BIND([this] {
                HandleEvents();
                HandleRetry();
                HandleUnregister();
            }))
        { }

        void ScheduleUnregister(TPollableEntryPtr entry)
        {
            UnregisterEntries_.Enqueue(std::move(entry));
        }

    protected:
        virtual void OnStart() override
        {
            Poller_->StartLatch_.CountDown();
        }

        virtual TClosure BeginExecute() override
        {
            if (ExecutingCallbacks_) {
                SetCurrentInvoker(Poller_->Invoker_);

                auto result = Poller_->Invoker_->ExecuteCallbacks();
                if (result) {
                    return result;
                }

                ExecutingCallbacks_ = false;
            }

            return ExecuteCallback_;
        }

        virtual void EndExecute() override
        {
            SetCurrentInvoker(nullptr);
        }

        virtual void AfterShutdown() override
        {
            HandleUnregister();
        }

    private:
        TThreadPoolPoller* const Poller_;
        const NLogging::TLogger Logger;

        bool ExecutingCallbacks_ = false;

        TMultipleProducerSingleConsumerLockFreeStack<TPollableEntryPtr> UnregisterEntries_;

        TClosure ExecuteCallback_;

        void HandleEvents()
        {
            if (Poller_->ShutdownStarted_.load()) {
                return;
            }

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

        void HandleRetry()
        {
            if (Poller_->ShutdownStarted_.load()) {
                return;
            }

            IPollable* pollable;

            // Dequeue one by one to let other threads do their job
            while (Poller_->Retry_.Dequeue(&pollable)) {
                pollable->OnEvent(EPollControl::Retry);
            }
        }

        void HandleUnregister()
        {
            auto entries = UnregisterEntries_.DequeueAll();

            std::vector<TPollableEntryPtr> deadEntries;
            for (const auto& entry : entries) {
                if (++entry->UnregisterSeenBy == Poller_->ThreadCount_) {
                    deadEntries.push_back(entry);
                }
            }

            if (deadEntries.empty()) {
                return;
            }

            for (const auto& entry : deadEntries) {
                entry->Pollable->OnShutdown();
                YT_LOG_DEBUG("Pollable unregistered (%v)",
                    entry->Pollable->GetLoggingId());
            }

            {
                auto guard = Guard(Poller_->SpinLock_);
                for (const auto& entry : deadEntries) {
                    YT_VERIFY(Poller_->Pollables_.erase(entry->Pollable) == 1);
                }
            }

            for (const auto& entry : deadEntries) {
                entry->UnregisterPromise.Set();
            }
        }
    };

    using TThreadPtr = TIntrusivePtr<TThread>;

    std::vector<TThreadPtr> Threads_;

    TCountDownLatch StartLatch_;
    std::atomic<bool> ShutdownStarted_ = {false};
    std::atomic<bool> ShutdownFinished_ = {false};

    TSpinLock SpinLock_;
    THashMap<IPollablePtr, TPollableEntryPtr> Pollables_;

    TLockFreeQueue<IPollable*> Retry_;

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
            Callbacks_.Enqueue(std::move(callback));
            DrainQueueIfNeeded();
            WakeupHandle_.Raise();
        }

        TClosure ExecuteCallbacks()
        {
            TClosure callback;
            if (!Callbacks_.Dequeue(&callback)) {
                return TClosure();
            }

            if (Owner_->ShutdownStarted_.load()) {
                return BIND([] { });
            }

            return callback;
        }

        void DrainQueue()
        {
            TClosure callback;
            while (Callbacks_.Dequeue(&callback)) {
                callback.Reset();
            }
            YT_VERIFY(Callbacks_.IsEmpty()); // As a side effect, this releases free lists.
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
            Owner_->Impl_.Set(nullptr, WakeupHandle_.GetFD(), CONT_POLL_READ | CONT_POLL_EDGE_TRIGGERED);
        }

        void RaiseWakeupHandle()
        {
            WakeupHandle_.Raise();
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


        void DrainQueueIfNeeded()
        {
            if (Owner_->ShutdownStarted_.load()) {
                DrainQueue();
            }
        }
    };

    const TIntrusivePtr<TInvoker> Invoker_;

    // Only makes sense for "select" backend.
    struct TMutexLocking
    {
        using TMyMutex = TMutex;
    };

    TPollerImpl<TMutexLocking> Impl_;
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
