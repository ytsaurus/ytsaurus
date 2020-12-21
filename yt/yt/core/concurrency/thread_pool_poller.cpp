#include "thread_pool_poller.h"
#include "poller.h"
#include "count_down_latch.h"
#include "private.h"

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/ref_tracked.h>

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

struct TPollableCookie
    : public TRefCounted
{
    static TPollableCookie* FromPollable(const IPollablePtr& pollable)
    {
        return static_cast<TPollableCookie*>(pollable->GetCookie());
    }

    std::atomic<int> UnregisterSeenBy = 0;
    std::atomic<bool> UnregisterLock = false;
    const TPromise<void> UnregisterPromise = NewPromise<void>();
};

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
            for (const auto& pollable : Pollables_) {
                pollables.push_back(pollable);
            }
        }

        YT_LOG_INFO("Thread pool poller is waiting for pollables to shut down (PollableCount: %v)",
            pollables.size());

        std::vector<TFuture<void>> shutdownResults;
        for (const auto& pollable : pollables) {
            shutdownResults.push_back(Unregister(pollable));
        }

        AllSucceeded(shutdownResults)
            .Get();

        {
            IPollablePtr pollable;
            while (RetryQueue_.Dequeue(&pollable)) {
            }
        }

        YT_LOG_INFO("Shutting down poller threads");

        for (const auto& thread : Threads_) {
            thread->Shutdown();
        }

        YT_VERIFY(Invoker_->DrainQueue());

        {
            auto guard = Guard(SpinLock_);
            YT_VERIFY(Pollables_.empty());
            YT_VERIFY(RetryQueue_.IsEmpty());
            ShutdownFinished_.store(true);
        }

        YT_LOG_INFO("Thread pool poller finished");
    }

    virtual void Register(const IPollablePtr& pollable) override
    {
        pollable->SetCookie(New<TPollableCookie>());
        {
            auto guard = Guard(SpinLock_);
            if (ShutdownStarted_.load()) {
                return;
            }
            YT_VERIFY(Pollables_.insert(pollable).second);
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

            const auto& pollable = *it;
            auto* cookie = TPollableCookie::FromPollable(pollable);
            future = cookie->UnregisterPromise.ToFuture();

            YT_VERIFY(!ShutdownFinished_.load());

            if (!cookie->UnregisterLock.exchange(true)) {
                for (const auto& thread : Threads_) {
                    thread->ScheduleUnregister(pollable);
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
        YT_LOG_TRACE("Arming poller (FD: %v, Control: %v, %v)",
            fd,
            control,
            pollable->GetLoggingId());
        Impl_.Set(pollable.Get(), fd, ToImplControl(control));
    }

    virtual void Unarm(int fd) override
    {
        YT_LOG_TRACE("Unarming poller (FD: %v)",
            fd);
        Impl_.Remove(fd);
    }

    virtual void Retry(const IPollablePtr& pollable, bool wakeup) override
    {
        YT_LOG_TRACE("Scheduling poller retry (%v, Wakeup: %v)",
            pollable->GetLoggingId(),
            wakeup);
        RetryQueue_.Enqueue(pollable);
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

        void ScheduleUnregister(IPollablePtr pollable)
        {
            UnregisterQueue_.Enqueue(std::move(pollable));
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
        const TClosure ExecuteCallback_;

        bool ExecutingCallbacks_ = false;
        TMultipleProducerSingleConsumerLockFreeStack<IPollablePtr> UnregisterQueue_;

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
                    YT_LOG_TRACE("Got pollable event (Pollable: %v, Control: %v)",
                        pollable->GetLoggingId(),
                        control);
                    pollable->OnEvent(control);
                } else {
                    YT_LOG_TRACE("Got poller callback");
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

            // Dequeue one by one to let other threads do their job.
            IPollablePtr pollable;
            while (Poller_->RetryQueue_.Dequeue(&pollable)) {
                auto* cookie = TPollableCookie::FromPollable(pollable);
                if (!cookie->UnregisterLock.load()) {
                    pollable->OnEvent(EPollControl::Retry);
                }
            }
        }

        void HandleUnregister()
        {
            auto pollables = UnregisterQueue_.DequeueAll();

            std::vector<IPollablePtr> deadPollables;
            for (const auto& pollable : pollables) {
                auto* cookie = TPollableCookie::FromPollable(pollable);
                if (++cookie->UnregisterSeenBy == Poller_->ThreadCount_) {
                    deadPollables.push_back(pollable);
                }
            }

            if (deadPollables.empty()) {
                return;
            }

            for (const auto& pollable : deadPollables) {
                pollable->OnShutdown();
                YT_LOG_DEBUG("Pollable unregistered (%v)",
                    pollable->GetLoggingId());
            }

            {
                auto guard = Guard(Poller_->SpinLock_);
                for (const auto& pollable : deadPollables) {
                    YT_VERIFY(Poller_->Pollables_.erase(pollable) == 1);
                }
            }

            for (const auto& pollable : deadPollables) {
                auto* cookie = TPollableCookie::FromPollable(pollable);
                cookie->UnregisterPromise.Set();
            }
        }
    };

    using TThreadPtr = TIntrusivePtr<TThread>;

    std::vector<TThreadPtr> Threads_;

    TCountDownLatch StartLatch_;
    std::atomic<bool> ShutdownStarted_ = false;
    std::atomic<bool> ShutdownFinished_ = false;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    THashSet<IPollablePtr> Pollables_;

    TLockFreeQueue<IPollablePtr> RetryQueue_;

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

        bool DrainQueue()
        {
            TClosure callback;
            while (Callbacks_.Dequeue(&callback)) {
                callback.Reset();
            }
            return Callbacks_.IsEmpty(); // As a side effect, this releases free lists.
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


        void ArmPoller()
        {
            Owner_->Impl_.Set(nullptr, WakeupHandle_.GetFD(), CONT_POLL_READ | CONT_POLL_EDGE_TRIGGERED);
        }

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
