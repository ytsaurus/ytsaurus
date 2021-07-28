#include "thread_pool_poller.h"
#include "poller.h"
#include "count_down_latch.h"
#include "private.h"
#include "profiling_helpers.h"
#include "notification_handle.h"
#include "scheduler_thread.h"

#include <yt/yt/core/misc/lock_free.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ref_tracked.h>

#include <util/system/thread.h>

#include <util/thread/lfqueue.h>

#include <util/network/pollerimpl.h>

#include <array>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto PollerThreadQuantum = TDuration::MilliSeconds(100);

static constexpr int MaxEventsPerPoll = 16;

static constexpr int MaxThreadCount = 64;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TPollableCookie
    : public TRefCounted
{
    static TPollableCookie* FromPollable(const IPollablePtr& pollable)
    {
        return static_cast<TPollableCookie*>(pollable->GetCookie());
    }

    std::atomic<int> PendingUnregisterCount = -1;
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
    if (Any(control & EPollControl::ReadHup)) {
        implControl |= CONT_POLL_RDHUP;
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
    if (implControl & CONT_POLL_RDHUP) {
        control |= EPollControl::ReadHup;
    }
    return control;
}

} // namespace

class TThreadPoolPoller
    : public IPoller
{
public:
    TThreadPoolPoller(int threadCount, const TString& threadNamePrefix)
        : ThreadNamePrefix_(threadNamePrefix)
        , Logger(ConcurrencyLogger.WithTag("ThreadNamePrefix: %v", ThreadNamePrefix_))
        , StartLatch_(threadCount)
        , Threads_(threadCount)
        , PollerImpl_(std::make_shared<TPollerImpl>())
        , Invoker_(New<TInvoker>(this))
    {
        for (int index = 0; index < threadCount; ++index) {
            Threads_[index] = New<TThread>(Invoker_->GetCallbackEventCount(), this, index);
        }
    }

    ~TThreadPoolPoller()
    {
        Shutdown();
    }

    void Start()
    {
        {
            auto guard = Guard(SpinLock_);
            for (const auto& thread : Threads_) {
                thread->Start();
            }
        }
        StartLatch_.Wait();
        Invoker_->Start();
        YT_LOG_INFO("Thread pool poller started");
    }

    virtual void Reconfigure(int threadCount) override
    {
        if (ShutdownStarted_.load()) {
            return;
        }

        threadCount = std::clamp(threadCount, 1, MaxThreadCount);

        int oldThreadCount;
        std::vector<TThreadPtr> newThreads;
        {
            auto guard = Guard(SpinLock_);
            if (threadCount == std::ssize(Threads_)) {
                return;
            }

            oldThreadCount = std::ssize(Threads_);

            while (threadCount > std::ssize(Threads_)) {
                Threads_.push_back(New<TThread>(
                    Invoker_->GetCallbackEventCount(),
                    this,
                    std::ssize(Threads_)));
                newThreads.push_back(Threads_.back());
            }

            while (threadCount < std::ssize(Threads_)) {
                auto thread = Threads_.back();
                Threads_.pop_back();
                YT_VERIFY(DyingThreads_.emplace(thread->GetId(), thread).second);
                thread->MarkDying();
            }
        }

        for (const auto& thread : newThreads) {
            thread->Start();
        }

        YT_LOG_DEBUG("Poller thread pool size reconfigured (ThreadPoolSize: %v -> %v)",
            oldThreadCount,
            threadCount);
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

        Invoker_->Stop();

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

        {
            auto guard = Guard(SpinLock_);
            for (const auto& thread : Threads_) {
                thread->Shutdown();
            }
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
            pollable->GetLoggingTag());
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
                    pollable->GetLoggingTag());
                return VoidFuture;
            }

            const auto& pollable = *it;
            auto* cookie = TPollableCookie::FromPollable(pollable);
            future = cookie->UnregisterPromise.ToFuture();

            YT_VERIFY(!ShutdownFinished_.load());

            if (!cookie->UnregisterLock.exchange(true)) {
                cookie->PendingUnregisterCount = std::ssize(Threads_) + std::ssize(DyingThreads_);
                for (const auto& thread : Threads_) {
                    thread->ScheduleUnregister(pollable);
                }
                for (const auto& thread : DyingThreads_) {
                    thread.second->ScheduleUnregister(pollable);
                }
                firstTime = true;
            }
        }

        YT_LOG_DEBUG("Requesting pollable unregistration (%v, FirstTime: %v)",
            pollable->GetLoggingTag(),
            firstTime);
        return future;
    }

    virtual void Arm(int fd, const IPollablePtr& pollable, EPollControl control) override
    {
        YT_LOG_TRACE("Arming poller (FD: %v, Control: %v, %v)",
            fd,
            control,
            pollable->GetLoggingTag());
        PollerImpl_->Set(pollable.Get(), fd, ToImplControl(control));
    }

    virtual void Unarm(int fd) override
    {
        YT_LOG_TRACE("Unarming poller (FD: %v)",
            fd);
        PollerImpl_->Remove(fd);
    }

    virtual void Retry(const IPollablePtr& pollable, bool wakeup) override
    {
        YT_LOG_TRACE("Scheduling poller retry (%v, Wakeup: %v)",
            pollable->GetLoggingTag(),
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

private:
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
                poller->ThreadNamePrefix_,
                Format("%v:%v", poller->ThreadNamePrefix_, index))
            , Poller_(poller)
            , ExecuteCallback_(BIND([this] {
                HandleEvents();
                HandleRetry();
                HandleUnregister();
            }))
            , DieCallback_(BIND([this] {
                Die();
                Shutdown();
            }))
            , Logger(Poller_->Logger.WithTag("ThreadIndex: %v", index))
        { }

        void ScheduleUnregister(IPollablePtr pollable)
        {
            UnregisterQueue_.Enqueue(std::move(pollable));
        }

        void MarkDying()
        {
            YT_VERIFY(!Dying_.exchange(true));
        }

    protected:
        virtual void OnStart() override
        {
            Poller_->StartLatch_.CountDown();
        }

        virtual TClosure BeginExecute() override
        {
            if (MustExecuteCallbacks_) {
                SetCurrentInvoker(Poller_->Invoker_);

                if (auto callback = Poller_->Invoker_->DequeCallback()) {
                    return callback;
                } else {
                    MustExecuteCallbacks_ = false;
                }
            }

            if (Dying_.load()) {
                return DieCallback_;
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
        const TClosure ExecuteCallback_;
        const TClosure DieCallback_;

        const NLogging::TLogger Logger;

        bool MustExecuteCallbacks_ = false;
        TMultipleProducerSingleConsumerLockFreeStack<IPollablePtr> UnregisterQueue_;

        std::atomic<bool> Dying_ = false;


        void HandleEvents()
        {
            if (Poller_->ShutdownStarted_.load()) {
                return;
            }

            std::array<TPollerImpl::TEvent, MaxEventsPerPoll> events;
            int eventCount = Poller_->PollerImpl_->Wait(events.data(), MaxEventsPerPoll, PollerThreadQuantum.MicroSeconds());
            if (eventCount == 0) {
                return;
            }

            for (int index = 0; index < eventCount; ++index) {
                const auto& event = events[index];
                auto control = FromImplControl(Poller_->PollerImpl_->ExtractFilter(&event));
                auto* pollable = static_cast<IPollable*>(Poller_->PollerImpl_->ExtractEvent(&event));
                if (pollable) {
                    YT_LOG_TRACE("Got pollable event (Pollable: %v, Control: %v)",
                        pollable->GetLoggingTag(),
                        control);
                    pollable->OnEvent(control);
                } else {
                    YT_LOG_TRACE("Got poller callback");
                    MustExecuteCallbacks_ = true;
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
                auto pendingUnregisterCount = --cookie->PendingUnregisterCount;
                YT_VERIFY(pendingUnregisterCount >= 0);
                if (pendingUnregisterCount == 0) {
                    deadPollables.push_back(pollable);
                }
            }

            if (deadPollables.empty()) {
                return;
            }

            for (const auto& pollable : deadPollables) {
                pollable->OnShutdown();
                YT_LOG_DEBUG("Pollable unregistered (%v)",
                    pollable->GetLoggingTag());
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

        void Die()
        {
            auto guard = Guard(Poller_->SpinLock_);

            YT_VERIFY(Poller_->DyingThreads_.erase(GetId()) == 1);
        }
    };

    using TThreadPtr = TIntrusivePtr<TThread>;

    TCountDownLatch StartLatch_;
    std::atomic<bool> ShutdownStarted_ = false;
    std::atomic<bool> ShutdownFinished_ = false;

    // TODO(akozhikhov): We could try using mutex here.
    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    THashSet<IPollablePtr> Pollables_;
    std::vector<TThreadPtr> Threads_;
    THashMap<TThreadId, TThreadPtr> DyingThreads_;

    TLockFreeQueue<IPollablePtr> RetryQueue_;

    // Only makes sense for "select" backend.
    struct TMutexLocking
    {
        using TMyMutex = TMutex;
    };

    using TPollerImpl = ::TPollerImpl<TMutexLocking>;

    const std::shared_ptr<TPollerImpl> PollerImpl_;

    class TInvoker
        : public IInvoker
    {
    public:
        explicit TInvoker(TThreadPoolPoller* owner)
            : PollerImpl_(owner->PollerImpl_)
            , CallbackEventCount_(std::make_shared<TEventCount>())
        { }

        void Start()
        {
            ArmPoller();
        }

        void Stop()
        {
            ShutdownStarted_.store(true);
        }

        const std::shared_ptr<TEventCount>& GetCallbackEventCount() const
        {
            return CallbackEventCount_;
        }

        // IInvoker implementation.
        virtual void Invoke(TClosure callback) override
        {
            Callbacks_.Enqueue(std::move(callback));
            DrainQueueIfNeeded();
            RaiseWakeupHandle();
        }

        TClosure DequeCallback()
        {
            TClosure callback;
            if (!Callbacks_.Dequeue(&callback)) {
                return TClosure();
            }

            if (ShutdownStarted_.load()) {
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
        const std::shared_ptr<TPollerImpl> PollerImpl_;
        const std::shared_ptr<TEventCount> CallbackEventCount_;

        std::atomic<bool> ShutdownStarted_ = false;
        TLockFreeQueue<TClosure> Callbacks_;
        TNotificationHandle WakeupHandle_;


        void ArmPoller()
        {
            PollerImpl_->Set(nullptr, WakeupHandle_.GetFD(), CONT_POLL_READ | CONT_POLL_EDGE_TRIGGERED);
        }

        void DrainQueueIfNeeded()
        {
            if (ShutdownStarted_.load()) {
                DrainQueue();
            }
        }
    };

    const TIntrusivePtr<TInvoker> Invoker_;
};

////////////////////////////////////////////////////////////////////////////////

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
