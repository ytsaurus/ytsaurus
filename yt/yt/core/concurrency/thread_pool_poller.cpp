#include "thread_pool_poller.h"

#include "poller.h"
#include "count_down_latch.h"
#include "scheduler_thread.h"
#include "private.h"
#include "notification_handle.h"
#include "moody_camel_concurrent_queue.h"
#include "thread.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/mpsc_queue.h>
#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <util/system/thread.h>
#include <util/system/sanitizers.h>

#include <util/network/pollerimpl.h>

#include <array>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto PollerThreadQuantum = TDuration::MilliSeconds(1000);
static constexpr int MaxEventsPerPoll = 1024;
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
    std::atomic<bool> UnregisterStartedLock = false;
    std::atomic<bool> UnregisterCompletedLock = false;
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
    return static_cast<EContPoll>(implControl);
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

void* ToImplPollable(const IPollablePtr& pollable)
{
    return pollable.Get();
}

IPollable* FromImplPollable(void* pollable)
{
    return static_cast<IPollable*>(pollable);
}

struct TPollerEvent
{
    IPollablePtr Pollable;
    EPollControl Control;
};

// Only makes sense for "select" backend.
struct TMutexLocking
{
    using TMyMutex = TMutex;
};

using TPollerImpl = ::TPollerImpl<TMutexLocking>;
using TPollerImplEvent = TPollerImpl::TEvent;

} // namespace

class TThreadPoolPoller
    : public IPoller
{
public:
    TThreadPoolPoller(int threadCount, const TString& threadNamePrefix)
        : ThreadNamePrefix_(threadNamePrefix)
        , Logger(ConcurrencyLogger.WithTag("ThreadNamePrefix: %v", ThreadNamePrefix_))
        , ShutdownCookie_(RegisterShutdownCallback(
            Format("ThreadPoolPoller(%v)", threadNamePrefix),
            BIND(&TThreadPoolPoller::Shutdown, MakeWeak(this)),
            /*priority*/ 300))
        , Invoker_(New<TInvoker>(this))
        , PollerThread_(New<TPollerThread>(this))
        , HandlerThreads_(threadCount)
    {
        for (int index = 0; index < threadCount; ++index) {
            HandlerThreads_[index] = New<THandlerThread>(this, HandlerEventCount_, index);
        }
    }

    ~TThreadPoolPoller()
    {
        Shutdown();
    }

    void Start()
    {
        PollerThread_->Start();
        for (const auto& thread : HandlerThreads_) {
            thread->Start();
        }
        YT_LOG_INFO("Thread pool poller started");
    }

    void Shutdown() override
    {
        std::vector<IPollablePtr> pollables;
        std::vector<THandlerThreadPtr> handlerThreads;
        {
            auto guard = Guard(SpinLock_);

            if (ShutdownStarted_.exchange(true)) {
                return;
            }

            YT_LOG_INFO("Shutting down thread pool invoker");

            pollables.insert(pollables.end(), Pollables_.begin(), Pollables_.end());

            handlerThreads.insert(handlerThreads.end(), HandlerThreads_.begin(), HandlerThreads_.end());
            handlerThreads.insert(handlerThreads.end(), DyingHandlerThreads_.begin(), DyingHandlerThreads_.end());
        }

        Invoker_->Shutdown();

        PollerThread_->Stop();
        for (const auto& thread : handlerThreads) {
            thread->Stop();
        }

        for (const auto& pollable : pollables) {
            InvokeUnregisterCompleted(pollable);
        }

        {
            auto guard = Guard(SpinLock_);

            Pollables_.clear();
            HandlerThreads_.clear();
            DyingHandlerThreads_.clear();
        }

        DrainRetryQueue();
        DrainPollerEventQueue();
        PollerThread_->DrainRetryQueue();
        PollerThread_->DrainUnregisterQueueAndList();
        Invoker_->DrainQueue();
        for (const auto& thread : handlerThreads) {
            thread->DrainUnregisterQueue();
        }
    }

    void Reconfigure(int threadCount) override
    {
        threadCount = std::clamp(threadCount, 1, MaxThreadCount);

        int oldThreadCount;
        std::vector<THandlerThreadPtr> newThreads;
        {
            auto guard = Guard(SpinLock_);

            if (ShutdownStarted_.load()) {
                return;
            }

            if (threadCount == std::ssize(HandlerThreads_)) {
                return;
            }

            oldThreadCount = std::ssize(HandlerThreads_);

            while (threadCount > std::ssize(HandlerThreads_)) {
                HandlerThreads_.push_back(New<THandlerThread>(
                    this,
                    HandlerEventCount_,
                    std::ssize(HandlerThreads_)));
                newThreads.push_back(HandlerThreads_.back());
            }

            while (threadCount < std::ssize(HandlerThreads_)) {
                auto thread = HandlerThreads_.back();
                HandlerThreads_.pop_back();
                thread->MarkDying();
            }
        }

        for (const auto& thread : newThreads) {
            thread->Start();
        }

        YT_LOG_INFO("Poller thread pool size reconfigured (ThreadPoolSize: %v -> %v)",
            oldThreadCount,
            threadCount);
    }

    bool TryRegister(const IPollablePtr& pollable) override
    {
        auto guard = Guard(SpinLock_);

        if (ShutdownStarted_.load()) {
            YT_LOG_DEBUG("Cannot register pollable since poller is already shutting down (%v)",
                pollable->GetLoggingTag());
            return false;
        }

        auto cookie = New<TPollableCookie>();
        pollable->SetCookie(cookie);
        InsertOrCrash(Pollables_, pollable);

        YT_LOG_DEBUG("Pollable registered (%v)",
            pollable->GetLoggingTag());

        return true;
    }

    TFuture<void> Unregister(const IPollablePtr& pollable) override
    {
        auto guard = Guard(SpinLock_);

        auto it = Pollables_.find(pollable);
        if (it == Pollables_.end()) {
            guard.Release();
            YT_LOG_DEBUG("Pollable is not registered (%v)",
                pollable->GetLoggingTag());
            return VoidFuture;
        }

        auto* cookie = TPollableCookie::FromPollable(pollable);
        if (!cookie->UnregisterStartedLock.exchange(true) && !ShutdownStarted_.load()) {
            PollerThread_->ScheduleUnregister(pollable);
        }

        return cookie->UnregisterPromise.ToFuture();
    }

    void Arm(int fd, const IPollablePtr& pollable, EPollControl control) override
    {
        PollerThread_->Arm(fd, pollable, control);
    }

    void Unarm(int fd, const IPollablePtr& pollable) override
    {
        PollerThread_->Unarm(fd, pollable);
    }

    void Retry(const IPollablePtr& pollable, bool wakeup) override
    {
        PollerThread_->Retry(pollable, wakeup);
    }

    IInvokerPtr GetInvoker() const override
    {
        return Invoker_;
    }

private:
    const TString ThreadNamePrefix_;

    const NLogging::TLogger Logger;

    const TShutdownCookie ShutdownCookie_;

    const TIntrusivePtr<NThreading::TEventCount> HandlerEventCount_ = New<NThreading::TEventCount>();

    class TPollerThread
        : public TThread
    {
    public:
        explicit TPollerThread(TThreadPoolPoller* poller)
            : TThread(Format("%v:Poll", poller->ThreadNamePrefix_))
            , Poller_(poller)
            , Logger(Poller_->Logger)
            , PollerEventQueueToken_(Poller_->PollerEventQueue_)
            , PollerRetryQueueToken_(Poller_->RetryQueue_)
            , RetryQueueToken_(RetryQueue_)
        {
            PollerImpl_.Set(nullptr, WakeupHandle_.GetFD(), CONT_POLL_EDGE_TRIGGERED | CONT_POLL_READ);
        }

        void ScheduleUnregister(IPollablePtr pollable)
        {
            YT_LOG_DEBUG("Requesting pollable unregistration (%v)",
                pollable->GetLoggingTag());

            UnregisterQueue_.Enqueue(std::move(pollable));
            ScheduleWakeup();
        }

        void Arm(int fd, const IPollablePtr& pollable, EPollControl control)
        {
            YT_LOG_DEBUG("Arming poller (%v, FD: %v, Control: %v)",
                pollable->GetLoggingTag(),
                fd,
                control);

            PollerImpl_.Set(ToImplPollable(pollable), fd, ToImplControl(control));
        }

        void Unarm(int fd, const IPollablePtr& pollable)
        {
            YT_LOG_DEBUG("Unarming poller (%v, FD: %v)",
                pollable->GetLoggingTag(),
                fd);

            PollerImpl_.Remove(fd);
        }

        void Retry(const IPollablePtr& pollable, bool wakeup)
        {
            YT_LOG_TRACE("Scheduling poller retry (%v, Wakeup: %v)",
                pollable->GetLoggingTag(),
                wakeup);

            if (wakeup) {
                RetryQueue_.enqueue(pollable);
                if (!RetryScheduled_.exchange(true)) {
                    ScheduleWakeup();
                }
            } else {
                Poller_->RetryQueue_.enqueue(pollable);
            }

            if (Y_UNLIKELY(Poller_->ShutdownStarted_.load())) {
                DrainRetryQueue();
                Poller_->DrainRetryQueue();
            }
        }

        void DrainRetryQueue()
        {
            IPollablePtr pollable;
            while (RetryQueue_.try_dequeue(pollable));
        }

        void DrainUnregisterQueueAndList()
        {
            IPollablePtr pollable;
            while (UnregisterQueue_.TryDequeue(&pollable));
            UnregisterList_.clear();
        }

    private:
        TThreadPoolPoller* const Poller_;
        const NLogging::TLogger Logger;

        TPollerImpl PollerImpl_;

        TNotificationHandle WakeupHandle_;
#ifndef _linux_
        std::atomic<bool> WakeupScheduled_ = false;
#endif

        std::array<TPollerImplEvent, MaxEventsPerPoll> PollerImplEvents_;
        std::array<TPollerEvent, MaxEventsPerPoll> PollerEvents_;

        moodycamel::ProducerToken PollerEventQueueToken_;
        moodycamel::ProducerToken PollerRetryQueueToken_;

        TMpscQueue<IPollablePtr> UnregisterQueue_;
        std::vector<IPollablePtr> UnregisterList_;

        std::atomic<bool> RetryScheduled_ = false;

        moodycamel::ConcurrentQueue<IPollablePtr> RetryQueue_;
        moodycamel::ConsumerToken RetryQueueToken_;


        virtual void StopPrologue() override
        {
            ScheduleWakeup();
        }

        void ThreadMain() override
        {
            while (!IsStopping()) {
                ThreadMainLoopStep();
            }
            DrainRetryQueue();
            DrainUnregisterQueueAndList();
        }

        void ThreadMainLoopStep()
        {
            DequeueUnregisterRequests();
            int count = 0;
            count += HandlePollerEvents();
            count += HandleRetries();
            Poller_->HandlerEventCount_->NotifyMany(count);
        }

        int HandlePollerEvents()
        {
            int count = 0;
            bool mustDrain = !UnregisterList_.empty();
            do {
                int subcount = WaitForPollerEvents(mustDrain ? TDuration::Zero() : PollerThreadQuantum);
                if (subcount == 0) {
                    HandleUnregisterRequests();
                    mustDrain = false;
                }
                count += subcount;
            } while (mustDrain);
            return count;
        }

        int WaitForPollerEvents(TDuration timeout)
        {
            int implEventCount = PollerImpl_.Wait(PollerImplEvents_.data(), PollerImplEvents_.size(), timeout.MicroSeconds());

            int eventCount = 0;
            for (int index = 0; index < implEventCount; ++index) {
                const auto& implEvent = PollerImplEvents_[index];
                auto control = FromImplControl(TPollerImpl::ExtractFilter(&implEvent));
                if (auto* pollable = FromImplPollable(TPollerImpl::ExtractEvent(&implEvent))) {
                    auto* cookie = TPollableCookie::FromPollable(pollable);
                    YT_VERIFY(cookie->PendingUnregisterCount.load() == -1);
                    PollerEvents_[eventCount++] = {
                        .Pollable = pollable,
                        .Control = control
                    };
                }
            }

            Poller_->PollerEventQueue_.enqueue_bulk(
                PollerEventQueueToken_,
                std::make_move_iterator(PollerEvents_.data()),
                eventCount);
#ifndef _linux_
            // Drain wakeup handle in order to prevent deadlocking on pipe.
            WakeupHandle_.Clear();
            WakeupScheduled_.store(false);
#endif
            return implEventCount;
        }

        void DequeueUnregisterRequests()
        {
            IPollablePtr pollable;
            while (UnregisterQueue_.TryDequeue(&pollable)) {
                UnregisterList_.push_back(std::move(pollable));
            }
        }

        void HandleUnregisterRequests()
        {
            if (UnregisterList_.empty()) {
                return;
            }

            auto guard = Guard(Poller_->SpinLock_);
            for (const auto& pollable : UnregisterList_) {
                auto* cookie = TPollableCookie::FromPollable(pollable);
                cookie->PendingUnregisterCount = std::ssize(Poller_->HandlerThreads_) + std::ssize(Poller_->DyingHandlerThreads_);
                for (const auto& thread : Poller_->HandlerThreads_) {
                    thread->ScheduleUnregister(pollable);
                }
                for (const auto& thread : Poller_->DyingHandlerThreads_) {
                    thread->ScheduleUnregister(pollable);
                }
            }

            UnregisterList_.clear();
        }

        int HandleRetries()
        {
            RetryScheduled_.store(false);

            std::vector<IPollablePtr> pollables;
            {
                IPollablePtr pollable;
                while (RetryQueue_.try_dequeue(RetryQueueToken_, pollable)) {
                    pollables.push_back(std::move(pollable));
                }
            }

            if (pollables.empty()) {
                return 0;
            }

            std::reverse(pollables.begin(), pollables.end());
            int count = std::ssize(pollables);
            Poller_->RetryQueue_.enqueue_bulk(PollerRetryQueueToken_, std::make_move_iterator(pollables.begin()), count);
            return count;
        }

        void ScheduleWakeup()
        {
#ifndef _linux_
            // Under non-linux platforms notification handle is implemented over pipe, so
            // performing lots consecutive wakeups may lead to blocking on pipe which may
            // lead to dead-lock in case when handle is raised under spinlock.
            if (WakeupScheduled_.load(std::memory_order_relaxed)) {
                return;
            }
            if (WakeupScheduled_.exchange(true)) {
                return;
            }
#endif

            WakeupHandle_.Raise();
        }
    };

    using TPollerThreadPtr = TIntrusivePtr<TPollerThread>;

    class THandlerThread
        : public TSchedulerThread
    {
    public:
        THandlerThread(
            TThreadPoolPoller* poller,
            TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
            int index)
            : TSchedulerThread(
                std::move(callbackEventCount),
                poller->ThreadNamePrefix_,
                Format("%v:%v", poller->ThreadNamePrefix_, index))
            , Poller_(poller)
            , Logger(Poller_->Logger.WithTag("ThreadIndex: %v", index))
            , RetryQueueToken_(Poller_->RetryQueue_)
            , PollerEventQueueToken_(Poller_->PollerEventQueue_)
        { }

        void MarkDying()
        {
            VERIFY_SPINLOCK_AFFINITY(Poller_->SpinLock_);
            InsertOrCrash(Poller_->DyingHandlerThreads_, this);
            YT_VERIFY(!Dying_.exchange(true));
            CallbackEventCount_->NotifyAll();
        }

        void ScheduleUnregister(IPollablePtr pollable)
        {
            UnregisterQueue_.Enqueue(std::move(pollable));
            CallbackEventCount_->NotifyAll();
        }

        void DrainUnregisterQueue()
        {
            IPollablePtr pollable;
            while (UnregisterQueue_.TryDequeue(&pollable));
        }

    protected:
        void OnStop() override
        {
            HandleUnregisterRequests();
        }

        TClosure BeginExecute() override
        {
            if (Dying_.load()) {
                MarkDead();
                Stop();
                return {};
            }

            bool didAnything = false;
            didAnything |= HandlePollerEvents();
            didAnything |= HandleRetries();
            didAnything |= HandleUnregisterRequests();
            if (didAnything) {
                return DummyCallback_;
            }

            SetCurrentInvoker(Poller_->Invoker_);
            return Poller_->Invoker_->DequeCallback();
        }

        void EndExecute() override
        {
            SetCurrentInvoker(nullptr);
        }

    private:
        TThreadPoolPoller* const Poller_;
        const NLogging::TLogger Logger;

        const TClosure DummyCallback_ = BIND([] { });

        moodycamel::ConsumerToken RetryQueueToken_;
        moodycamel::ConsumerToken PollerEventQueueToken_;

        TMpscQueue<IPollablePtr> UnregisterQueue_;

        std::atomic<bool> Dying_ = false;


        bool HandlePollerEvents()
        {
            bool gotEvent = false;
            while (true) {
                TPollerEvent event;
                if (!Poller_->PollerEventQueue_.try_dequeue(PollerEventQueueToken_, event)) {
                    break;
                }
                gotEvent = true;
                auto* cookie = TPollableCookie::FromPollable(event.Pollable);
                if (!cookie->UnregisterStartedLock.load()) {
                    YT_LOG_TRACE("Got pollable event (Pollable: %v, Control: %v)",
                        event.Pollable->GetLoggingTag(),
                        event.Control);
                    event.Pollable->OnEvent(event.Control);
                }
            }
            return gotEvent;
        }

        bool HandleRetries()
        {
            bool gotRetry = false;
            while (true) {
                IPollablePtr pollable;
                if (!Poller_->RetryQueue_.try_dequeue(RetryQueueToken_, pollable)) {
                    break;
                }
                gotRetry = true;
                auto* cookie = TPollableCookie::FromPollable(pollable);
                if (!cookie->UnregisterStartedLock.load()) {
                    pollable->OnEvent(EPollControl::Retry);
                }
            }
            return gotRetry;
        }

        bool HandleUnregisterRequests()
        {
            std::vector<IPollablePtr> deadPollables;
            bool didAnything = false;
            {
                IPollablePtr pollable;
                while (UnregisterQueue_.TryDequeue(&pollable)) {
                    didAnything = true;
                    auto* cookie = TPollableCookie::FromPollable(pollable);
                    auto pendingUnregisterCount = --cookie->PendingUnregisterCount;
                    YT_VERIFY(pendingUnregisterCount >= 0);
                    if (pendingUnregisterCount == 0) {
                        deadPollables.push_back(pollable);
                    }
                }
            }

            if (!deadPollables.empty()) {
                {
                    auto guard = Guard(Poller_->SpinLock_);
                    for (const auto& pollable : deadPollables) {
                        EraseOrCrash(Poller_->Pollables_, pollable);
                    }
                }

                for (const auto& pollable : deadPollables) {
                    Poller_->InvokeUnregisterCompleted(pollable);
                }
            }

            return didAnything;
        }

        void MarkDead()
        {
            auto guard = Guard(Poller_->SpinLock_);
            EraseOrCrash(Poller_->DyingHandlerThreads_, this);
        }
    };

    using THandlerThreadPtr = TIntrusivePtr<THandlerThread>;

    class TInvoker
        : public IInvoker
    {
    public:
        explicit TInvoker(TThreadPoolPoller* poller)
            : HandlerEventCount_(poller->HandlerEventCount_)
        { }

        void Shutdown()
        {
            ShutdownStarted_.store(true);
        }

        // IInvoker implementation.
        void Invoke(TClosure callback) override
        {
            Callbacks_.enqueue(std::move(callback));
            if (Y_UNLIKELY(ShutdownStarted_.load())) {
                DrainQueue();
                return;
            }
            HandlerEventCount_->NotifyOne();
        }

        TClosure DequeCallback()
        {
            if (ShutdownStarted_.load()) {
                return DummyCallback_;
            }

            TClosure callback;
            Callbacks_.try_dequeue(callback);
            return callback;
        }

        void DrainQueue()
        {
            TClosure callback;
            while (Callbacks_.try_dequeue(callback));
        }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        NConcurrency::TThreadId GetThreadId() const override
        {
            return InvalidThreadId;
        }

        bool CheckAffinity(const IInvokerPtr& /*invoker*/) const override
        {
            return true;
        }
#endif

    private:
        const TIntrusivePtr<NThreading::TEventCount> HandlerEventCount_;

        const TClosure DummyCallback_ = BIND([] { });

        std::atomic<bool> ShutdownStarted_ = false;
        moodycamel::ConcurrentQueue<TClosure> Callbacks_;
    };

    const TIntrusivePtr<TInvoker> Invoker_;

    std::atomic<bool> ShutdownStarted_ = false;

    moodycamel::ConcurrentQueue<IPollablePtr> RetryQueue_;
    moodycamel::ConcurrentQueue<TPollerEvent> PollerEventQueue_;

    YT_DECLARE_SPINLOCK(NThreading::TSpinLock, SpinLock_);
    THashSet<IPollablePtr> Pollables_;
    TPollerThreadPtr PollerThread_;
    std::vector<THandlerThreadPtr> HandlerThreads_;
    THashSet<THandlerThreadPtr> DyingHandlerThreads_;


    void InvokeUnregisterCompleted(const IPollablePtr& pollable)
    {
        auto* cookie = TPollableCookie::FromPollable(pollable);
        if (cookie->UnregisterCompletedLock.exchange(true)) {
            return;
        }

        YT_LOG_DEBUG("Pollable unregistered (%v)",
            pollable->GetLoggingTag());
        pollable->OnShutdown();
        cookie->UnregisterPromise.Set();
    }

    void DrainRetryQueue()
    {
        IPollablePtr pollable;
        while (RetryQueue_.try_dequeue(pollable));
    }

    void DrainPollerEventQueue()
    {
        TPollerEvent event;
        while (PollerEventQueue_.try_dequeue(event));
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
