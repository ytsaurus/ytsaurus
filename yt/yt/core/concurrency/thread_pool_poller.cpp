#include "thread_pool_poller.h"

#include "poller.h"
#include "count_down_latch.h"
#include "fiber_scheduler.h"
#include "private.h"
#include "notification_handle.h"
#include "moody_camel_concurrent_queue.h"

#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <util/system/thread.h>

#include <util/network/pollerimpl.h>

#include <array>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto PollerThreadQuantum = TDuration::MilliSeconds(1000);
static constexpr int MaxEventsPerPoll = 1024;
static constexpr int MaxThreadCount = 64;
static constexpr int MaxPollerThreadSpinIterations = 100;

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
        , Invoker_(New<TInvoker>(this))
        , StartLatch_(threadCount + 1)
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
        StartLatch_.Wait();
        YT_LOG_INFO("Thread pool poller started");
    }

    virtual void Shutdown() override
    {
        std::vector<IPollablePtr> pollables;
        std::vector<THandlerThreadPtr> handlerThreads;
        {
            auto guard = Guard(SpinLock_);

            if (ShutdownStarted_.exchange(true)) {
                return;
            }

            pollables.insert(pollables.end(), Pollables_.begin(), Pollables_.end());
            handlerThreads.insert(handlerThreads.end(), HandlerThreads_.begin(), HandlerThreads_.end());
            handlerThreads.insert(handlerThreads.end(), DyingHandlerThreads_.begin(), DyingHandlerThreads_.end());
        }

        Invoker_->Shutdown();

        YT_LOG_INFO("Thread pool poller is waiting for pollables to shut down (PollableCount: %v)",
            pollables.size());

        std::vector<TFuture<void>> unregisterFutures;
        for (const auto& pollable : pollables) {
            unregisterFutures.push_back(Unregister(pollable));
        }

        AllSucceeded(unregisterFutures)
            .Get();

        YT_LOG_INFO("Shutting down poller threads");

        PollerThread_->Shutdown();
        for (const auto& thread : handlerThreads) {
            thread->Shutdown();
        }

        YT_LOG_INFO("Thread pool poller finished");

        {
            IPollablePtr pollable;
            while (RetryQueue_.try_dequeue(pollable));
        }

        Invoker_->DrainQueue();
    }

    virtual void Reconfigure(int threadCount) override
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

    virtual bool TryRegister(const IPollablePtr& pollable) override
    {
        {
            auto guard = Guard(SpinLock_);

            if (ShutdownStarted_.load()) {
                YT_LOG_DEBUG("Cannot register pollable since poller is already shutting down (%v)",
                    pollable->GetLoggingTag());
                return false;
            }

            auto cookie = New<TPollableCookie>();
            pollable->SetCookie(cookie);
            YT_VERIFY(Pollables_.insert(pollable).second);
        }

        YT_LOG_DEBUG("Pollable registered (%v)",
            pollable->GetLoggingTag());
        return true;
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
                YT_LOG_DEBUG("Pollable is not registered (%v)",
                    pollable->GetLoggingTag());
                return VoidFuture;
            }

            const auto& pollable = *it;
            auto* cookie = TPollableCookie::FromPollable(pollable);
            future = cookie->UnregisterPromise.ToFuture();

            if (!cookie->UnregisterLock.exchange(true)) {
                PollerThread_->ScheduleUnregister(pollable);
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
        if (auto* cookie = TPollableCookie::FromPollable(pollable)) {
            PollerThread_->Arm(fd, pollable, control);
        }
    }

    virtual void Unarm(int fd, const IPollablePtr& pollable) override
    {
        if (auto* cookie = TPollableCookie::FromPollable(pollable)) {
            PollerThread_->Unarm(fd, pollable);
        }
    }

    virtual void Retry(const IPollablePtr& pollable, bool wakeup) override
    {
        if (auto* cookie = TPollableCookie::FromPollable(pollable)) {
            PollerThread_->Retry(pollable, wakeup);
        }
    }

    virtual IInvokerPtr GetInvoker() const override
    {
        return Invoker_;
    }

private:
    const TString ThreadNamePrefix_;

    const NLogging::TLogger Logger;

    const TIntrusivePtr<TEventCount> HandlerEventCount_ = New<TEventCount>();

    // Only makes sense for "select" backend.
    struct TMutexLocking
    {
        using TMyMutex = TMutex;
    };
    using TPollerImpl = ::TPollerImpl<TMutexLocking>;
    const std::shared_ptr<TPollerImpl> PollerImpl_ = std::make_shared<TPollerImpl>();

    class TPollerThread
        : public TRefCounted
    {
    public:
        explicit TPollerThread(TThreadPoolPoller* poller)
            : Poller_(poller)
            , Logger(Poller_->Logger)
            , Thread_(&ThreadMainTrampoline, this)
            , PollerEventQueueToken_(Poller_->PollerEventQueue_)
            , RetryQueueToken_(Poller_->RetryQueue_)
        { }

        void Start()
        {
            Poller_->PollerImpl_->Set(nullptr, WakeupHandle_.GetFD(), CONT_POLL_EDGE_TRIGGERED | CONT_POLL_READ);
            Thread_.Start();
        }

        void Shutdown()
        {
            ShutdownStarted_.store(true);
            WakeupHandle_.Raise();
            Thread_.Join();
        }

        void ScheduleUnregister(IPollablePtr pollable)
        {
            UnregisterQueue_.Enqueue(std::move(pollable));
            WakeupHandle_.Raise();
        }

        void Arm(int fd, const IPollablePtr& pollable, EPollControl control)
        {
            YT_LOG_DEBUG("Arming poller (FD: %v, Control: %v, %v)",
                fd,
                control,
                pollable->GetLoggingTag());
            Poller_->PollerImpl_->Set(pollable.Get(), fd, ToImplControl(control));
        }

        void Unarm(int fd, const IPollablePtr& pollable)
        {
            YT_LOG_DEBUG("Unarming poller (FD: %v, %v)",
                fd,
                pollable->GetLoggingTag());
            Poller_->PollerImpl_->Remove(fd);
        }

        void Retry(const IPollablePtr& pollable, bool wakeup)
        {
            YT_LOG_TRACE("Scheduling poller retry (%v, Wakeup: %v)",
                pollable->GetLoggingTag(),
                wakeup);
            if (wakeup) {
                RetryQueue_.Enqueue(pollable);
                if (!RetryScheduled_.exchange(true)) {
                    WakeupHandle_.Raise();
                }
            } else {
                Poller_->RetryQueue_.enqueue(pollable);
            }
        }

    private:
        TThreadPoolPoller* const Poller_;

        const NLogging::TLogger Logger;

        ::TThread Thread_;

        std::atomic<bool> ShutdownStarted_ = false;

        TNotificationHandle WakeupHandle_;

        std::array<TPollerImpl::TEvent, MaxEventsPerPoll> PollerEvents_;

        moodycamel::ProducerToken PollerEventQueueToken_;
        moodycamel::ProducerToken RetryQueueToken_;

        TMpscStack<IPollablePtr> UnregisterQueue_;

        std::atomic<bool> RetryScheduled_ = false;
        TMpscStack<IPollablePtr> RetryQueue_;

        static void* ThreadMainTrampoline(void* opaque)
        {
            static_cast<TPollerThread*>(opaque)->ThreadMain();
            return nullptr;
        }

        void ThreadMain()
        {
            TThread::SetCurrentThreadName(Format("%v:Poll", Poller_->ThreadNamePrefix_).c_str());

            Poller_->StartLatch_.CountDown();

            while (!ShutdownStarted_.load()) {
                ThreadMainLoopStep();
            }
        }

        void ThreadMainLoopStep()
        {
            int count = 0;
            int spinIteration = 0;
            while (true) {
                int subcount = WaitForPollerEvents(count == 0 ? PollerThreadQuantum : TDuration::Zero());
                if (count == 0 && subcount == 0) {
                    continue;
                }
                if (subcount == 0) {
                    if (spinIteration++ == MaxPollerThreadSpinIterations) {
                        break;
                    }
                }
                count += subcount;
            }
            count += HandleRetries();
            HandleUnregisterRequests();

            Poller_->HandlerEventCount_->NotifyMany(count);
        }


        int WaitForPollerEvents(TDuration timeout)
        {
            int count = Poller_->PollerImpl_->Wait(PollerEvents_.data(), PollerEvents_.size(), timeout.MicroSeconds());
            auto it = std::remove_if(
                PollerEvents_.begin(),
                PollerEvents_.begin() + count,
                [] (const auto& event) {
                    return TPollerImpl::ExtractEvent(&event) == nullptr;
                });
            if (it != PollerEvents_.begin()) {
                Poller_->PollerEventQueue_.enqueue_bulk(PollerEventQueueToken_, PollerEvents_.begin(), std::distance(PollerEvents_.begin(), it));
            }
            return count;
        }

        void HandleUnregisterRequests()
        {
            auto pollables = UnregisterQueue_.DequeueAll();
            if (pollables.empty()) {
                return;
            }

            auto guard = Guard(Poller_->SpinLock_);
            for (const auto& pollable : pollables) {
                auto* cookie = TPollableCookie::FromPollable(pollable);
                cookie->PendingUnregisterCount = std::ssize(Poller_->HandlerThreads_) + std::ssize(Poller_->DyingHandlerThreads_);
                for (const auto& thread : Poller_->HandlerThreads_) {
                    thread->ScheduleUnregister(pollable);
                }
                for (const auto& thread : Poller_->DyingHandlerThreads_) {
                    thread->ScheduleUnregister(pollable);
                }
            }
        }

        int HandleRetries()
        {
            RetryScheduled_.store(false);
            auto pollables = RetryQueue_.DequeueAll();
            if (pollables.empty()) {
                return 0;
            }

            std::reverse(pollables.begin(), pollables.end());
            int count = std::ssize(pollables);
            Poller_->RetryQueue_.enqueue_bulk(RetryQueueToken_, std::make_move_iterator(pollables.begin()), count);
            return count;
        }
    };

    using TPollerThreadPtr = TIntrusivePtr<TPollerThread>;

    class THandlerThread
        : public TSchedulerThread
    {
    public:
        THandlerThread(
            TThreadPoolPoller* poller,
            TIntrusivePtr<TEventCount> callbackEventCount,
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
            YT_VERIFY(Poller_->DyingHandlerThreads_.insert(this).second);
            YT_VERIFY(!Dying_.exchange(true));
            CallbackEventCount_->NotifyAll();
        }

        void ScheduleUnregister(IPollablePtr pollable)
        {
            UnregisterQueue_.Enqueue(std::move(pollable));
            CallbackEventCount_->NotifyAll();
        }

    protected:
        virtual void OnStart() override
        {
            Poller_->StartLatch_.CountDown();
        }

        virtual TClosure BeginExecute() override
        {
            if (Dying_.load()) {
                MarkDead();
                Shutdown();
                return {};
            }

            bool didAnything = false;
            didAnything |= HandlePollerEvents();
            didAnything |= HandleRetries();
            didAnything |= HandleUnregisterRequests();
            if (didAnything) {
                static const auto DummyCallback = BIND([] { });
                return DummyCallback;
            }

            SetCurrentInvoker(Poller_->Invoker_);
            return Poller_->Invoker_->DequeCallback();
        }

        virtual void EndExecute() override
        {
            SetCurrentInvoker(nullptr);
        }

        virtual void AfterShutdown() override
        {
            HandleUnregisterRequests();
        }

    private:
        TThreadPoolPoller* const Poller_;
        const NLogging::TLogger Logger;

        moodycamel::ConsumerToken RetryQueueToken_;
        moodycamel::ConsumerToken PollerEventQueueToken_;

        TMpscStack<IPollablePtr> UnregisterQueue_;

        std::atomic<bool> Dying_ = false;


        bool HandlePollerEvents()
        {
            bool gotEvent = false;
            while (true) {
                TPollerImpl::TEvent event;
                if (!Poller_->PollerEventQueue_.try_dequeue(PollerEventQueueToken_, event)) {
                    break;
                }
                gotEvent = true;
                auto control = FromImplControl(TPollerImpl::ExtractFilter(&event));
                auto* pollable = static_cast<IPollable*>(TPollerImpl::ExtractEvent(&event));
                auto* cookie = TPollableCookie::FromPollable(pollable);
                if (!cookie->UnregisterLock.load()) {
                    YT_LOG_TRACE("Got pollable event (Pollable: %v, Control: %v)",
                        pollable->GetLoggingTag(),
                        control);
                    pollable->OnEvent(control);
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
                if (!cookie->UnregisterLock.load()) {
                    pollable->OnEvent(EPollControl::Retry);
                }
            }
            return gotRetry;
        }

        bool HandleUnregisterRequests()
        {
            auto pollables = UnregisterQueue_.DequeueAll();
            if (pollables.empty()) {
                return false;
            }

            std::vector<IPollablePtr> deadPollables;
            for (const auto& pollable : pollables) {
                auto* cookie = TPollableCookie::FromPollable(pollable);
                auto pendingUnregisterCount = --cookie->PendingUnregisterCount;
                YT_VERIFY(pendingUnregisterCount >= 0);
                if (pendingUnregisterCount == 0) {
                    deadPollables.push_back(pollable);
                }
            }

            if (!deadPollables.empty()) {
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

            return true;
        }

        void MarkDead()
        {
            auto guard = Guard(Poller_->SpinLock_);
            YT_VERIFY(Poller_->DyingHandlerThreads_.erase(this) == 1);
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
        virtual void Invoke(TClosure callback) override
        {
            Callbacks_.enqueue(std::move(callback));
            if (ShutdownStarted_.load()) {
                DrainQueue();
                return;
            }
            HandlerEventCount_->NotifyOne();
        }

        TClosure DequeCallback()
        {
            if (ShutdownStarted_.load()) {
                static const auto DummyCallback = BIND([] { });
                return DummyCallback;
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
        virtual NConcurrency::TThreadId GetThreadId() const override
        {
            return InvalidThreadId;
        }

        virtual bool CheckAffinity(const IInvokerPtr& /*invoker*/) const override
        {
            return true;
        }
#endif

    private:
        const TIntrusivePtr<TEventCount> HandlerEventCount_;

        std::atomic<bool> ShutdownStarted_ = false;
        moodycamel::ConcurrentQueue<TClosure> Callbacks_;
    };

    const TIntrusivePtr<TInvoker> Invoker_;

    TCountDownLatch StartLatch_;
    std::atomic<bool> ShutdownStarted_ = false;

    moodycamel::ConcurrentQueue<IPollablePtr> RetryQueue_;
    moodycamel::ConcurrentQueue<TPollerImpl::TEvent> PollerEventQueue_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    THashSet<IPollablePtr> Pollables_;
    TPollerThreadPtr PollerThread_;
    std::vector<THandlerThreadPtr> HandlerThreads_;
    THashSet<THandlerThreadPtr> DyingHandlerThreads_;
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
