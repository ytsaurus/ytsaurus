#include "thread_pool_poller.h"

#include "poller.h"
#include "count_down_latch.h"
#include "scheduler_thread.h"
#include "private.h"
#include "notification_handle.h"
#include "moody_camel_concurrent_queue.h"
#include "thread.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/relaxed_mpsc_queue.h>
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
    if (Any(control & EPollControl::BacklogEmpty)) {
        implControl |= CONT_POLL_BACKLOG_EMPTY;
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
    : public IThreadPoolPoller
{
public:
    explicit TThreadPoolPoller(const TString& threadNamePrefix)
        : ThreadNamePrefix_(threadNamePrefix)
        , Logger(ConcurrencyLogger.WithTag("ThreadNamePrefix: %v", ThreadNamePrefix_))
        , ShutdownCookie_(RegisterShutdownCallback(
            Format("ThreadPoolPoller(%v)", threadNamePrefix),
            BIND(&TThreadPoolPoller::Shutdown, MakeWeak(this)),
            /*priority*/ 300))
        , PollerThread_(New<TPollerThread>(this))
        , Invoker_(New<TInvoker>(this, EPollablePriority::Default))
    { }

    ~TThreadPoolPoller()
    {
        Shutdown();
    }

    void Start(int threadCount)
    {
        for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
            ReconfigureHandlerThreadSet(priority, threadCount);
        }

        PollerThread_->Start();

        YT_LOG_INFO("Thread pool poller started (ThreadCount: %v)",
            threadCount);
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
            handlerThreads = CollectAllHandlerThreads();
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

            for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
                ClearHandlerThreads(priority);
            }
        }

        for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
            DrainRetryQueue(priority);
            DrainPollerEventQueue(priority);
        }

        PollerThread_->DrainRetryQueue();
        PollerThread_->DrainUnregisterQueueAndList();

        Invoker_->DrainQueue();

        for (const auto& thread : handlerThreads) {
            thread->DrainUnregisterQueue();
        }
    }

    void Reconfigure(int threadCount) override
    {
        for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
            ReconfigureHandlerThreadSet(priority, threadCount);
        }

        YT_LOG_INFO("Poller thread pool size reconfigured (ThreadCount: %v)",
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

    void Arm(TFileDescriptor fd, const IPollablePtr& pollable, EPollControl control) override
    {
        PollerThread_->Arm(fd, pollable, control);
    }

    void Unarm(TFileDescriptor fd, const IPollablePtr& pollable) override
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

    class TPollerThread
        : public TThread
    {
    public:
        explicit TPollerThread(TThreadPoolPoller* poller)
            : TThread(Format("%v:Poll", poller->ThreadNamePrefix_))
            , Poller_(poller)
            , Logger(Poller_->Logger)
        {
            for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
                auto& set = Poller_->HandlerThreadSets_[priority];
                EventQueueTokens_[priority].emplace(set.EventQueue);
                RetryQueueTokens_[priority].emplace(set.RetryQueue);
            }

            PollerImpl_.Set(nullptr, WakeupHandle_.GetFD(), CONT_POLL_EDGE_TRIGGERED | CONT_POLL_READ);
        }

        void ScheduleUnregister(IPollablePtr pollable)
        {
            YT_LOG_DEBUG("Requesting pollable unregistration (%v)",
                pollable->GetLoggingTag());

            UnregisterQueue_.Enqueue(std::move(pollable));
            ScheduleWakeup();
        }

        void Arm(TFileDescriptor fd, const IPollablePtr& pollable, EPollControl control)
        {
            YT_LOG_DEBUG("Arming poller (%v, FD: %v, Control: %v)",
                pollable->GetLoggingTag(),
                fd,
                control);

            PollerImpl_.Set(ToImplPollable(pollable), fd, ToImplControl(control));
        }

        void Unarm(TFileDescriptor fd, const IPollablePtr& pollable)
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

            auto priority = pollable->GetPriority();

            if (wakeup) {
                LocalRetryQueue_.enqueue(pollable);
                if (!RetryScheduled_.exchange(true)) {
                    ScheduleWakeup();
                }
            } else {
                Poller_->HandlerThreadSets_[priority].RetryQueue.enqueue(pollable);
            }

            if (Y_UNLIKELY(Poller_->ShutdownStarted_.load())) {
                DrainRetryQueue();
                Poller_->DrainRetryQueue(priority);
            }
        }

        void DrainRetryQueue()
        {
            IPollablePtr pollable;
            while (LocalRetryQueue_.try_dequeue(pollable));
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

        std::array<TPollerImplEvent, MaxEventsPerPoll> PooledImplEvents_;
        TEnumIndexedVector<EPollablePriority, std::vector<TPollerEvent>> PooledEvents_;
        TEnumIndexedVector<EPollablePriority, std::vector<IPollablePtr>> PooledRetries_;

        TEnumIndexedVector<EPollablePriority, std::optional<moodycamel::ProducerToken>> EventQueueTokens_;
        TEnumIndexedVector<EPollablePriority, std::optional<moodycamel::ProducerToken>> RetryQueueTokens_;

        TRelaxedMpscQueue<IPollablePtr> UnregisterQueue_;
        std::vector<IPollablePtr> UnregisterList_;

        std::atomic<bool> RetryScheduled_ = false;

        moodycamel::ConcurrentQueue<IPollablePtr> LocalRetryQueue_;
        moodycamel::ConsumerToken LocalRetryQueueToken_{LocalRetryQueue_};


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

            TEnumIndexedVector<EPollablePriority, int> handlerEventCounts;
            HandleEvents(&handlerEventCounts);
            HandleRetries(&handlerEventCounts);

            for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
                if (auto count = handlerEventCounts[priority]; count > 0) {
                    auto& set = Poller_->HandlerThreadSets_[priority];
                    set.EventCount->NotifyMany(count);
                }
            }
        }

        void HandleEvents(TEnumIndexedVector<EPollablePriority, int>* handlerEventCounts)
        {
            bool mustDrain = !UnregisterList_.empty();
            do {
                auto timeout = mustDrain ? TDuration::Zero() : PollerThreadQuantum;
                if (!WaitForEvents(timeout, handlerEventCounts)) {
                    HandleUnregisterRequests();
                    mustDrain = false;
                }
            } while (mustDrain);
        }

        bool WaitForEvents(TDuration timeout, TEnumIndexedVector<EPollablePriority, int>* handlerEventCounts)
        {
            int implEventCount = PollerImpl_.Wait(PooledImplEvents_.data(), PooledImplEvents_.size(), timeout.MicroSeconds());

            for (int index = 0; index < implEventCount; ++index) {
                const auto& implEvent = PooledImplEvents_[index];
                auto control = FromImplControl(TPollerImpl::ExtractFilter(&implEvent));
                if (auto* pollable = FromImplPollable(TPollerImpl::ExtractEvent(&implEvent))) {
                    auto priority = pollable->GetPriority();
                    auto* cookie = TPollableCookie::FromPollable(pollable);
                    YT_VERIFY(cookie->PendingUnregisterCount.load() == -1);
                    PooledEvents_[priority].push_back({
                        .Pollable = pollable,
                        .Control = control
                    });
                }
            }

            for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
                auto& events = PooledEvents_[priority];
                auto& set = Poller_->HandlerThreadSets_[priority];
                (*handlerEventCounts)[priority] += std::ssize(events);
                set.EventQueue.enqueue_bulk(
                    *EventQueueTokens_[priority],
                    std::make_move_iterator(events.begin()),
                    events.size());
                events.clear();
            }

#ifndef _linux_
            // Drain wakeup handle in order to prevent deadlocking on pipe.
            WakeupHandle_.Clear();
            WakeupScheduled_.store(false);
#endif

            return implEventCount > 0;
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

            auto handlerThreads = Poller_->CollectAllHandlerThreads();

            for (const auto& pollable : UnregisterList_) {
                auto* cookie = TPollableCookie::FromPollable(pollable);
                cookie->PendingUnregisterCount = std::ssize(handlerThreads);
                for (const auto& thread : handlerThreads) {
                    thread->ScheduleUnregister(pollable);
                }
            }
            UnregisterList_.clear();
        }

        void HandleRetries(TEnumIndexedVector<EPollablePriority, int>* handlerEventCounts)
        {
            RetryScheduled_.store(false);

            IPollablePtr pollable;
            while (LocalRetryQueue_.try_dequeue(LocalRetryQueueToken_, pollable)) {
                auto priority = pollable->GetPriority();
                PooledRetries_[priority].push_back(std::move(pollable));
            }

            for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
                auto& set = Poller_->HandlerThreadSets_[priority];
                auto& pollables = PooledRetries_[priority];
                (*handlerEventCounts)[priority] += std::ssize(pollables);
                std::reverse(pollables.begin(), pollables.end());
                set.RetryQueue.enqueue_bulk(
                    *RetryQueueTokens_[priority],
                    std::make_move_iterator(pollables.begin()),
                    pollables.size());
                pollables.clear();
            }
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
            const TString& threadNamePrefix,
            EPollablePriority priority,
            int index)
            : TSchedulerThread(
                poller->HandlerThreadSets_[priority].EventCount,
                threadNamePrefix,
                Format("%v:%v", threadNamePrefix, index))
            , Poller_(poller)
            , Priority_(priority)
            , Logger(Poller_->Logger.WithTag("ThreadIndex: %v", index))
            , RetryQueueToken_(Poller_->HandlerThreadSets_[priority].RetryQueue)
            , EventQueueToken_(Poller_->HandlerThreadSets_[priority].EventQueue)
        { }

        void MarkDying()
        {
            VERIFY_SPINLOCK_AFFINITY(Poller_->SpinLock_);
            InsertOrCrash(Poller_->HandlerThreadSets_[Priority_].DyingHandlerThreads, this);
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
        void OnStart() override
        {
            if (Priority_ == EPollablePriority::RealTime) {
                EnableRealTimePriority();
            }
        }

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
        const EPollablePriority Priority_;
        const NLogging::TLogger Logger;

        const TClosure DummyCallback_ = BIND([] { });

        moodycamel::ConsumerToken RetryQueueToken_;
        moodycamel::ConsumerToken EventQueueToken_;

        TRelaxedMpscQueue<IPollablePtr> UnregisterQueue_;

        std::atomic<bool> Dying_ = false;


        bool HandlePollerEvents()
        {
            bool gotEvent = false;
            while (true) {
                TPollerEvent event;
                if (!Poller_->HandlerThreadSets_[Priority_].EventQueue.try_dequeue(EventQueueToken_, event)) {
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
                if (!Poller_->HandlerThreadSets_[Priority_].RetryQueue.try_dequeue(RetryQueueToken_, pollable)) {
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
            EraseOrCrash(Poller_->HandlerThreadSets_[Priority_].DyingHandlerThreads, this);
        }
    };

    using THandlerThreadPtr = TIntrusivePtr<THandlerThread>;

    struct THandlerThreadSet
    {
        std::vector<THandlerThreadPtr> ActiveHandlerThreads;
        THashSet<THandlerThreadPtr> DyingHandlerThreads;

        const TIntrusivePtr<NThreading::TEventCount> EventCount = New<NThreading::TEventCount>();

        moodycamel::ConcurrentQueue<TPollerEvent> EventQueue{};
        moodycamel::ConcurrentQueue<IPollablePtr> RetryQueue{};
    };

    class TInvoker
        : public IInvoker
    {
    public:
        explicit TInvoker(TThreadPoolPoller* poller, EPollablePriority priority)
            : HandlerEventCount_(poller->HandlerThreadSets_[priority].EventCount)
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

        NConcurrency::TThreadId GetThreadId() const override
        {
            return InvalidThreadId;
        }

        bool CheckAffinity(const IInvokerPtr& /*invoker*/) const override
        {
            return true;
        }

        bool IsSerialized() const override
        {
            return false;
        }

    private:
        const TIntrusivePtr<NThreading::TEventCount> HandlerEventCount_;

        const TClosure DummyCallback_ = BIND([] { });

        std::atomic<bool> ShutdownStarted_ = false;
        moodycamel::ConcurrentQueue<TClosure> Callbacks_;
    };

    std::atomic<bool> ShutdownStarted_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashSet<IPollablePtr> Pollables_;
    TEnumIndexedVector<EPollablePriority, THandlerThreadSet> HandlerThreadSets_;

    const TPollerThreadPtr PollerThread_;
    const TIntrusivePtr<TInvoker> Invoker_;


    std::vector<THandlerThreadPtr> CollectAllHandlerThreads()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        std::vector<THandlerThreadPtr> threads;
        for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
            auto& set = HandlerThreadSets_[priority];
            threads.insert(threads.end(), set.ActiveHandlerThreads.begin(), set.ActiveHandlerThreads.end());
            threads.insert(threads.end(), set.DyingHandlerThreads.begin(), set.DyingHandlerThreads.end());
        }

        return threads;
    }

    void ClearHandlerThreads(EPollablePriority priority)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        auto& set = HandlerThreadSets_[priority];
        set.ActiveHandlerThreads.clear();
        set.DyingHandlerThreads.clear();
    }

    TString MakeThreadNamePrefix(EPollablePriority priority)
    {
        auto result = ThreadNamePrefix_;
        if (priority == EPollablePriority::RealTime) {
            result += "RT";
        }
        return result;
    }

    void ReconfigureHandlerThreadSet(EPollablePriority priority, int newThreadCount)
    {
        int oldThreadCount;
        std::vector<THandlerThreadPtr> newThreads;

        newThreadCount = std::clamp(newThreadCount, 1, MaxThreadCount);

        {
            auto guard = Guard(SpinLock_);

            if (ShutdownStarted_.load()) {
                return;
            }

            auto& set = HandlerThreadSets_[priority];

            if (newThreadCount == std::ssize(set.ActiveHandlerThreads)) {
                return;
            }

            oldThreadCount = std::ssize(set.ActiveHandlerThreads);

            auto threadNamePrefix = MakeThreadNamePrefix(priority);

            while (newThreadCount > std::ssize(set.ActiveHandlerThreads)) {
                set.ActiveHandlerThreads.push_back(New<THandlerThread>(
                    this,
                    threadNamePrefix,
                    priority,
                    std::ssize(set.ActiveHandlerThreads)));
                newThreads.push_back(set.ActiveHandlerThreads.back());
            }

            while (newThreadCount < std::ssize(set.ActiveHandlerThreads)) {
                auto thread = set.ActiveHandlerThreads.back();
                set.ActiveHandlerThreads.pop_back();
                thread->MarkDying();
            }
        }

        for (const auto& thread : newThreads) {
            thread->Start();
        }

        YT_LOG_INFO("Poller thread pool size reconfigured (PollablePriority: %v, ThreadPoolSize: %v -> %v)",
            priority,
            oldThreadCount,
            newThreadCount);
    }

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

    void DrainRetryQueue(EPollablePriority priority)
    {
        auto& set = HandlerThreadSets_[priority];
        IPollablePtr pollable;
        while (set.RetryQueue.try_dequeue(pollable));
    }

    void DrainPollerEventQueue(EPollablePriority priority)
    {
        auto& set = HandlerThreadSets_[priority];
        TPollerEvent event;
        while (set.EventQueue.try_dequeue(event));
    }
};

////////////////////////////////////////////////////////////////////////////////

IThreadPoolPollerPtr CreateThreadPoolPoller(
    int threadCount,
    const TString& threadNamePrefix)
{
    auto poller = New<TThreadPoolPoller>(threadNamePrefix);
    poller->Start(threadCount);
    return poller;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
