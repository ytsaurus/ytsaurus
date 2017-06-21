#include "thread_pool_poller.h"
#include "poller.h"
#include "count_down_latch.h"
#include "private.h"

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/proc.h>

#include <yt/core/concurrency/notification_handle.h>

#include <util/system/thread.h>

#include <util/thread/lfqueue.h>

#include <util/network/pollerimpl.h>

#include <array>

namespace NYT {
namespace NConcurrency {

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
            Threads_[index] = std::make_unique<TThread>(this, index);
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
    }

    // IPoller implementation.
    virtual void Shutdown() override
    {
        ShutdownRequested_.store(true);
        for (const auto& thread : Threads_) {
            thread->Stop();
        }
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
        for (const auto& thread : Threads_) {
            thread->ScheduleUnregister(entry);
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
    {
    public:
        TThread(TThreadPoolPoller* poller, int index)
            : Poller_(poller)
            , Index_(index)
            , Thread_(&TThread::ThreadMainTrampoline, this)
        { }

        void Start()
        {
            Thread_.Start();
        }

        void Stop()
        {
            Thread_.Join();
        }

        void ScheduleUnregister(const TUnregisterEntryPtr& entry)
        {
            UnregisterEntries_.Enqueue(entry);
        }

    private:
        TThreadPoolPoller* const Poller_;
        const int Index_;

        ::TThread Thread_;

        TMultipleProducerSingleConsumerLockFreeStack<TUnregisterEntryPtr> UnregisterEntries_;

        static void* ThreadMainTrampoline(void* opaque)
        {
            static_cast<TThread*>(opaque)->ThreadMain();
            return nullptr;
        }

        TString GenerateThreadName()
        {
            return Poller_->ThreadCount_ == 1
                ? Poller_->ThreadNamePrefix_
                : Format("%v:%v", Poller_->ThreadNamePrefix_, Index_);
        }

        void ThreadMain()
        {
            auto threadName = GenerateThreadName();
            ::TThread::CurrentThreadSetName(threadName.c_str());

            LOG_DEBUG("Poller thread started (Name: %v)", threadName);

            Poller_->StartLatch_.CountDown();

            while (!Poller_->ShutdownRequested_.load()) {
                // While this is not a fiber-friendly environment, it's nice to have a unique
                // fiber id installed, for diagnostic purposes.
                SetCurrentFiberId(GenerateFiberId());
                HandleEvents();
                HandleUnregister();
            }

            LOG_DEBUG("Poller thread stopped (Name: %v)", threadName);
        }

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
                    Poller_->Invoker_->OnCallback();
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

    std::vector<std::unique_ptr<TThread>> Threads_;

    TCountDownLatch StartLatch_;
    std::atomic<bool> ShutdownRequested_ = {false};

    TSpinLock SpinLock_;
    yhash_set<IPollablePtr> Pollables_;

    class TInvoker
        : public IInvoker
    {
    public:
        explicit TInvoker(TThreadPoolPoller* owner)
            : Owner_(owner)
        { }

        void Start()
        {
            ArmPoller();
        }

        void OnCallback()
        {
            WakeupHandle_.Clear();

            TClosure callback;
            while (Callbacks_.Dequeue(&callback)) {
                callback.Run();
            }

            ArmPoller();
        }

        // IInvoker implementation.
        virtual void Invoke(TClosure callback) override
        {
            Callbacks_.Enqueue(std::move(callback));
            WakeupHandle_.Raise();
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
    private:
        TThreadPoolPoller* const Owner_;

        TLockFreeQueue<TClosure> Callbacks_;
        TNotificationHandle WakeupHandle_;

        void ArmPoller()
        {
            Owner_->Impl_.Set(nullptr, WakeupHandle_.GetFD(), CONT_POLL_READ|CONT_POLL_ONE_SHOT);
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

} // namespace NConcurrency
} // namespace NYT
