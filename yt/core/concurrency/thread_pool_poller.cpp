#include "poller_thread_pool.h"

namespace NYT {
namespace NConcurrency {

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

class TDispatcherPoller
    : public IDispatcherPoller
{
public:
    TDispatcherPoller(int threadCount, const TString& threadNamePrefix)
        : ThreadCount_(threadCount)
          , ThreadNamePrefix_(threadNamePrefix)
          , Threads_(ThreadCount_)
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
        Invoker_->Start();
    }

    ~TDispatcherPoller()
    {
        Shutdown();
    }

    // IDispatcherPoller implementation.
    virtual void Shutdown() override
    {
        for (const auto& thread : Threads_) {
            thread->Stop();
        }
    }

    virtual void Register(const IPollablePtr& pollable) override
    {
        {
            auto guard = Guard(SpinLock_);
            YCHECK(Pollables_.insert(pollable).second);
        }
        LOG_DEBUG("Pollable registered (%v)", pollable->GetLoggingId());
    }

    virtual TFuture<void> Unregister(const IPollablePtr& pollable) override
    {
        LOG_DEBUG("Pollable unregistration requested (%v)", pollable->GetLoggingId());

        int fd = pollable->GetFD();
        if (fd >= 0) {
            Impl_.Remove(fd);
        }

        auto entry = New<TUnregisterEntry>();
        entry->Pollable = pollable;
        for (const auto& thread : Threads_) {
            thread->ScheduleUnregister(entry);
        }
        return entry->Promise;
    }

    virtual void Arm(const IPollablePtr& pollable, EPollControl control) override
    {
        int fd = pollable->GetFD();
        Y_ASSERT(fd >= 0);
        Impl_.Set(pollable.Get(), fd, ToImplControl(control));
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
        TThread(TDispatcherPoller* poller, int index)
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
            ShutdownRequested_.store(true);
            Thread_.Join();
        }

        void ScheduleUnregister(const TUnregisterEntryPtr& entry)
        {
            UnregisterEntries_.Enqueue(entry);
        }

    private:
        TDispatcherPoller* const Poller_;
        const int Index_;

        std::atomic<bool> ShutdownRequested_ = {false};

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

            while (!ShutdownRequested_.load()) {
                // While this is not a fiber-friendly environment, it's nice to have a unique
                // fiber id installed, for diagnostic purposes.
                SetCurrentFiberId(GenerateFiberId());
                HandleEvent();
                HandleUnregister();
            }

            LOG_DEBUG("Poller thread stopped (Name: %v)", threadName);
        }

        void HandleEvent()
        {
            decltype(Poller_->Impl_)::TEvent event;
            if (Poller_->Impl_.Wait(&event, 1, PollerThreadQuantum.MicroSeconds()) == 0) {
                return;
            }

            auto control = FromImplControl(Poller_->Impl_.ExtractFilter(&event));
            auto* pollable = static_cast<IPollable*>(Poller_->Impl_.ExtractEvent(&event));
            if (pollable) {
                pollable->OnEvent(control);
            } else {
                Poller_->Invoker_->OnCallback();
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

    TSpinLock SpinLock_;
    yhash_set<IPollablePtr> Pollables_;

    class TInvoker
        : public IInvoker
    {
    public:
        explicit TInvoker(TDispatcherPoller* owner)
            : Owner_(owner)
        { }

        void Start()
        {
            SafePipe(PipeFDs);
            YCHECK(fcntl(PipeFDs[0], F_SETFL, O_NONBLOCK) == 0);
            ArmPoller();
        }

        void OnCallback()
        {
            TClosure callback;
            while (Callbacks_.Dequeue(&callback)) {
                callback.Run();
            }
            ReadFromPipe();
            ArmPoller();
        }

        // IInvoker implementation.
        virtual void Invoke(TClosure callback) override
        {
            Callbacks_.Enqueue(std::move(callback));
            WriteToPipe();
        }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        virtual NConcurrency::TThreadId GetThreadId() const override
        {
            return InvalidThreadId;
        }

        virtual bool CheckAffinity(IInvokerPtr /*invoker*/) const
        {
            return true;
        }
#endif
    private:
        TDispatcherPoller* const Owner_;

        TLockFreeQueue<TClosure> Callbacks_;
        int PipeFDs[2];

        void WriteToPipe()
        {
            char dummy = 0;
            YCHECK(write(PipeFDs[1], &dummy, 1) == 1);
        }

        void ReadFromPipe()
        {
            char dummy;
            while (read(PipeFDs[0], &dummy, 1) > 0);
        }

        void ArmPoller()
        {
            Owner_->Impl_.Set(nullptr, PipeFDs[0], CONT_POLL_READ|CONT_POLL_ONE_SHOT);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
