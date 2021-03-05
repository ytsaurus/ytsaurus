#include "single_queue_scheduler_thread.h"
#include "profiling_helpers.h"

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/shutdown.h>

#include <util/system/yield.h>
#include <util/system/env.h>
#include <util/system/backtrace.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFinalizerThread
{
private:
    static std::atomic<bool> ShutdownStarted;
    static std::atomic<bool> ShutdownFinished;
    static constexpr int ShutdownSpinCount = 100;

    class TInvoker
        : public IInvoker
    {
    public:
        explicit TInvoker(TFinalizerThread* owner)
            : Owner_(owner)
        {
            YT_VERIFY(Owner_->Refs_.fetch_add(1, std::memory_order_acquire) > 0);

            if (Owner_->Debug_) {
                CaptureBacktrace();

                auto guard = Guard(Owner_->InvokerLock_);
                Owner_->AliveInvokers_.insert(this);
            }
        }

        virtual ~TInvoker() override
        {
            if (Owner_->Debug_) {
                auto guard = Guard(Owner_->InvokerLock_);
                Owner_->AliveInvokers_.erase(this);
            }

            YT_VERIFY(Owner_->Refs_.fetch_sub(1, std::memory_order_release) > 0);
        }

        virtual void Invoke(TClosure callback) override
        {
            Owner_->Invoke(BIND([this_ = MakeStrong(this), callback = std::move(callback)] {
                TCurrentInvokerGuard guard(std::move(this_));
                callback.Run();
            }));
        }

        const std::vector<void*>& GetBacktrace() const
        {
            return Backtrace_;
        }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        virtual NConcurrency::TThreadId GetThreadId() const override
        {
            return Owner_->Queue_->GetThreadId();
        }

        virtual bool CheckAffinity(const IInvokerPtr& invoker) const override
        {
            return invoker->GetThreadId() == Owner_->Queue_->GetThreadId();
        }
#endif
    private:
        TFinalizerThread* Owner_;
        std::vector<void*> Backtrace_;

        void CaptureBacktrace()
        {
            Backtrace_.resize(128);
            while (true) {
                auto size = BackTrace(Backtrace_.data(), Backtrace_.size());
                if (size != Backtrace_.size()) {
                    Backtrace_.resize(size);
                    break;
                }

                Backtrace_.resize(Backtrace_.size() * 2);
            }
        }
    };

    bool IsSameProcess()
    {
        return getpid() == OwningPid_;
    }

public:
    TFinalizerThread()
        : Debug_(GetEnv("YT_FINALIZER_DEBUG") != "")
        , ThreadName_("Finalizer")
        , Queue_(New<TMpscInvokerQueue>(
            CallbackEventCount_,
            GetThreadTags("Finalizer")))
        , Thread_(New<TMpscSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            ThreadName_,
            GetThreadTags("Finalizer")))
        , OwningPid_(getpid())
    { }

    ~TFinalizerThread()
    {
        Shutdown();
    }

    void Shutdown()
    {
        bool expected = false;
        if (!ShutdownStarted.compare_exchange_strong(expected, true)) {
            while (!ShutdownFinished) {
                SchedYield();
            }
            return;
        }

        if (IsSameProcess()) {
            // Wait until all alive invokers would terminate.
            if (Refs_ != 1) {
                // Spin for 30s.
                for (int i = 0; i < 30000; ++i) {
                    if (Refs_ == 1) {
                        break;
                    }
                    Sleep(TDuration::MilliSeconds(1));
                }
                if (Refs_ != 1) {
                    // Things gone slightly bad.
                    TRefCountedTrackerFacade::Dump();

                    {
                        auto guard = Guard(InvokerLock_);
                        for (auto invoker : AliveInvokers_) {
                            auto backtrace = invoker->GetBacktrace();
                            Cerr << "\nTFinalizerThread::TInvoker 0x%x" << static_cast<void*>(invoker) << " allocated at:\n";
                            FormatBackTrace(&Cerr, backtrace.data(), backtrace.size());
                        }
                    }

                    YT_VERIFY(false && "Hung during ShutdownFinalizerThread. Run with YT_FINALIZER_DEBUG=1 to see detailed output.");
                }
            }

            // There might be pending actions (i. e. finalizer thread may execute TFuture::dtor
            // which temporary acquires finalizer invoker). Spin for a while to give pending actions
            // some time to finish.
            for (int i = 0; i < ShutdownSpinCount; ++i) {
                BIND([] () {}).AsyncVia(Queue_).Run().Get();
            }

            int refs = 1;
            YT_VERIFY(Refs_.compare_exchange_strong(refs, 0));

            Queue_->Shutdown();
            Thread_->Shutdown();

            Queue_->Drain();
        }

        ShutdownFinished = true;
    }

    void Invoke(TClosure callback)
    {
        YT_VERIFY(!ShutdownFinished);
        EnsureStarted();
        Queue_->Invoke(std::move(callback));
    }

    IInvokerPtr GetInvoker()
    {
        EnsureStarted();
        return New<TInvoker>(this);
    }

private:
    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const std::shared_ptr<TEventCount> ShutdownEventCount_ = std::make_shared<TEventCount>();

    const bool Debug_;
    YT_DECLARE_SPINLOCK(TAdaptiveLock, InvokerLock_);
    THashSet<TInvoker*> AliveInvokers_;

    const TString ThreadName_;
    const TMpscInvokerQueuePtr Queue_;
    const TMpscSingleQueueSchedulerThreadPtr Thread_;

    int OwningPid_ = 0;
    std::atomic<int> Refs_ = 1;

    void EnsureStarted()
    {
        Thread_->Start();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::atomic<bool> TFinalizerThread::ShutdownStarted = {false};
std::atomic<bool> TFinalizerThread::ShutdownFinished = {false};

////////////////////////////////////////////////////////////////////////////////

static TFinalizerThread& GetFinalizerThread()
{
    static TFinalizerThread thread;
    return thread;
}

IInvokerPtr GetFinalizerInvoker()
{
    return GetFinalizerThread().GetInvoker();
}

void ShutdownFinalizerThread()
{
    return GetFinalizerThread().Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(1, ShutdownFinalizerThread);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

