#include "single_queue_scheduler_thread.h"
#include "profiling_helpers.h"

#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/shutdown.h>

#include <yt/core/misc/ref_counted_tracker.h>

namespace NYT {
namespace NConcurrency {

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
            YCHECK(Owner_->Refs_.fetch_add(1, std::memory_order_acquire) > 0);
        }

        virtual ~TInvoker() override
        {
            YCHECK(Owner_->Refs_.fetch_sub(1, std::memory_order_release) > 0);
        }

        virtual void Invoke(TClosure callback) override
        {
            Owner_->Invoke(std::move(callback));
        }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        virtual NConcurrency::TThreadId GetThreadId() const override
        {
            return Owner_->Queue_->GetThreadId();
        }

        virtual bool CheckAffinity(const IInvokerPtr& invoker) const override
        {
            return Owner_->Queue_->CheckAffinity(invoker);
        }
#endif
    private:
        TFinalizerThread* Owner_;
    };

public:
    TFinalizerThread()
        : ThreadName_("Finalizer")
        , Queue_(New<TInvokerQueue>(
            CallbackEventCount_,
            GetThreadTagIds(false, ThreadName_),
            false,
            false))
        , Thread_(New<TSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            ThreadName_,
            GetThreadTagIds(false, ThreadName_),
            false,
            false))
        , OwningPid_(getpid())
    { }

    ~TFinalizerThread()
    {
        Shutdown();
    }

    bool IsSameProcess()
    {
        return getpid() == OwningPid_;
    }

    void Start()
    {
        Thread_->Start();
        // XXX(sandello): Racy! Fix me by moving this into OnThreadStart().
        Queue_->SetThreadId(Thread_->GetId());
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
                    // Things have gone really bad.
                    fprintf(stderr, "Hung during FinalizerThread shutdown\n");
                    TRefCountedTrackerFacade::Dump();
                    _exit(100);
                }
            }

            // There might be pending actions (i. e. finalizer thread may execute TFuture::dtor
            // which temporary acquires finalizer invoker). Spin for a while to give pending actions
            // some time to finish.
            for (int i = 0; i < ShutdownSpinCount; ++i) {
                BIND([] () {}).AsyncVia(Queue_).Run().Get();
            }

            int refs = 1;
            YCHECK(Refs_.compare_exchange_strong(refs, 0));

            Queue_->Shutdown();
            Thread_->Shutdown();

            Queue_->Drain();
        }

        ShutdownFinished = true;
    }

    bool IsStarted() const
    {
        return Thread_->IsStarted();
    }

    void Invoke(TClosure callback)
    {
        YCHECK(!ShutdownFinished);
        if (!Y_UNLIKELY(IsStarted())) {
            Start();
        }
        Queue_->Invoke(std::move(callback));
    }

    IInvokerPtr GetInvoker()
    {
        // XXX(sandello): Better-than-static lifetime for TFinalizerThread?
        return New<TInvoker>(this);
    }

private:
    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const std::shared_ptr<TEventCount> ShutdownEventCount_ = std::make_shared<TEventCount>();

    const TString ThreadName_;
    const TInvokerQueuePtr Queue_;
    const TSingleQueueSchedulerThreadPtr Thread_;

    int OwningPid_ = 0;
    std::atomic<int> Refs_ = {1};
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

} // namespace NConcurrency
} // namespace NYT

