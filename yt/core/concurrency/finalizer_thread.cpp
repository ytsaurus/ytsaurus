#include "single_queue_scheduler_thread.h"
#include "profiler_utils.h"

namespace NYT {
namespace NConcurrency {

///////////////////////////////////////////////////////////////////////////////

class TFinalizerThread
{
    static const Stroka ThreadName;
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
            Owner_->ShutdownEventCount_->NotifyAll();
        }

        virtual void Invoke(const TClosure& callback) override
        {
            Owner_->Invoke(callback);
        }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        virtual NConcurrency::TThreadId GetThreadId() const override
        {
            return Owner_->Queue_->GetThreadId();
        }

        virtual bool CheckAffinity(IInvokerPtr invoker) const override
        {
            return Owner_->Queue_->CheckAffinity(std::move(invoker));
        }
#endif
    private:
        TFinalizerThread* Owner_;
    };

public:
    TFinalizerThread()
        : Queue_(New<TInvokerQueue>(
            CallbackEventCount_,
            GetThreadTagIds(false, ThreadName),
            false,
            false))
        , Thread_(New<TSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            ThreadName,
            GetThreadTagIds(false, ThreadName),
            false,
            false,
            false))
    { }

    ~TFinalizerThread()
    {
        Shutdown();
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

        Refs_.fetch_sub(1, std::memory_order_relaxed);

        // Wait until all alive invokers would terminate.
        ShutdownEventCount_->Await([&] () {
            return Refs_.load(std::memory_order_relaxed) == 0;
        });

        // Make sure all pending actions are
        // Spin for a while to give pending actions some time to finish.
        for (int i = 0; i < ShutdownSpinCount; ++i) {
            BIND([] () {}).AsyncVia(Queue_).Run().Get();
        }

        Queue_->Shutdown();
        Thread_->Shutdown();

        ShutdownFinished = true;
    }

    bool IsStarted() const
    {
        return Thread_->IsStarted();
    }

    void Invoke(const TClosure& callback)
    {
        if (!Y_UNLIKELY(IsStarted())) {
            Start();
        }
        Queue_->Invoke(callback);
    }

    IInvokerPtr GetInvoker()
    {
        // XXX(sandello): Better-than-static lifetime for TFinalizerThread?
        return New<TInvoker>(this);
    }

private:
    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const std::shared_ptr<TEventCount> ShutdownEventCount_ = std::make_shared<TEventCount>();

    const TInvokerQueuePtr Queue_;
    const TSingleQueueSchedulerThreadPtr Thread_;

    std::atomic<int> Refs_ = {1};
};

///////////////////////////////////////////////////////////////////////////////

const Stroka TFinalizerThread::ThreadName = "Finalizer";
std::atomic<bool> TFinalizerThread::ShutdownStarted = {false};
std::atomic<bool> TFinalizerThread::ShutdownFinished = {false};

///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

