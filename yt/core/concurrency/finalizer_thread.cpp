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

    IInvokerPtr GetInvoker()
    {
        YCHECK(!ShutdownFinished);
        if (!Y_UNLIKELY(IsStarted())) {
            Start();
        }
        return Queue_;
    }

private:
    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const TInvokerQueuePtr Queue_;
    const TSingleQueueSchedulerThreadPtr Thread_;
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

