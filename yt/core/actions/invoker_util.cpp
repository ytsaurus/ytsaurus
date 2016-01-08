#include "invoker_util.h"

#include <yt/core/actions/bind.h>
#include <yt/core/actions/callback.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/fls.h>

#include <yt/core/misc/lazy_ptr.h>
#include <yt/core/misc/singleton.h>

#include <stack>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSyncInvoker
    : public IInvoker
{
public:
    virtual void Invoke(const TClosure& callback) override
    {
        callback.Run();
    }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual bool CheckAffinity(IInvokerPtr invoker) const override
    {
        return invoker.Get() == this;
    }

    virtual TThreadId GetThreadId() const override
    {
        return InvalidThreadId;
    }
#endif
};

IInvokerPtr GetSyncInvoker()
{
    return RefCountedSingleton<TSyncInvoker>();
}

////////////////////////////////////////////////////////////////////////////////

class TNullInvoker
    : public IInvoker
{
public:
    virtual void Invoke(const TClosure& /*callback*/) override
    { }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual bool CheckAffinity(IInvokerPtr /*invoker*/) const override
    {
        return false;
    }

    virtual TThreadId GetThreadId() const override
    {
        return InvalidThreadId;
    }
#endif
};

IInvokerPtr GetNullInvoker()
{
    return RefCountedSingleton<TNullInvoker>();
}

////////////////////////////////////////////////////////////////////////////////

static std::atomic<bool> FinalizerThreadShutdownStarted = {false};
static std::atomic<bool> FinalizerThreadShutdownFinished = {false};
static const int FinalizerThreadShutdownSpinCount = 100;

static TActionQueuePtr GetFinalizerThread()
{
    static auto queue = New<TActionQueue>("Finalizer", false, false);
    return queue;
}

IInvokerPtr GetFinalizerInvoker()
{
    if (FinalizerThreadShutdownFinished) {
        return GetNullInvoker();
    }

    return GetFinalizerThread()->GetInvoker();
}

void ShutdownFinalizerThread()
{
    bool expected = false;
    if (!FinalizerThreadShutdownStarted.compare_exchange_strong(expected, true)) {
        return;
    }

    auto thread = GetFinalizerThread();
    auto invoker = thread->GetInvoker();
    // Spin for a while to give pending actions a chance to complete.
    for (int i = 0; i < FinalizerThreadShutdownSpinCount; ++i) {
        BIND([] () { }).AsyncVia(invoker).Run().Get();
    }

    // Now shutdown the finalizer thread.
    FinalizerThreadShutdownFinished = true;
    thread->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

void GuardedInvoke(
    IInvokerPtr invoker,
    TClosure onSuccess,
    TClosure onCancel)
{
    YASSERT(invoker);
    YASSERT(onSuccess);
    YASSERT(onCancel);

    class TGuard
    {
    public:
        explicit TGuard(TClosure onCancel)
            : OnCancel_(std::move(onCancel))
        { }

        TGuard(TGuard&& other) = default;

        ~TGuard()
        {
            if (OnCancel_) {
                OnCancel_.Run();
            }
        }

        void Release()
        {
            OnCancel_.Reset();
        }

    private:
        TClosure OnCancel_;

    };

    auto doInvoke = [] (TClosure onSuccess, TGuard guard) {
        guard.Release();
        onSuccess.Run();
    };

    invoker->Invoke(BIND(
        std::move(doInvoke),
        Passed(std::move(onSuccess)),
        Passed(TGuard(std::move(onCancel)))));
}

////////////////////////////////////////////////////////////////////////////////

static TFls<IInvokerPtr>& CurrentInvoker()
{
    static TFls<IInvokerPtr> invoker;
    return invoker;
}

IInvokerPtr GetCurrentInvoker()
{
    auto invoker = *CurrentInvoker();
    if (!invoker) {
        invoker = GetSyncInvoker();
    }
    return invoker;
}

void SetCurrentInvoker(IInvokerPtr invoker)
{
    *CurrentInvoker().Get() = std::move(invoker);
}

void SetCurrentInvoker(IInvokerPtr invoker, TFiber* fiber)
{
    *CurrentInvoker().Get(fiber) = std::move(invoker);
}

////////////////////////////////////////////////////////////////////////////////

TCurrentInvokerGuard::TCurrentInvokerGuard(IInvokerPtr invoker)
    : SavedInvoker_(std::move(invoker))
{
    CurrentInvoker()->Swap(SavedInvoker_);
}

TCurrentInvokerGuard::~TCurrentInvokerGuard()
{
    CurrentInvoker()->Swap(SavedInvoker_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
