#include "stdafx.h"
#include "invoker_util.h"

#include <stack>

#include <core/misc/singleton.h>
#include <core/misc/lazy_ptr.h>

#include <core/actions/bind.h>
#include <core/actions/callback.h>

#include <core/concurrency/fls.h>
#include <core/concurrency/action_queue.h>

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

static TLazyIntrusivePtr<TActionQueue> FinalizerThread(
    TActionQueue::CreateFactory("Finalizer", false, false));
static std::atomic<bool> FinalizerThreadShutdown;

IInvokerPtr GetFinalizerInvoker()
{
    return FinalizerThreadShutdown ? GetNullInvoker() : FinalizerThread->GetInvoker();
}

void ShutdownFinalizerThread()
{
    if (FinalizerThread.HasValue()) {
        FinalizerThread->Shutdown();
    }
    FinalizerThreadShutdown = true;
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
