#include "stdafx.h"
#include "invoker_util.h"

#include <stack>

#include <core/misc/singleton.h>

#include <core/concurrency/fls.h>

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

    virtual NConcurrency::TThreadId GetThreadId() const override
    {
        return NConcurrency::InvalidThreadId;
    }
};

IInvokerPtr GetSyncInvoker()
{
    return RefCountedSingleton<TSyncInvoker>();
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

        TGuard(TGuard&& other)
            : OnCancel_(std::move(other.OnCancel_))
        { }

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

TFls<IInvokerPtr> CurrentInvokerInFiber;
TLS_STATIC IInvoker* CurrentInvokerInThread = nullptr;

IInvokerPtr GetCurrentInvoker()
{
    auto scheduler = TryGetCurrentScheduler();
    if (scheduler) {
        return *CurrentInvokerInFiber.GetFor(scheduler->GetCurrentFiber());
    } else {
        if (!CurrentInvokerInThread) {
            CurrentInvokerInThread = GetSyncInvoker().Get();
            CurrentInvokerInThread->Ref();
        }
        return CurrentInvokerInThread;
    }
}

void SetCurrentInvoker(IInvokerPtr invoker)
{
    *CurrentInvokerInFiber = std::move(invoker);
}

void SetCurrentInvoker(IInvokerPtr invoker, TFiber* fiber)
{
    *CurrentInvokerInFiber.GetFor(fiber) = std::move(invoker);
}

TCurrentInvokerGuard::TCurrentInvokerGuard(IInvokerPtr invoker)
    : SavedInvoker_(std::move(invoker))
{
    Swap();
}

TCurrentInvokerGuard::~TCurrentInvokerGuard()
{
    Swap();
}

void TCurrentInvokerGuard::Swap()
{
    auto scheduler = TryGetCurrentScheduler();
    if (scheduler) {
        CurrentInvokerInFiber->Swap(SavedInvoker_);
    } else {
        auto saved = CurrentInvokerInThread;
        if (!saved) {
            saved = GetSyncInvoker().Get();
            saved->Ref();
        }
        CurrentInvokerInThread = SavedInvoker_.Get();
        CurrentInvokerInThread->Ref();
        SavedInvoker_ = IInvokerPtr(saved, false);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
