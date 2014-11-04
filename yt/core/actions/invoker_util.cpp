#include "stdafx.h"
#include "invoker_util.h"

#include <stack>

#include <core/misc/singleton.h>

#include <core/actions/bind.h>
#include <core/actions/callback.h>
#include <core/actions/invoker.h>

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

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual void VerifyAffinity() const override
    {
        YUNREACHABLE();
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
    virtual void VerifyAffinity() const override
    {
        YUNREACHABLE();
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
