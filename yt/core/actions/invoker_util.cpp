#include "invoker_util.h"
#include "invoker.h"

#include <yt/core/actions/bind.h>
#include <yt/core/actions/callback.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/fls.h>
#include <yt/core/concurrency/finalizer_thread.h>

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
    virtual void Invoke(TClosure callback) override
    {
        callback.Run();
    }

    virtual TDuration GetAverageWaitTime() const override
    {
        return TDuration();
    }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual bool CheckAffinity(const IInvokerPtr& invoker) const override
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
    virtual void Invoke(TClosure /*callback*/) override
    { }

    virtual TDuration GetAverageWaitTime() const override
    {
        return TDuration();
    }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual bool CheckAffinity(const IInvokerPtr& /*invoker*/) const override
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

IInvokerPtr GetFinalizerInvoker()
{
    return NConcurrency::GetFinalizerInvoker();
}

void ShutdownFinalizerThread()
{
    return NConcurrency::ShutdownFinalizerThread();
}

////////////////////////////////////////////////////////////////////////////////

void GuardedInvoke(
    const IInvokerPtr& invoker,
    TClosure onSuccess,
    TClosure onCancel)
{
    YT_ASSERT(invoker);
    YT_ASSERT(onSuccess);
    YT_ASSERT(onCancel);

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

} // namespace NYT
