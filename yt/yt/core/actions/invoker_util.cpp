#include "invoker_util.h"
#include "invoker.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/fls.h>
#include <yt/yt/core/concurrency/finalizer_thread.h>

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <stack>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSyncInvoker
    : public IInvoker
{
public:
    void Invoke(TClosure callback) override
    {
        auto& state = *FiberState_;

        // We optimize invocation of recursion-free callbacks, i.e. those that do
        // not invoke anything in our invoker. This is done by introducing the AlreadyInvoking
        // flag which allows us to handle such case without allocation of the deferred
        // callback queue.

        if (state.AlreadyInvoking) {
            // Ensure deferred callback queue exists and push our callback into it.
            if (!state.DeferredCallbacks) {
                state.DeferredCallbacks.emplace();
            }
            state.DeferredCallbacks->push(std::move(callback));
        } else {
            // We are the outermost callback; execute synchronously.
            state.AlreadyInvoking = true;
            callback.Run();
            callback.Reset();
            // If some callbacks were deferred, execute them until the queue is drained.
            // Note that some of the callbacks may defer new callbacks, which is perfectly valid.
            if (state.DeferredCallbacks) {
                while (!state.DeferredCallbacks->empty()) {
                    state.DeferredCallbacks->front().Run();
                    state.DeferredCallbacks->pop();
                }
                // Reset queue to reduce fiber memory footprint.
                state.DeferredCallbacks.reset();
            }
            state.AlreadyInvoking = false;
        }
    }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    bool CheckAffinity(const IInvokerPtr& invoker) const override
    {
        return invoker.Get() == this;
    }

    TThreadId GetThreadId() const override
    {
        return InvalidThreadId;
    }
#endif

private:
    struct TFiberState
    {
        bool AlreadyInvoking = false;
        std::optional<TRingQueue<TClosure>> DeferredCallbacks;
    };

    static TFls<TFiberState> FiberState_;
};

TFls<TSyncInvoker::TFiberState> TSyncInvoker::FiberState_;

IInvokerPtr GetSyncInvoker()
{
    return LeakyRefCountedSingleton<TSyncInvoker>();
}

////////////////////////////////////////////////////////////////////////////////

class TNullInvoker
    : public IInvoker
{
public:
    void Invoke(TClosure /*callback*/) override
    { }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    bool CheckAffinity(const IInvokerPtr& /*invoker*/) const override
    {
        return false;
    }

    TThreadId GetThreadId() const override
    {
        return InvalidThreadId;
    }
#endif
};

IInvokerPtr GetNullInvoker()
{
    return LeakyRefCountedSingleton<TNullInvoker>();
}

IInvokerPtr GetFinalizerInvoker()
{
    return NConcurrency::GetFinalizerInvoker();
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

    invoker->Invoke(BIND_DONT_CAPTURE_TRACE_CONTEXT(
        std::move(doInvoke),
        Passed(std::move(onSuccess)),
        Passed(TGuard(std::move(onCancel)))));
}

////////////////////////////////////////////////////////////////////////////////

thread_local IInvokerPtr CurrentInvoker;

IInvokerPtr GetCurrentInvoker()
{
    return CurrentInvoker ? CurrentInvoker : GetSyncInvoker();
}

void SetCurrentInvoker(IInvokerPtr invoker)
{
    CurrentInvoker = std::move(invoker);
}

TCurrentInvokerGuard::TCurrentInvokerGuard(IInvokerPtr invoker)
    : NConcurrency::TContextSwitchGuard(
        [this] () noexcept {
            Restore();
        },
        nullptr)
    , Active_(true)
    , SavedInvoker_(std::move(invoker))
{
    CurrentInvoker.Swap(SavedInvoker_);
}

void TCurrentInvokerGuard::Restore()
{
    if (!Active_) {
        return;
    }
    Active_ = false;
    CurrentInvoker = std::move(SavedInvoker_);
}

TCurrentInvokerGuard::~TCurrentInvokerGuard()
{
    Restore();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
