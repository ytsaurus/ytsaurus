#include "scheduler_api.h"
#include "fls.h"
#include "private.h"
#include "propagating_storage.h"

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/source_location.h>

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT::NConcurrency {

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TFiber)

void ResumeFiber(TFiberPtr fiber);

void YieldFiber(TClosure afterSwitch);

TFiberPtr& CurrentFiber();

////////////////////////////////////////////////////////////////////////////////

class TSwitchHandler;

TSwitchHandler* GetSwitchHandler();

////////////////////////////////////////////////////////////////////////////////

static class TFiberIdGenerator
{
public:
    TFiberIdGenerator()
    {
        Seed_.store(static_cast<TFiberId>(::time(nullptr)));
    }

    TFiberId Generate()
    {
        const TFiberId Factor = std::numeric_limits<TFiberId>::max() - 173864;
        YT_ASSERT(Factor % 2 == 1); // Factor must be coprime with 2^n.

        while (true) {
            auto seed = Seed_++;
            auto id = seed * Factor;
            if (id != InvalidFiberId) {
                return id;
            }
        }
    }

private:
    std::atomic<TFiberId> Seed_;

} FiberIdGenerator;

thread_local TFiberId CurrentFiberId;

TFiberId GetCurrentFiberId()
{
    return CurrentFiberId;
}

void SetCurrentFiberId(TFiberId id)
{
    CurrentFiberId = id;
}

////////////////////////////////////////////////////////////////////////////////

TContextSwitchGuard::TContextSwitchGuard(std::function<void()> out, std::function<void()> in)
{
    PushContextHandler(std::move(out), std::move(in));
}

TContextSwitchGuard::~TContextSwitchGuard()
{
    PopContextHandler();
}

TOneShotContextSwitchGuard::TOneShotContextSwitchGuard(std::function<void()> handler)
    : TContextSwitchGuard(
        [this, handler = std::move(handler)] () noexcept {
            if (!Active_) {
                return;
            }
            Active_ = false;
            handler();
        },
        nullptr)
    , Active_(true)
{ }

TForbidContextSwitchGuard::TForbidContextSwitchGuard()
    : TOneShotContextSwitchGuard( [] { YT_ABORT(); })
{ }

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCanceler)

class TCanceler
    : public ::NYT::NDetail::TBindStateBase
{
public:
    explicit TCanceler(TFiberId id)
        : TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            TSourceLocation("", 0)
#endif
        )
        , FiberId_(id)
    { }

    bool IsCanceled() const
    {
        return Canceled_.load(std::memory_order::relaxed);
    }

    void SetFuture(TFuture<void> awaitable)
    {
        auto guard = Guard(Lock_);
        Future_ = std::move(awaitable);
    }

    void ResetFuture()
    {
        auto guard = Guard(Lock_);
        Future_.Reset();
    }

    void Cancel(const TError& error)
    {
        bool expected = false;
        if (!Canceled_.compare_exchange_strong(expected, true, std::memory_order::relaxed)) {
            return;
        }

        TFuture<void> future;
        {
            auto guard = Guard(Lock_);
            CancelationError_ = error;
            future = std::move(Future_);
        }

        if (future) {
            YT_LOG_DEBUG("Sending cancelation to fiber, propagating to the awaited future (TargetFiberId: %x)",
                FiberId_);
            future.Cancel(error);
        } else {
            YT_LOG_DEBUG("Sending cancelation to fiber (TargetFiberId: %x)",
                FiberId_);
        }
    }

    TError GetCancelationError() const
    {
        auto guard = Guard(Lock_);
        return CancelationError_;
    }

    void Run(const TError& error)
    {
        Cancel(error);
    }

    static void StaticInvoke(const TError& error, NYT::NDetail::TBindStateBase* stateBase)
    {
        auto* state = static_cast<TCanceler*>(stateBase);
        return state->Run(error);
    }

    TFiberId GetFiberId() const
    {
        return FiberId_;
    }

private:
    const TFiberId FiberId_;

    std::atomic<bool> Canceled_ = false;
    NThreading::TSpinLock Lock_;
    TError CancelationError_;
    TFuture<void> Future_;
};

DEFINE_REFCOUNTED_TYPE(TCanceler)

TCancelerPtr& GetCanceler();

TFiberCanceler GetCurrentFiberCanceler()
{
    if (!GetSwitchHandler()) {
        // Not in fiber context.
        return TFiberCanceler();
    }

    if (!GetCanceler()) {
        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        GetCanceler() = New<TCanceler>(GetCurrentFiberId());
    }

    return TFiberCanceler(GetCanceler(), &TCanceler::StaticInvoke);
}

////////////////////////////////////////////////////////////////////////////////

//! All context thread local variables which must be preserved for each fiber are listed here.
class TBaseSwitchHandler
{
protected:
    void OnSwitch()
    {
        MemoryTag_ = SwapMemoryTag(MemoryTag_);
        FsdHolder_ = NDetail::SetCurrentFsdHolder(FsdHolder_);
        FiberId_ = SwapFiberId(FiberId_);
        PropagatingStorage_ = SwapCurrentPropagatingStorage(PropagatingStorage_);
    }

    ~TBaseSwitchHandler()
    {
        YT_VERIFY(MemoryTag_ == NYTAlloc::NullMemoryTag);
        YT_VERIFY(!FsdHolder_);
        YT_VERIFY(FiberId_ == InvalidFiberId);
        YT_VERIFY(PropagatingStorage_.IsNull());
    }

private:
    NYTAlloc::TMemoryTag MemoryTag_ = NYTAlloc::NullMemoryTag;
    NDetail::TFsdHolder* FsdHolder_ = nullptr;
    TFiberId FiberId_ = InvalidFiberId;
    TPropagatingStorage PropagatingStorage_;

    static NYTAlloc::TMemoryTag SwapMemoryTag(NYTAlloc::TMemoryTag tag)
    {
        auto result = NYTAlloc::GetCurrentMemoryTag();
        NYTAlloc::SetCurrentMemoryTag(tag);
        return result;
    }

    static TFiberId SwapFiberId(TFiberId fiberId)
    {
        auto result = GetCurrentFiberId();
        SetCurrentFiberId(fiberId);
        return result;
    }
};

struct TContextSwitchHandlers
{
    std::function<void()> Out;
    std::function<void()> In;
};

class TSwitchHandler
    : public TBaseSwitchHandler
{
public:
    // On start fiber running.
    TSwitchHandler()
    {
        SavedThis_ = This_;
        This_ = this;
    }

    TSwitchHandler(const TSwitchHandler&) = delete;
    TSwitchHandler(TSwitchHandler&&) = delete;

    // On finish fiber running.
    ~TSwitchHandler()
    {
        // Support case when current fiber has been resumed, but finished without WaitFor.
        // There is preserved context of resumer fiber saved in switchHandler. Restore it.
        // If there are no values for resumer the following call will swap null with null.

        YT_VERIFY(This_ == this);
        YT_VERIFY(UserHandlers_.empty());

        OnSwitch();
    }

    friend void PushContextHandler(std::function<void()> out, std::function<void()> in);
    friend void PopContextHandler();
    friend TSwitchHandler* GetSwitchHandler();
    friend TCancelerPtr& GetCanceler();

    class TGuard
    {
    public:
        TGuard(const TGuard&) = delete;
        TGuard(TGuard&&) = delete;

        TGuard()
            : SwitchHandler_(This_)
        {
            YT_VERIFY(SwitchHandler_);
            SwitchHandler_->OnOut();
        }

        ~TGuard()
        {
            SwitchHandler_->OnIn();
        }

    private:
        TSwitchHandler* const SwitchHandler_;
    };

private:
    TCompactVector<TContextSwitchHandlers, 16> UserHandlers_;
    TSwitchHandler* SavedThis_;
    static thread_local TSwitchHandler* This_;
    // Canceler could be thread_local variable but non POD TLS does not work properly in shared libraries.
    TCancelerPtr Canceler_;

    void OnSwitch()
    {
        // In user defined context switch callbacks (ContextSwitchGuard) Swap must be used. It preserves context
        // from fiber resumer.
        // In internal SwitchIn/SwitchOut Get/Set must be used.

        TBaseSwitchHandler::OnSwitch();

        std::swap(SavedThis_, This_);
    }

    // On finish fiber running.
    void OnOut()
    {
        for (auto it = UserHandlers_.rbegin(); it != UserHandlers_.rend(); ++it) {
            if (it->Out) {
                it->Out();
            }
        }
        OnSwitch();
    }

    // On start fiber running.
    void OnIn()
    {
        OnSwitch();
        for (auto it = UserHandlers_.rbegin(); it != UserHandlers_.rend(); ++it) {
            if (it->In) {
                it->In();
            }
        }
    }
};

thread_local TSwitchHandler* TSwitchHandler::This_;

TSwitchHandler* GetSwitchHandler()
{
    return TSwitchHandler::This_;
}

TCancelerPtr& GetCanceler()
{
    auto* switchHandler = GetSwitchHandler();
    YT_VERIFY(switchHandler);
    return switchHandler->Canceler_;
}

void PushContextHandler(std::function<void()> out, std::function<void()> in)
{
    if (auto* switchHandler = GetSwitchHandler()) {
        switchHandler->UserHandlers_.push_back({std::move(out), std::move(in)});
    }
}

void PopContextHandler()
{
    if (auto switchHandler = GetSwitchHandler()) {
        YT_VERIFY(!switchHandler->UserHandlers_.empty());
        switchHandler->UserHandlers_.pop_back();
    }
}

void RunInFiberContext(TClosure callback)
{
    TSwitchHandler switchHandler;

    YT_VERIFY(GetCurrentFiberId() == InvalidFiberId);
    auto fiberId = FiberIdGenerator.Generate();
    SetCurrentFiberId(fiberId);

    // Enable fiber local storage.
    NDetail::TFsdHolder fsdHolder;
    auto* oldFsd = NDetail::SetCurrentFsdHolder(&fsdHolder);
    YT_VERIFY(!oldFsd);
    auto fsdGuard = Finally([&] {
        auto* oldFsd = NDetail::SetCurrentFsdHolder(nullptr);
        YT_VERIFY(oldFsd == &fsdHolder);

        YT_VERIFY(GetCurrentFiberId() == fiberId);
        SetCurrentFiberId(InvalidFiberId);
    });

    TNullPropagatingStorageGuard propagatingStorageGuard;

    callback();
}

////////////////////////////////////////////////////////////////////////////////

// Compared to GuardedInvoke TResumeGuard reduces frame count in backtrace.
class TResumeGuard
{
public:
    TResumeGuard(TFiberPtr fiber, TCancelerPtr canceler)
        : Fiber_(std::move(fiber))
        , Canceler_(std::move(canceler))
    { }

    explicit TResumeGuard(TResumeGuard&& other)
        : Fiber_(std::move(other.Fiber_))
        , Canceler_(std::move(other.Canceler_))
    { }

    TResumeGuard(const TResumeGuard&) = delete;

    TResumeGuard& operator=(const TResumeGuard&) = delete;
    TResumeGuard& operator=(TResumeGuard&&) = delete;

    void operator()()
    {
        YT_VERIFY(Fiber_);
        Canceler_.Reset();
        ResumeFiber(std::move(Fiber_));
    }

    ~TResumeGuard()
    {
        if (Fiber_) {
            YT_LOG_TRACE("Unwinding fiber (TargetFiberId: %x)", Canceler_->GetFiberId());

            Canceler_->Run(TError("Fiber resumer is lost"));
            Canceler_.Reset();

            GetFinalizerInvoker()->Invoke(
                BIND_NO_PROPAGATE(&ResumeFiber, Passed(std::move(Fiber_))));
        }
    }

private:
    TFiberPtr Fiber_;
    TCancelerPtr Canceler_;
};

void WaitUntilSet(TFuture<void> future, IInvokerPtr invoker)
{
    YT_VERIFY(future);
    YT_ASSERT(invoker);

    auto* currentFiber = CurrentFiber().Get();
    if (!currentFiber) {
        // When called from a fiber-unfriendly context, we fallback to blocking wait.
        YT_VERIFY(invoker == GetCurrentInvoker());
        YT_VERIFY(invoker == GetSyncInvoker());
        YT_VERIFY(future.Wait());
        return;
    }

    YT_VERIFY(invoker != GetSyncInvoker());

    // Ensure canceler created.
    GetCurrentFiberCanceler();

    auto canceler = GetCanceler();
    if (canceler->IsCanceled()) {
        future.Cancel(canceler->GetCancelationError());
    }

    canceler->SetFuture(future);
    auto finally = Finally([&] {
        canceler->ResetFuture();
    });

    // TODO(lukyan): transfer resumer as argumnet of AfterSwitch.
    // Use CallOnTop like in boost.
    TClosure afterSwitch;
    {
        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        afterSwitch = BIND_NO_PROPAGATE([
            canceler,
            invoker = std::move(invoker),
            future = std::move(future),
            fiber = MakeStrong(currentFiber)
        ] () mutable {
            future.Subscribe(BIND_NO_PROPAGATE([
                invoker = std::move(invoker),
                fiber = std::move(fiber),
                canceler = std::move(canceler)
            ] (const TError&) mutable {
                YT_LOG_DEBUG("Waking up fiber (TargetFiberId: %x)",
                    canceler->GetFiberId());

                invoker->Invoke(BIND_NO_PROPAGATE(TResumeGuard{std::move(fiber), std::move(canceler)}));
            }));
        });
    }

    TSwitchHandler::TGuard switchGuard;
    YieldFiber(std::move(afterSwitch));

    if (canceler->IsCanceled()) {
        YT_LOG_DEBUG("Throwing fiber cancelation exception");
        throw TFiberCanceledException();
    }
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency

