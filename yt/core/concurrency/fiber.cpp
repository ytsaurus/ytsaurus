#include "fiber.h"

namespace NYT::NConcurrency {

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

TContextSwitchGuard::TContextSwitchGuard(std::function<void()> out, std::function<void()> in)
{
    PushContextHandler(std::move(out), std::move(in));
}

TContextSwitchGuard::~TContextSwitchGuard()
{
    PopContextHandler();
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

TForbidContextSwitchGuard::TForbidContextSwitchGuard()
    : TOneShotContextSwitchGuard( [] { YT_ABORT(); })
{ }

thread_local TFiberId CurrentFiberId;

//! Returns the current fiber id.
TFiberId GetCurrentFiberId()
{
    return CurrentFiberId;
}

//! Sets the current fiber id.
void SetCurrentFiberId(TFiberId id)
{
    CurrentFiberId = id;
}

////////////////////////////////////////////////////////////////////////////////

TFiber::TFiber(
    TClosure callee,
    EExecutionStackKind stackKind)
    : Callee_(std::move(callee))
    , Stack_(CreateExecutionStack(stackKind))
    , Context_({
        this,
        TArrayRef(static_cast<char*>(Stack_->GetStack()), Stack_->GetSize())})
{
    TFiberExecutionStackProfiler::Get()->StackAllocated(Stack_->GetSize());
    RegenerateId();
    YT_LOG_DEBUG("Fiber created (Id: %llx)", Id_);
}

TFiber::~TFiber()
{
    YT_VERIFY(Terminated);
    YT_LOG_DEBUG("Fiber destroyed (Id: %llx)", Id_);
}

void TFiber::InvokeContextOutHandlers()
{
    for (auto it = SwitchHandlers_.rbegin(); it != SwitchHandlers_.rend(); ++it) {
        if (it->Out) {
            it->Out();
        }
    }
}

void TFiber::InvokeContextInHandlers()
{
    for (auto it = SwitchHandlers_.rbegin(); it != SwitchHandlers_.rend(); ++it) {
        if (it->In) {
            it->In();
        }
    }
}

void TFiber::OnSwitchInto()
{
    SetCurrentFiberId(Id_);
    YT_LOG_TRACE("Switched into fiber (Id: %llx)", Id_);

    OnStartRunning();

    NYTAlloc::SetCurrentMemoryTag(MemoryTag_);
    NYTAlloc::SetCurrentMemoryZone(MemoryZone_);
}

void TFiber::OnSwitchOut()
{
    OnFinishRunning();

    MemoryTag_ = NYTAlloc::GetCurrentMemoryTag();
    MemoryZone_ = NYTAlloc::GetCurrentMemoryZone();

    YT_LOG_TRACE("Switching out fiber (Id: %llx)", Id_);
    SetCurrentFiberId(InvalidFiberId);
}

NProfiling::TCpuDuration TFiber::GetRunCpuTime() const
{
    return RunCpuTime_ + std::max<NProfiling::TCpuDuration>(0, NProfiling::GetCpuInstant() - RunStartInstant_);
}

void TFiber::OnStartRunning()
{
    RunStartInstant_ = NProfiling::GetCpuInstant();
    InstallTraceContext(RunStartInstant_, std::move(SavedTraceContext_));

    NDetail::SetCurrentFsdHolder(&FsdHolder_);
}

void TFiber::OnFinishRunning()
{
    auto now = NProfiling::GetCpuInstant();
    SavedTraceContext_ = NTracing::UninstallTraceContext(now);
    RunCpuTime_ += std::max<NProfiling::TCpuDuration>(0, now - RunStartInstant_);

    NDetail::SetCurrentFsdHolder(nullptr);
}

void TFiber::Cancel()
{
    bool expected = false;
    if (!Canceled_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
        return;
    }

    TAwaitable awaitable;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        awaitable = std::move(Awaitable_);
    }

    if (awaitable) {
        YT_LOG_DEBUG("Sending cancelation to fiber, propagating to the awaited future (TargetFiberId: %llx)",
            Id_);
        awaitable.Cancel();
    } else {
        YT_LOG_DEBUG("Sending cancelation to fiber (TargetFiberId: %llx)",
            Id_);
    }
}

void TFiber::ResetForReuse()
{
    ++Epoch_;
    Canceled_ = false;

    {
        TGuard<TSpinLock> guard(SpinLock_);
        Canceler_.Reset();
        Awaitable_.Reset();
    }

    RunCpuTime_ = 0;
    RunStartInstant_ = NProfiling::GetCpuInstant();

    auto oldId = Id_;
    RegenerateId();
    YT_LOG_DEBUG("Reusing fiber (Id: %llx -> %llx)", oldId, Id_);
}

const TClosure& TFiber::GetCanceler()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);
    if (!Canceler_) {
        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        Canceler_ = BIND_DONT_CAPTURE_TRACE_CONTEXT(&TFiber::CancelEpoch, MakeWeak(this), Epoch_.load());
    }

    return Canceler_;
}

void TFiber::SetAwaitable(TAwaitable awaitable)
{
    TGuard<TSpinLock> guard(SpinLock_);
    Awaitable_ = std::move(awaitable);
}

void TFiber::ResetAwaitable()
{
    TGuard<TSpinLock> guard(SpinLock_);
    Awaitable_.Reset();
}

void TFiber::DoRunNaked()
{
    if (!Canceled_.load(std::memory_order_relaxed)) {
        OnStartRunning();

        try {
            Callee_.Run();
        } catch (const TFiberCanceledException&) {
            // Thrown intentionally, ignore.
            YT_LOG_DEBUG("Fiber canceled");
        }

        OnFinishRunning();

        // NB: All other uncaught exceptions will lead to std::terminate().
        // This way we preserve the much-needed backtrace.
    }

    Terminated = true;

    SwitchToFiber(nullptr);

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

thread_local TFiberPtr CurrentFiber = nullptr;

bool CheckFreeStackSpace(size_t space)
{
    return !CurrentFiber || CurrentFiber->CheckFreeStackSpace(space);
}

void PushContextHandler(std::function<void()> out, std::function<void()> in)
{
    const auto& this_ = CurrentFiber;
    if (!this_) {
        return;
    }

    this_->SwitchHandlers_.push_back({std::move(out), std::move(in)});
}

void PopContextHandler()
{
    const auto& this_ = CurrentFiber;
    if (!this_) {
        return;
    }
    YT_VERIFY(!this_->SwitchHandlers_.empty());
    this_->SwitchHandlers_.pop_back();
}

NProfiling::TCpuDuration GetCurrentFiberRunCpuTime()
{
    return CurrentFiber->GetRunCpuTime();
}

TClosure GetCurrentFiberCanceler()
{
    return CurrentFiber ? CurrentFiber->GetCanceler() : TClosure();
}

thread_local TExceptionSafeContext ThreadContext;

TExceptionSafeContext* GetContext(const TFiberPtr& target)
{
    return target ? &target->Context_ : &ThreadContext;
}

thread_local TClosure AfterSwitch;

void SwitchToFiber(TFiberPtr target)
{
    YT_VERIFY(CurrentFiber != target);

    if (CurrentFiber) {
        CurrentFiber->OnSwitchOut();
    }

    auto context = GetContext(CurrentFiber);
    // Here current fiber could be destroyed. But it must be saved in AfterSwitch callback or other place.
    YT_VERIFY(!CurrentFiber || CurrentFiber->GetRefCount() > 1);
    CurrentFiber = std::move(target);
    context->SwitchTo(GetContext(CurrentFiber));

    // Allows set new AfterSwitch inside it.
    if (auto afterSwitch = std::move(AfterSwitch)) {
        afterSwitch.Run();
    }

    if (CurrentFiber) {
        CurrentFiber->OnSwitchInto();
    }
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
