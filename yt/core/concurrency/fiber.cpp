#include "fiber.h"

#include "atomic_flag_spinlock.h"

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
    NYTAlloc::SetCurrentFiberId(id);
    CurrentFiberId = id;
}

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

////////////////////////////////////////////////////////////////////////////////

class TFiberRegistry
{
public:
    std::list<TFiber*>::iterator Register(TFiber* fiber)
    {
        TGuard<std::atomic_flag> guard(Lock_);
        return Fibers_.insert(Fibers_.begin(), fiber);
    }

    void Unregister(std::list<TFiber*>::iterator iterator)
    {
        TGuard<std::atomic_flag> guard(Lock_);
        Fibers_.erase(iterator);
    }

private:
    std::atomic_flag Lock_ = ATOMIC_FLAG_INIT;
    std::list<TFiber*> Fibers_;

};

// Cache registry in static variable to simplify introspection.
static TFiberRegistry* FiberRegistry;

TFiberRegistry* GetFiberRegistry()
{
    if (!FiberRegistry) {
        FiberRegistry = Singleton<TFiberRegistry>();
    }
    return FiberRegistry;
}

////////////////////////////////////////////////////////////////////////////////

class TFiberExecutionStackProfiler
{
public:
    void StackAllocated(int stackSize)
    {
        Profiler_.Increment(BytesAllocated_, stackSize);
        Profiler_.Increment(BytesAlive_, stackSize);
    }

    void StackFreed(int stackSize)
    {
        Profiler_.Increment(BytesFreed_, stackSize);
        Profiler_.Increment(BytesAlive_, -stackSize);
    }

    static TFiberExecutionStackProfiler* Get()
    {
        return Singleton<TFiberExecutionStackProfiler>();
    }

private:
    NProfiling::TProfiler Profiler_{"/fiber_execution_stack"};
    NProfiling::TMonotonicCounter BytesAllocated_{"/bytes_allocated"};
    NProfiling::TMonotonicCounter BytesFreed_{"/bytes_freed"};
    NProfiling::TSimpleGauge BytesAlive_{"/bytes_alive"};
};

////////////////////////////////////////////////////////////////////////////////

TFiber::TFiber(EExecutionStackKind stackKind)
    : Stack_(CreateExecutionStack(stackKind))
    , Context_({
        this,
        TArrayRef(static_cast<char*>(Stack_->GetStack()), Stack_->GetSize())})
    , Registry_(GetFiberRegistry())
    , Iterator_(Registry_->Register(this))
{
    TFiberExecutionStackProfiler::Get()->StackAllocated(Stack_->GetSize());
    RegenerateId();
}

TFiber::~TFiber()
{
    TFiberExecutionStackProfiler::Get()->StackFreed(Stack_->GetSize());
    GetFiberRegistry()->Unregister(Iterator_);
}

TFiberId TFiber::GetId()
{
    return Id_;
}

bool TFiber::CheckFreeStackSpace(size_t space) const
{
    return reinterpret_cast<char*>(Stack_->GetStack()) + space < __builtin_frame_address(0);
}

TExceptionSafeContext* TFiber::GetContext()
{
    return &Context_;
}

void TFiber::RegenerateId()
{
    Id_ = FiberIdGenerator.Generate();
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

void TFiber::OnSwitchIn()
{
    OnStartRunning();

    NYTAlloc::SetCurrentMemoryTag(MemoryTag_);
    NYTAlloc::SetCurrentMemoryZone(MemoryZone_);
}

void TFiber::OnSwitchOut()
{
    OnFinishRunning();

    MemoryTag_ = NYTAlloc::GetCurrentMemoryTag();
    MemoryZone_ = NYTAlloc::GetCurrentMemoryZone();
}

NProfiling::TCpuDuration TFiber::GetRunCpuTime() const
{
    return RunCpuTime_ + std::max<NProfiling::TCpuDuration>(0, NProfiling::GetCpuInstant() - RunStartInstant_);
}

void TFiber::OnStartRunning()
{
    YT_VERIFY(!Running_.exchange(true));

    YT_VERIFY(CurrentFiberId == InvalidFiberId);
    SetCurrentFiberId(Id_);

    RunStartInstant_ = NProfiling::GetCpuInstant();
    InstallTraceContext(RunStartInstant_, std::move(SavedTraceContext_));

    NDetail::SetCurrentFsdHolder(&FsdHolder_);
}

void TFiber::OnFinishRunning()
{
    auto isRunning = Running_.exchange(false);
    YT_VERIFY(isRunning);

    auto now = NProfiling::GetCpuInstant();
    SavedTraceContext_ = NTracing::UninstallTraceContext(now);
    RunCpuTime_ += std::max<NProfiling::TCpuDuration>(0, now - RunStartInstant_);

    NDetail::SetCurrentFsdHolder(nullptr);

    YT_VERIFY(CurrentFiberId == Id_);
    SetCurrentFiberId(InvalidFiberId);
}

void TFiber::CancelEpoch(size_t epoch, const TError& error)
{
    TFuture<void> future;

    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (Epoch_.load(std::memory_order_relaxed) != epoch) {
            return;
        }

        bool expected = false;
        if (!Canceled_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
            return;
        }

        CancelationError_ = error;

        future = std::move(Future_);
    }

    if (future) {
        YT_LOG_DEBUG("Sending cancelation to fiber, propagating to the awaited future (TargetFiberId: %llx)",
            Id_);
        future.Cancel(error);
    } else {
        YT_LOG_DEBUG("Sending cancelation to fiber (TargetFiberId: %llx)",
            Id_);
    }
}

void TFiber::ResetForReuse()
{
    {
        TGuard<TSpinLock> guard(SpinLock_);

        ++Epoch_;
        Canceled_ = false;
        CancelationError_ = {};

        Canceler_.Reset();
        Future_.Reset();
    }

    RunCpuTime_ = 0;
    RunStartInstant_ = NProfiling::GetCpuInstant();
    SavedTraceContext_.Reset();

    auto oldId = Id_;
    RegenerateId();
    YT_LOG_TRACE("Reusing fiber (Id: %llx -> %llx)", oldId, Id_);
}

bool TFiber::IsCanceled() const
{
    return Canceled_.load(std::memory_order_relaxed);
}

TError TFiber::GetCancelationError() const
{
    TGuard<TSpinLock> guard(SpinLock_);
    return CancelationError_;
}

const TFiberCanceler& TFiber::GetCanceler()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);
    if (!Canceler_) {
        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        Canceler_ = BIND_DONT_CAPTURE_TRACE_CONTEXT(&TFiber::CancelEpoch, MakeWeak(this), Epoch_.load(std::memory_order_relaxed));
    }

    return Canceler_;
}

void TFiber::SetFuture(TFuture<void> future)
{
    TGuard<TSpinLock> guard(SpinLock_);
    Future_ = std::move(future);
}

void TFiber::ResetFuture()
{
    TGuard<TSpinLock> guard(SpinLock_);
    Future_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TFiberPtr& CurrentFiber();

bool CheckFreeStackSpace(size_t space)
{
    auto* currentFiber = CurrentFiber().Get();
    return !currentFiber || currentFiber->CheckFreeStackSpace(space);
}

void PushContextHandler(std::function<void()> out, std::function<void()> in)
{
    if (auto* currentFiber = CurrentFiber().Get()) {
        currentFiber->SwitchHandlers_.push_back({std::move(out), std::move(in)});
    }
}

void PopContextHandler()
{
    if (auto* currentFiber = CurrentFiber().Get()) {
        YT_VERIFY(!currentFiber->SwitchHandlers_.empty());
        currentFiber->SwitchHandlers_.pop_back();
    }
}

NProfiling::TCpuDuration GetCurrentFiberRunCpuTime()
{
    return CurrentFiber()->GetRunCpuTime();
}

TFiberCanceler GetCurrentFiberCanceler()
{
    auto* currentFiber = CurrentFiber().Get();
    return currentFiber ? currentFiber->GetCanceler() : TFiberCanceler();
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
