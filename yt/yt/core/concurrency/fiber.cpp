#include "fiber.h"

#include "atomic_flag_spinlock.h"

#include <atomic>

#include <yt/core/misc/singleton.h>

#include <yt/yt/library/profiling/producer.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

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

TFiberRegistry* GetFiberRegistry()
{
    return Singleton<TFiberRegistry>();
}

////////////////////////////////////////////////////////////////////////////////

class TFiberExecutionStackProfiler
    : public ISensorProducer
{
public:
    TFiberExecutionStackProfiler()
    {
        TRegistry{""}.AddProducer("/fiber_execution_stack", MakeStrong(this));
    }

    void StackAllocated(int stackSize)
    {
        BytesAllocated_.fetch_add(stackSize, std::memory_order_relaxed);
        BytesAlive_.fetch_add(stackSize, std::memory_order_relaxed);
    }

    void StackFreed(int stackSize)
    {
        BytesFreed_.fetch_add(stackSize, std::memory_order_relaxed);
        BytesAlive_.fetch_sub(stackSize, std::memory_order_relaxed);
    }

    void Collect(ISensorWriter* writer)
    {
        writer->AddCounter("/bytes_allocated", BytesAllocated_);
        writer->AddCounter("/bytes_freed", BytesFreed_);
        writer->AddGauge("/bytes_alive", BytesAlive_);
    }

    static TFiberExecutionStackProfiler* Get()
    {
        struct TLeaker
        {
            TIntrusivePtr<TFiberExecutionStackProfiler> Ptr = New<TFiberExecutionStackProfiler>();
        };

        return LeakySingleton<TLeaker>()->Ptr.Get();
    }

private:
    std::atomic<i64> BytesAllocated_ = 0;
    std::atomic<i64> BytesFreed_ = 0;
    std::atomic<i64> BytesAlive_ = 0;
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
}

void TFiber::OnSwitchOut()
{
    OnFinishRunning();
}

void TFiber::OnStartRunning()
{
    YT_VERIFY(!Running_.exchange(true));

    YT_VERIFY(CurrentFiberId == InvalidFiberId);
    SetCurrentFiberId(Id_);
}

void TFiber::OnFinishRunning()
{
    auto isRunning = Running_.exchange(false);
    YT_VERIFY(isRunning);

    YT_VERIFY(CurrentFiberId == Id_);
    SetCurrentFiberId(InvalidFiberId);
}

void TFiber::CancelEpoch(size_t epoch, const TError& error)
{
    TFuture<void> future;

    {
        auto guard = Guard(SpinLock_);

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
        auto guard = Guard(SpinLock_);

        ++Epoch_;
        Canceled_ = false;
        CancelationError_ = {};

        Canceler_.Reset();
        Future_.Reset();
    }

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
    auto guard = Guard(SpinLock_);
    return CancelationError_;
}

const TFiberCanceler& TFiber::GetCanceler()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    if (!Canceler_) {
        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        Canceler_ = BIND_DONT_CAPTURE_TRACE_CONTEXT(&TFiber::CancelEpoch, MakeWeak(this), Epoch_.load(std::memory_order_relaxed));
    }

    return Canceler_;
}

void TFiber::SetFuture(TFuture<void> future)
{
    auto guard = Guard(SpinLock_);
    Future_ = std::move(future);
}

void TFiber::ResetFuture()
{
    auto guard = Guard(SpinLock_);
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

TFiberCanceler GetCurrentFiberCanceler()
{
    auto* currentFiber = CurrentFiber().Get();
    return currentFiber ? currentFiber->GetCanceler() : TFiberCanceler();
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
