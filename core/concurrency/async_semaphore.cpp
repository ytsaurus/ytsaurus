#include "async_semaphore.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TAsyncSemaphore::TAsyncSemaphore(i64 totalSlots)
    : TotalSlots_(totalSlots)
    , FreeSlots_(totalSlots)
{
    YCHECK(TotalSlots_ >= 0);
}

void TAsyncSemaphore::Release(i64 slots /* = 1 */)
{
    YCHECK(slots >= 0);

    {
        TGuard<TSpinLock> guard(SpinLock_);

        FreeSlots_ += slots;
        Y_ASSERT(FreeSlots_ <= TotalSlots_);

        if (Releasing_) {
            return;
        }

        Releasing_ = true;
    }

    while (true) {
        std::vector<TWaiter> waitersToRelease;
        TPromise<void> readyEventToSet;

        {
            TGuard<TSpinLock> guard(SpinLock_);

            while (!Waiters_.empty() && FreeSlots_ >= Waiters_.front().Slots) {
                auto& waiter = Waiters_.front();
                FreeSlots_ -= waiter.Slots;
                waitersToRelease.push_back(std::move(waiter));
                Waiters_.pop();
            }

            if (ReadyEvent_ && FreeSlots_ > 0) {
                swap(readyEventToSet, ReadyEvent_);
            }

            if (waitersToRelease.empty() && !readyEventToSet) {
                Releasing_ = false;
                break;
            }
        }

        for (const auto& waiter : waitersToRelease) {
            // NB: This may lead to a reentrant invocation of Release if the invoker discards the callback.
            waiter.Invoker->Invoke(BIND(waiter.Handler, Passed(TAsyncSemaphoreGuard(this, waiter.Slots))));
        }

        if (readyEventToSet) {
            readyEventToSet.Set();
        }
    }
}

void TAsyncSemaphore::Acquire(i64 slots /* = 1 */)
{
    YCHECK(slots >= 0);

    TGuard<TSpinLock> guard(SpinLock_);
    FreeSlots_ -= slots;
}

bool TAsyncSemaphore::TryAcquire(i64 slots /*= 1*/)
{
    YCHECK(slots >= 0);

    TGuard<TSpinLock> guard(SpinLock_);
    if (FreeSlots_ < slots) {
        return false;
    }
    FreeSlots_ -= slots;
    return true;
}

void TAsyncSemaphore::AsyncAcquire(
    const TCallback<void(TAsyncSemaphoreGuard)>& handler,
    IInvokerPtr invoker,
    i64 slots)
{
    YCHECK(slots >= 0);

    TGuard<TSpinLock> guard(SpinLock_);
    if (FreeSlots_ >= slots) {
        FreeSlots_ -= slots;
        guard.Release();
        invoker->Invoke(BIND(handler, Passed(TAsyncSemaphoreGuard(this, slots))));
    } else {
        Waiters_.push(TWaiter{handler, std::move(invoker), slots});
    }
}

bool TAsyncSemaphore::IsReady() const
{
    return FreeSlots_ > 0;
}

bool TAsyncSemaphore::IsFree() const
{
    return FreeSlots_ == TotalSlots_;
}

i64 TAsyncSemaphore::GetTotal() const
{
    return TotalSlots_;
}

i64 TAsyncSemaphore::GetUsed() const
{
    return TotalSlots_ - FreeSlots_;
}

i64 TAsyncSemaphore::GetFree() const
{
    return FreeSlots_;
}

TFuture<void> TAsyncSemaphore::GetReadyEvent()
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (IsReady()) {
        return VoidFuture;
    } else if (!ReadyEvent_) {
        ReadyEvent_ = NewPromise<void>();
    }

    return ReadyEvent_;
}

////////////////////////////////////////////////////////////////////////////////

TProfiledAsyncSemaphore::TProfiledAsyncSemaphore(
    i64 totalSlots,
    const NProfiling::TProfiler& profiler,
    const NYPath::TYPath& path,
    const NProfiling::TTagIdList& tagIds)
    : TAsyncSemaphore(totalSlots)
    , Profiler(profiler)
    , Path_(path)
    , TagIds_(tagIds)
{ }

void TProfiledAsyncSemaphore::Release(i64 slots /* = 1 */)
{
    TAsyncSemaphore::Release(slots);
    Profiler.Enqueue(Path_, GetUsed(), NProfiling::EMetricType::Gauge, TagIds_);
}

void TProfiledAsyncSemaphore::Acquire(i64 slots /* = 1 */)
{
    TAsyncSemaphore::Acquire(slots);
    Profiler.Enqueue(Path_, GetUsed(), NProfiling::EMetricType::Gauge, TagIds_);
}

bool TProfiledAsyncSemaphore::TryAcquire(i64 slots /* = 1 */)
{
    if (TAsyncSemaphore::TryAcquire(slots)) {
        Profiler.Enqueue(Path_, GetUsed(), NProfiling::EMetricType::Gauge, TagIds_);
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TAsyncSemaphoreGuard::TAsyncSemaphoreGuard(TAsyncSemaphoreGuard&& other)
{
    MoveFrom(std::move(other));
}

TAsyncSemaphoreGuard::~TAsyncSemaphoreGuard()
{
    Release();
}

TAsyncSemaphoreGuard& TAsyncSemaphoreGuard::operator=(TAsyncSemaphoreGuard&& other)
{
    if (this != &other) {
        Release();
        MoveFrom(std::move(other));
    }
    return *this;
}

void TAsyncSemaphoreGuard::MoveFrom(TAsyncSemaphoreGuard&& other)
{
    Semaphore_ = other.Semaphore_;
    Slots_ = other.Slots_;

    other.Semaphore_ = nullptr;
    other.Slots_ = 0;
}

void swap(TAsyncSemaphoreGuard& lhs, TAsyncSemaphoreGuard& rhs)
{
    std::swap(lhs.Semaphore_, rhs.Semaphore_);
    std::swap(lhs.Slots_, rhs.Slots_);
}

TAsyncSemaphoreGuard::TAsyncSemaphoreGuard()
    : Slots_(0)
{ }

TAsyncSemaphoreGuard::TAsyncSemaphoreGuard(TAsyncSemaphorePtr semaphore, i64 slots)
    : Slots_(slots)
    , Semaphore_(semaphore)
{ }

TAsyncSemaphoreGuard TAsyncSemaphoreGuard::Acquire(TAsyncSemaphorePtr semaphore, i64 slots /*= 1*/)
{
    semaphore->Acquire(slots);
    return TAsyncSemaphoreGuard(semaphore, slots);
}

TAsyncSemaphoreGuard TAsyncSemaphoreGuard::TryAcquire(TAsyncSemaphorePtr semaphore, i64 slots /*= 1*/)
{
    if (semaphore->TryAcquire(slots)) {
        return TAsyncSemaphoreGuard(semaphore, slots);
    } else {
        return TAsyncSemaphoreGuard();
    }
}

TAsyncSemaphoreGuard TAsyncSemaphoreGuard::TransferSlots(i64 slotsToTransfer)
{
    YCHECK(slotsToTransfer >= 0 && slotsToTransfer <= Slots_);
    Slots_ -= slotsToTransfer;
    TAsyncSemaphoreGuard spawnedGuard;
    spawnedGuard.Semaphore_ = Semaphore_;
    spawnedGuard.Slots_ = slotsToTransfer;
    return spawnedGuard;
} 

void TAsyncSemaphoreGuard::Release()
{
    if (Semaphore_) {
        Semaphore_->Release(Slots_);
        Semaphore_ = nullptr;
        Slots_ = 0;
    }
}

TAsyncSemaphoreGuard::operator bool() const
{
    return static_cast<bool>(Semaphore_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
