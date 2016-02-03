#include "async_semaphore.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TAsyncSemaphore::TAsyncSemaphore(i64 totalSlots)
    : TotalSlots_(totalSlots)
    , FreeSlots_(totalSlots)
{
    YCHECK(TotalSlots_ > 0);
}

void TAsyncSemaphore::Release(i64 slots /* = 1 */)
{
    YCHECK(slots >= 0);

    TPromise<void> readyEventToSet;
    TPromise<void> freeEventToSet;
    std::vector<TWaiter> waitersToRelease;

    {
        TGuard<TSpinLock> guard(SpinLock_);

        FreeSlots_ += slots;
        YASSERT(FreeSlots_ <= TotalSlots_);

        while (!Waiters_.empty() && FreeSlots_ >= Waiters_.front().Slots) {
            auto& waiter = Waiters_.front();
            FreeSlots_ -= waiter.Slots;
            waitersToRelease.push_back(std::move(waiter));
            Waiters_.pop();
        }

        if (ReadyEvent_ && FreeSlots_ > 0) {
            swap(readyEventToSet, ReadyEvent_);
        }

        if (FreeEvent_ && FreeSlots_ == TotalSlots_) {
            swap(freeEventToSet, FreeEvent_);
        }
    }

    for (const auto& waiter : waitersToRelease) {
        waiter.Invoker->Invoke(BIND(waiter.Handler, Passed(TAsyncSemaphoreGuard(this, waiter.Slots))));
    }

    if (readyEventToSet) {
        readyEventToSet.Set();
    }

    if (freeEventToSet) {
        freeEventToSet.Set();
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
        YCHECK(!ReadyEvent_);
        return VoidFuture;
    } else if (!ReadyEvent_) {
        ReadyEvent_ = NewPromise<void>();
    }

    return ReadyEvent_;
}

TFuture<void> TAsyncSemaphore::GetFreeEvent()
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (FreeSlots_ == TotalSlots_) {
        YCHECK(!FreeEvent_);
        return VoidFuture;
    } else if (!FreeEvent_) {
        FreeEvent_ = NewPromise<void>();
    }

    return FreeEvent_;
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
}

TAsyncSemaphoreGuard::TAsyncSemaphoreGuard()
    : Semaphore_(nullptr)
    , Slots_(0)
{ }

TAsyncSemaphoreGuard::TAsyncSemaphoreGuard(TAsyncSemaphore* semaphore, i64 slots)
    : Semaphore_(semaphore)
    , Slots_(slots)
{ }

TAsyncSemaphoreGuard TAsyncSemaphoreGuard::Acquire(TAsyncSemaphore* semaphore, i64 slots /*= 1*/)
{
    semaphore->Acquire(slots);
    return TAsyncSemaphoreGuard(semaphore, slots);
}

TAsyncSemaphoreGuard TAsyncSemaphoreGuard::TryAcquire(TAsyncSemaphore* semaphore, i64 slots /*= 1*/)
{
    if (semaphore->TryAcquire(slots)) {
        return TAsyncSemaphoreGuard(semaphore, slots);
    } else {
        return TAsyncSemaphoreGuard();
    }
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
    return Semaphore_ != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
