#include "resource_limiter.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TResourceLimiter::TResourceLimiter(size_t limit)
    : Limit_(limit)
{
    Y_ENSURE(Limit_ > 0);
}

void TResourceLimiter::Acquire(size_t lockAmount) {
    Y_ENSURE(lockAmount <= Limit_);
    with_lock(Mutex_) {
        CondVar_.Wait(Mutex_, [this, lockAmount]{ return CurrentUsage_ + lockAmount <= Limit_; });
        CurrentUsage_ += lockAmount;
    }
}

void TResourceLimiter::Release(size_t lockAmount) {
    with_lock(Mutex_) {
        CurrentUsage_ -= lockAmount;
    }
    // Broadcast because:
    // 1. We may have locked on assigning small chunks of data, which in sum smaller than unlocked lockAmount
    //    and we want to assign all, not only one next. Only one will be inefficient;
    // 2. We may not care about all threads awaken problem because lock/unlock in TParallelFileWriter is really rare.
    CondVar_.BroadCast();
}

size_t TResourceLimiter::GetLimit() const {
    return Limit_;
}

////////////////////////////////////////////////////////////////////////////////

TResourceGuard::TResourceGuard(const ::TIntrusivePtr<TResourceLimiter>& limiter, size_t lockAmount)
    : Limiter_(limiter)
    , LockAmount_(lockAmount)
{
    Limiter_->Acquire(LockAmount_);
}

TResourceGuard::TResourceGuard(TResourceGuard&& other) {
    Limiter_ = other.Limiter_;
    LockAmount_ = other.LockAmount_;
    other.LockAmount_ = 0;
}

TResourceGuard::~TResourceGuard() {
    if (LockAmount_ > 0) {
        Limiter_->Release(LockAmount_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
