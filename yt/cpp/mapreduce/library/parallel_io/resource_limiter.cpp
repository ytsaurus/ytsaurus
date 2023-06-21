#include "resource_limiter.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TResourceLimiter::TResourceLimiter(size_t limit)
    : Limit_(limit)
{
    Y_ENSURE(Limit_ > 0);
}

bool TResourceLimiter::HasEnoughHardMemoryLimit(size_t lockAmount) {
    return lockAmount <= Limit_ - CurrentHardUsage_;
}

bool TResourceLimiter::CanLock(size_t lockAmount) {
    return CurrentSoftUsage_ + lockAmount <= Limit_ - CurrentHardUsage_;
}

void TResourceLimiter::Acquire(size_t lockAmount, EResourceLimiterLockType lockType) {
    with_lock(Mutex_) {
        CondVar_.Wait(Mutex_, [this, lockAmount]{
            return CanLock(lockAmount) || !HasEnoughHardMemoryLimit(lockAmount);
        });
        if (!HasEnoughHardMemoryLimit(lockAmount)) {
            ythrow yexception() << "Acquire" << " " << lockAmount << " >= Limit_ - CurrentHardUsage_ "
                    << "(" << Limit_ << " - " << CurrentHardUsage_ << ") = " << Limit_ - CurrentHardUsage_;
        }
        switch (lockType) {
            case EResourceLimiterLockType::SOFT: {
                CurrentSoftUsage_ += lockAmount;
                break;
            }
            case EResourceLimiterLockType::HARD: {
                CurrentHardUsage_ += lockAmount;
                break;
            }
        };
    }
    // Acquiring hard limit may decrease available lock amount to such values that
    // previously valid (and currently blocked) acquire calls will no longer be possible
    // Broadcast all waiters to check HasEnoughHardMemoryLimit
    if (lockType == EResourceLimiterLockType::HARD) {
        CondVar_.BroadCast();
    }
}

void TResourceLimiter::Release(size_t lockAmount, EResourceLimiterLockType lockType) {
    with_lock(Mutex_) {
        switch (lockType) {
            case EResourceLimiterLockType::SOFT: {
                CurrentSoftUsage_ -= lockAmount;
                break;
            }
            case EResourceLimiterLockType::HARD: {
                CurrentHardUsage_ -= lockAmount;
                break;
            }
        };
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

TResourceGuard::TResourceGuard(
    const ::TIntrusivePtr<TResourceLimiter>& limiter,
    size_t lockAmount,
    EResourceLimiterLockType lockType
)
    : Limiter_(limiter)
    , LockAmount_(lockAmount)
    , LockType_(lockType)
{
    Limiter_->Acquire(LockAmount_, LockType_);
}

TResourceGuard::TResourceGuard(TResourceGuard&& other) {
    Limiter_ = other.Limiter_;
    LockAmount_ = other.LockAmount_;
    LockType_ = other.LockType_;
    other.LockAmount_ = 0;
}

TResourceGuard::~TResourceGuard() {
    if (LockAmount_ > 0) {
        Limiter_->Release(LockAmount_, LockType_);
    }
}

size_t TResourceGuard::GetLockedAmount() const {
    return LockAmount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
