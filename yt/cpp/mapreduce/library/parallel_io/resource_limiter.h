#pragma once

#include <library/cpp/yt/memory/ref.h>

#include <util/system/condvar.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

enum EResourceLimiterLockType {
    SOFT,
    HARD
};

/// @brief Allow to limit usage of a resouce like RAM etc.
///
/// For example, you can pass it to several TParallelFileWriter-s and limit their total RAM usage.
class TResourceLimiter
    : public TMoveOnly
    , public TThrRefBase
{
public:
    TResourceLimiter(size_t limit);

    size_t GetLimit() const;

    void Acquire(size_t lockAmount, EResourceLimiterLockType lockType = EResourceLimiterLockType::SOFT);

    void Release(size_t lockAmount, EResourceLimiterLockType lockType = EResourceLimiterLockType::SOFT);

private:
    friend class TResourceGuard;

    const size_t Limit_;

    size_t CurrentSoftUsage_ = 0;
    size_t CurrentHardUsage_ = 0;

    TMutex Mutex_;
    TCondVar CondVar_;

private:
    bool CanLock(size_t lockAmount);

    bool HasEnoughHardMemoryLimit(size_t lockAmount);
};

////////////////////////////////////////////////////////////////////////////////

class TResourceGuard
    : public TMoveOnly
{
public:
    TResourceGuard(
        const ::TIntrusivePtr<TResourceLimiter>& limiter,
        size_t lockAmount,
        EResourceLimiterLockType lockType = EResourceLimiterLockType::SOFT
    );

    TResourceGuard(TResourceGuard&& other);

    ~TResourceGuard();
private:
    ::TIntrusivePtr<TResourceLimiter> Limiter_;
    size_t LockAmount_;
    EResourceLimiterLockType LockType_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
