#pragma once

#include <library/cpp/yt/memory/ref.h>

#include <util/system/condvar.h>
#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

enum EResourceLimiterLockType {
    SOFT,
    HARD
};

/// @brief Allow to limit usage of a resource like RAM etc.
///
/// For example, you can pass it to several TParallelFileWriter-s and limit their total RAM usage.
class IResourceLimiter
    : public TMoveOnly
    , public TThrRefBase
{
public:
    virtual size_t GetLimit() const noexcept = 0;

    virtual const TString& GetName() const noexcept = 0;

    // common Acquire/Release with default lockType SOFT
    void Acquire(size_t lockAmount);

    void Release(size_t lockAmount);

    virtual void Acquire(size_t lockAmount, EResourceLimiterLockType lockType) = 0;

    virtual void Release(size_t lockAmount, EResourceLimiterLockType lockType) = 0;
};

using IResourceLimiterPtr = ::TIntrusivePtr<IResourceLimiter>;


/// Basic implementation of resource limiter
class TResourceLimiter
    : public IResourceLimiter
{
public:
    TResourceLimiter(size_t limit, const TString &name = "");

    size_t GetLimit() const noexcept override;

    const TString& GetName() const noexcept override;

    void Acquire(size_t lockAmount, EResourceLimiterLockType lockType) override;

    void Release(size_t lockAmount, EResourceLimiterLockType lockType) override;

private:

    const size_t Limit_;
    const TString Name_;

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
        const IResourceLimiterPtr& limiter,
        size_t lockAmount,
        EResourceLimiterLockType lockType = EResourceLimiterLockType::SOFT
    );

    TResourceGuard(TResourceGuard&& other);

    ~TResourceGuard();

    size_t GetLockedAmount() const;

private:
    IResourceLimiterPtr Limiter_;
    size_t LockAmount_;
    EResourceLimiterLockType LockType_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
