#pragma once

#include <library/cpp/yt/memory/ref.h>

#include <util/system/condvar.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

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

    void Acquire(size_t lockAmount);

    void Release(size_t lockAmount);

private:
    friend class TResourceGuard;

    const size_t Limit_;

    size_t CurrentUsage_ = 0;

    TMutex Mutex_;
    TCondVar CondVar_;
};

////////////////////////////////////////////////////////////////////////////////

class TResourceGuard
    : public TMoveOnly
{
public:
    TResourceGuard(const ::TIntrusivePtr<TResourceLimiter>& limiter, size_t lockAmount);

    TResourceGuard(TResourceGuard&& other);

    ~TResourceGuard();

private:
    ::TIntrusivePtr<TResourceLimiter> Limiter_;
    size_t LockAmount_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
