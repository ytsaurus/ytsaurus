#pragma once

#include "public.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Wraps TSpinLock and additionally acquires a global read lock preventing
//! concurrent forks from happening.
class TForkAwareSpinLock
    : private TNonCopyable
{
public:
    void Acquire() noexcept;
    void Release() noexcept;

    bool IsLocked() noexcept;

private:
    TAdaptiveLock SpinLock_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
