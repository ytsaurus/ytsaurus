#pragma once

#include "public.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Wraps TSpinLock and additionally acquires a global read lock preventing
//! concurrent forks from happening.
class TForkAwareSpinLock
    : private TNonCopyable
{
public:
    void Acquire();
    void Release();

private:
    TSpinLock SpinLock_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
