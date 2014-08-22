#pragma once

#include "public.h"

#include <util/system/spinlock.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Wraps TSpinLock and additionally acquires a global read lock preventing
//! concurrent forks from happening.
class TForkAwareSpinLock
    : private TNonCopyable
{
public:
    TForkAwareSpinLock();
    ~TForkAwareSpinLock();

    void Acquire();
    void Release();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

//! Acquires a global write lock around |::fork| call.
pid_t SafeFork();

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
