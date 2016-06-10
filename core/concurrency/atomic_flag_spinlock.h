#pragma once

#include "public.h"

#include <atomic>

////////////////////////////////////////////////////////////////////////////////

//! Specializes Arcadia's TCommonLockOps for std::atomic_flag.
template <>
struct TCommonLockOps<std::atomic_flag>
{
    static void Acquire(std::atomic_flag* flag) throw ()
    {
        while (flag->test_and_set(std::memory_order_acquire));
    }

    static void Release(std::atomic_flag* flag) throw ()
    {
        flag->clear(std::memory_order_release);
    }
};

////////////////////////////////////////////////////////////////////////////////
