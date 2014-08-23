#pragma once

#include "public.h"

#include <util/system/rwlock.h>

#include <atomic>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Single-writer multiple-readers spin lock.
/*!
 *  Reader-side calls are pretty cheap.
 *  If no writers are present then readers never spin.
 *  The lock is unfair.
 */
class TReaderWriterSpinLock
{
public:
    TReaderWriterSpinLock()
        : Value_(0)
    { }

    void AcquireReader()
    {
        for (int counter = 0; ;++counter) {
            if (TryAcquireReader())
                break;
            if (counter > YieldThreshold) {
                SpinLockPause();
            }
        }
    }

    void ReleaseReader()
    {
        Value_.fetch_sub(ReaderDelta, std::memory_order_release);
    }

    void AcquireWriter()
    {
        for (int counter = 0; ;++counter) {
            if (TryAcquireWriter())
                break;
            if (counter > YieldThreshold) {
                SpinLockPause();
            }
        }
    }

    void ReleaseWriter()
    {
        Value_.store(0, std::memory_order_release);
    }

private:
    std::atomic<ui32> Value_;

    static const ui32 WriterMask = 1;
    static const ui32 ReaderDelta = 2;

    static const int YieldThreshold = 1000;


    bool TryAcquireReader()
    {
        ui32 oldValue = Value_.fetch_add(ReaderDelta, std::memory_order_acquire);
        if (oldValue & WriterMask) {
            Value_.fetch_sub(ReaderDelta, std::memory_order_relaxed);
            return false;
        }
        return true;
    }

    bool TryAcquireWriter()
    {
        ui32 expected = 0;
        if (!Value_.compare_exchange_weak(expected, WriterMask, std::memory_order_acquire)) {
            return false;
        }
        return true;
    }

};

////////////////////////////////////////////////////////////////////////////////

struct TReaderSpinlockTraits
{
    static void Acquire(TReaderWriterSpinLock* spinlock)
    {
        spinlock->AcquireReader();
    }

    static void Release(TReaderWriterSpinLock* spinlock)
    {
        spinlock->ReleaseReader();
    }
};

struct TWriterSpinlockTraits
{
    static inline void Acquire(TReaderWriterSpinLock* spinlock)
    {
        spinlock->AcquireWriter();
    }

    static inline void Release(TReaderWriterSpinLock* spinlock)
    {
        spinlock->ReleaseWriter();
    }
};

typedef TGuard<TReaderWriterSpinLock, TReaderSpinlockTraits> TReaderGuard;
typedef TGuard<TReaderWriterSpinLock, TWriterSpinlockTraits> TWriterGuard;

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
