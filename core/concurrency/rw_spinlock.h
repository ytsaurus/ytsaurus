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
    TReaderWriterSpinLock();

    void AcquireReader();
    void ReleaseReader();

    void AcquireWriter();
    void ReleaseWriter();

private:
    std::atomic<ui32> Value_;

    static const ui32 WriterMask = 1;
    static const ui32 ReaderDelta = 2;

    static const int YieldThreshold = 1000;


    bool TryAcquireReader();
    bool TryAcquireWriter();

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

#define RW_SPINLOCK_INL_H_
#include "rw_spinlock-inl.h"
#undef RW_SPINLOCK_INL_H_
