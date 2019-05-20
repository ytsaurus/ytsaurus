#pragma once

#include "public.h"

#include <util/system/rwlock.h>

#include <atomic>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Single-writer multiple-readers spin lock.
/*!
 *  Reader-side calls are pretty cheap.
 *  The lock is unfair.
 */
class TReaderWriterSpinLock
{
public:
    //! Acquires the reader lock.
    /*!
     *  Optimized for the case of read-intensive workloads.
     *  Cheap (just one atomic increment and no spinning if no writers are present).
     *  Don't use this call if forks are possible: forking at some
     *  intermediate point inside #AcquireReader may leave the lock
     *  forever stuck for the child process.
     */
    void AcquireReader() noexcept;
    //! Acquires the reader lock.
    /*!
     *  A more expensive version of #AcquireReader (includes at least
     *  one atomic load and CAS; also may spin even if just readers are present).
     *  In contrast to #AcquireReader, this call is safe to use in presence of forks.
     */
    void AcquireReaderForkFriendly() noexcept;
    //! Releases the reader lock.
    /*!
     *  Cheap (just one atomic decrement).
     */
    void ReleaseReader() noexcept;

    //! Acquires the writer lock.
    /*!
     *  Rather cheap (just one CAS).
     */
    void AcquireWriter() noexcept;
    //! Releases the writer lock.
    /*!
     *  Cheap (just one atomic store).
     */
    void ReleaseWriter() noexcept;

    //! Returns true if the lock is taken (either by a reader of writer).
    /*!
     *  This is inherently racy.
     *  Only use for debugging and diagnostic purposes.
     */
    bool IsLocked() noexcept;

private:
    std::atomic<ui32> Value_ = {0};

    static const ui32 WriterMask = 1;
    static const ui32 ReaderDelta = 2;

    static const int YieldThreshold = 1000;


    bool TryAcquireReader();
    bool TryAcquireReaderForkFriendly();
    bool TryAcquireWriter();
};

////////////////////////////////////////////////////////////////////////////////

//! A variant of TReaderWriterSpinLock occupyig the whole cache line.
class TPaddedReaderWriterSpinLock
    : public TReaderWriterSpinLock
{
private:
    [[maybe_unused]]
    char Padding_[64 - sizeof(TReaderWriterSpinLock)];
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

} // namespace NYT::NConcurrency

#define RW_SPINLOCK_INL_H_
#include "rw_spinlock-inl.h"
#undef RW_SPINLOCK_INL_H_
