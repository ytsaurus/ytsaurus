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
 *  Writers take precedence over readers and can make the latter starve.
 */
class TReaderWriterSpinLock
{
public:
    TReaderWriterSpinLock()
        : Value_(0)
    { }

    void AcquireReader()
    {
        while (true) {
            if (TryAcquireReader())
                break;
            SpinLockPause();
        }
    }

    void ReleaseReader()
    {
        Value_.fetch_sub(ReaderDelta, std::memory_order_release);
    }

    void AcquireWriter()
    {
        while (true) {
            if (TryAcquireWriter())
                break;
            SpinLockPause();
        }
    }

    void ReleaseWriter()
    {
        Value_.store(0, std::memory_order_release);
    }

private:
    std::atomic<intptr_t> Value_;

    static const intptr_t WriterMask = 1;
    static const intptr_t PendingWriterMask = 2;
    static const intptr_t ReaderDelta = 4;

    bool TryAcquireReader()
    {
        // Fail to take the lock in case of pending or active writers.
        auto value = Value_.load(std::memory_order_relaxed);
        if (value & (WriterMask | PendingWriterMask)) {
            return false;
        }

        auto oldValue = Value_.fetch_add(ReaderDelta, std::memory_order_acquire);
        if (oldValue & (WriterMask | PendingWriterMask)) {
            // Backoff.
            Value_.fetch_sub(ReaderDelta, std::memory_order_relaxed);
            return false;
        }

        return true;
    }

    bool TryAcquireWriter()
    {
        // Prevent more readers from acquiring the lock.
        auto value = Value_.fetch_or(PendingWriterMask, std::memory_order_acquire);
        if (value & ~PendingWriterMask) {
            // Fail if there are active readers or writers.
            return false;
        }

        auto expected = PendingWriterMask;
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
