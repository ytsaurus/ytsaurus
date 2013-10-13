#pragma once

#include "public.h"

#include <util/system/rwlock.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Single-writer multiple-readers spin lock.
class TReaderWriterSpinlock
{
public:
    TReaderWriterSpinlock()
        : Value_(0)
    { }

    void AcquireReader()
    {
        while (true) {
            auto value = AtomicGet(Value_);
            if (value != 1) {
                if (AtomicCas(&Value_, value + 2, value))
                    return;
            }
            SpinLockPause();
        }
    }

    void ReleaseReader()
    {
        AtomicAdd(Value_, -2);
    }

    void AcquireWriter()
    {
        if (AtomicCas(&Value_, 1, 0))
            return;

        while (true) {
            auto value = AtomicGet(Value_);
            if (value == 0) {
                if (AtomicCas(&Value_, 1, 0))
                    return;
            }
            SpinLockPause();
        }
    }

    void ReleaseWriter()
    {
        ATOMIC_COMPILER_BARRIER();
        AtomicSet(Value_, 0);
    }

private:
    //! 0 if not taken.
    //! 1 if taken by a writer.
    //! Number of readers * 2 if taken by readers.
    TAtomic Value_;

};

////////////////////////////////////////////////////////////////////////////////

struct TReaderSpinlockTraits
{
    static void Acquire(TReaderWriterSpinlock* spinlock)
    {
        spinlock->AcquireReader();
    }

    static void Release(TReaderWriterSpinlock* spinlock)
    {
        spinlock->ReleaseReader();
    }
};

struct TWriterSpinlockTraits
{
    static inline void Acquire(TReaderWriterSpinlock* spinlock)
    {
        spinlock->AcquireWriter();
    }

    static inline void Release(TReaderWriterSpinlock* spinlock)
    {
        spinlock->ReleaseWriter();
    }
};

typedef TGuard<TReaderWriterSpinlock, TReaderSpinlockTraits> TReaderGuard;
typedef TGuard<TReaderWriterSpinlock, TWriterSpinlockTraits> TWriterGuard;

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
