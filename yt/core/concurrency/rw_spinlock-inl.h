#ifndef RW_SPINLOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include rw_spinlock.h"
#endif
#undef RW_SPINLOCK_INL_H_

#include <util/system/yield.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

inline TReaderWriterSpinLock::TReaderWriterSpinLock()
    : Value_(0)
{ }

inline void TReaderWriterSpinLock::AcquireReader()
{
    for (int counter = 0; !TryAcquireReader(); ++counter) {
        if (counter > YieldThreshold) {
            SchedYield();
        }
    }
}

inline void TReaderWriterSpinLock::ReleaseReader()
{
    ui32 prevValue = Value_.fetch_sub(ReaderDelta, std::memory_order_release);
    YASSERT((prevValue & ~WriterMask) != 0);
}

inline void TReaderWriterSpinLock::AcquireWriter()
{
    for (int counter = 0; !TryAcquireWriter(); ++counter) {
        if (counter > YieldThreshold) {
            SchedYield();
        }
    }
}

inline void TReaderWriterSpinLock::ReleaseWriter()
{
    ui32 prevValue = Value_.fetch_and(~WriterMask, std::memory_order_release);
    YASSERT(prevValue & WriterMask);
}

inline bool TReaderWriterSpinLock::TryAcquireReader()
{
    ui32 oldValue = Value_.fetch_add(ReaderDelta, std::memory_order_acquire);
    if (oldValue & WriterMask) {
        Value_.fetch_sub(ReaderDelta, std::memory_order_relaxed);
        return false;
    }
    return true;
}

inline bool TReaderWriterSpinLock::TryAcquireWriter()
{
    ui32 expected = 0;
    if (!Value_.compare_exchange_weak(expected, WriterMask, std::memory_order_acquire)) {
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
