#ifndef SPINLOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include spinlock.h"
// For the sake of sane code completion.
#include "spinlock.h"
#endif
#undef SPINLOCK_INL_H_

#include <atomic>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

Y_FORCE_INLINE i64 GetCpuInstant()
{
    // See datetime.h
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc"
    : "=a"(lo), "=d"(hi));
    return static_cast<unsigned long long>(lo) | (static_cast<unsigned long long>(hi) << 32);
}

extern std::atomic<TSpinlockHiccupHandler> SpinlockHiccupHandler;
extern std::atomic<i64> SpinlockHiccupThreshold;

} // namespace NDetail

#ifdef YT_ENABLE_SPINLOCK_PROFILING

#define DEFINE_CTOR(className) \
    template <class TUnderlying> \
    className<TUnderlying>::className(const ::TSourceLocation& location) \
        : Location_(location) \
    { }

#define DELEGATE_AS_IS(className, returnType, methodName, signature) \
    template <class TUnderlying> \
    returnType className<TUnderlying>::methodName signature \
    { \
        return Underlying_.methodName(); \
    }

#define DELEGATE_WITH_PROFILING(className, methodName, activityKind) \
    template <class TUnderlying> \
    void className<TUnderlying>::methodName() noexcept \
    { \
        if (Underlying_.Try ## methodName()) { \
            return; \
        } \
    \
        auto startInstant = NDetail::GetCpuInstant(); \
        Underlying_.methodName(); \
        auto finishInstant = NDetail::GetCpuInstant(); \
    \
        auto elapsedTicks = finishInstant - startInstant; \
        auto thresholdTicks = NDetail::SpinlockHiccupThreshold.load(std::memory_order_relaxed); \
        if (Y_UNLIKELY(elapsedTicks > thresholdTicks)) { \
            if (auto handler = NDetail::SpinlockHiccupHandler.load(std::memory_order_relaxed)) { \
                handler(Location_, activityKind, elapsedTicks); \
            } \
        } \
    }

DEFINE_CTOR(TProfilingSpinlockWrapperImpl)
DELEGATE_AS_IS(TProfilingSpinlockWrapperImpl, bool, IsLocked, () const noexcept)
DELEGATE_AS_IS(TProfilingSpinlockWrapperImpl, bool, TryAcquire, () noexcept)
DELEGATE_AS_IS(TProfilingSpinlockWrapperImpl, void, Release, () noexcept)
DELEGATE_WITH_PROFILING(TProfilingSpinlockWrapperImpl, Acquire, ESpinlockActivityKind::ReadWrite)

DEFINE_CTOR(TProfilingReaderWriterSpinlockWrapperImpl)
DELEGATE_AS_IS(TProfilingReaderWriterSpinlockWrapperImpl, bool, IsLocked, () const noexcept)
DELEGATE_AS_IS(TProfilingReaderWriterSpinlockWrapperImpl, bool, IsLockedByReader, () const noexcept)
DELEGATE_AS_IS(TProfilingReaderWriterSpinlockWrapperImpl, bool, IsLockedByWriter, () const noexcept)
DELEGATE_AS_IS(TProfilingReaderWriterSpinlockWrapperImpl, bool, TryAcquireReader, () noexcept)
DELEGATE_AS_IS(TProfilingReaderWriterSpinlockWrapperImpl, bool, TryAcquireReaderForkFriendly, () noexcept)
DELEGATE_AS_IS(TProfilingReaderWriterSpinlockWrapperImpl, bool, TryAcquireWriter, () noexcept)
DELEGATE_AS_IS(TProfilingReaderWriterSpinlockWrapperImpl, void, ReleaseReader, () noexcept)
DELEGATE_AS_IS(TProfilingReaderWriterSpinlockWrapperImpl, void, ReleaseWriter, () noexcept)
DELEGATE_WITH_PROFILING(TProfilingReaderWriterSpinlockWrapperImpl, AcquireReader, ESpinlockActivityKind::Read)
DELEGATE_WITH_PROFILING(TProfilingReaderWriterSpinlockWrapperImpl, AcquireReaderForkFriendly, ESpinlockActivityKind::Read)
DELEGATE_WITH_PROFILING(TProfilingReaderWriterSpinlockWrapperImpl, AcquireWriter, ESpinlockActivityKind::Write)

#undef DEFINE_CTOR
#undef DELEGATE_AS_IS
#undef DELEGATE_WITH_PROFILING

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
