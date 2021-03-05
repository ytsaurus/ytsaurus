#pragma once

#include <yt/yt/core/misc/enum.h>
#include <yt/yt/core/misc/port.h>

#include <library/cpp/ytalloc/core/concurrency/rw_spinlock.h>

#include <util/system/compiler.h>
#include <util/system/spinlock.h>
#include <util/system/src_location.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

/*
 *  Use YT_DECLARE_SPINLOCK to declare a spinlock.
 *  Typical examples of supported types are TSpinLock, TAdaptiveLock, and TReaderWriterSpinLock.
 *
 *  If YT_ENABLE_SPINLOCK_PROFILING is set then the original spinlock type
 *  is wrapped into TProfilingSpinlockWrapper. This wrapper captures the source location
 *  of the declaration and detects slow acquisitions (see #SetSpinlockHiccupHandler).
 *
 *  In case of no contention, the overhead is negligible.
 *  If some contention is present but the hiccup threshold is not reached,
 *  the penalty is 2 * RDTSC + some simple arithmetics and (predictable) branching.
 *  In case of high contention the hiccup handler is invoked (which is slow but who cares, anyway?).
 *
 *  If YT_ENABLE_SPINLOCK_PROFILING is not set then YT_DECLARE_SPINLOCK just declares
 *  the variable of the given type.
 */

#ifdef YT_ENABLE_SPINLOCK_PROFILING

template <class TUnderlying>
class TProfilingSpinlockWrapperImpl
{
public:
    explicit TProfilingSpinlockWrapperImpl(const ::TSourceLocation& location);

    bool IsLocked() const noexcept;

    bool TryAcquire() noexcept;
    void Acquire() noexcept;
    void Release() noexcept;

private:
    const ::TSourceLocation Location_;

    TUnderlying Underlying_;
};

template <class TUnderlying>
class TProfilingReaderWriterSpinlockWrapperImpl
{
public:
    explicit TProfilingReaderWriterSpinlockWrapperImpl(const ::TSourceLocation& location);

    bool IsLocked() const noexcept;
    bool IsLockedByReader() const noexcept;
    bool IsLockedByWriter() const noexcept;

    void AcquireReader() noexcept;
    void AcquireReaderForkFriendly() noexcept;
    bool TryAcquireReader() noexcept;
    bool TryAcquireReaderForkFriendly() noexcept;
    void ReleaseReader() noexcept;
    void AcquireWriter() noexcept;
    bool TryAcquireWriter() noexcept;
    void ReleaseWriter() noexcept;

private:
    const ::TSourceLocation Location_;

    TUnderlying Underlying_;
};

template <class TUnderlying, class = void>
struct TProfilingSpinlockTraits;

template <class TUnderlying>
struct TProfilingSpinlockTraits<
    TUnderlying,
    decltype(static_cast<TUnderlying*>(nullptr)->Acquire())
>
{
    using TType = TProfilingSpinlockWrapperImpl<TUnderlying>;
};

template <class TUnderlying>
struct TProfilingSpinlockTraits<
    TUnderlying,
    decltype(static_cast<TUnderlying*>(nullptr)->AcquireReader())
>
{
    using TType = TProfilingReaderWriterSpinlockWrapperImpl<TReaderWriterSpinLock>;
};

template <class TUnderlying>
using TProfilingSpinlockWrapper = typename TProfilingSpinlockTraits<TUnderlying>::TType;

#define YT_DECLARE_SPINLOCK(type, name) \
    ::NYT::NConcurrency::TProfilingSpinlockWrapper<type> name{__LOCATION__}

#else

template <class TUnderlying>
using TProfilingSpinlockWrapper = TUnderlying;

#define YT_DECLARE_SPINLOCK(type, name) \
    type name

#endif

template <class TUnderlying>
using TSpinlockGuard = TGuard<TProfilingSpinlockWrapper<TUnderlying>>;
template <class TUnderlying>
using TSpinlockTryGuard = TTryGuard<TProfilingSpinlockWrapper<TUnderlying>>;
template <class TUnderlying>
using TSpinlockReaderGuard = TReaderGuard<TProfilingSpinlockWrapper<TUnderlying>>;
template <class TUnderlying>
using TSpinlockWriterGuard = TWriterGuard<TProfilingSpinlockWrapper<TUnderlying>>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESpinlockActivityKind,
    (Read)
    (Write)
    (ReadWrite)
);

using TSpinlockHiccupHandler = void (*) (
    const ::TSourceLocation& location,
    ESpinlockActivityKind activityKind,
    i64 ticksElapsed);

//! Installs a handler to be invoked each time a spinlock acquisition
//! is taking too long.
void SetSpinlockHiccupHandler(TSpinlockHiccupHandler handler);

//! Controls the acquisition time limit (measured in CPU ticks).
//! Acquisitions exceeding the limit are considered slow and reported.
void SetSpinlockHiccupThresholdTicks(i64 thresholdTicks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define SPINLOCK_INL_H_
#include "spinlock-inl.h"
#undef SPINLOCK_INL_H_
