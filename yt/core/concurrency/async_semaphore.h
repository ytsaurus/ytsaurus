#pragma once

#include "public.h"

#include <core/actions/future.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Custom semaphore class with async acquire operation.
class TAsyncSemaphore
    : private TNonCopyable
{
public:
    explicit TAsyncSemaphore(i64 totalSlots);

    //! Releases a given number of slots.
    void Release(i64 slots = 1);

    //! Acquires a given number of slots.
    //! Cannot fail, may lead to an overcommit.
    void Acquire(i64 slots = 1);

    //! Tries to acquire a given number of slots.
    //! Returns |true| on success (the number of remaining slots is non-negative).
    bool TryAcquire(i64 slots = 1);

    //! Returns |true| iff at least one slot is free.
    bool IsReady() const;

    //! Returns |true| iff all slots are free.
    bool IsFree() const;

    //! Returns the total number of slots.
    i64 GetTotal() const;

    //! Returns the number of used slots.
    i64 GetUsed() const;

    //! Returns the number of free slots.
    i64 GetFree() const;

    TFuture<void> GetReadyEvent();
    TFuture<void> GetFreeEvent();

private:
    TSpinLock SpinLock_;

    const i64 TotalSlots_;
    volatile i64 FreeSlots_;

    TPromise<void> ReadyEvent_;
    TPromise<void> FreeEvent_;

};

////////////////////////////////////////////////////////////////////////////////

class TAsyncSemaphoreGuard
    : private TNonCopyable
{
public:
    TAsyncSemaphoreGuard(TAsyncSemaphoreGuard&& other);
    ~TAsyncSemaphoreGuard();

    TAsyncSemaphoreGuard& operator=(TAsyncSemaphoreGuard&& other);

    static TAsyncSemaphoreGuard Acquire(TAsyncSemaphore* semaphore, i64 slots = 1);
    static TAsyncSemaphoreGuard TryAcquire(TAsyncSemaphore* semaphore, i64 slots = 1);

    friend void swap(TAsyncSemaphoreGuard& lhs, TAsyncSemaphoreGuard& rhs);

    void Release();

    explicit operator bool() const;

private:
    TAsyncSemaphore* Semaphore_;
    i64 Slots_;

    TAsyncSemaphoreGuard();

    void MoveFrom(TAsyncSemaphoreGuard&& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
