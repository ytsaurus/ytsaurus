#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TAsyncSemaphoreGuard;

//! Custom semaphore class with async acquire operation.
class TAsyncSemaphore
    : public TIntrinsicRefCounted
{
public:
    explicit TAsyncSemaphore(i64 totalSlots);

    //! Releases a given number of slots.
    virtual void Release(i64 slots = 1);

    //! Acquires a given number of slots.
    //! Cannot fail, may lead to an overcommit.
    virtual void Acquire(i64 slots = 1);

    //! Tries to acquire a given number of slots.
    //! Returns |true| on success (the number of remaining slots is non-negative).
    virtual bool TryAcquire(i64 slots = 1);

    //! Runs #handler when a given number of slots becomes available.
    //! These slots are immediately captured by TAsyncSemaphoreGuard instance passed to #handler.
    // XXX(babenko): passing invoker is a temporary workaround until YT-3801 is fixed
    void AsyncAcquire(
        const TCallback<void(TAsyncSemaphoreGuard)>& handler,
        IInvokerPtr invoker,
        i64 slots = 1);

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

private:
    const i64 TotalSlots_;

    TSpinLock SpinLock_;

    volatile i64 FreeSlots_;

    bool Releasing_ = false;

    TPromise<void> ReadyEvent_;

    struct TWaiter
    {
        TCallback<void(TAsyncSemaphoreGuard)> Handler;
        IInvokerPtr Invoker;
        i64 Slots;
    };

    std::queue<TWaiter> Waiters_;

};

DEFINE_REFCOUNTED_TYPE(TAsyncSemaphore)

////////////////////////////////////////////////////////////////////////////////

class TProfiledAsyncSemaphore
    : public TAsyncSemaphore
{
public:
    TProfiledAsyncSemaphore(
        i64 totalSlots,
        const NProfiling::TProfiler& profiler,
        const NYPath::TYPath& path,
        const NProfiling::TTagIdList& tagIds = NProfiling::EmptyTagIds);

    virtual void Release(i64 slots = 1) override;
    virtual void Acquire(i64 slots = 1) override;
    virtual bool TryAcquire(i64 slots = 1) override;

private:
    const NProfiling::TProfiler Profiler;
    const NYPath::TYPath Path_;
    const NProfiling::TTagIdList TagIds_;
};

DEFINE_REFCOUNTED_TYPE(TProfiledAsyncSemaphore)

////////////////////////////////////////////////////////////////////////////////

class TAsyncSemaphoreGuard
    : private TNonCopyable
{
public:
    DEFINE_BYVAL_RO_PROPERTY(i64, Slots);

public:
    TAsyncSemaphoreGuard();
    TAsyncSemaphoreGuard(TAsyncSemaphoreGuard&& other);
    ~TAsyncSemaphoreGuard();

    TAsyncSemaphoreGuard& operator=(TAsyncSemaphoreGuard&& other);

    static TAsyncSemaphoreGuard Acquire(TAsyncSemaphorePtr semaphore, i64 slots = 1);
    static TAsyncSemaphoreGuard TryAcquire(TAsyncSemaphorePtr semaphore, i64 slots = 1);

    friend void swap(TAsyncSemaphoreGuard& lhs, TAsyncSemaphoreGuard& rhs);

    TAsyncSemaphoreGuard TransferSlots(i64 slotsToTransfer);

    void Release();

    explicit operator bool() const;

private:
    friend class TAsyncSemaphore;

    TAsyncSemaphorePtr Semaphore_;

    TAsyncSemaphoreGuard(TAsyncSemaphorePtr semaphore, i64 slots);

    void MoveFrom(TAsyncSemaphoreGuard&& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
