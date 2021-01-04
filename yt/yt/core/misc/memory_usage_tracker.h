#pragma once

#include "error.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IMemoryUsageTracker
    : public TRefCounted
{
    virtual TError TryAcquire(i64 size) = 0;
    virtual TError TryChange(i64 size) = 0;
    virtual void Acquire(i64 size) = 0;
    virtual void Release(i64 size) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMemoryUsageTracker)

IMemoryUsageTrackerPtr GetNullMemoryUsageTracker();

////////////////////////////////////////////////////////////////////////////////

class TMemoryUsageTrackerGuard
    : private TNonCopyable
{
public:
    TMemoryUsageTrackerGuard() = default;
    TMemoryUsageTrackerGuard(const TMemoryUsageTrackerGuard& other) = delete;
    TMemoryUsageTrackerGuard(TMemoryUsageTrackerGuard&& other);
    ~TMemoryUsageTrackerGuard();

    TMemoryUsageTrackerGuard& operator=(const TMemoryUsageTrackerGuard& other) = delete;
    TMemoryUsageTrackerGuard& operator=(TMemoryUsageTrackerGuard&& other);

    static TMemoryUsageTrackerGuard Acquire(
        IMemoryUsageTrackerPtr tracker,
        i64 size,
        i64 granularity = 1);
    static TErrorOr<TMemoryUsageTrackerGuard> TryAcquire(
        IMemoryUsageTrackerPtr tracker,
        i64 size,
        i64 granularity = 1);

    friend void swap(TMemoryUsageTrackerGuard& lhs, TMemoryUsageTrackerGuard& rhs);

    void Release();

    explicit operator bool() const;

    i64 GetSize() const;
    void SetSize(i64 size);
    void IncrementSize(i64 sizeDelta);

private:
    IMemoryUsageTrackerPtr Tracker_;
    i64 Size_ = 0;
    i64 AcquiredSize_ = 0;
    i64 Granularity_ = 0;

    void MoveFrom(TMemoryUsageTrackerGuard&& other);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
