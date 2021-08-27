#include "memory_usage_tracker.h"
#include "singleton.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNullMemoryUsageTracker
    : public IMemoryUsageTracker
{
public:
    TError TryAcquire(i64 /*size*/) override
    {
        return {};
    }

    TError TryChange(i64 /*size*/) override
    {
        return {};
    }

    void Acquire(i64 /*size*/) override
    { }

    void Release(i64 /*size*/) override
    { }
};

IMemoryUsageTrackerPtr GetNullMemoryUsageTracker()
{
    return RefCountedSingleton<TNullMemoryUsageTracker>();
}

////////////////////////////////////////////////////////////////////////////////

TMemoryUsageTrackerGuard::TMemoryUsageTrackerGuard(TMemoryUsageTrackerGuard&& other)
{
    MoveFrom(std::move(other));
}

TMemoryUsageTrackerGuard::~TMemoryUsageTrackerGuard()
{
    Release();
}

TMemoryUsageTrackerGuard& TMemoryUsageTrackerGuard::operator=(TMemoryUsageTrackerGuard&& other)
{
    if (this != &other) {
        Release();
        MoveFrom(std::move(other));
    }
    return *this;
}

void TMemoryUsageTrackerGuard::MoveFrom(TMemoryUsageTrackerGuard&& other)
{
    Tracker_ = other.Tracker_;
    Size_ = other.Size_;
    AcquiredSize_ = other.AcquiredSize_;
    Granularity_ = other.Granularity_;

    other.Tracker_ = nullptr;
    other.Size_ = 0;
    other.AcquiredSize_ = 0;
    other.Granularity_ = 0;
}

void swap(TMemoryUsageTrackerGuard& lhs, TMemoryUsageTrackerGuard& rhs)
{
    std::swap(lhs.Tracker_, rhs.Tracker_);
}

TMemoryUsageTrackerGuard TMemoryUsageTrackerGuard::Acquire(
    IMemoryUsageTrackerPtr tracker,
    i64 size,
    i64 granularity)
{
    YT_VERIFY(size >= 0);
    TMemoryUsageTrackerGuard guard;
    guard.Tracker_ = tracker;
    guard.Size_ = size;
    guard.Granularity_ = granularity;
    if (size >= granularity) {
        guard.AcquiredSize_ = size;
        tracker->Acquire(size);
    }
    return guard;
}

TErrorOr<TMemoryUsageTrackerGuard> TMemoryUsageTrackerGuard::TryAcquire(
    IMemoryUsageTrackerPtr tracker,
    i64 size,
    i64 granularity)
{
    YT_VERIFY(size >= 0);
    auto error = tracker->TryAcquire(size);
    if (!error.IsOK()) {
        return error;
    }
    TMemoryUsageTrackerGuard guard;
    guard.Tracker_ = tracker;
    guard.Size_ = size;
    guard.AcquiredSize_ = size;
    guard.Granularity_ = granularity;
    return std::move(guard);
}

void TMemoryUsageTrackerGuard::Release()
{
    if (Tracker_) {
        Tracker_->Release(AcquiredSize_);
        Tracker_ = nullptr;
        Size_ = 0;
        AcquiredSize_ = 0;
        Granularity_ = 0;
    }
}

TMemoryUsageTrackerGuard::operator bool() const
{
    return Tracker_.operator bool();
}

i64 TMemoryUsageTrackerGuard::GetSize() const
{
    return Size_;
}

void TMemoryUsageTrackerGuard::SetSize(i64 size)
{
    YT_VERIFY(Tracker_);
    YT_VERIFY(size >= 0);
    Size_ = size;
    if (std::abs(Size_ - AcquiredSize_) >= Granularity_) {
        if (Size_ > AcquiredSize_) {
            Tracker_->Acquire(Size_ - AcquiredSize_);
        } else {
            Tracker_->Release(AcquiredSize_ - Size_);
        }
        AcquiredSize_ = Size_;
    }
}

void TMemoryUsageTrackerGuard::IncrementSize(i64 sizeDelta)
{
    SetSize(Size_ + sizeDelta);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

