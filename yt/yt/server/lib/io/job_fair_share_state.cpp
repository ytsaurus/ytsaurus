#include "job_fair_share_state.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <limits>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

std::partial_ordering CompareJobIOConsumptions(
    i64 lhsConsumption,
    double lhsWeight,
    i64 rhsConsumption,
    double rhsWeight)
{
    bool lhsZero = lhsWeight == 0.0;
    bool rhsZero = rhsWeight == 0.0;

    if (lhsZero != rhsZero) {
        return rhsWeight <=> lhsWeight;
    }

    if (lhsZero) {
        return lhsConsumption <=> rhsConsumption;
    }

    return static_cast<long double>(lhsConsumption) * rhsWeight <=> static_cast<long double>(rhsConsumption) * lhsWeight;
}

////////////////////////////////////////////////////////////////////////////////

TIOJobState::TIOJobState(TDuration window)
    : Window_(DurationToCpuDuration(window))
{ }

void TIOJobState::UpdateReport(i64 ioConsumed, double weight, TCpuInstant snapshotAt)
{
    auto guard = Guard(Lock_);
    if (HasReport_ && snapshotAt < SnapshotAt_) {
        return;
    }
    HasReport_ = true;
    ReportedIOConsumed_ = ioConsumed;
    Weight_ = weight;
    SnapshotAt_ = snapshotAt;
}

TJobIOConsumption TIOJobState::ComputeConsumption(TCpuInstant now)
{
    auto guard = Guard(Lock_);
    TrimHistory(now);
    auto consumption = std::max(ComputeDecayedReport(now), CompletedInWindow_);
    auto remainingCapacity = std::numeric_limits<i64>::max() - consumption;
    return {
        .Consumption = consumption + std::min(Reserved_, remainingCapacity),
        .Weight = Weight_,
    };
}

void TIOJobState::SetWindow(TDuration window, TCpuInstant now)
{
    auto cpuWindow = DurationToCpuDuration(window);
    auto guard = Guard(Lock_);
    if (cpuWindow == Window_) {
        return;
    }

    TrimHistory(now);
    Window_ = cpuWindow;
    TrimHistory(now);
}

TCpuDuration TIOJobState::GetWindow()
{
    auto guard = Guard(Lock_);
    return Window_;
}

void TIOJobState::Reserve(i64 cost)
{
    if (cost <= 0) {
        return;
    }

    auto guard = Guard(Lock_);
    Reserved_ += cost;
}

void TIOJobState::Complete(i64 cost)
{
    if (cost <= 0) {
        return;
    }

    auto guard = Guard(Lock_);
    auto now = GetCpuInstant();
    Reserved_ -= cost;
    YT_VERIFY(Reserved_ >= 0);
    CompletedHistory_.emplace(now, cost);
    CompletedInWindow_ += cost;
    TrimHistory(now);
}

void TIOJobState::AcquireRequest()
{
    auto guard = Guard(Lock_);
    ++RequestCount_;
}

void TIOJobState::ReleaseRequest()
{
    auto guard = Guard(Lock_);
    --RequestCount_;
    YT_VERIFY(RequestCount_ >= 0);
}

bool TIOJobState::IsExpired(TCpuInstant now)
{
    auto guard = Guard(Lock_);
    TrimHistory(now);
    return RequestCount_ == 0 &&
        Reserved_ == 0 &&
        CompletedInWindow_ == 0 &&
        (!HasReport_ || now - SnapshotAt_ >= Window_);
}

i64 TIOJobState::ComputeDecayedReport(TCpuInstant now) const
{
    YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

    if (!HasReport_) {
        return 0;
    }

    auto age = now - SnapshotAt_;
    if (age <= 0) {
        return ReportedIOConsumed_;
    }
    if (age >= Window_) {
        return 0;
    }

    auto remainingFraction = 1.0 - static_cast<double>(age) / static_cast<double>(Window_);
    return static_cast<i64>(static_cast<double>(ReportedIOConsumed_) * remainingFraction);
}

void TIOJobState::TrimHistory(TCpuInstant now)
{
    YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

    while (!CompletedHistory_.empty() &&
        now - CompletedHistory_.front().first >= Window_)
    {
        CompletedInWindow_ -= CompletedHistory_.front().second;
        CompletedHistory_.pop();
    }
}

////////////////////////////////////////////////////////////////////////////////

std::partial_ordering CompareHeadRequestJobs(
    TIOJobState* lhsJobState,
    TIOJobState* rhsJobState,
    const THashMap<TIOJobState*, TJobIOConsumption>& consumptions)
{
    if (!lhsJobState || !rhsJobState) {
        return static_cast<bool>(lhsJobState) <=> static_cast<bool>(rhsJobState);
    }

    if (lhsJobState == rhsJobState) {
        return std::partial_ordering::equivalent;
    }

    const auto& lhsConsumption = GetOrCrash(consumptions, lhsJobState);
    const auto& rhsConsumption = GetOrCrash(consumptions, rhsJobState);
    return CompareJobIOConsumptions(
        lhsConsumption.Consumption,
        lhsConsumption.Weight,
        rhsConsumption.Consumption,
        rhsConsumption.Weight);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
