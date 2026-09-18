#include "processing_rate_estimator.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TProcessingRateEstimator::TCounter TProcessingRateEstimator::MakeCounter()
{
    return TCounter({TDuration::Minutes(1), TDuration::Minutes(10)});
}

TProcessingRateEstimator::TProcessingRateEstimator(TInstant startTime)
    : LastCommitTime_(startTime)
{
    Count_.Update(0, startTime);
    Bytes_.Update(0, startTime);
    ProcessingTime_.Update(0, startTime);
    WallTime_.Update(0, startTime);
}

void TProcessingRateEstimator::StartEpoch(const THashMap<EEpochPartKind, IComputationTracer::TPartState>& partStates)
{
    // Preserve the previous committed epoch's tail, but discard a failed attempt.
    if (!EpochCommitted_) {
        BaselineProcessingTime_ = GetOrDefault(partStates, EEpochPartKind::Processing).TotalDuration;
        BaselineWaitingTime_ = GetOrDefault(partStates, EEpochPartKind::Waiting).TotalDuration;
    }
    EpochCommitted_ = false;
    PendingCount_ = 0;
    PendingBytes_ = 0;
}

void TProcessingRateEstimator::AddInputs(i64 count, i64 byteSize)
{
    PendingCount_ += count;
    PendingBytes_ += byteSize;
}

TComputationProcessingRatesPtr TProcessingRateEstimator::Commit(
    const THashMap<EEpochPartKind, IComputationTracer::TPartState>& partStates,
    TInstant now)
{
    YT_VERIFY(now >= LastCommitTime_);
    const auto totalProcessingTime = GetOrDefault(partStates, EEpochPartKind::Processing).TotalDuration;
    const auto totalWaitingTime = GetOrDefault(partStates, EEpochPartKind::Waiting).TotalDuration;
    const auto processingTime = totalProcessingTime - BaselineProcessingTime_;
    const auto waitingTime = totalWaitingTime - BaselineWaitingTime_;
    BaselineProcessingTime_ = totalProcessingTime;
    BaselineWaitingTime_ = totalWaitingTime;
    EpochCommitted_ = true;
    TotalCount_ += PendingCount_;
    TotalBytes_ += PendingBytes_;
    Count_.Update(TotalCount_, now);
    Bytes_.Update(TotalBytes_, now);
    TotalProcessingTime_ += processingTime.SecondsFloat();
    TotalWallTime_ += (processingTime + waitingTime).SecondsFloat();
    PendingCount_ = 0;
    PendingBytes_ = 0;
    ProcessingTime_.Update(TotalProcessingTime_, now);
    WallTime_.Update(TotalWallTime_, now);
    LastCommitTime_ = now;

    auto result = New<TComputationProcessingRates>();
    const std::array windows = {&result->Rate1m, &result->Rate10m};
    for (int index = 0; index < std::ssize(windows); ++index) {
        const auto wallTime = WallTime_.GetRate(index, now);
        if (!wallTime || *wallTime <= 0) {
            continue;
        }
        const auto processingTimeRate = ProcessingTime_.GetRate(index, now).value();
        const auto count = Count_.GetRate(index, now).value();
        const auto bytes = Bytes_.GetRate(index, now).value();
        auto& rate = windows[index]->emplace();
        rate.Processed.ProcessedMessagesPerSecond = count / *wallTime;
        rate.Processed.ProcessedBytesPerSecond = bytes / *wallTime;
        if (count > 0 && processingTimeRate > 0) {
            rate.Capacity.emplace();
            rate.Capacity->ProcessedMessagesPerSecond = count / processingTimeRate;
            rate.Capacity->ProcessedBytesPerSecond = bytes / processingTimeRate;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
