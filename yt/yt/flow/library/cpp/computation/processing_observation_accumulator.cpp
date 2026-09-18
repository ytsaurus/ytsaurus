#include "processing_observation_accumulator.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TProcessingObservationAccumulator::TProcessingObservationAccumulator(TInstant startTime)
    : LastCommitTime_(startTime)
{
}

void TProcessingObservationAccumulator::StartEpoch(const THashMap<EEpochPartKind, IComputationTracer::TPartState>& partStates)
{
    // Preserve the previous committed epoch's tail, but discard a failed attempt.
    if (!EpochCommitted_) {
        for (int index = 0; index < std::ssize(PartKinds_); ++index) {
            BaselineTimes_[index] = GetOrDefault(partStates, PartKinds_[index]).TotalDuration;
        }
    }
    EpochCommitted_ = false;
    PendingCount_ = 0;
    PendingBytes_ = 0;
}

void TProcessingObservationAccumulator::AddInputs(i64 count, i64 byteSize)
{
    PendingCount_ += count;
    PendingBytes_ += byteSize;
}

TProcessingObservationPtr TProcessingObservationAccumulator::Commit(
    const THashMap<EEpochPartKind, IComputationTracer::TPartState>& partStates,
    TInstant now)
{
    YT_VERIFY(now >= LastCommitTime_);
    auto previousWallTime = TotalWallTime_;
    for (int index = 0; index < std::ssize(PartKinds_); ++index) {
        auto total = GetOrDefault(partStates, PartKinds_[index]).TotalDuration;
        YT_VERIFY(total >= BaselineTimes_[index]);
        auto delta = total - BaselineTimes_[index];
        BaselineTimes_[index] = total;
        TotalTimes_[index] += delta;
        TotalWallTime_ += delta;
    }
    EpochCommitted_ = true;
    TotalCount_ += PendingCount_;
    TotalBytes_ += PendingBytes_;
    PendingCount_ = 0;
    PendingBytes_ = 0;
    auto observation = New<TProcessingObservation>();
    observation->Sequence = ++Sequence_;
    observation->CapturedAt = now;
    observation->ObservationDuration = TotalWallTime_ - previousWallTime;
    observation->ProcessedCount = TotalCount_;
    observation->ProcessedByteSize = TotalBytes_;
    observation->ProcessingTime = TotalTimes_[0];
    observation->OtherWaitingTime = TotalTimes_[1];
    observation->InputWaitingTime = TotalTimes_[2];
    observation->OutputWaitingTime = TotalTimes_[3];
    LastCommitTime_ = now;

    return observation;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
