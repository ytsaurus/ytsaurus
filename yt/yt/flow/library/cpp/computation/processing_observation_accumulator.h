#pragma once

#include "computation_tracer.h"

#include <yt/yt/flow/library/cpp/common/computation_statistics.h>

#include <array>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TProcessingObservationAccumulator
{
public:
    explicit TProcessingObservationAccumulator(TInstant startTime);

    void StartEpoch(const THashMap<EEpochPartKind, IComputationTracer::TPartState>& partStates);
    void AddInputs(i64 count, i64 byteSize);
    TProcessingObservationPtr Commit(
        const THashMap<EEpochPartKind, IComputationTracer::TPartState>& partStates,
        TInstant now = TInstant::Now());

private:
    static constexpr std::array PartKinds_ = {
        EEpochPartKind::Processing,
        EEpochPartKind::Waiting,
        EEpochPartKind::WaitingForInput,
        EEpochPartKind::WaitingForOutput,
    };

    TInstant LastCommitTime_;
    std::array<TDuration, PartKinds_.size()> BaselineTimes_;
    std::array<TDuration, PartKinds_.size()> TotalTimes_;
    i64 Sequence_ = 0;
    bool EpochCommitted_ = false;
    i64 TotalCount_ = 0;
    i64 TotalBytes_ = 0;
    i64 PendingCount_ = 0;
    i64 PendingBytes_ = 0;
    TDuration TotalWallTime_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
