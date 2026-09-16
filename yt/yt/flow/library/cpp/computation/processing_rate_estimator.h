#pragma once

#include "computation_tracer.h"

#include <yt/yt/core/misc/ema_counter.h>
#include <yt/yt/flow/library/cpp/common/computation_statistics.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TProcessingRateEstimator
{
public:
    explicit TProcessingRateEstimator(TInstant startTime);

    void StartEpoch(const THashMap<EEpochPartKind, IComputationTracer::TPartState>& partStates);
    void AddInputs(i64 count, i64 byteSize);
    TComputationProcessingRatesPtr Commit(
        const THashMap<EEpochPartKind, IComputationTracer::TPartState>& partStates,
        TInstant now = TInstant::Now());

private:
    using TCounter = TEmaCounter<double, 2>;

    static TCounter MakeCounter();

    TInstant LastCommitTime_;
    TDuration BaselineProcessingTime_;
    TDuration BaselineWaitingTime_;
    bool EpochCommitted_ = false;
    TCounter Count_ = MakeCounter();
    TCounter Bytes_ = MakeCounter();
    i64 TotalCount_ = 0;
    i64 TotalBytes_ = 0;
    i64 PendingCount_ = 0;
    i64 PendingBytes_ = 0;
    double TotalProcessingTime_ = 0;
    double TotalWallTime_ = 0;
    TCounter ProcessingTime_ = MakeCounter();
    TCounter WallTime_ = MakeCounter();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
