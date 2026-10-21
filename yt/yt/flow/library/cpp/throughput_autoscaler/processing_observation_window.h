#pragma once

#include <yt/yt/flow/library/cpp/common/computation_statistics.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <deque>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProcessingObservationAddResult,
    (Accepted)
    (Repeated)
    (Rejected)
);

//! Paired work and time deltas for one current job.
class TProcessingObservationWindow
{
public:
    struct TStatistics
    {
        i64 ProcessedCount = 0;
        std::optional<i64> NonemptyEpochCount;
        std::optional<TDuration> NonemptyEpochDuration;
        TDuration ProcessingTime;
        TDuration InputWaitingTime;
        TDuration OutputWaitingTime;
        TDuration OtherWaitingTime;
    };

    struct TInterval
    {
        TInstant Begin;
        TInstant End;
        TStatistics Statistics;
    };

    explicit TProcessingObservationWindow(TJobId jobId);

    bool Matches(TJobId jobId) const;
    EProcessingObservationAddResult Add(
        const TJobStatus& status,
        TInstant now,
        TDuration retentionPeriod = TDuration::Minutes(30));
    std::optional<std::pair<TInstant, TInstant>> GetCoverage() const;
    TDuration GetLastObservationDuration() const;
    std::optional<std::pair<i64, TDuration>> GetNonemptyEpochStatistics() const;
    const std::deque<TInterval>& GetIntervals() const;

private:
    struct TPoint
    {
        i64 Sequence = 0;
        TInstant CapturedAt;
        TDuration ObservationDuration;
        TStatistics Statistics;
    };

    TJobId JobId_;
    std::optional<TPoint> Last_;
    TInstant FirstCapture_;

    std::deque<TInterval> Intervals_;
    i64 NonemptyEpochCount_ = 0;
    TDuration NonemptyEpochDuration_;
    int IncompleteEpochIntervalCount_ = 0;

    void UpdateNonemptyEpochStatistics(const TStatistics& statistics, int sign);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
