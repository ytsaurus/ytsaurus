#include "processing_observation_window.h"

#include <algorithm>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TStatistics = TProcessingObservationWindow::TStatistics;

bool Dominates(const TStatistics& next, const TStatistics& previous)
{
    return next.ProcessedCount >= previous.ProcessedCount &&
        next.ProcessingTime >= previous.ProcessingTime &&
        next.InputWaitingTime >= previous.InputWaitingTime &&
        next.OutputWaitingTime >= previous.OutputWaitingTime &&
        next.OtherWaitingTime >= previous.OtherWaitingTime &&
        (!next.NonemptyEpochCount || !previous.NonemptyEpochCount ||
        *next.NonemptyEpochCount >= *previous.NonemptyEpochCount) &&
        (!next.NonemptyEpochDuration || !previous.NonemptyEpochDuration ||
        *next.NonemptyEpochDuration >= *previous.NonemptyEpochDuration);
}

TStatistics Subtract(const TStatistics& next, const TStatistics& previous)
{
    return {
        .ProcessedCount = next.ProcessedCount - previous.ProcessedCount,
        .NonemptyEpochCount = next.NonemptyEpochCount && previous.NonemptyEpochCount
            ? std::optional(*next.NonemptyEpochCount - *previous.NonemptyEpochCount)
            : std::nullopt,
        .NonemptyEpochDuration = next.NonemptyEpochDuration && previous.NonemptyEpochDuration
            ? std::optional(*next.NonemptyEpochDuration - *previous.NonemptyEpochDuration)
            : std::nullopt,
        .ProcessingTime = next.ProcessingTime - previous.ProcessingTime,
        .InputWaitingTime = next.InputWaitingTime - previous.InputWaitingTime,
        .OutputWaitingTime = next.OutputWaitingTime - previous.OutputWaitingTime,
        .OtherWaitingTime = next.OtherWaitingTime - previous.OtherWaitingTime,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TProcessingObservationWindow::TProcessingObservationWindow(TJobId jobId)
    : JobId_(jobId)
{ }

bool TProcessingObservationWindow::Matches(TJobId jobId) const
{
    return JobId_ == jobId;
}

EProcessingObservationAddResult TProcessingObservationWindow::Add(
    const TJobStatus& status,
    TInstant now,
    TDuration retentionPeriod)
{
    const auto& observation = status.ProcessingObservation;
    if (!Matches(status.JobId) ||
        status.IsFinished || !status.Error.IsOK() || !status.RetryableErrors.empty() ||
        !status.InitedTime || status.UpdateTime > now ||
        !observation ||
        observation->Sequence <= 0 || observation->CapturedAt > status.UpdateTime)
    {
        return EProcessingObservationAddResult::Rejected;
    }

    TPoint point{
        .Sequence = observation->Sequence,
        .CapturedAt = observation->CapturedAt,
        .ObservationDuration = observation->ObservationDuration,
        .Statistics = {
            .ProcessedCount = observation->ProcessedCount,
            .NonemptyEpochCount = observation->NonemptyEpochCount,
            .NonemptyEpochDuration = observation->NonemptyEpochDuration,
            .ProcessingTime = observation->ProcessingTime,
            .InputWaitingTime = observation->InputWaitingTime,
            .OutputWaitingTime = observation->OutputWaitingTime,
            .OtherWaitingTime = observation->OtherWaitingTime,
        },
    };
    if (!Dominates(point.Statistics, {}) ||
        (point.Statistics.NonemptyEpochCount && *point.Statistics.NonemptyEpochCount < 0) ||
        point.Statistics.NonemptyEpochCount.has_value() != point.Statistics.NonemptyEpochDuration.has_value())
    {
        return EProcessingObservationAddResult::Rejected;
    }
    if (Last_) {
        const auto& previous = *Last_;
        if (point.Sequence == previous.Sequence && point.CapturedAt == previous.CapturedAt &&
            Dominates(point.Statistics, previous.Statistics) && Dominates(previous.Statistics, point.Statistics))
        {
            return EProcessingObservationAddResult::Repeated;
        }
        if (point.Sequence <= previous.Sequence || point.CapturedAt < previous.CapturedAt ||
            !Dominates(point.Statistics, previous.Statistics))
        {
            return EProcessingObservationAddResult::Rejected;
        }
    }
    if (point.ObservationDuration > point.CapturedAt - TInstant::Zero()) {
        return EProcessingObservationAddResult::Rejected;
    }
    if (Last_) {
        const auto delta = Subtract(point.Statistics, Last_->Statistics);
        if (point.CapturedAt > Last_->CapturedAt) {
            Intervals_.push_back({Last_->CapturedAt, point.CapturedAt, delta});
            UpdateNonemptyEpochStatistics(delta, /*sign*/ 1);
        } else if (!Intervals_.empty()) {
            auto& statistics = Intervals_.back().Statistics;
            UpdateNonemptyEpochStatistics(statistics, /*sign*/ -1);
            statistics.ProcessedCount += delta.ProcessedCount;
            statistics.NonemptyEpochCount = statistics.NonemptyEpochCount && delta.NonemptyEpochCount
                ? std::optional(*statistics.NonemptyEpochCount + *delta.NonemptyEpochCount)
                : std::nullopt;
            statistics.NonemptyEpochDuration = statistics.NonemptyEpochDuration && delta.NonemptyEpochDuration
                ? std::optional(*statistics.NonemptyEpochDuration + *delta.NonemptyEpochDuration)
                : std::nullopt;
            statistics.ProcessingTime += delta.ProcessingTime;
            statistics.InputWaitingTime += delta.InputWaitingTime;
            statistics.OutputWaitingTime += delta.OutputWaitingTime;
            statistics.OtherWaitingTime += delta.OtherWaitingTime;
            UpdateNonemptyEpochStatistics(statistics, /*sign*/ 1);
            point.ObservationDuration = std::max(point.ObservationDuration, Last_->ObservationDuration);
        }
        while (Intervals_.size() > 1 && point.CapturedAt - Intervals_.front().End >= retentionPeriod) {
            UpdateNonemptyEpochStatistics(Intervals_.front().Statistics, /*sign*/ -1);
            Intervals_.pop_front();
        }
    } else {
        FirstCapture_ = point.CapturedAt;
    }
    Last_ = point;
    return EProcessingObservationAddResult::Accepted;
}

std::optional<std::pair<TInstant, TInstant>> TProcessingObservationWindow::GetCoverage() const
{
    return Last_
        ? std::optional(std::pair(Intervals_.empty() ? FirstCapture_ : Intervals_.front().Begin, Last_->CapturedAt))
        : std::nullopt;
}

TDuration TProcessingObservationWindow::GetLastObservationDuration() const
{
    return Last_ ? Last_->ObservationDuration : TDuration::Zero();
}

std::optional<std::pair<i64, TDuration>> TProcessingObservationWindow::GetNonemptyEpochStatistics() const
{
    if (Intervals_.empty() || IncompleteEpochIntervalCount_ > 0) {
        return std::nullopt;
    }
    return std::pair(NonemptyEpochCount_, NonemptyEpochDuration_);
}

void TProcessingObservationWindow::UpdateNonemptyEpochStatistics(const TStatistics& statistics, int sign)
{
    if (!statistics.NonemptyEpochCount || !statistics.NonemptyEpochDuration) {
        IncompleteEpochIntervalCount_ += sign;
        return;
    }
    NonemptyEpochCount_ += sign * *statistics.NonemptyEpochCount;
    if (sign > 0) {
        NonemptyEpochDuration_ += *statistics.NonemptyEpochDuration;
    } else {
        NonemptyEpochDuration_ -= *statistics.NonemptyEpochDuration;
    }
}

const std::deque<TProcessingObservationWindow::TInterval>& TProcessingObservationWindow::GetIntervals() const
{
    return Intervals_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
