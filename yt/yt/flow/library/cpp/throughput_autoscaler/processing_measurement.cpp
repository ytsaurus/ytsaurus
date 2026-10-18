#include "processing_measurement.h"

#include "processing_observation_window.h"

#include <algorithm>
#include <cmath>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

std::optional<TThroughputProcessingMeasurement> MeasureThroughputProcessing(
    const TThroughputSpecPtr& config,
    TConstArrayRef<const TProcessingObservationWindow*> observations,
    TInstant now,
    TDuration period,
    TInstant readyAt)
{
    if (observations.empty()) {
        return std::nullopt;
    }
    TThroughputProcessingMeasurement result;
    result.End = now;
    for (const auto* observation : observations) {
        if (!observation) {
            return std::nullopt;
        }
        const auto coverage = observation->GetCoverage();
        if (!coverage) {
            return std::nullopt;
        }
        result.Begin = std::max(result.Begin, coverage->first);
        result.End = std::min(result.End, coverage->second);
    }
    if (result.End <= result.Begin) {
        return std::nullopt;
    }
    if (result.End - result.Begin > period) {
        result.Begin = result.End - period;
    }

    struct TRates
    {
        double Processed{};
        double Busy{};
        double InputWaiting{};
        double OutputWaiting{};
    };

    struct TChange
    {
        TInstant At;
        int JobIndex{};
        TRates Rates;
    };

    const double duration = (result.End - result.Begin).SecondsFloat();
    double containedCount = 0;
    double wholeCount = 0;
    auto creditBegin = result.Begin;
    auto creditEnd = result.End;
    int busyPartitionCount = 0;
    std::vector<TChange> changes;
    for (int index = 0; index < std::ssize(observations); ++index) {
        double busyIntegral = 0;
        const auto& intervals = observations[index]->GetIntervals();
        auto intervalIt = std::ranges::upper_bound(intervals, result.Begin, {}, &TProcessingObservationWindow::TInterval::End);
        for (; intervalIt != intervals.end() && intervalIt->Begin < result.End; ++intervalIt) {
            const auto& interval = *intervalIt;
            const auto& statistics = interval.Statistics;
            // Exploration credit includes whole work intervals, never clipped batch counts.
            if (interval.Begin >= readyAt && interval.End <= now && statistics.ProcessedCount > 0) {
                if (interval.Begin >= result.Begin && interval.End <= result.End) {
                    containedCount += statistics.ProcessedCount;
                }
                wholeCount += statistics.ProcessedCount;
                creditBegin = std::min(creditBegin, interval.Begin);
                creditEnd = std::max(creditEnd, interval.End);
            }
            const double wall = (statistics.ProcessingTime + statistics.InputWaitingTime +
                statistics.OutputWaitingTime + statistics.OtherWaitingTime)
                .SecondsFloat();
            if (wall <= 0) {
                return std::nullopt;
            }
            const auto overlap = std::min(result.End, interval.End) - std::max(result.Begin, interval.Begin);
            busyIntegral += statistics.ProcessingTime.SecondsFloat() / wall * overlap.SecondsFloat();
            changes.push_back({
                .At = std::max(result.Begin, interval.Begin),
                .JobIndex = index,
                .Rates = {
                    .Processed = statistics.ProcessedCount / wall,
                    .Busy = statistics.ProcessingTime.SecondsFloat() / wall,
                    .InputWaiting = statistics.InputWaitingTime.SecondsFloat() / wall,
                    .OutputWaiting = statistics.OutputWaitingTime.SecondsFloat() / wall,
                },
            });
        }
        busyPartitionCount += busyIntegral >= config->MinimumBusyTimeShare * duration;
    }
    result.BusyPartitionShare = static_cast<double>(busyPartitionCount) / observations.size();
    std::ranges::sort(changes, {}, &TChange::At);
    std::vector<TRates> jobRates(observations.size());
    // Leaves are job busy shares; each parent stores its children's maximum.
    std::vector<double> busyShares(2 * observations.size());
    TRates totals;
    TRates integrals;
    auto previous = result.Begin;
    auto integrate = [&] (TInstant end) {
        const double seconds = (end - previous).SecondsFloat();
        integrals.Processed += std::max(0.0, totals.Processed) * seconds;
        integrals.Busy += busyShares[1] * seconds;
        integrals.InputWaiting += std::max(0.0, totals.InputWaiting) * seconds;
        integrals.OutputWaiting += std::max(0.0, totals.OutputWaiting) * seconds;
        previous = end;
    };
    for (const auto& change : changes) {
        integrate(change.At);
        auto& rates = jobRates[change.JobIndex];
        totals.Processed += change.Rates.Processed - rates.Processed;
        totals.InputWaiting += change.Rates.InputWaiting - rates.InputWaiting;
        totals.OutputWaiting += change.Rates.OutputWaiting - rates.OutputWaiting;
        int busyIndex = change.JobIndex + std::ssize(observations);
        busyShares[busyIndex] = change.Rates.Busy;
        for (busyIndex /= 2; busyIndex > 0; busyIndex /= 2) {
            const double busy = std::max(busyShares[2 * busyIndex], busyShares[2 * busyIndex + 1]);
            if (busyShares[busyIndex] == busy) {
                break;
            }
            busyShares[busyIndex] = busy;
        }
        rates = change.Rates;
    }
    integrate(result.End);
    const double processed = integrals.Processed / duration;
    if (std::isfinite(processed) && processed >= 0) {
        result.Processed = processed;
        const double containedRate = containedCount / duration;
        const double wholeRate = wholeCount / (creditEnd - creditBegin).SecondsFloat();
        const double credit = std::max(containedRate, wholeRate);
        if (std::isfinite(credit)) {
            result.ProcessedCredit = std::min(processed, credit);
            result.ProcessedCreditBegin = wholeRate > containedRate ? creditBegin : result.Begin;
            result.ProcessedCreditEnd = wholeRate > containedRate ? creditEnd : result.End;
        }
        if (integrals.Busy > 0) {
            const double capacity = integrals.Processed / integrals.Busy;
            if (std::isfinite(capacity)) {
                result.Capacity = capacity;
            }
        }
    }
    result.InputWaitingShare = integrals.InputWaiting / (duration * observations.size());
    result.OutputWaitingShare = integrals.OutputWaiting / (duration * observations.size());
    return result;
}

TDuration ComputeThroughputBatchDuration(TConstArrayRef<const TProcessingObservationWindow*> observations)
{
    i64 count = 0;
    TDuration duration;
    TDuration latestDuration;
    bool complete = true;
    for (const auto* observation : observations) {
        latestDuration = std::max(latestDuration, observation->GetLastObservationDuration());
        if (const auto statistics = observation->GetNonemptyEpochStatistics()) {
            count += statistics->first;
            duration += statistics->second;
        } else {
            complete = false;
        }
    }
    if (!complete) {
        return latestDuration;
    }
    return count > 0 ? duration / count : TDuration::Zero();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
