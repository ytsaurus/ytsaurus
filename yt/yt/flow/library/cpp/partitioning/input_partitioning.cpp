#include "partitioning_coordinator.h"

#include "partitioning_helpers.h"

#include <library/cpp/iterator/concatenate.h>

#include <cmath>

namespace NYT::NFlow::NPartitioning {

////////////////////////////////////////////////////////////////////////////////

TPartitioningCoordinator::TInputAutoPartitioningContext::TInputAutoPartitioningContext(
    const TFlowViewPtr& flowView,
    const THashMap<TPartitionId, TKeyRange>& partitionRanges)
    : FlowView(flowView)
    , PartitionRanges(partitionRanges)
{ }

namespace {

template <class T>
struct TAverage
{
    TAverage(double limit, const char* name)
        : Limit(limit)
        , Name(name)
    { }

    void Add(const TPartitionId& partitionId, const T& value)
    {
        Value += value;
        ++Count;
        Weights.emplace(partitionId, value);
    }

    T Get() const
    {
        return Value / (Count ? Count : 1);
    }

    T Limit;
    const char* Name;
    T Value = {};
    ssize_t Count = 0;
    THashMap<TPartitionId, T> Weights;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitioningCoordinator::InputAutoPartitioningCollectData(
    TComputationState& state,
    TInputAutoPartitioningContext& context) const
{
    const auto& Logger = state.Logger;

    context.AllPartitionsHaveStatuses = true;
    context.AllPartitionsHavePivots = true;
    const auto& partitionStatuses = context.FlowView->Feedback->PartitionJobStatuses;
    for (const auto& [partitionId, keyRange] : context.PartitionRanges) {
        auto it = partitionStatuses.find(partitionId);
        if (it == partitionStatuses.end() || !it->second->CurrentJobStatus) {
            context.AllPartitionsHaveStatuses = false;
            context.AllPartitionsHavePivots = false;
            break;
        }
        if (it->second->CurrentJobStatus->InputMetrics->Global.Pivots.empty()) {
            context.AllPartitionsHavePivots = false;
        }
    }

    const auto& pipelineState = context.FlowView->State->ExecutionSpec->PipelineState;
    if (pipelineState->GetValue() != EPipelineState::Working) {
        context.NormalFlightDuration = TDuration::Zero();
        YT_TLOG_INFO("Partitioning: wait for working state")
            .With("PipelineState", pipelineState->GetValue())
            .With("LastRepartitionTime", state.LastRepartitionTime)
            .With("PipelineStateUpdate", pipelineState->GetLastUpdate())
            .With("NormalFlightDuration", context.NormalFlightDuration);
    } else {
        TInstant baseInstant = std::max(state.LastRepartitionTime, pipelineState->GetLastUpdate());
        context.AnotherComputationRecentlyRepartitioned =
            LastRepartitioningInstant_ > state.LastObservedCommonRepartitioningInstant;
        if (!context.AnotherComputationRecentlyRepartitioned) {
            baseInstant = std::max(baseInstant, LastRepartitioningInstant_);
        }
        // Both operands use the cluster clock; node-clock skew would corrupt the cooldown.
        context.NormalFlightDuration =
            TInstant::Seconds(context.FlowView->State->CurrentTimestamp.Underlying()) - baseInstant;
        YT_TLOG_INFO("Partitioning: calculating normal flight duration")
            .With("PipelineState", pipelineState->GetValue())
            .With("LastRepartitionTime", state.LastRepartitionTime)
            .With("PipelineStateUpdate", pipelineState->GetLastUpdate())
            .With("NormalFlightDuration", context.NormalFlightDuration);
    }
}

double TPartitioningCoordinator::ApplyPeakHoldRelease(
    double previous,
    double target,
    TDuration elapsed,
    TDuration releaseHalfDelay)
{
    if (target >= previous || releaseHalfDelay == TDuration::Zero()) {
        // Growth is immediate; smoothing can also be disabled explicitly.
        return target;
    }
    // The remaining gap to the lower target halves every release half-delay.
    double alpha = 1.0 - std::exp2(-elapsed.SecondsFloat() / releaseHalfDelay.SecondsFloat());
    return previous + alpha * (target - previous);
}

void TPartitioningCoordinator::InputAutoPartitioningCalculateOptimalCount(
    TComputationState& state,
    const TRangePartitioningParameters& parameters,
    TInputAutoPartitioningContext& context)
{
    const auto& Logger = state.Logger;
    const auto& spec = parameters.PartitioningSpec;

    i64 minInputPartitionCount = spec->MinPartitionCount.value_or(DefaultMinInputPartitionCount);
    i64 maxInputPartitionCount = spec->MaxPartitionCount.value_or(DefaultMaxInputPartitionCount);
    if (maxInputPartitionCount < minInputPartitionCount) {
        if (spec->MinPartitionCount && !spec->MaxPartitionCount) {
            maxInputPartitionCount = minInputPartitionCount;
        } else {
            YT_ASSERT(!spec->MinPartitionCount && spec->MaxPartitionCount);
            minInputPartitionCount = maxInputPartitionCount;
        }
    }
    const i64 sinkChannelMultiplier =
        spec->SinkChannelMultiplier.value_or(DefaultSinkChannelMultiplier);
    const double desiredAveragePartitionCpuLoad =
        spec->DesiredAveragePartitionCpuLoad.value_or(DefaultDesiredAveragePartitionCpuLoad);
    const double desiredAveragePartitionMemoryUsed =
        spec->DesiredAveragePartitionMemoryUsed.value_or(DefaultDesiredAveragePartitionMemoryUsed);
    const double desiredAveragePartitionMessagesPerSecond =
        spec->DesiredAveragePartitionMessagesPerSecond.value_or(
        DefaultDesiredAveragePartitionMessagesPerSecond);
    const double desiredAveragePartitionBytesPerSecond =
        spec->DesiredAveragePartitionBytesPerSecond.value_or(
        DefaultDesiredAveragePartitionBytesPerSecond);
    const double desiredAveragePartitionTimerCount =
        spec->DesiredAveragePartitionTimerCount.value_or(DefaultDesiredAveragePartitionTimerCount);
    const double allowedPartitionCountDeviation =
        spec->AllowedPartitionCountDeviation.value_or(DefaultAllowedPartitionCountDeviation);
    const TDuration partitionCountDoubleDelay =
        spec->PartitionCountDoubleDelay.value_or(DefaultPartitionCountDoubleDelay);
    const TDuration partitionCountHalfDelay =
        spec->PartitionCountHalfDelay.value_or(DefaultPartitionCountHalfDelay);

    const i64 currentPartitionCount = std::ssize(context.PartitionRanges);
    const i64 workerCount = std::ssize(context.FlowView->State->Workers);

    struct TProposition
    {
        i64 Count;
        std::string_view Criteria;
        THashMap<TPartitionId, double> Weights;
    };

    std::vector<TProposition> suggestions;

    if (context.MaxSinkChannelCount) {
        i64 count = *context.MaxSinkChannelCount * sinkChannelMultiplier;
        count = std::max(count, workerCount);
        suggestions.emplace_back(count, "sink channels");
        YT_TLOG_INFO("Partitioning: collected sink channels")
            .With("ChannelCount", *context.MaxSinkChannelCount)
            .With("ProposedPartitionCount", count);
    }

    if (!context.PartitionRanges.empty()) {
        const auto& partitionStatuses = context.FlowView->Feedback->PartitionJobStatuses;
        TAverage<double> averageCpuUsage(desiredAveragePartitionCpuLoad, "average CPU load");
        TAverage<double> averageMemoryUsage(desiredAveragePartitionMemoryUsed, "average memory usage");
        TAverage<double> averageMessagesPerSecond(
            desiredAveragePartitionMessagesPerSecond,
            "average messages per second");
        TAverage<double> averageBytesPerSecond(
            desiredAveragePartitionBytesPerSecond,
            "average bytes per second");
        TAverage<double> averageTimerCount(desiredAveragePartitionTimerCount, "average timer count");
        for (const auto& [partitionId, keyRange] : context.PartitionRanges) {
            auto it = partitionStatuses.find(partitionId);
            if (it == partitionStatuses.end() || !it->second->CurrentJobStatus) {
                continue;
            }
            const auto& status = it->second->CurrentJobStatus;
            if (status->PerformanceMetrics &&
                (status->PerformanceMetrics->CpuUsage10m || status->PerformanceMetrics->CpuUsage30s))
            {
                averageCpuUsage.Add(
                    partitionId,
                    status->PerformanceMetrics->CpuUsage10m
                        ? *status->PerformanceMetrics->CpuUsage10m
                        : *status->PerformanceMetrics->CpuUsage30s);
            }
            if (status->PerformanceMetrics && status->PerformanceMetrics->MemoryUsage10m) {
                averageMemoryUsage.Add(partitionId, status->PerformanceMetrics->MemoryUsage10m);
            }
            if (status->InputMetrics) {
                averageMessagesPerSecond.Add(partitionId, status->InputMetrics->Global.MessagesPerSecond);
                averageBytesPerSecond.Add(partitionId, status->InputMetrics->Global.BytesPerSecond);
            }
            if (status->FromPartitionTraverseData && status->FromPartitionTraverseData->Node) {
                ssize_t timerCount = 0;
                const auto& nodeStreams = status->FromPartitionTraverseData->Node->Streams;
                for (const auto& streamId : parameters.TimerStreamIds) {
                    auto streamIt = nodeStreams.find(streamId);
                    if (streamIt != nodeStreams.end() && streamIt->second->InflightMetrics) {
                        timerCount += streamIt->second->InflightMetrics->Count;
                    }
                }
                averageTimerCount.Add(partitionId, timerCount);
            }
        }

        YT_TLOG_INFO("Partitioning: collected metrics")
            .With("AverageCpuUsage", averageCpuUsage.Get())
            .With("AverageMemoryUsage", averageMemoryUsage.Get())
            .With("AverageMessagesPerSecond", averageMessagesPerSecond.Get())
            .With("AverageBytesPerSecond", averageBytesPerSecond.Get())
            .With("AverageTimerCount", averageTimerCount.Get());

        const ssize_t minStatusCount =
            static_cast<ssize_t>(currentPartitionCount / allowedPartitionCountDeviation);
        const TInstant now = TInstant::Now();
        TStringBuilder smoothingReport;
        for (auto* averageMetric : {
                &averageCpuUsage,
                &averageMemoryUsage,
                &averageMessagesPerSecond,
                &averageBytesPerSecond,
                &averageTimerCount})
        {
            if (averageMetric->Limit <= 0 || averageMetric->Count < minStatusCount) {
                continue;
            }
            const double ratio = averageMetric->Get() / averageMetric->Limit;
            const double proposedByCriterion = std::round(currentPartitionCount * ratio);
            // Peak-hold smoothing lets a criterion grow instantly but releases it only with the
            // configured half-delay, avoiding rapid shrinkage after a transient load dip.
            const std::string criterion(averageMetric->Name);
            auto [it, inserted] = state.CriterionProposedCountEma.emplace(
                criterion,
                TCriterionEmaState{proposedByCriterion, now});
            if (!inserted) {
                it->second.ProposedCount = ApplyPeakHoldRelease(
                    it->second.ProposedCount,
                    proposedByCriterion,
                    now - it->second.UpdatedAt,
                    partitionCountHalfDelay);
                it->second.UpdatedAt = now;
            }
            if (smoothingReport.GetLength() > 0) {
                smoothingReport.AppendString("; ");
            }
            smoothingReport.AppendFormat(
                "%v: %v -> %v",
                criterion,
                proposedByCriterion,
                it->second.ProposedCount);
            suggestions.emplace_back(
                static_cast<ssize_t>(std::llround(it->second.ProposedCount)),
                averageMetric->Name,
                std::move(averageMetric->Weights));
        }
        if (smoothingReport.GetLength() > 0) {
            YT_TLOG_INFO("Partitioning: peak-hold smoothed criterion proposals")
                .With("RawToSmoothed", smoothingReport.Flush());
        }
        for (auto& suggestion : suggestions) {
            if (suggestion.Count < std::ssize(context.PartitionRanges)) {
                suggestion.Count = std::min(
                    suggestion.Count * 3,
                    std::ssize(context.PartitionRanges));
            }
        }
    }

    auto finalizeWeights = [&] {
        double sum = 0.0;
        for (const auto& [partitionId, weight] : context.Weights) {
            sum += weight;
        }
        const double average = context.Weights.empty() ? 1.0 : sum / std::ssize(context.Weights);
        for (const auto& [partitionId, range] : context.PartitionRanges) {
            if (!context.Weights.contains(partitionId)) {
                context.Weights[partitionId] = average;
            }
        }
    };

    auto strictResult = [&] (i64 proposedCount, THashMap<TPartitionId, double>&& weights) {
        context.ProposedCount = proposedCount;
        context.RecreateNow = proposedCount != currentPartitionCount;
        context.Weights = std::move(weights);
        finalizeWeights();
    };

    auto shouldRecreate = [&] (i64 proposedCount) {
        if (currentPartitionCount < minInputPartitionCount || currentPartitionCount > maxInputPartitionCount) {
            return true;
        }
        if (
            proposedCount <= currentPartitionCount * allowedPartitionCountDeviation &&
            proposedCount >= currentPartitionCount / allowedPartitionCountDeviation)
        {
            return false;
        }
        if (!context.AllPartitionsHaveStatuses || context.NormalFlightDuration == TDuration::Zero()) {
            return false;
        }

        TDuration delay;
        if (proposedCount > currentPartitionCount) {
            const double proposedMultiple =
                static_cast<double>(proposedCount) / static_cast<double>(currentPartitionCount);
            const TDuration referenceDelay = partitionCountDoubleDelay;
            delay = std::max(referenceDelay / log(proposedMultiple) * log(2), referenceDelay / 2);
            if (context.AnotherComputationRecentlyRepartitioned) {
                const TDuration timeSinceAnyRepartitioning =
                    TInstant::Seconds(context.FlowView->State->CurrentTimestamp.Underlying()) -
                    LastRepartitioningInstant_;
                const TDuration crossComputationAffectDuration = std::min(referenceDelay, delay) / 2;
                if (timeSinceAnyRepartitioning < crossComputationAffectDuration) {
                    const double coefficient =
                        0.5 + 0.5 * (timeSinceAnyRepartitioning / crossComputationAffectDuration);
                    delay *= coefficient;
                    YT_TLOG_INFO("Partitioning: delay decreased due to cross computation affect")
                        .With("Coefficient", coefficient)
                        .With("NewDelay", delay.Seconds());
                }
            }
        } else {
            const double proposedMultiple =
                static_cast<double>(currentPartitionCount) / static_cast<double>(proposedCount);
            const TDuration referenceDelay = partitionCountHalfDelay;
            delay = std::max(referenceDelay / log(proposedMultiple) * log(2), referenceDelay / 2);
        }
        return context.NormalFlightDuration > delay;
    };

    if (suggestions.empty()) {
        if (spec->DesiredPartitionCount) {
            return strictResult(*spec->DesiredPartitionCount, {});
        }
        context.ProposedCount = std::clamp(workerCount, minInputPartitionCount, maxInputPartitionCount);
        context.RecreateNow = shouldRecreate(context.ProposedCount);
        finalizeWeights();
        YT_TLOG_INFO("Partitioning: no suggestions for optimal partition count, use reasonable minimum")
            .With("ProposedCount", context.ProposedCount)
            .With("RecreateNow", context.RecreateNow)
            .With("CurrentCount", currentPartitionCount)
            .With("Criteria", workerCount ? "minimum" : "worker count")
            .With("NormalFlightDuration", context.NormalFlightDuration.Seconds());
        return;
    }

    ssize_t maxSuggestionIndex = 0;
    for (ssize_t index = 1; index < std::ssize(suggestions); ++index) {
        if (suggestions[index].Count > suggestions[maxSuggestionIndex].Count) {
            maxSuggestionIndex = index;
        }
    }
    if (spec->DesiredPartitionCount) {
        return strictResult(
            *spec->DesiredPartitionCount,
            std::move(suggestions[maxSuggestionIndex].Weights));
    }

    context.ProposedCount = std::clamp(
        suggestions[maxSuggestionIndex].Count,
        minInputPartitionCount,
        maxInputPartitionCount);
    if (context.ProposedCount < currentPartitionCount / 2) {
        context.ProposedCount = std::clamp(
            currentPartitionCount / 2,
            minInputPartitionCount,
            maxInputPartitionCount);
    }
    context.RecreateNow = shouldRecreate(context.ProposedCount);
    context.Weights = std::move(suggestions[maxSuggestionIndex].Weights);
    finalizeWeights();

    YT_TLOG_INFO("Partitioning: calculated optimal partition count")
        .With("ProposedCount", context.ProposedCount)
        .With("RecreateNow", context.RecreateNow)
        .With("CurrentCount", currentPartitionCount)
        .With("Criteria", suggestions[maxSuggestionIndex].Criteria)
        .With("NormalFlightDuration", context.NormalFlightDuration.Seconds());
}

void TPartitioningCoordinator::InputAutoPartitioningBuildRanges(
    const TComputationState& state,
    const TRangePartitioningParameters& parameters,
    TInputAutoPartitioningContext& context) const
{
    if (context.ProposedCount == 0) {
        return;
    }

    const auto& Logger = state.Logger;

    if (!parameters.EnableNonUintKey) {
        YT_VERIFY(parameters.FirstKeyIsUint);
        context.NewRanges = SplitUintKeyRange(UniversalKeyRange(), context.ProposedCount);
        return;
    }

    if (context.PartitionRanges.empty()) {
        context.NewRanges = parameters.FirstKeyIsUint
            ? SplitUintKeyRange(UniversalKeyRange(), context.ProposedCount)
            : std::vector<TKeyRange>{UniversalKeyRange()};
        return;
    }

    auto oldSortedRanges = GetSortedPartitionRanges(context.PartitionRanges);

    struct TWeightedRange
        : TKeyRange
    {
        double Weight;
    };

    std::vector<TWeightedRange> splitRanges;
    const auto& partitionStatuses = context.FlowView->Feedback->PartitionJobStatuses;
    for (const auto& [partitionId, range] : oldSortedRanges) {
        double weight = context.Weights.at(partitionId);
        auto it = partitionStatuses.find(partitionId);
        if (
            it == partitionStatuses.end() ||
            !it->second->CurrentJobStatus ||
            !it->second->CurrentJobStatus->InputMetrics ||
            it->second->CurrentJobStatus->InputMetrics->Global.Pivots.empty())
        {
            splitRanges.push_back({range, weight});
            continue;
        }
        const auto& splitters = it->second->CurrentJobStatus->InputMetrics->Global.Pivots;
        const double subWeight = weight / (std::ssize(splitters) + 1);
        splitRanges.push_back({{range.Lower, splitters.front()}, subWeight});
        weight -= subWeight;
        for (ssize_t index = 0; index < std::ssize(splitters) - 1; ++index) {
            splitRanges.push_back({{splitters[index], splitters[index + 1]}, subWeight});
            weight -= subWeight;
        }
        splitRanges.push_back({{splitters.back(), range.Upper}, weight});
    }

    context.NewRanges.reserve(context.ProposedCount);
    context.NewWeights.reserve(context.ProposedCount);
    double remainingWeight = Accumulate(context.Weights | std::views::elements<1>, 0.0);
    auto remainingCount = context.ProposedCount;
    ssize_t index = 0;
    while (remainingCount > 0 && index < std::ssize(splitRanges)) {
        const double requiredWeight = remainingWeight / remainingCount;
        double consumedWeight = splitRanges[index].Weight;
        ssize_t endIndex = index + 1;
        while (consumedWeight < requiredWeight && endIndex < std::ssize(splitRanges)) {
            consumedWeight += splitRanges[endIndex].Weight;
            ++endIndex;
        }
        if (consumedWeight > requiredWeight && endIndex > index + 1) {
            if (
                consumedWeight - requiredWeight >
                requiredWeight - consumedWeight - splitRanges[endIndex - 1].Weight)
            {
                --endIndex;
                consumedWeight -= splitRanges[endIndex].Weight;
            }
        }

        context.NewRanges.push_back({
            splitRanges[index].Lower,
            splitRanges[endIndex - 1].Upper,
        });
        context.NewWeights.push_back(consumedWeight);
        --remainingCount;
        remainingWeight -= consumedWeight;
        index = endIndex;
    }
    context.NewRanges.back().Upper = splitRanges.back().Upper;
    if (std::ssize(context.NewRanges) != context.ProposedCount || !context.AllPartitionsHavePivots) {
        if (parameters.FirstKeyIsUint) {
            YT_TLOG_INFO("Partitioning: fall back to uniform uint split")
                .With("ResultSize", std::ssize(context.NewRanges))
                .With("PartitionCount", context.ProposedCount)
                .With("AllPartitionsHaveSplitters", context.AllPartitionsHavePivots)
                .With("SubrangeCount", std::ssize(splitRanges));
            context.NewRanges = SplitUintKeyRange(UniversalKeyRange(), context.ProposedCount);
        } else {
            YT_TLOG_INFO("Partitioning: cannot recreate now, must wait for partition pivots")
                .With("ResultSize", std::ssize(context.NewRanges))
                .With("PartitionCount", context.ProposedCount)
                .With("AllPartitionsHaveSplitters", context.AllPartitionsHavePivots)
                .With("SubrangeCount", std::ssize(splitRanges));
            context.RecreateNow = false;
        }
        // The calculated weights no longer describe NewRanges after fallback or deferral.
        context.NewWeights.clear();
    }
}

void TPartitioningCoordinator::InputAutoPartitioningTryRebalance(
    const TComputationState& state,
    const TRangePartitioningParameters& parameters,
    TInputAutoPartitioningContext& context) const
{
    const auto& Logger = state.Logger;
    const auto& spec = parameters.PartitioningSpec;

    const double allowedPartitionCountDeviation =
        spec->AllowedPartitionCountDeviation.value_or(
        DefaultAllowedPartitionCountDeviation);
    if (
        !context.RecreateNow &&
        context.AllPartitionsHavePivots &&
        !context.NewWeights.empty() &&
        !context.PartitionRanges.empty() &&
        context.NormalFlightDuration != TDuration::Zero() &&
        context.ProposedCount <= std::ssize(context.PartitionRanges) * allowedPartitionCountDeviation &&
        context.ProposedCount >= std::ssize(context.PartitionRanges) / allowedPartitionCountDeviation)
    {
        auto getDeviation = [] (const auto& weights) {
            const double average = Accumulate(weights, 0.0) / std::ssize(weights);
            double deviation = Accumulate(weights, 0.0, [&] (double sum, double weight) {
                return sum + std::pow(weight - average, 2);
            });
            return std::sqrt(deviation / std::ssize(weights));
        };
        const double oldDeviation = getDeviation(context.Weights | std::views::elements<1>);
        const double newDeviation = getDeviation(context.NewWeights) + 1e-5;
        if (newDeviation < oldDeviation) {
            const double multiple = oldDeviation / newDeviation;
            const auto partitionCountDoubleDelay =
                spec->PartitionCountDoubleDelay.value_or(
                DefaultPartitionCountDoubleDelay);
            const auto delay = std::max(
                partitionCountDoubleDelay / log(multiple) * log(2),
                partitionCountDoubleDelay);
            if (context.NormalFlightDuration > delay) {
                context.RecreateNow = true;
                YT_TLOG_INFO("Partitioning: decided to rebuild partitions to make more uniform distribution")
                    .With("OldDeviation", oldDeviation)
                    .With("NewDeviation", newDeviation);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NPartitioning
