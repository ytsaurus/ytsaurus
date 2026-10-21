#include "processing_meter.h"

#include <yt/yt/flow/library/cpp/client/public.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <algorithm>

namespace NYT::NFlow {
namespace {

struct TRateAccumulator
{
    std::optional<double> Sum = 0;

    void Add(const std::optional<double>& value)
    {
        Sum = Sum && value ? std::optional(*Sum + *value) : std::nullopt;
    }
};

struct TStreamAccumulator
{
    TRateAccumulator ReadyCount;
    TRateAccumulator OfferedCountRate;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TProcessingMeter::TObservations TProcessingMeter::Collect(
    const TFlowStatePtr& committedState,
    const TFlowViewPtr& flowView,
    const TThroughputSpecPtr& config,
    const TComputations& computations)
{
    EraseNodesIf(Histories_, [&] (const auto& item) {
        return !computations.contains(item.first);
    });
    TObservations result;

    const auto& layout = committedState->ExecutionSpec->Layout;
    const auto& statuses = flowView->Feedback->PartitionJobStatuses;
    const auto observationNow = flowView->Feedback->UpdateTime;
    THashMap<TComputationId, TDuration> retentionPeriods;
    for (const auto& [computationId, registered] : computations) {
        auto captureLag = TDuration::Zero();
        if (const auto* runtime = Histories_.FindPtr(computationId)) {
            for (const auto& [_, observation] : runtime->Jobs) {
                captureLag = std::max(captureLag, 2 * observation.GetLastObservationDuration());
            }
        }
        retentionPeriods[computationId] =
            std::max(registered.Period, config->MaxRetryWarmupPeriod) + captureLag;
    }
    const bool pipelineWorking =
        committedState->ExecutionSpec->PipelineState->GetValue() == EPipelineState::Working;

    struct TComputationAccumulator
    {
        THashMap<TStreamId, TStreamAccumulator> Streams;
        THashMap<TPartitionId, TProcessingObservationWindow> Jobs;
        i64 CompletedPartitionCount = 0;
        i64 UnchangedJobCount = 0;
        bool AllJobsHealthy = true;
    };

    THashMap<TComputationId, TComputationAccumulator> computationAccumulators;
    for (const auto& [computationId, _] : computations) {
        result.try_emplace(computationId);
        computationAccumulators.try_emplace(computationId);
    }

    for (const auto& [partitionId, partition] : layout->Partitions) {
        const auto* registered = computations.FindPtr(partition->ComputationId);
        if (!registered) {
            continue;
        }
        auto& sample = result[partition->ComputationId];
        auto& accumulator = computationAccumulators[partition->ComputationId];
        if (partition->State == EPartitionState::Interrupting) {
            sample.Repartitioning = true;
            continue;
        }
        if (partition->State == EPartitionState::Completed) {
            ++accumulator.CompletedPartitionCount;
            continue;
        }
        if (!partition->IsWorking()) {
            continue;
        }
        ++sample.PartitionCount;
        if (partition->State != EPartitionState::Executing) {
            sample.Repartitioning = true;
            accumulator.AllJobsHealthy = false;
            continue;
        }

        if (!partition->CurrentJobId) {
            accumulator.AllJobsHealthy = false;
            continue;
        }
        auto [observationIt, inserted] = accumulator.Jobs.emplace(
            partitionId,
            TProcessingObservationWindow(*partition->CurrentJobId));
        YT_VERIFY(inserted);
        auto& observation = observationIt->second;
        auto& runtime = Histories_[partition->ComputationId];
        if (auto* previous = runtime.Jobs.FindPtr(partitionId);
            previous && previous->Matches(*partition->CurrentJobId))
        {
            observation = std::move(*previous);
            ++accumulator.UnchangedJobCount;
        }
        const auto* partitionStatus = statuses.FindPtr(partitionId);
        if (!partitionStatus || !(*partitionStatus)->CurrentJobStatus ||
            (*partitionStatus)->CurrentJobId != partition->CurrentJobId)
        {
            accumulator.AllJobsHealthy = false;
            continue;
        }

        const auto& jobStatus = (*partitionStatus)->CurrentJobStatus;
        if (jobStatus->JobId != *partition->CurrentJobId) {
            accumulator.AllJobsHealthy = false;
            continue;
        }

        const auto added = observation.Add(
            *jobStatus,
            observationNow,
            GetOrCrash(retentionPeriods, partition->ComputationId));
        if (added == EProcessingObservationAddResult::Rejected) {
            accumulator.AllJobsHealthy = false;
            continue;
        }
        if (!jobStatus->FromPartitionTraverseData || !jobStatus->FromPartitionTraverseData->Node) {
            accumulator.AllJobsHealthy = false;
            continue;
        }

        const auto& node = jobStatus->FromPartitionTraverseData->Node;
        for (const auto& [streamId, stream] : node->Streams) {
            if (!stream || !stream->InflightMetrics) {
                continue;
            }
            const auto& metrics = stream->InflightMetrics;
            auto& streamAccumulator = accumulator.Streams[streamId];
            streamAccumulator.ReadyCount.Add(metrics->ReadyCount);
            streamAccumulator.OfferedCountRate.Add(metrics->OfferedCountPerSec);
        }
    }

    for (const auto& [computationId, registered] : computations) {
        auto& sample = result[computationId];
        auto& runtime = Histories_[computationId];
        auto& accumulator = computationAccumulators[computationId];

        sample.AllJobsHealthy = accumulator.AllJobsHealthy && sample.PartitionCount > 0;

        const i64 unchangedJobCount = accumulator.UnchangedJobCount;
        const bool cohortChanged = unchangedJobCount != std::ssize(runtime.Jobs) ||
            unchangedJobCount != std::ssize(accumulator.Jobs);
        if (cohortChanged) {
            runtime.ReadyAt = observationNow;
        }
        runtime.Jobs = std::move(accumulator.Jobs);
        if (!pipelineWorking || sample.Repartitioning || !sample.AllJobsHealthy) {
            runtime.ReadyAt = observationNow;
        }
        sample.ReadyAt = runtime.ReadyAt;
        if (pipelineWorking && !sample.Repartitioning && sample.AllJobsHealthy) {
            std::vector<const TProcessingObservationWindow*> observations;
            observations.reserve(runtime.Jobs.size());
            for (const auto& [_, observation] : runtime.Jobs) {
                observations.push_back(&observation);
            }
            sample.Measurement = MeasureThroughputProcessing(
                config,
                observations,
                observationNow,
                registered.Period,
                sample.ReadyAt);
            if (registered.RecoveryPeriod) {
                sample.RecoveryMeasurement = MeasureThroughputProcessing(
                    config,
                    observations,
                    observationNow,
                    *registered.RecoveryPeriod,
                    sample.ReadyAt);
            }
            sample.ActualBatchDuration = ComputeThroughputBatchDuration(observations);
        }

        for (const auto& [streamId, streamAccumulator] : accumulator.Streams) {
            auto& stream = sample.Streams[streamId];
            stream.Ready = sample.AllJobsHealthy ? streamAccumulator.ReadyCount.Sum : std::nullopt;
            stream.Offered = sample.AllJobsHealthy ? streamAccumulator.OfferedCountRate.Sum : std::nullopt;
        }

        if (sample.PartitionCount == 0 && !sample.Repartitioning && accumulator.CompletedPartitionCount > 0) {
            for (const auto& [streamId, _] : registered.Spec->SourceStreams) {
                sample.Streams[streamId] = {
                    .Ready = 0,
                    .Offered = 0,
                };
            }
        }

        for (const auto& outputStreamId : registered.Spec->OutputStreamIds) {
            const auto* parentStreamIds = registered.Spec->StreamsDependency.FindPtr(outputStreamId);
            if (!parentStreamIds) {
                continue;
            }
            auto& parentRates = sample.LineageRatios[outputStreamId];
            const auto globalOutputStreamId = MakeGlobalStreamId(
                computationId,
                outputStreamId,
                registered.Spec);
            const auto* aggregateParentRates =
                flowView->EphemeralState->LineageRatios.FindPtr(globalOutputStreamId);
            for (const auto& parentStreamId : *parentStreamIds) {
                const auto globalParentStreamId = MakeGlobalStreamId(
                    computationId,
                    parentStreamId,
                    registered.Spec);
                if (aggregateParentRates) {
                    if (const auto* aggregateRate = aggregateParentRates->FindPtr(globalParentStreamId)) {
                        parentRates[parentStreamId] = *aggregateRate;
                        continue;
                    }
                }
                parentRates.try_emplace(parentStreamId);
            }
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
