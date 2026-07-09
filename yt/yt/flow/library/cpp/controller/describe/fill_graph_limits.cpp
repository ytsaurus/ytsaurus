#include "fill_graph_limits.h"
#include "graph_entity_id.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/string_builder.h>

#include <algorithm>
#include <set>

namespace NYT::NFlow::NDescribe {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

void AccumulateLimits(
    TStreamLimitStats& stats,
    const TJobEntityLimitStatus& status,
    const TPartitionId& partitionId)
{
    double fillRate = static_cast<double>(status.Used) / std::max<i64>(1, status.Limit);
    if (fillRate > stats.GetMaxFillRate()) {
        stats.Max = status;
        stats.MaxPartitionId = partitionId;
    }
    stats.Total.Used += status.Used;
    stats.Total.Limit += status.Limit;
    if (status.Pending) {
        stats.Total.Pending = stats.Total.Pending.value_or(0) + *status.Pending;
    }
}

// Aggregates one partition's limits into ITS OWN computation's stats, keyed by graph entity id.
// Stream ids the spec does not declare for this computation (absent from |knownStreamIds|) are
// skipped, so partitions of different computations sharing a global stream never collide.
void AggregateLimits(
    const THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>>& jobLimits,
    const TPartitionId& partitionId,
    const THashSet<TStreamId>& knownStreamIds,
    THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& computationStats)
{
    for (const auto& [limitType, streamLimits] : jobLimits) {
        for (const auto& [rawStreamId, status] : streamLimits) {
            if (!knownStreamIds.contains(rawStreamId)) {
                // Stream id is not declared for this computation; skip.
                continue;
            }
            AccumulateLimits(computationStats[MakeStreamGraphId(rawStreamId)][limitType], status, partitionId);
        }
    }
}

} // namespace

void FillGraphLimits(
    const TFlowViewPtr& flowView,
    const THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>>& /*intermediateDescriptions*/,
    TPipelineDescription& pipeline)
{
    if (!flowView->Feedback) {
        return;
    }

    const auto& pipelineSpec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();

    // Buffer limits (filled only when per-partition Feedback is available).
    //
    // Partitions of DIFFERENT computations may consume the same shared global input stream, so
    // limits MUST be aggregated per (computation, stream) independently. We route each partition's
    // limits to ITS OWN computation via the layout's Partitions container, restricted to the stream
    // ids the spec declares for that computation.

    const auto& layout = flowView->State->ExecutionSpec->Layout;

    // Per-computation declared stream ids, used to drop ids unknown to the spec.
    THashMap<TComputationId, THashSet<TStreamId>> inputStreamIds;
    THashMap<TComputationId, THashSet<TStreamId>> outputStreamIds;
    for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
        if (!pipeline.Computations.contains(computationId)) {
            continue;
        }
        inputStreamIds[computationId] = computationSpec->InputStreamIds;
        outputStreamIds[computationId] = computationSpec->OutputStreamIds;
    }

    for (const auto& [partitionId, partitionJobStatusPtr] : flowView->Feedback->PartitionJobStatuses) {
        if (!partitionJobStatusPtr || !partitionJobStatusPtr->CurrentJobStatus) {
            continue;
        }
        const auto* partition = layout->Partitions.FindPtr(partitionId);
        if (!partition) {
            // Partition is absent from the layout (e.g. removed); cannot route its limits.
            continue;
        }
        const auto& computationId = (*partition)->ComputationId;

        auto computationIt = pipeline.Computations.find(computationId);
        if (computationIt == pipeline.Computations.end()) {
            continue;
        }
        auto& computation = computationIt->second;

        const auto& jobStatus = *partitionJobStatusPtr->CurrentJobStatus;
        AggregateLimits(jobStatus.InputLimits, partitionId, inputStreamIds[computationId], computation.InputLimitStats);
        AggregateLimits(jobStatus.OutputLimits, partitionId, outputStreamIds[computationId], computation.OutputLimitStats);
    }
}

////////////////////////////////////////////////////////////////////////////////

void FillReadDelayEdges(const TFlowViewPtr& flowView, TPipelineDescription& pipeline)
{
    const auto& pipelineSpec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();

    // Per-computation read delays (for display) and one TReadDelayEdge per
    // unique (blocker_stream, delayed_stream) pair (for the graph).
    std::set<std::pair<TGraphEntityId, TGraphEntityId>> seenReadDelayEdges;
    for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
        if (!computationSpec->WatermarkStrategy ||
            !computationSpec->WatermarkStrategy->WatermarkAlignment)
        {
            continue;
        }
        const auto& readDelays = computationSpec->WatermarkStrategy->WatermarkAlignment->ReadDelays;
        if (!readDelays || readDelays->empty()) {
            continue;
        }

        // Per-computation read delays (blocker globalStreamId -> delay), for display.
        if (auto* computation = pipeline.Computations.FindPtr(computationId)) {
            for (const auto& [blockerStreamId, delay] : *readDelays) {
                computation->ReadDelays[blockerStreamId] = delay;
            }
        }

        for (const auto& [localDelayedStreamId, sourceSpec] : computationSpec->SourceStreams) {
            Y_UNUSED(sourceSpec);
            // localDelayedStreamId → globalStreamId (via MakeGlobalStreamId) → graphEntityId.
            auto delayedEntityId = MakeStreamGraphId(MakeGlobalStreamId(computationId, localDelayedStreamId, computationSpec));

            for (const auto& [blockerStreamId, delay] : *readDelays) {
                // blockerStreamId in ReadDelays is already a globalStreamId (see spec.cpp).
                auto blockerEntityId = MakeStreamGraphId(blockerStreamId);

                if (!seenReadDelayEdges.insert({blockerEntityId, delayedEntityId}).second) {
                    continue; // Skip duplicate (blocker, delayed) pairs.
                }

                auto& edge = pipeline.ReadDelayEdges.emplace_back();
                edge.BlockerStreamGraphEntityId = blockerEntityId;
                edge.DelayedStreamGraphEntityId = delayedEntityId;
                edge.Delay = delay;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

double GetMaxFillRateForStream(
    const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
    const TGraphEntityId& streamId)
{
    auto it = limitStats.find(streamId);
    if (it == limitStats.end()) {
        return 0.0;
    }
    double maxRate = 0.0;
    for (const auto& [limitType, stats] : it->second) {
        maxRate = std::max(maxRate, stats.GetMaxFillRate());
    }
    return maxRate;
}

std::string MakeEdgeLabel(
    const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
    const TGraphEntityId& streamId,
    bool boldOverThreshold)
{
    auto it = limitStats.find(streamId);
    if (it == limitStats.end()) {
        return {};
    }

    std::vector<std::pair<double, std::string>> entries;
    for (const auto& [limitType, stats] : it->second) {
        double fillRate = stats.GetMaxFillRate();
        if (fillRate >= MinVisibleFillRate) {
            entries.emplace_back(fillRate, limitType);
        }
    }
    if (entries.empty()) {
        return {};
    }

    std::sort(entries.begin(), entries.end(), [] (const auto& a, const auto& b) {
        return a.first > b.first;
    });

    TStringBuilder sb;
    bool first = true;
    for (const auto& [fillRate, limitType] : entries) {
        if (!first) {
            sb.AppendChar(' ');
        }
        if (boldOverThreshold && fillRate >= WarningFillRateThreshold) {
            sb.AppendFormat("%v=**%.3f**", limitType, fillRate);
        } else {
            sb.AppendFormat("%v=%.3f", limitType, fillRate);
        }
        first = false;
    }
    return sb.Flush();
}

////////////////////////////////////////////////////////////////////////////////

void FillExtendedStreams(const TFlowViewPtr& flowView, TPipelineDescription& pipeline)
{
    const auto& pipelineSpec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();

    // Map graphEntityId -> stream state, recorded on the producing (output) side.
    THashMap<TGraphEntityId, EStreamState> streamStates;
    for (const auto& [computationId, nodeTraverseData] : flowView->State->TraverseData->Computations) {
        auto computationSpecIt = pipelineSpec->Computations.find(computationId);
        if (computationSpecIt == pipelineSpec->Computations.end()) {
            continue;
        }
        const auto& computationSpec = computationSpecIt->second;
        for (const auto& [localStreamId, streamTraverseData] : nodeTraverseData->Streams) {
            // Input streams carry the producing computation's state, so record output streams only.
            if (computationSpec->InputStreamIds.contains(localStreamId)) {
                continue;
            }
            auto graphId = MakeStreamGraphId(MakeGlobalStreamId(computationId, localStreamId, computationSpec));
            streamStates[graphId] = streamTraverseData->State;
        }
    }

    auto makeEdge = [&] (
        const TGraphEntityId& streamId,
        const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats) -> TStreamEdge {
        TStreamEdge edge;
        edge.StreamGraphEntityId = streamId;
        if (auto it = streamStates.find(streamId); it != streamStates.end()) {
            edge.Completed = it->second == EStreamState::Completed;
            edge.Drained = it->second == EStreamState::Drained;
        }
        edge.BackpressureDetected = GetMaxFillRateForStream(limitStats, streamId) >= WarningFillRateThreshold;

        auto label = MakeEdgeLabel(limitStats, streamId);
        if (!label.empty()) {
            auto& message = edge.Messages.emplace_back();
            message.Text = "fill ratio: " + label;
            // Over-threshold fill ratios are bolded.
            message.MarkdownText = "fill ratio: " + MakeEdgeLabel(limitStats, streamId, /*boldOverThreshold*/ true);
            message.Level = edge.BackpressureDetected ? ELogLevel::Warning : ELogLevel::Info;
        }
        return edge;
    };

    auto sortedStreams = [] (const THashSet<TGraphEntityId>& streams) {
        std::vector<TGraphEntityId> result(streams.begin(), streams.end());
        std::sort(result.begin(), result.end());
        return result;
    };

    for (auto& [computationId, computation] : pipeline.Computations) {
        for (const auto& streamId : sortedStreams(computation.InputStreams)) {
            computation.ExtendedInputStreams.push_back(makeEdge(streamId, computation.InputLimitStats));
        }
        for (const auto& streamId : sortedStreams(computation.SourceStreams)) {
            computation.ExtendedSourceStreams.push_back(makeEdge(streamId, computation.InputLimitStats));
        }
        for (const auto& streamId : sortedStreams(computation.OutputStreams)) {
            computation.ExtendedOutputStreams.push_back(makeEdge(streamId, computation.OutputLimitStats));
        }
        for (const auto& streamId : sortedStreams(computation.TimerStreams)) {
            computation.ExtendedTimerStreams.push_back(makeEdge(streamId, computation.InputLimitStats));
        }

        // For now, fold all per-edge messages into one concatenated computation message.
        TStringBuilder markdown;
        auto maxLevel = ELogLevel::Info;
        bool hasMessages = false;
        auto appendEdges = [&] (TStringBuf kind, const std::vector<TStreamEdge>& edges) {
            for (const auto& edge : edges) {
                for (const auto& message : edge.Messages) {
                    hasMessages = true;
                    maxLevel = std::max(maxLevel, message.Level);
                    markdown.AppendFormat("- %v `%v`: %v\n", kind, edge.StreamGraphEntityId, message.MarkdownText.value_or(message.Text));
                }
            }
        };
        appendEdges("input", computation.ExtendedInputStreams);
        appendEdges("source", computation.ExtendedSourceStreams);
        appendEdges("output", computation.ExtendedOutputStreams);
        appendEdges("timer", computation.ExtendedTimerStreams);

        if (hasMessages) {
            auto& message = computation.Messages.emplace_back();
            message.Text = "Stream buffer fill ratios";
            message.MarkdownText = markdown.Flush();
            message.Level = maxLevel;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
