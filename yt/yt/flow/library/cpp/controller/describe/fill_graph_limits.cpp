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
    if (status.BlockedTimeShare > stats.MaxBlockedTimeShare) {
        stats.MaxBlockedTimeShare = status.BlockedTimeShare;
        stats.MaxBlockedPartitionId = partitionId;
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
    const THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>>& intermediateDescriptions,
    TPipelineDescription& pipeline)
{
    const auto& pipelineSpec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();

    // Partitions of DIFFERENT computations may consume the same shared global input stream, so
    // limits MUST be aggregated per (computation, stream) independently. The intermediate
    // descriptions already route each partition to its own computation, and we restrict the
    // aggregation to the stream ids the spec declares for that computation.
    for (const auto& [computationId, partitionDescriptions] : intermediateDescriptions) {
        auto computationIt = pipeline.Computations.find(computationId);
        auto specIt = pipelineSpec->Computations.find(computationId);
        if (computationIt == pipeline.Computations.end() || specIt == pipelineSpec->Computations.end()) {
            continue;
        }
        auto& computation = computationIt->second;
        const auto& computationSpec = specIt->second;

        for (const auto& partitionDescription : partitionDescriptions) {
            if (!partitionDescription.Partition ||
                !partitionDescription.PartitionJobStatus ||
                !partitionDescription.PartitionJobStatus->CurrentJobStatus)
            {
                continue;
            }
            const auto& partitionId = partitionDescription.Partition->PartitionId;
            const auto& jobStatus = *partitionDescription.PartitionJobStatus->CurrentJobStatus;
            AggregateLimits(jobStatus.InputLimits, partitionId, computationSpec->InputStreamIds, computation.InputLimitStats);
            AggregateLimits(jobStatus.OutputLimits, partitionId, computationSpec->OutputStreamIds, computation.OutputLimitStats);
        }
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

THashMap<TGraphEntityId, TWriterBlockedShare> ComputeWriterBlockedTimeShares(const TPipelineDescription& pipeline)
{
    THashMap<TGraphEntityId, TWriterBlockedShare> shares;
    for (const auto& [computationId, computation] : pipeline.Computations) {
        for (const auto& [streamId, limitStats] : computation.OutputLimitStats) {
            for (const auto& [limitType, stats] : limitStats) {
                if (limitType == ControllerLimitType) {
                    continue;
                }
                if (stats.MaxBlockedTimeShare <= 0) {
                    continue;
                }
                auto& share = shares[streamId];
                if (stats.MaxBlockedTimeShare > share.Share) {
                    share = {stats.MaxBlockedTimeShare, stats.MaxBlockedPartitionId};
                }
            }
        }
    }
    return shares;
}

namespace {

std::string FormatBytesShort(i64 bytes)
{
    static constexpr std::array<TStringBuf, 5> Units{"B", "KB", "MB", "GB", "TB"};
    double value = static_cast<double>(bytes);
    size_t unit = 0;
    while (std::abs(value) >= 1024 && unit + 1 < Units.size()) {
        value /= 1024;
        ++unit;
    }
    if (unit > 0 && std::abs(value) < 10) {
        return Format("%.1f%v", value, Units[unit]);
    }
    return Format("%.0f%v", value, Units[unit]);
}

std::string FormatCountShort(i64 count)
{
    static constexpr std::array<TStringBuf, 4> Units{"", "K", "M", "G"};
    double value = static_cast<double>(count);
    size_t unit = 0;
    while (std::abs(value) >= 1000 && unit + 1 < Units.size()) {
        value /= 1000;
        ++unit;
    }
    if (unit > 0 && std::abs(value) < 10) {
        return Format("%.1f%v", value, Units[unit]);
    }
    return Format("%.0f%v", value, Units[unit]);
}

struct TRenderedKind
{
    double Severity = 0;
    bool Blocks = false;
    std::string Markdown;
};

//! Renders one buffer entry. A blocking entry names its own worst partition,
//! since different limits may point at different partitions.
std::optional<TRenderedKind> RenderKindStats(const std::string& kind, const TStreamLimitStats& stats, bool boldOverThreshold)
{
    double fill = stats.GetMaxFillRate();
    double blocked = stats.MaxBlockedTimeShare;
    if (fill < MinVisibleFillRate && blocked < MinVisibleBlockedTimeShare) {
        return std::nullopt;
    }
    bool fillBlocks = fill >= WarningFillRateThreshold;
    bool blockedBlocks = blocked >= WarningBlockedTimeShareThreshold;

    TRenderedKind result;
    result.Blocks = fillBlocks || blockedBlocks;
    double fillScore = fill / WarningFillRateThreshold;
    double blockedScore = blocked / WarningBlockedTimeShareThreshold;
    result.Severity = std::max(fillScore, blockedScore);

    // Count limits (e.g. output_store_count) are counts, not bytes.
    bool isCount = kind.ends_with("_count");
    auto formatValue = [&] (i64 value) {
        return isCount ? FormatCountShort(value) : FormatBytesShort(value);
    };

    bool isController = kind == ControllerLimitType;

    TStringBuilder sb;
    sb.AppendFormat("%v{", kind);
    if (blocked >= MinVisibleBlockedTimeShare) {
        if (boldOverThreshold && blockedBlocks) {
            sb.AppendFormat("blocked_share=**%.2f**", blocked);
        } else {
            sb.AppendFormat("blocked_share=%.2f", blocked);
        }
        if (!isController) {
            sb.AppendString(", ");
        }
    }
    if (isController) {
        if (result.Blocks) {
            sb.AppendFormat(", backpressured_partition=%v", stats.MaxBlockedPartitionId);
        }
        sb.AppendString("}");
        result.Markdown = sb.Flush();
        return result;
    }
    sb.AppendFormat("limit=%v, ", formatValue(stats.Max.Limit));
    // Percentages of a zero limit are meaningless: fall back to absolute values.
    auto ofLimit = [&] (i64 value) -> std::string {
        if (stats.Max.Limit > 0) {
            return Format("%.0f%%", 100.0 * static_cast<double>(value) / static_cast<double>(stats.Max.Limit));
        }
        return formatValue(value);
    };
    if (boldOverThreshold && fillBlocks) {
        sb.AppendFormat("used=**%v**", ofLimit(stats.Max.Used));
    } else {
        sb.AppendFormat("used=%v", ofLimit(stats.Max.Used));
    }
    if (stats.Max.Pending) {
        sb.AppendFormat(", pending=%v", ofLimit(*stats.Max.Pending));
    }
    if (result.Blocks) {
        sb.AppendFormat(", backpressured_partition=%v",
            blockedScore >= fillScore ? stats.MaxBlockedPartitionId : stats.MaxPartitionId);
    }
    sb.AppendString("}");
    result.Markdown = sb.Flush();
    return result;
}

struct TRenderedStream
{
    std::string Markdown;
    bool Blocking = false;
};

std::optional<TRenderedStream> RenderStreamBuffers(
    TStringBuf side,
    const TGraphEntityId& streamId,
    const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
    const TWriterBlockedShare& writerBlocked)
{
    std::vector<TRenderedKind> entries;
    if (const auto* kinds = limitStats.FindPtr(streamId)) {
        for (const auto& [kind, stats] : *kinds) {
            if (auto rendered = RenderKindStats(kind, stats, /*boldOverThreshold*/ true)) {
                entries.push_back(std::move(*rendered));
            }
        }
    }

    bool writerVisible = writerBlocked.Share >= MinVisibleBlockedTimeShare;
    bool writerBlocks = writerBlocked.Share >= WarningBlockedTimeShareThreshold;
    if (entries.empty() && !writerVisible) {
        return std::nullopt;
    }

    std::sort(entries.begin(), entries.end(), [] (const auto& a, const auto& b) {
        return std::tie(b.Severity, b.Markdown) < std::tie(a.Severity, a.Markdown);
    });

    TRenderedStream result;
    TStringBuilder line;
    line.AppendFormat("- %v `%v`:", side, streamId);
    bool first = true;
    for (const auto& entry : entries) {
        line.AppendString(first ? " " : ", ");
        line.AppendString(entry.Markdown);
        first = false;
        result.Blocking |= entry.Blocks;
    }
    if (writerVisible) {
        // The share of time the stream's WRITER spends blocked on it: a full
        // reader-side buffer never blocks its own job, it stalls the writer.
        line.AppendString(first ? " writer{" : ", writer{");
        if (writerBlocks) {
            line.AppendFormat("blocked_share=**%.2f**, backpressured_partition=%v", writerBlocked.Share, writerBlocked.ExamplePartitionId);
            result.Blocking = true;
        } else {
            line.AppendFormat("blocked_share=%.2f", writerBlocked.Share);
        }
        line.AppendString("}");
    }
    result.Markdown = line.Flush();
    return result;
}

} // namespace

std::optional<TMessage> BuildBuffersAndBackpressureMessage(
    const TPipelineComputationDescription& computation,
    const THashMap<TGraphEntityId, TWriterBlockedShare>& writerBlockedShares)
{
    TStringBuilder markdown;
    bool any = false;
    bool anyBlocking = false;

    auto sortedStreams = [] (const THashSet<TGraphEntityId>& streams) {
        std::vector<TGraphEntityId> result(streams.begin(), streams.end());
        std::sort(result.begin(), result.end());
        return result;
    };

    auto renderGroup = [&] (
        TStringBuf side,
        const THashSet<TGraphEntityId>& streams,
        const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
        bool isReaderSide) {
        for (const auto& streamId : sortedStreams(streams)) {
            auto writerBlocked = isReaderSide
                ? GetOrDefault(writerBlockedShares, streamId, TWriterBlockedShare{})
                : TWriterBlockedShare{};
            if (auto rendered = RenderStreamBuffers(side, streamId, limitStats, writerBlocked)) {
                any = true;
                anyBlocking |= rendered->Blocking;
                markdown.AppendString(rendered->Markdown);
                markdown.AppendChar('\n');
            }
        }
    };
    renderGroup("input", computation.InputStreams, computation.InputLimitStats, /*isReaderSide*/ true);
    renderGroup("source", computation.SourceStreams, computation.InputLimitStats, /*isReaderSide*/ true);
    renderGroup("timer", computation.TimerStreams, computation.InputLimitStats, /*isReaderSide*/ true);
    renderGroup("output", computation.OutputStreams, computation.OutputLimitStats, /*isReaderSide*/ false);

    if (!any) {
        return std::nullopt;
    }
    TMessage message;
    message.Text = "Stream buffers and backpressure";
    message.MarkdownText = markdown.Flush();
    message.Level = anyBlocking ? ELogLevel::Warning : ELogLevel::Info;
    return message;
}

double GetMaxBlockedTimeShareForStream(
    const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
    const TGraphEntityId& streamId)
{
    auto it = limitStats.find(streamId);
    if (it == limitStats.end()) {
        return 0.0;
    }
    double maxShare = 0.0;
    for (const auto& [limitType, stats] : it->second) {
        maxShare = std::max(maxShare, stats.MaxBlockedTimeShare);
    }
    return maxShare;
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

    std::vector<TRenderedKind> entries;
    for (const auto& [kind, stats] : it->second) {
        if (auto rendered = RenderKindStats(kind, stats, boldOverThreshold)) {
            entries.push_back(std::move(*rendered));
        }
    }
    if (entries.empty()) {
        return {};
    }
    std::sort(entries.begin(), entries.end(), [] (const auto& a, const auto& b) {
        return std::tie(b.Severity, b.Markdown) < std::tie(a.Severity, a.Markdown);
    });

    TStringBuilder sb;
    bool first = true;
    for (const auto& entry : entries) {
        if (!first) {
            sb.AppendString(", ");
        }
        sb.AppendString(entry.Markdown);
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

    auto writerBlockedShares = ComputeWriterBlockedTimeShares(pipeline);

    auto makeEdge = [&] (
        const TGraphEntityId& streamId,
        const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
        bool isReaderSide) -> TStreamEdge {
        TStreamEdge edge;
        edge.StreamGraphEntityId = streamId;
        if (auto it = streamStates.find(streamId); it != streamStates.end()) {
            edge.Completed = it->second == EStreamState::Completed;
            edge.Drained = it->second == EStreamState::Drained;
        }
        double writerBlocked = isReaderSide ? GetOrDefault(writerBlockedShares, streamId, TWriterBlockedShare{}).Share : 0.0;
        edge.BackpressureDetected =
            GetMaxFillRateForStream(limitStats, streamId) >= WarningFillRateThreshold ||
            GetMaxBlockedTimeShareForStream(limitStats, streamId) >= WarningBlockedTimeShareThreshold ||
            writerBlocked >= WarningBlockedTimeShareThreshold;

        auto appendWriterBlocked = [&] (std::string label, bool bold) -> std::string {
            if (writerBlocked < MinVisibleBlockedTimeShare) {
                return label;
            }
            TStringBuilder sb;
            sb.AppendString(label);
            if (!label.empty()) {
                sb.AppendString(", ");
            }
            if (bold && writerBlocked >= WarningBlockedTimeShareThreshold) {
                sb.AppendFormat("writer{blocked_share=**%.2f**}", writerBlocked);
            } else {
                sb.AppendFormat("writer{blocked_share=%.2f}", writerBlocked);
            }
            return sb.Flush();
        };

        auto label = appendWriterBlocked(MakeEdgeLabel(limitStats, streamId), /*bold*/ false);
        if (!label.empty()) {
            auto& message = edge.Messages.emplace_back();
            message.Text = "buffers: " + label;
            message.MarkdownText = "buffers: " +
                appendWriterBlocked(MakeEdgeLabel(limitStats, streamId, /*boldOverThreshold*/ true), /*bold*/ true);
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
            computation.ExtendedInputStreams.push_back(makeEdge(streamId, computation.InputLimitStats, /*isReaderSide*/ true));
        }
        for (const auto& streamId : sortedStreams(computation.SourceStreams)) {
            computation.ExtendedSourceStreams.push_back(makeEdge(streamId, computation.InputLimitStats, /*isReaderSide*/ true));
        }
        for (const auto& streamId : sortedStreams(computation.OutputStreams)) {
            computation.ExtendedOutputStreams.push_back(makeEdge(streamId, computation.OutputLimitStats, /*isReaderSide*/ false));
        }
        for (const auto& streamId : sortedStreams(computation.TimerStreams)) {
            computation.ExtendedTimerStreams.push_back(makeEdge(streamId, computation.InputLimitStats, /*isReaderSide*/ true));
        }

        if (auto message = BuildBuffersAndBackpressureMessage(computation, writerBlockedShares)) {
            computation.Messages.push_back(std::move(*message));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
