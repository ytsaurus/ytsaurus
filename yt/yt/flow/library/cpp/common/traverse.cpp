#include "traverse.h"

#include "message.h"
#include "spec.h"

#include <library/cpp/iterator/concatenate.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TInflightMetrics::Register(TRegistrar registrar)
{
    registrar.Parameter("count", &TThis::Count)
        .Default(0);
    registrar.Parameter("byte_size", &TThis::ByteSize)
        .Default();
    registrar.Parameter("last_idle_timestamp", &TThis::LastIdleTimestamp)
        .Default();
    registrar.Parameter("idle_duration", &TThis::IdleDuration)
        .Default();
    registrar.Parameter("unavailable_instant", &TThis::UnavailableTimestamp)
        .Default();

    registrar.Parameter("new_count_per_sec", &TThis::NewCountPerSec)
        .Default();
    registrar.Parameter("new_bytes_per_sec", &TThis::NewBytesPerSec)
        .Default();
    registrar.Parameter("processed_count_per_sec", &TThis::ProcessedCountPerSec)
        .Default();
    registrar.Parameter("processed_bytes_per_sec", &TThis::ProcessedBytesPerSec)
        .Default();
}

void InPlaceMergeInflightMetrics(
    const TInflightMetricsPtr& current,
    const TInflightMetricsPtr& other,
    EInflightMerge inflightMerge,
    bool allowPartial)
{
    if (inflightMerge == EInflightMerge::None) {
        current->Count = 0;
        current->ByteSize = {};
        current->IdleDuration = {};
        current->NewCountPerSec = {};
        current->ProcessedCountPerSec = {};
        current->NewBytesPerSec = {};
        current->ProcessedBytesPerSec = {};
        current->UnavailableTimestamp = {};
        return;
    }

    if (inflightMerge == EInflightMerge::Sum) {
        current->Count += other->Count;
        auto update = [&] (auto& current, auto& other) {
            if (current && other) {
                current = *current + *other;
            } else if (!allowPartial) {
                current = {};
            } else if (other) {
                current = other;
            }
        };
        update(current->ByteSize, other->ByteSize);
        update(current->NewCountPerSec, other->NewCountPerSec);
        update(current->ProcessedCountPerSec, other->ProcessedCountPerSec);
        update(current->NewBytesPerSec, other->NewBytesPerSec);
        update(current->ProcessedBytesPerSec, other->ProcessedBytesPerSec);
    } else if (inflightMerge == EInflightMerge::Max) {
        current->Count = std::max(current->Count, other->Count);
        auto update = [&] (auto& current, auto& other) {
            if (current && other) {
                current = std::max(*current, *other);
            } else if (!allowPartial) {
                current = {};
            } else if (other) {
                current = other;
            }
        };
        update(current->ByteSize, other->ByteSize);
        update(current->NewCountPerSec, other->NewCountPerSec);
        update(current->ProcessedCountPerSec, other->ProcessedCountPerSec);
        update(current->NewBytesPerSec, other->NewBytesPerSec);
        update(current->ProcessedBytesPerSec, other->ProcessedBytesPerSec);
    }

    if (current->Count == 0) {
        if (current->IdleDuration && other->IdleDuration) {
            current->IdleDuration = std::min(*current->IdleDuration, *other->IdleDuration);
        } else if (!allowPartial) {
            current->IdleDuration = {};
        } else if (other->IdleDuration) {
            current->IdleDuration = other->IdleDuration;
        }

        if (current->LastIdleTimestamp && other->LastIdleTimestamp) {
            current->LastIdleTimestamp = std::min(*current->LastIdleTimestamp, *other->LastIdleTimestamp);
        } else if (!allowPartial) {
            current->LastIdleTimestamp = {};
        } else if (other->LastIdleTimestamp) {
            current->LastIdleTimestamp = other->LastIdleTimestamp;
        }
    } else {
        current->IdleDuration = {};
        current->LastIdleTimestamp = {};
    }

    if (current->UnavailableTimestamp && other->UnavailableTimestamp) {
        current->UnavailableTimestamp = std::min(*current->UnavailableTimestamp, *other->UnavailableTimestamp);
    } else if (!allowPartial) {
        current->UnavailableTimestamp = {};
    } else if (other->UnavailableTimestamp) {
        current->UnavailableTimestamp = other->UnavailableTimestamp;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TStreamTraverseData::Register(TRegistrar registrar)
{
    registrar.Parameter("epoch", &TThis::Epoch)
        .Default(-1);
    registrar.Parameter("state", &TThis::State)
        .Default(EStreamState::Active);
    registrar.Parameter("system_watermark", &TThis::SystemWatermark)
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("event_watermark", &TThis::EventWatermark)
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("inflight_metrics", &TThis::InflightMetrics)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TStreamTraverseDataPtr MakeCompletedStreamTraverseData(
    i64 epoch,
    TSystemTimestamp timestamp)
{
    auto streamTraverseData = New<TStreamTraverseData>();
    streamTraverseData->Epoch = epoch;
    streamTraverseData->State = EStreamState::Completed;
    streamTraverseData->SystemWatermark = timestamp;
    streamTraverseData->EventWatermark = timestamp;
    streamTraverseData->InflightMetrics->Count = 0;
    streamTraverseData->InflightMetrics->ByteSize = 0;
    streamTraverseData->InflightMetrics->NewCountPerSec = 0;
    streamTraverseData->InflightMetrics->ProcessedCountPerSec = 0;
    streamTraverseData->InflightMetrics->NewBytesPerSec = 0;
    streamTraverseData->InflightMetrics->ProcessedBytesPerSec = 0;
    return streamTraverseData;
}

TStreamTraverseDataPtr MergeStreamTraverseData(
    const std::vector<TStreamTraverseDataPtr>& streams,
    EInflightMerge inflightMerge,
    bool allowPartial)
{
    if (streams.empty()) {
        return New<TStreamTraverseData>();
    }

    auto mergedStream = NYTree::CloneYsonStruct(streams[0]);

    if (inflightMerge == EInflightMerge::None) {
        mergedStream->InflightMetrics = New<TInflightMetrics>();
    }

    for (i64 i = 1; i < std::ssize(streams); ++i) {
        const auto& stream = streams[i];
        mergedStream->Epoch = std::min(mergedStream->Epoch, stream->Epoch);
        mergedStream->SystemWatermark = std::min(mergedStream->SystemWatermark, stream->SystemWatermark);
        mergedStream->EventWatermark = std::min(mergedStream->EventWatermark, stream->EventWatermark);
        mergedStream->State = std::min(mergedStream->State, stream->State);

        InPlaceMergeInflightMetrics(mergedStream->InflightMetrics, stream->InflightMetrics, inflightMerge, allowPartial);
    }

    return mergedStream;
}

TStreamTraverseDataPtr AdvanceStreamTraverseData(
    const TStreamTraverseDataPtr& currentStream,
    const TStreamTraverseDataPtr& newStream)
{
    THROW_ERROR_EXCEPTION_UNLESS(newStream, "New stream is empty");
    if (!currentStream) {
        return newStream;
    }

    if (newStream->Epoch < 0) {
        // Negative epoch means that information is partial.
        return currentStream;
    }

    if (newStream->Epoch < currentStream->Epoch) {
        THROW_ERROR_EXCEPTION("New stream epoch %v is less than current stream epoch %v",
            newStream->Epoch,
            currentStream->Epoch);
    }

    auto advanced = NYTree::CloneYsonStruct(newStream);
    advanced->SystemWatermark = std::max(advanced->SystemWatermark, currentStream->SystemWatermark);
    advanced->EventWatermark = std::max(advanced->EventWatermark, currentStream->EventWatermark);
    // Other fields just taken from new stream.
    return advanced;
}

////////////////////////////////////////////////////////////////////////////////

void TInflightStreamTraverseData::Register(TRegistrar registrar)
{
    registrar.Parameter("suspended", &TThis::Suspended)
        .Default(false);
    registrar.Parameter("empty", &TThis::Empty)
        .Default(false);
    registrar.Parameter("min_system_timestamp", &TThis::MinSystemTimestamp)
        .Default();
    registrar.Parameter("min_event_timestamp", &TThis::MinEventTimestamp)
        .Default();
    registrar.Parameter("inflight_metrics", &TThis::InflightMetrics)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TStreamTraverseDataPtr ApplyInflightTraverseData(
    const TStreamTraverseDataPtr& stream,
    const TInflightStreamTraverseDataPtr& inflight,
    TSystemTimestamp systemWatermark)
{
    auto consumingStream = NYTree::CloneYsonStruct(stream);
    bool completed = consumingStream->State == EStreamState::Completed && inflight->Empty;
    bool suspended = inflight->Suspended || (inflight->Empty && consumingStream->State >= EStreamState::Drained);
    if (completed) {
        consumingStream->State = EStreamState::Completed;
    } else if (suspended) {
        consumingStream->State = EStreamState::Drained;
    } else {
        consumingStream->State = EStreamState::Active;
    }
    consumingStream->SystemWatermark = systemWatermark;

    if (inflight->MinSystemTimestamp) {
        consumingStream->SystemWatermark = std::min(consumingStream->SystemWatermark, *inflight->MinSystemTimestamp);
    }
    if (inflight->MinEventTimestamp) {
        consumingStream->EventWatermark = std::min(consumingStream->EventWatermark, *inflight->MinEventTimestamp);
    }

    InPlaceMergeInflightMetrics(consumingStream->InflightMetrics, inflight->InflightMetrics, EInflightMerge::Sum, /*allowPartial*/ true);
    return consumingStream;
}

TInflightStreamTraverseDataPtr MergeInflightTraverseData(
    const std::vector<TInflightStreamTraverseDataPtr>& inflights)
{
    if (inflights.empty()) {
        return New<TInflightStreamTraverseData>();
    }

    auto merged = NYTree::CloneYsonStruct(inflights[0]);

    for (i64 i = 1; i < std::ssize(inflights); ++i) {
        const auto& part = inflights[i];
        if (part->MinSystemTimestamp) {
            if (merged->MinSystemTimestamp) {
                merged->MinSystemTimestamp = std::min(*merged->MinSystemTimestamp, *part->MinSystemTimestamp);
            } else {
                merged->MinSystemTimestamp = part->MinSystemTimestamp;
            }
        }

        if (part->MinEventTimestamp) {
            if (merged->MinEventTimestamp) {
                merged->MinEventTimestamp = std::min(*merged->MinEventTimestamp, *part->MinEventTimestamp);
            } else {
                merged->MinEventTimestamp = part->MinEventTimestamp;
            }
        }

        InPlaceMergeInflightMetrics(merged->InflightMetrics, part->InflightMetrics, EInflightMerge::Sum);
    }

    return merged;
}

////////////////////////////////////////////////////////////////////////////////

void TNodeTraverseData::Register(TRegistrar registrar)
{
    registrar.Parameter("report_time", &TThis::ReportTime)
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("streams", &TThis::Streams)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TNodeTraverseDataPtr MergeNodeTraverseData(const std::vector<TNodeTraverseDataPtr>& nodes, const TComputationSpecPtr& spec)
{
    if (nodes.empty()) {
        return New<TNodeTraverseData>();
    }

    for (const auto& node : nodes) {
        if (node->Streams.size() != nodes[0]->Streams.size()) {
            THROW_ERROR_EXCEPTION("Different streams sizes")
                << TErrorAttribute("got", node->Streams.size())
                << TErrorAttribute("expected", nodes[0]->Streams.size());
        }
    }

    auto result = New<TNodeTraverseData>();
    result->ReportTime = nodes[0]->ReportTime;
    for (const auto& node : nodes) {
        result->ReportTime = std::min(result->ReportTime, node->ReportTime);
    }

    for (const auto& streamId : GetKeys(nodes[0]->Streams)) {
        std::vector<TStreamTraverseDataPtr> streamsTraverse;
        for (const auto& node : nodes) {
            streamsTraverse.push_back(GetOrCrash(node->Streams, streamId));
        }
        auto mergeType = EInflightMerge::Sum;
        if (spec->InputStreamIds.contains(streamId)) {
            mergeType = EInflightMerge::None;
        }
        result->Streams[streamId] = MergeStreamTraverseData(streamsTraverse, mergeType);
    }
    return result;
}

TNodeTraverseDataPtr AdvanceNodeTraverseData(
    const TNodeTraverseDataPtr& currentNode,
    const TNodeTraverseDataPtr& newNode)
{
    THROW_ERROR_EXCEPTION_UNLESS(newNode, "New node is empty");
    if (!currentNode) {
        return newNode;
    }
    if (currentNode->ReportTime > newNode->ReportTime) {
        THROW_ERROR_EXCEPTION("ReportTime monotony is broken")
            << TErrorAttribute("current", currentNode->ReportTime)
            << TErrorAttribute("new", newNode->ReportTime);
    }

    auto result = NYTree::CloneYsonStruct(newNode);
    for (auto& [streamId, traverseData] : result->Streams) {
        traverseData = AdvanceStreamTraverseData(GetOrDefault(currentNode->Streams, streamId, nullptr), traverseData);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TFromPartitionTraverseData::Register(TRegistrar registrar)
{
    registrar.Parameter("node", &TThis::Node)
        .DefaultNew();
}

TFromPartitionTraverseDataPtr MakeCompletedPartitionTraverseData(
    i64 epoch,
    TSystemTimestamp timestamp,
    const TExtendedComputationSpecPtr& spec)
{
    auto traverseData = New<TFromPartitionTraverseData>();
    traverseData->Node = New<TNodeTraverseData>();
    traverseData->Node->ReportTime = timestamp;

    for (const auto& streamId : spec->AllStreamIds) {
        traverseData->Node->Streams[streamId] = MakeCompletedStreamTraverseData(epoch, timestamp);
    }

    return traverseData;
}

////////////////////////////////////////////////////////////////////////////////

void TToPartitionTraverseData::Register(TRegistrar registrar)
{
    registrar.Parameter("input_streams", &TThis::InputStreams)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void ValidateComputationInflights(
    const THashMap<TStreamId, TInflightStreamTraverseDataPtr>& inflights,
    const TComputationSpecPtr& spec)
{
    THashSet<TStreamId> allStreamIds;
    for (const auto& streamId : spec->OutputStreamIds) {
        allStreamIds.insert(streamId);
        if (!inflights.contains(streamId)) {
            THROW_ERROR_EXCEPTION("No inflight data for output stream %Qv",
                streamId);
        }
    }

    for (const auto& streamId : GetKeys(spec->TimerStreams)) {
        allStreamIds.insert(streamId);
        if (!inflights.contains(streamId)) {
            THROW_ERROR_EXCEPTION("No inflight data for timer stream %Qv",
                streamId);
        }
    }

    for (const auto& streamId : GetKeys(spec->KeyVisitorStreams)) {
        allStreamIds.insert(streamId);
        if (!inflights.contains(streamId)) {
            THROW_ERROR_EXCEPTION("No inflight data for key-visitor stream %Qv",
                streamId);
        }
    }

    for (const auto& [streamId, source] : spec->SourceStreams) {
        allStreamIds.insert(streamId);
        if (!inflights.contains(streamId)) {
            THROW_ERROR_EXCEPTION("No inflight data for source stream %Qv",
                streamId);
        }
    }

    for (const auto& streamId : GetKeys(inflights)) {
        if (spec->InputStreamIds.contains(streamId)) {
            THROW_ERROR_EXCEPTION("Inflight data for input streams is not supported, but found for %Qv",
                streamId);
        }
        if (!allStreamIds.contains(streamId)) {
            THROW_ERROR_EXCEPTION("Inflight for unknown stream %Qv",
                streamId);
        }
    }
}

void ValidateFromPartitionTraverseData(
    const TFromPartitionTraverseDataPtr& traverseData,
    const TComputationSpecPtr& /*spec*/,
    const TExtendedComputationSpecPtr& extendedSpec)
{
    for (const auto& streamId : extendedSpec->AllStreamIds) {
        if (!traverseData->Node->Streams.contains(streamId)) {
            THROW_ERROR_EXCEPTION("Node traverse data does not contain stream %Qv",
                streamId);
        }
    }
    for (const auto& streamId : GetKeys(traverseData->Node->Streams)) {
        if (!extendedSpec->AllStreamIds.contains(streamId)) {
            THROW_ERROR_EXCEPTION("Node traverse data contains unknown stream %Qv",
                streamId);
        }
    }
}

void ValidateToPartitionTraverseData(
    const TToPartitionTraverseDataPtr& traverseData,
    const TComputationSpecPtr& spec,
    const TExtendedComputationSpecPtr& /*extendedSpec*/)
{
    for (const auto& streamId : spec->InputStreamIds) {
        if (!traverseData->InputStreams.contains(streamId)) {
            THROW_ERROR_EXCEPTION("Node traverse data does not contain stream %Qv",
                streamId);
        }
    }
    for (const auto& streamId : GetKeys(traverseData->InputStreams)) {
        if (!spec->InputStreamIds.contains(streamId)) {
            THROW_ERROR_EXCEPTION("Node traverse data contains unknown stream %Qv",
                streamId);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPipelineTraverseData::Register(TRegistrar registrar)
{
    registrar.Parameter("computations", &TThis::Computations)
        .Default();

    registrar.Parameter("streams", &TThis::Streams)
        .Default();

    registrar.Parameter("united_source_stream", &TThis::UnitedSourceStream)
        .DefaultNew();

    registrar.Parameter("united_timer_stream", &TThis::UnitedTimerStream)
        .DefaultNew();

    registrar.Parameter("united_key_visitor_stream", &TThis::UnitedKeyVisitorStream)
        .DefaultNew();

    registrar.Parameter("united_output_stream", &TThis::UnitedOutputStream)
        .DefaultNew();

    registrar.Parameter("united_stream", &TThis::UnitedStream)
        .DefaultNew();

    registrar.Parameter("input_system_watermark", &TThis::InputSystemWatermark)
        .Default(ZeroSystemTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
