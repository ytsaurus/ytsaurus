#pragma once

#include "common.h"
#include "graph_entity_id.h"
#include "intermediate_description.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/yt_connector.h>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

// See graph_entity_id.h for the stream-id naming convention (local / global / graph).

struct TStream
    : public NYTree::TYsonStructLite
{
    TGraphEntityId Id;    // Graph entity id, e.g. "stm-heavy_reader/queue".
    TStreamId Name;       // Local stream name (within a computation).
    TStreamId GlobalName; // Global stream id (no prefix), used for display.
    NLogging::ELogLevel Status{};
    std::vector<TMessage> Messages;
    std::optional<TGraphEntityId> SourceGraphEntityId;
    std::optional<TGraphEntityId> SinkGraphEntityId;
    double MessagesPerSecond{};
    double BytesPerSecond{};
    double InflightRows{};
    double InflightBytes{};

    REGISTER_YSON_STRUCT_LITE(TStream);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

// Aggregated limit stats for one (stream_id, limit_type) across all partitions.
// Reuses TJobEntityLimitStatus (Limit, Used, Pending) from flow_view.h.
struct TStreamLimitStats
    : public NYTree::TYsonStructLite
{
    // Partition with the maximum fill rate (Max.Used / max(1, Max.Limit)).
    TJobEntityLimitStatus Max;
    TPartitionId MaxPartitionId;

    // Sum across all partitions (Total.Pending = sum of Pending values, 0 if absent).
    TJobEntityLimitStatus Total;

    // Partition with the maximum blocked-time share (see TJobEntityLimitStatus::BlockedTimeShare).
    double MaxBlockedTimeShare = 0;
    TPartitionId MaxBlockedPartitionId;

    double GetMaxFillRate() const
    {
        return static_cast<double>(Max.Used) / std::max<i64>(1, Max.Limit);
    }

    REGISTER_YSON_STRUCT_LITE(TStreamLimitStats);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

// A read-delay edge: the blocker stream delays reading of the delayed stream by Delay.
struct TReadDelayEdge
    : public NYTree::TYsonStructLite
{
    TGraphEntityId BlockerStreamGraphEntityId;
    TGraphEntityId DelayedStreamGraphEntityId;
    TDuration Delay;

    REGISTER_YSON_STRUCT_LITE(TReadDelayEdge);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

// Extended info about an edge between a computation and one of its streams.
// Carries the per-edge messages that the pipeline graph renders on this edge.
struct TStreamEdge
    : public NYTree::TYsonStructLite
{
    TGraphEntityId StreamGraphEntityId;
    bool Completed = false;
    bool Drained = false;
    // True if the pipeline graph would highlight this edge (buffer fill rate over the warning threshold).
    bool BackpressureDetected = false;
    std::vector<TMessage> Messages;

    REGISTER_YSON_STRUCT_LITE(TStreamEdge);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TPipelineComputationDescription
    : public TComputationDescription
{
    TGraphEntityId Id; // Graph entity id, e.g. "cmp-common_reducer".

    // Stream graph entity ids referenced by this computation.
    THashSet<TGraphEntityId> InputStreams;
    THashSet<TGraphEntityId> OutputStreams;
    THashSet<TGraphEntityId> TimerStreams;
    THashSet<TGraphEntityId> SourceStreams;

    std::vector<TStreamEdge> ExtendedInputStreams;
    std::vector<TStreamEdge> ExtendedOutputStreams;
    std::vector<TStreamEdge> ExtendedTimerStreams;
    std::vector<TStreamEdge> ExtendedSourceStreams;

    // graphEntityId(output) -> set of graphEntityId(input) it depends on.
    // Used for building the acyclic unrolled (streams) graph.
    THashMap<TGraphEntityId, THashSet<TGraphEntityId>> StreamsDependency;

    // graphEntityId -> (limit_type -> aggregated stats across partitions)
    // For input streams: limit_type = "input_buffer_bytes", etc.
    THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>> InputLimitStats;
    // For output streams: limit_type = "output_buffer_bytes", "output_store_bytes", etc.
    THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>> OutputLimitStats;

    // globalStreamId(blocker) -> delay.
    // Non-empty only for computations with WatermarkAlignment.ReadDelays.
    THashMap<TStreamId, TDuration> ReadDelays;

    // Partition counts for warning detection (mirrors Python graph_model.py logic).
    // TotalPartitionCount: partitions not in Interrupted/Completed state.
    // UnstablePartitionCount: partitions with no running job OR job inited < 5 min ago.
    i64 TotalPartitionCount = 0;
    i64 UnstablePartitionCount = 0;

    // State path from computationSpec->Parameters["state"]["path"], if present.
    std::string StatePath;

    REGISTER_YSON_STRUCT_LITE(TPipelineComputationDescription);

    static void Register(TRegistrar registrar);
};

struct TSource
    : public NYTree::TYsonStructLite
{
    TGraphEntityId Id; // Graph entity id, e.g. "src-heavy_reader/queue".
    TStreamId Name;    // Local stream name within a computation.
    std::string Description;
    NLogging::ELogLevel Status{};
    std::vector<TMessage> Messages;
    TGraphEntityId StreamGraphEntityId; // The stream this source feeds.

    REGISTER_YSON_STRUCT_LITE(TSource);

    static void Register(TRegistrar registrar);
};

struct TSink
    : public NYTree::TYsonStructLite
{
    TGraphEntityId Id; // Graph entity id, e.g. "snk-heavy_reader/queue".
    TSinkId Name;
    std::string Description;
    NLogging::ELogLevel Status{};
    std::vector<TMessage> Messages;
    TGraphEntityId StreamGraphEntityId; // The stream this sink reads.

    REGISTER_YSON_STRUCT_LITE(TSink);

    static void Register(TRegistrar registrar);
};

struct TPipelineDescription
    : public NYTree::TYsonStructLite
{
    EPipelineState Status{};
    std::vector<TMessage> Messages;

    THashMap<TStreamId, TStream> Streams;
    THashMap<TStreamId, TSource> Sources;
    THashMap<TSinkId, TSink> Sinks;
    THashMap<TComputationId, TPipelineComputationDescription> Computations;

    // Read-delay edges from WatermarkAlignment.ReadDelays of all computations.
    std::vector<TReadDelayEdge> ReadDelayEdges;

    REGISTER_YSON_STRUCT_LITE(TPipelineDescription);

    static void Register(TRegistrar registrar);
};

struct TDescribePipelineArguments
{
    TFlowViewPtr FlowView;
    THashMap<std::string, TError> ControllerErrors;
    NLogging::TLogger Logger;
    IPipelineAuthenticatorPtr Authenticator;
    bool StatusOnly = false; // If true, then only Status and Messages fields are filled.
    std::string ControllerFlowCoreVersion;
    // Leader-controller's build type, taken from its node_info. Empty means the controller
    // is on a binary that predates the field; describe omits the line and the slow-build
    // warning in that case.
    std::string ControllerBuildType;
    // An empty Bundle means it was not resolved (e.g. an old binary); describe omits the line.
    TFlowTablesBundleInfo FlowTablesBundle;
};

void RegisterStreams(const TPipelineSpecPtr& pipelineSpec, const TComputationId& computationId,
    TPipelineDescription& pipeline, TPipelineComputationDescription& computationDescription,
    const TDescribeTraitsContext& describeTraitsContext);

void FillStreamTraverseData(const TFlowViewPtr& flowView, const TPipelineSpecPtr pipelineSpec, TPipelineDescription& pipeline);

void FillStreamStatisticsMessages(const TFlowViewPtr& flowView, TPipelineDescription& pipeline);

void FillStreamStateMessage(
    const TFlowViewPtr& flowView,
    const THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>>& intermediateDescriptions,
    std::vector<TMessage>& messages);

TPipelineDescription DescribePipeline(const TDescribePipelineArguments& arguments);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
