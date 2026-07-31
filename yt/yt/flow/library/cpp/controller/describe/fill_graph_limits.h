#pragma once

#include "describe_pipeline.h"
#include "intermediate_description.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

//! Fills InputLimitStats and OutputLimitStats in each computation of |pipeline|.
//!
//! Must be called after DescribePipeline (which populates pipeline.Computations).
//!
//! For each (stream_id, limit_type), aggregates TJobEntityLimitStatus across all
//! partitions — tracking both the worst partition (max fill rate) and the sum
//! across all partitions. Reads the job statuses out of |intermediateDescriptions|
//! rather than walking the feedback again: the caller has already paid for that walk.
void FillGraphLimits(
    const TFlowViewPtr& flowView,
    const THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>>& intermediateDescriptions,
    TPipelineDescription& pipeline);

//! Fills pipeline.ReadDelayEdges and each computation's ReadDelays map from the spec
//! (WatermarkAlignment.ReadDelays). Independent of Feedback.
void FillReadDelayEdges(const TFlowViewPtr& flowView, TPipelineDescription& pipeline);

////////////////////////////////////////////////////////////////////////////////

// An edge whose buffer fill rate reaches this threshold is considered backpressured
// and is highlighted in the pipeline graph.
inline constexpr double WarningFillRateThreshold = 0.5;
// Limit types with a fill rate below this are omitted from edge labels.
inline constexpr double MinVisibleFillRate = 1e-3;

// A stream counts as blocking when a job spent at least this share of its
// lifetime blocked on the stream's buffer.
inline constexpr double WarningBlockedTimeShareThreshold = 0.3;
// Blocked-time shares below this are omitted from edge labels.
inline constexpr double MinVisibleBlockedTimeShare = 0.01;

//! Maximum buffer fill rate across all limit types for the given stream (0 if absent).
double GetMaxFillRateForStream(
    const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
    const TGraphEntityId& streamId);

//! Maximum blocked-time share across all limit types for the given stream (0 if absent).
double GetMaxBlockedTimeShareForStream(
    const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
    const TGraphEntityId& streamId);

//! Per-stream worst blocked-time share of the stream's writer, with the worst
//! partition as an example. Shown on the reader side, where the buffer itself
//! carries no share: a full input never blocks its owner, it blocks the writer.
struct TWriterBlockedShare
{
    double Share = 0;
    TPartitionId ExamplePartitionId;
};

THashMap<TGraphEntityId, TWriterBlockedShare> ComputeWriterBlockedTimeShares(const TPipelineDescription& pipeline);

//! The per-computation "Stream buffers and backpressure" message, shared by the
//! pipeline describe and the computation page.
std::optional<TMessage> BuildBuffersAndBackpressureMessage(
    const TPipelineComputationDescription& computation,
    const THashMap<TGraphEntityId, TWriterBlockedShare>& writerBlockedShares);

//! Builds the edge label for the given stream: per-limit entries sorted by
//! severity, with the blocking numbers in markdown bold when |boldOverThreshold|
//! is set. Empty if nothing is visible.
std::string MakeEdgeLabel(
    const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
    const TGraphEntityId& streamId,
    bool boldOverThreshold = false);

//! Fills the ExtendedInput/Output/Timer/SourceStreams of each computation in |pipeline|
//! and appends the shared "Stream buffers and backpressure" message (see
//! BuildBuffersAndBackpressureMessage) to each computation's Messages.
//!
//! Must be called after FillGraphLimits (which populates the limit stats this reads).
void FillExtendedStreams(const TFlowViewPtr& flowView, TPipelineDescription& pipeline);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
