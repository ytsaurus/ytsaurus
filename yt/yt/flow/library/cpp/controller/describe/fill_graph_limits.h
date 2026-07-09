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
//! across all partitions. Skipped silently when |flowView| has no Feedback.
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

//! Maximum buffer fill rate across all limit types for the given stream (0 if absent).
double GetMaxFillRateForStream(
    const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
    const TGraphEntityId& streamId);

//! Builds the edge label string ("limit_type=fill_rate ...", sorted descending) for the given stream.
//! When |boldOverThreshold| is set, fill rates over the warning threshold are wrapped in markdown bold.
//! Empty if there are no limit stats with a visible fill rate.
std::string MakeEdgeLabel(
    const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
    const TGraphEntityId& streamId,
    bool boldOverThreshold = false);

//! Fills the ExtendedInput/Output/Timer/SourceStreams of each computation in |pipeline|
//! and appends a concatenated per-edge stats message to each computation's Messages.
//!
//! Must be called after FillGraphLimits (which populates the limit stats this reads).
void FillExtendedStreams(const TFlowViewPtr& flowView, TPipelineDescription& pipeline);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
