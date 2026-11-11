#pragma once

#include "describe_pipeline.h"

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

//! Adds to |pipeline.Edges| a #EGraphEdgeKind::DataChannel edge for every sink and source of the pipeline such that
//! the sink writes and the source reads a common data channel alias. Entities absent from |parsedParameters| or with
//! unknown classes are skipped. |jobManagerState| may be null.
void FillDataChannelEdges(
    const TPipelineSpecPtr& pipelineSpec,
    const TParsedPipelineParameters& parsedParameters,
    const TJobManagerStatePtr& jobManagerState,
    const TDescribeTraitsContext& describeTraitsContext,
    const NLogging::TLogger& logger,
    TPipelineDescription& pipeline);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
