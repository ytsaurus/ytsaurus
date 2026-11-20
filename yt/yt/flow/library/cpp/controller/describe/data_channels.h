#pragma once

#include "describe_pipeline.h"

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

//! Adds to |pipeline.Edges| a #EGraphEdgeKind::DataChannel edge
//! - from a sink or a computation (through its external state managers) to a source or a key-visitor stream
//!   (through the external state joiners it visits) when they write and read a common data channel alias;
//! - from a computation to its key-visitor stream that visits its internal states or state managers.
//! |pipeline| must already contain its streams and sinks; sinks absent from |pipeline.Sinks| have no input streams
//! and are skipped. Sources and sinks absent from |parsedParameters| and entities with unknown classes or unparsable
//! parameters are skipped too, so one broken entity does not fail describe.
//! |jobManagerState| may be null.
void FillDataChannelEdges(
    const TPipelineSpecPtr& pipelineSpec,
    const TParsedPipelineParameters& parsedParameters,
    const TJobManagerStatePtr& jobManagerState,
    const TDescribeTraitsContext& describeTraitsContext,
    const NLogging::TLogger& logger,
    TPipelineDescription& pipeline);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
