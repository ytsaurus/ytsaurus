#pragma once

#include "describe_pipeline.h"

#include <yt/yt/core/misc/public.h>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGraphOrientation,
    (Vertical)
    (Horizontal)
);

struct TDrawPipelineGraphOptions
{
    EGraphOrientation Orientation = EGraphOrientation::Vertical;

    //! YT cluster name — used to build clickable computation URLs.
    //! Empty string means no links are emitted.
    std::string ClusterName;

    //! Pipeline path in YT — used to build clickable computation URLs.
    std::string PipelinePath;
};

////////////////////////////////////////////////////////////////////////////////

std::string BuildMermaidComputationsGraph(
    const TPipelineDescription& pipeline,
    const TDrawPipelineGraphOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

//! Build a Mermaid flowchart string for the unrolled (acyclic) streams graph.
std::string BuildMermaidUnrolledGraph(
    const TPipelineDescription& pipeline,
    const TDrawPipelineGraphOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
