#include <yt/yt/flow/library/cpp/controller/describe/describe_pipeline.h>
#include <yt/yt/flow/library/cpp/controller/describe/draw_pipeline_graph.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <contrib/python/py3c/py3c.h>
#include <library/cpp/pybind/v2.h>

namespace NYT::NFlow {

using namespace NYson;
using namespace NYTree;
using namespace NDescribe;

////////////////////////////////////////////////////////////////////////////////

//! Deserialize a YSON-encoded flow_view, run DescribePipeline (which internally calls
//! FillGraphLimits), and return the Mermaid computations graph as a UTF-8 string.
//!
//! NPyBind uses TString (Arcadia string) for Python str arguments, not std::string.
//!
//! \param flowViewYson   YSON bytes of the flow_view (as returned by yt_client.get_flow_view()).
//! \param orientation    "vertical" (TD) or "horizontal" (LR).
//! \param clusterName    YT cluster name for clickable computation URLs (empty = no links).
//! \param pipelinePath   Pipeline path in YT for clickable computation URLs.
// Helper: deserialize flow_view YSON and run DescribePipeline once.
// Returns (pipeline, options) ready for graph building.
static std::pair<TPipelineDescription, TDrawPipelineGraphOptions>
PrepareGraphInput(
    const TString& flowViewYson,
    const TString& orientation,
    const TString& clusterName,
    const TString& pipelinePath)
{
    auto node = ConvertToNode(TYsonStringBuf(flowViewYson));
    // Deserialize the flow view and rebuild its queryable layout, attaching a control backed by a null
    // storage handler so the view stays detached and queryable without touching any real database.
    auto control = New<TPersistedStateControl<std::string>>(New<TNullStorageHandler<std::string>>());
    auto flowView = TFlowView::LoadFromNode(node, control);

    TDescribePipelineArguments args;
    args.FlowView = flowView;
    args.StatusOnly = false;

    // DescribePipeline internally calls FillGraphLimits to populate
    // InputLimitStats/OutputLimitStats and ReadDelayEdges.
    auto pipeline = DescribePipeline(args);

    TDrawPipelineGraphOptions options;
    options.Orientation = orientation == "horizontal"
        ? EGraphOrientation::Horizontal
        : EGraphOrientation::Vertical;
    options.ClusterName = std::string(clusterName);
    options.PipelinePath = std::string(pipelinePath);

    return {std::move(pipeline), std::move(options)};
}

TString FlowViewYsonToMermaidComputationsGraph(
    const TString& flowViewYson,
    const TString& orientation,
    const TString& clusterName,
    const TString& pipelinePath)
{
    auto [pipeline, options] = PrepareGraphInput(flowViewYson, orientation, clusterName, pipelinePath);
    return TString(BuildMermaidComputationsGraph(pipeline, options));
}

TString FlowViewYsonToMermaidUnrolledGraph(
    const TString& flowViewYson,
    const TString& orientation,
    const TString& clusterName,
    const TString& pipelinePath)
{
    auto [pipeline, options] = PrepareGraphInput(flowViewYson, orientation, clusterName, pipelinePath);
    return TString(BuildMermaidUnrolledGraph(pipeline, options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

////////////////////////////////////////////////////////////////////////////////

MODULE_INIT_FUNC(bindings)
{
    DefFunc("flow_view_yson_to_mermaid_computations_graph",
        &NYT::NFlow::FlowViewYsonToMermaidComputationsGraph);
    DefFunc("flow_view_yson_to_mermaid_unrolled_graph",
        &NYT::NFlow::FlowViewYsonToMermaidUnrolledGraph);
    ::NPyBind::TPyModuleDefinition::InitModule("bindings");
    return ::NPyBind::TPyModuleDefinition::GetModule().M.RefGet();
}
