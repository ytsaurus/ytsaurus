#include <yt/yt/flow/library/cpp/controller/describe/describe_pipeline.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <contrib/python/py3c/py3c.h>
#include <library/cpp/pybind/v2.h>

namespace NYT::NFlow {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! DescribePipeline with string arguments: deserialize a YSON-encoded flow_view
//! (as returned by `yt flow get-flow-view`) and return the describe result as
//! pretty-printed YSON text.
//!
//! NPyBind uses TString (Arcadia string) for Python str/bytes arguments.
TString DescribePipeline(const TString& flowViewYson)
{
    auto node = ConvertToNode(TYsonStringBuf(flowViewYson));
    // Deserialize the flow view and rebuild its queryable layout, attaching a control backed by a null
    // storage handler so the view stays detached and queryable without touching any real database.
    auto control = New<TPersistedStateControl<std::string>>(New<TNullStorageHandler<std::string>>());
    auto flowView = TFlowView::LoadFromNode(node, control);

    NDescribe::TDescribePipelineArguments args;
    args.FlowView = flowView;
    args.StatusOnly = false;
    auto description = NDescribe::DescribePipeline(args);

    return TString(ConvertToYsonString(description, EYsonFormat::Pretty).ToString());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

////////////////////////////////////////////////////////////////////////////////

MODULE_INIT_FUNC(describe)
{
    DefFunc("describe_pipeline", &NYT::NFlow::DescribePipeline);
    ::NPyBind::TPyModuleDefinition::InitModule("describe");
    return ::NPyBind::TPyModuleDefinition::GetModule().M.RefGet();
}
