#include <yt/yt/flow/tests/key_visitor/cpp/lib/key_visitor_functions.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NFlow::NKeyVisitorTest;

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TKeyMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TVisitMessage);

YT_FLOW_DEFINE_PROCESS_FUNCTION(TPingingVisitTesterFunction);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TKeyMessage>("finite_keys");
    builder.RegisterStream<TVisitMessage>("visits");
    // The loop the visitor must not wait for: its own visits go out as ping requests and
    // come back as responses, so both streams outlive every pass.
    builder.RegisterStream<TKeyMessage>("ping_requests");
    builder.RegisterStream<TKeyMessage>("ping_responses");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
