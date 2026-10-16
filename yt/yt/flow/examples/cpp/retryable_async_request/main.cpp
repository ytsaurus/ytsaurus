#include "lib/retryable_async_request_functions.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TEventMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TRequestMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TResponseMessage);

// Register the functions; the pipeline spec hosts each under TProcessFunctionComputation via
// the `processing_function` field.
YT_FLOW_DEFINE_PROCESS_FUNCTION(TRequestProcessor);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TStateKeeper);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TEventMessage>("event");
    builder.RegisterStream<TRequestMessage>("request");
    builder.RegisterStream<TResponseMessage>("response");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
