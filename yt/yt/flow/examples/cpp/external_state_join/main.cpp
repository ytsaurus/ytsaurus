#include "lib/external_state_join_functions.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TEventRow);
YT_FLOW_DEFINE_YSON_MESSAGE(TEnrichedRow);

// Register the function; the pipeline spec hosts it under TProcessFunctionComputation via
// the `processing_function` field.
YT_FLOW_DEFINE_PROCESS_FUNCTION(TLookupJoinFunction);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TEventRow>("event");
    builder.RegisterStream<TEnrichedRow>("enriched");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
