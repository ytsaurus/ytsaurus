#include <yt/yt/flow/tests/state_joiner/pipeline/lib/state_joiner_functions.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NFlow::NStateJoinerTest;

////////////////////////////////////////////////////////////////////////////////

// Register the functions; the pipeline spec picks the hosting computation (transform) via
// `computation_class_name` and names the function via `processing_function`.
YT_FLOW_DEFINE_PROCESS_FUNCTION(TAccumulatorFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TJoinerFunction);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    Initialize(argc, argv);
    return TSimpleRunnerProgram().Run(argc, argv);
}
