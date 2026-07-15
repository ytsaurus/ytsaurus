#include <yt/yt/flow/examples/cpp/noop/pipeline/lib/noop_functions.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

// Register the function; the pipeline spec picks the hosting computation (the mode) and
// names the function via the `processing_function` parameter.
YT_FLOW_DEFINE_PROCESS_FUNCTION(TNoopFunction);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return TSimpleRunnerProgram().Run(argc, argv);
}
