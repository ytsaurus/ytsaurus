#include <yt/yt/flow/tools/ui_test_controller/fixture_pipeline/lib/computation.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

int main(int argc, const char** argv)
{
    NYT::NFlow::NUIScreenshotPipeline::LinkUIScreenshotPipeline();
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
