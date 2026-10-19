#include <yt/yt/flow/tests/process_function_graph/functions/functions.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

namespace NYT::NFlow::NProcessFunctionGraphTest {

////////////////////////////////////////////////////////////////////////////////

// [BEGIN register_streams]
void PrepareSpec(const TPipelineSpecPtr& spec, const TDynamicPipelineSpecPtr& dynamicSpec)
{
    TSimpleSpecBuilder streamBuilder;
    streamBuilder.RegisterStream<TKeyMessage>("left");
    streamBuilder.RegisterStream<TKeyMessage>("right");
    // Streams between graph nodes are registered like any other stream.
    streamBuilder.RegisterStream<TStageMessage>("left_counts");
    streamBuilder.RegisterStream<TStageMessage>("right_counts");
    streamBuilder.RegisterStream<TFinalMessage>("result");
    if (spec->Computations.contains("delivery")) {
        streamBuilder.RegisterStream<TFinalMessage>("delivered");
    }
    streamBuilder(spec, dynamicSpec);
}

// [END register_streams]

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NProcessFunctionGraphTest

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram(&NYT::NFlow::NProcessFunctionGraphTest::PrepareSpec)
        .Run(argc, argv);
}
