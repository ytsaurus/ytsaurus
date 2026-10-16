#include <yt/yt/flow/examples/cpp/word_count/lib/word_count_functions.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>


#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TWordMessage);

// Register the functions; the pipeline spec picks the hosting computation (the mode) and
// names the function via the `processing_function` parameter.
YT_FLOW_DEFINE_PROCESS_FUNCTION(TTextReadFunction, TTextReaderParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TWordCountFunction);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TWordMessage>("words");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
