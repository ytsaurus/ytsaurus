#include "file_resource_test.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NFlow::NFileResourceTest;

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TEnrichedMessage);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TEnrichWithFileFunction);
YT_FLOW_DEFINE_RESOURCE(TTestFileResource);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TEnrichedMessage>("enriched");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
