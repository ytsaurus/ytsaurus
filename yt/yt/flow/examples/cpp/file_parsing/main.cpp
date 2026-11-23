#include "lib/file_parsing_functions.h"
#include "lib/messages.h"
#include "lib/parse_executor_resource.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NFlow::NFileParsingExample;

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TParseTask);
YT_FLOW_DEFINE_YSON_MESSAGE(TParsedFile);
YT_FLOW_DEFINE_YSON_MESSAGE(TFailedFile);

YT_FLOW_DEFINE_PROCESS_FUNCTION(TEnqueueFileFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TParseFunction, TParseFunctionParameters);

YT_FLOW_DEFINE_RESOURCE(TParseExecutorResource);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    // The streams that the parse queue receives or feeds have the schema of the queue.
    builder.RegisterStream<TParseTask>("files");
    builder.RegisterStream<TParseTask>("requests");
    builder.RegisterStream<TParseTask>("retries");
    builder.RegisterStream<TParsedFile>("parsed");
    builder.RegisterStream<TFailedFile>("failed");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
