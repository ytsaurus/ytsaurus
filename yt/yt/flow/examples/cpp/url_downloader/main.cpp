#include "lib/url_downloader_functions.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TUrlMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TProcessedUrlMessage);

// Register the function with its dynamic parameters type (it has no static ones) so the engine can
// validate the dynamic `processing_function_parameters` block.
YT_FLOW_DEFINE_PROCESS_FUNCTION(
    TLimitedUrlDownloadFunction, TEmptyProcessFunctionParameters, TDynamicLimitedUrlDownloadParameters);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TUrlMessage>("urls");
    builder.RegisterStream<TProcessedUrlMessage>("processed_urls");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
