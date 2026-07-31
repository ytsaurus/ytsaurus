#include <yt/yt/flow/examples/cpp/word_count/lib/word_count_functions.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/companion/server/companion_main.h>
#include <yt/yt/flow/library/cpp/companion/server/pipeline.h>

using namespace NYT::NFlow;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

// The same functions as the in-process word_count example; the only difference
// is the hosting: the pipeline spec selects the companion shims and points the
// CompanionManager resource entrypoint at this binary.
YT_FLOW_DEFINE_YSON_MESSAGE(TWordMessage);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NCompanionServer::TPipeline pipeline;
    pipeline.AddSource<TTextReadFunction, TTextReaderParameters>("reader");
    pipeline.AddTransform<TWordCountFunction>("counter");
    return NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline));
}
