// This is bigrt pipeline.
//
// It
//  - reads input queue
//  - keeps only those dicts that contain `"keep": true` field

#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/bigrt/writers.h>

#include <yt/cpp/roren/bigrt/proto/config.pb.h>

#include <yt/cpp/roren/bigrt/test_medium/simple_pipeline_config/config.pb.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>

#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/json/json_reader.h>

#include <library/cpp/proto_config/json_to_proto_config.h>

#include <google/protobuf/util/json_util.h>

#include <util/stream/file.h>

#include <util/system/backtrace.h>

NYT::NProfiling::TProfiler MainProfiler("", "");

NYT::NProfiling::TCounter TotalProcessed = MainProfiler.Counter("metrics_pipeline.total_processed");

void ConstructPipeline(NRoren::TPipeline& pipeline)
{
    using namespace NRoren;

    pipeline | ReadMessageBatch() |
        ParDo([](const NBigRT::TMessageBatch& messageBatch, TOutput<TKV<ui64, TString>>&) {
            for (auto message : messageBatch.Messages) {
                TotalProcessed.Increment();
            }
        }) | NullWrite();
}

int main(int argc, const char** argv)
{
    try {
        auto program = NRoren::TBigRtProgram<TSimplePipelineConfig>::Create(argc, argv);
        program.Run(ConstructPipeline);
    } catch (...) {
        TBackTrace::FromCurrentException().PrintTo(Cerr);
        throw;
    }
}
