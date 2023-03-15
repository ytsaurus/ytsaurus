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

using namespace NRoren;

class TUnpackParDo : public IDoFn<NBigRT::TMessageBatch, TKV<ui64, TString>>
{
public:
    void Do(const NBigRT::TMessageBatch& messageBatch, TOutput<TKV<ui64, TString>>& output) override
    {
        for (auto message : messageBatch.Messages) {
            message.Unpack();
            NJson::TJsonValue value;
            try {
                NJson::ReadJsonTree(message.Data, &value, /*throwOnError*/ true);
            } catch (const std::exception& ex) {
                Cerr << "Cannot parse json: " << ex.what() << Endl;
                continue;
            }

            output.Add({0, message.Data});
        }
    }
};

void ConstructPipeline(NRoren::TPipeline& pipeline, const TSimplePipelineConfig& config)
{
    for (const auto& consumer : config.GetRorenConfig().GetConsumers()) {
        const auto& inputTag = consumer.GetInputTag();
        pipeline
            | ReadMessageBatch(inputTag)
            | ParDo(MakeIntrusive<TUnpackParDo>())
            | WriteRawYtQueue(config.GetYtQueueOutput());
    }
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
