// This is bigrt pipeline.
//
// It
//  - reads input queue
//  - parses input messages as json dicts
//  - keeps only those dicts that contain `"keep": true` field
//  - writes them to output queue

#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/bigrt/bigrt_execution_context.h>
#include <yt/cpp/roren/bigrt/writers.h>

#include <yt/cpp/roren/bigrt/proto/config.pb.h>

#include <yt/cpp/roren/bigrt/test_medium/simple_pipeline_config/config.pb.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>

#include <library/cpp/json/json_reader.h>

#include <library/cpp/proto_config/json_to_proto_config.h>

#include <google/protobuf/util/json_util.h>

#include <util/stream/file.h>

#include <util/system/backtrace.h>

using namespace NRoren;

class TFilterParDo : public IDoFn<NBigRT::TMessageBatch, TKV<ui64, TString>>
{
public:
    void Do(const NBigRT::TMessageBatch& messageBatch, TOutput<TKV<ui64, TString>>& output) override
    {
        for (auto message : messageBatch.Messages) {
            message.Unpack();
            NJson::TJsonValue value;
            Cerr << message.Data << Endl;
            try {
                NJson::ReadJsonTree(message.Data, &value, /*throwOnError*/ true);
            } catch (const std::exception& ex) {
                Cerr << "Cannot parse json: " << ex.what() << Endl;
                continue;
            }

            auto shard = GetExecutionContext()->As<IBigRtExecutionContext>()->GetShard();
            if (value["keep"].GetBooleanSafe(false) && shard == 0) {
                output.Add({0, message.Data});
            }
        }
    }
};


void ConstructPipeline(NRoren::TPipeline& pipeline, const TSimplePipelineConfig& config)
{
    using namespace NRoren;
    auto filtered = pipeline
        | ReadMessageBatch()
        | ParDo(MakeIntrusive<TFilterParDo>());

    if (config.HasYtQueueOutput()) {
        filtered | WriteRawYtQueue(config.GetYtQueueOutput());
    } else if (config.HasLogbrokerOutput()) {
        filtered | ParDo([] (const TKV<ui64, TString>& input) -> TLogbrokerData {
            NJson::TJsonValue value;
            NJson::ReadJsonTree(input.Value(), &value, /*throwOnError*/ true);
            ui64 seqNo  = value["seq_no"].GetInteger() + 1; // seq_no must be greater than 0, seqno != offset
            return TLogbrokerData{
                .Shard = input.Key(),
                .Value = input.Value(),
                .SeqNo = seqNo
            };
        }) | WriteRawLogbroker(config.GetLogbrokerOutput());
    } else {
        Y_FAIL("neither YtQueueOutput nor LogbrokerOutput is specified");
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
