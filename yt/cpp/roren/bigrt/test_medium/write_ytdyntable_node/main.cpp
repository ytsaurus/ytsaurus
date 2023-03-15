#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/bigrt/bigrt_execution_context.h>
#include <yt/cpp/roren/bigrt/test_medium/simple_pipeline_config/config.pb.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/getopt/last_getopt.h>
#include <util/stream/file.h>

class UnpackBatch:
    public NRoren::IDoFn<NBigRT::TMessageBatch, NYT::TNode> {
public:
    void Do(const NBigRT::TMessageBatch& batch, NRoren::TOutput<TOutputRow>& output) {
        for (auto message : batch.Messages) {
            if (!message.Unpack()) {
                continue;
            }
            try {
                NJson::TJsonValue data;
                NJson::ReadJsonTree(message.Data, &data, /*throwOnError*/ true);
                const auto shard = GetExecutionContext()->As<NRoren::IBigRtExecutionContext>()->GetShard();
                NYT::TNode node;
                node["shard"] = shard;
                node["key"] = data["key"].GetString();
                node["value"] = data["value"].GetString();
                output.Add(node);
            } catch (const std::exception& ex) {
                Cerr << "Cannot parse json: " << ex.what() << Endl;
            }
        }
    }
};

void ConstructPipeline(NRoren::TPipeline& pipeline, const TSimplePipelineConfig& config)
{
    pipeline | NRoren::ReadMessageBatch()
        | NRoren::MakeParDo<UnpackBatch>()
        | NRoren::WriteYtDynTable(config.GetYtDynTableOutputPath())
        ;
}

int main(int argc, const char** argv) {
    try {
        auto program = NRoren::TBigRtProgram<TSimplePipelineConfig>::Create(argc, argv);
        program.Run(ConstructPipeline);
    } catch (...) {
        TBackTrace::FromCurrentException().PrintTo(Cerr);
        throw;
    }
}
