#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/bigrt/bigrt_execution_context.h>
#include <yt/cpp/roren/bigrt/test_medium/simple_pipeline_config/config.pb.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/yt/memory/shared_range.h>
#include <util/stream/file.h>

NYT::NTableClient::TNameTablePtr GNameTable = NYT::New<NYT::NTableClient::TNameTable>();

class UnpackBatch:
    public NRoren::IDoFn<NBigRT::TMessageBatch, NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>> {
public:
    void Do(const NBigRT::TMessageBatch& batch, NRoren::TOutput<TOutputRow>& output) {
        NYT::NTableClient::TUnversionedRowsBuilder builder;
        for (auto message : batch.Messages) {
            if (!message.Unpack()) {
                continue;
            }
            try {
                NJson::TJsonValue data;
                NJson::ReadJsonTree(message.Data, &data, /*throwOnError*/ true);
                const auto shard = GetExecutionContext()->As<NRoren::IBigRtExecutionContext>()->GetShard();
                builder.AddRow(shard, data["key"].GetString(), data["value"].GetString());
            } catch (const std::exception& ex) {
                Cerr << "Cannot parse json: " << ex.what() << Endl;
            }
        }
        output.Add(builder.Build());
    }
};

void ConstructPipeline(NRoren::TPipeline& pipeline, const TSimplePipelineConfig& config)
{
    GNameTable->RegisterName("shard");
    GNameTable->RegisterName("key");
    GNameTable->RegisterName("value");
    pipeline | NRoren::ReadMessageBatch()
        | NRoren::MakeParDo<UnpackBatch>()
        | NRoren::WriteYtDynTable(config.GetYtDynTableOutputPath(), GNameTable)
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
