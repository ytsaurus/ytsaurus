// This is bigrt pipeline.
//
// It
//  - reads input queue
//  - keeps only those dicts that contain `"keep": true` field

#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/bigrt/writers.h>
#include <yt/cpp/roren/bigrt/read_dynamic_table.h>

#include <yt/cpp/roren/bigrt/proto/config.pb.h>

#include <yt/cpp/roren/bigrt/test_medium/simple_pipeline_config/config.pb.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>

#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/json/json_reader.h>

#include <library/cpp/proto_config/json_to_proto_config.h>

#include <google/protobuf/util/json_util.h>

#include <util/stream/file.h>

#include <util/system/backtrace.h>

using namespace NRoren;

class TMergingParDo : public IDoFn<std::tuple<TString, TString, std::optional<TDictMessage>>, TKV<ui64, TString>>
{
public:
    void Do(const std::tuple<TString, TString, std::optional<TDictMessage>>& entry, TOutput<TKV<ui64, TString>>& output) override
    {
        const auto& [key, mainValue, maybeDictValue] = entry;
        auto res = mainValue + "_";
        if (maybeDictValue) {
            res += maybeDictValue->GetValue();
        } else {
            res += "<none>";
        }
        output.Add({0, NYT::Format("{\"key\": %Qv, \"value\": %Qv}", key, res)});
    }
};

void ConstructPipeline(NRoren::TPipeline& pipeline, const TSimplePipelineConfig& config)
{
    const auto& consSystemConfigs = config
        .GetRorenConfig()
        .GetConsumers();
    Y_VERIFY(consSystemConfigs.size() == 1);
    const auto clusterName = consSystemConfigs[0]
        .GetConsumingSystemConfig()
        .GetCluster();

    auto dict =
        pipeline |
        BindToProtoDynamicTableInMemory<TDictMessage>(
            clusterName, config.GetDictTable(), TDuration::MilliSeconds(config.GetDictRefreshPeriodMs()));

    pipeline |
        ReadMessageBatch() |
        ParDo([] (const NBigRT::TMessageBatch& messageBatch, TOutput<TKV<TString, TString>>& output) {
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
                output.Add({value["key"].GetStringSafe(), value["value"].GetStringSafe()});
            }
        }) |
        DictJoin(dict) |
        MakeParDo<TMergingParDo>() |
        WriteRawYtQueue(config.GetYtQueueOutput());
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
