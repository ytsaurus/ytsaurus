// This is bigrt pipeline.
//
// It
//  - reads input queue
//  - parses input messages as json dicts
//  - expects messages to be like {"key": "foo", "value": 1}
//  - counts sum of "value" keys and stores this sum inside `"key"`
//  - writes nothing

#include <yt/cpp/roren/library/kv_state_manager/kv_state_manager.h>

#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/bigrt/bigrt_execution_context.h>
#include <yt/cpp/roren/bigrt/writers.h>

#include <yt/cpp/roren/bigrt/proto/config.pb.h>

#include <yt/cpp/roren/bigrt/test_medium/simple_pipeline_config/config.pb.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>

#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/json/json_reader.h>

#include <library/cpp/getoptpb/getoptpb.h>

#include <util/stream/file.h>

#include <util/system/backtrace.h>

using namespace NRoren;

class TExtractKeyFn
    : public IDoFn<TString, TKV<TString, int>>
{
public:
    void Do(const TString& message, TOutput<TOutputRow>& output) override
    {
        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(message, &json, /*throwOnError*/ true);
        } catch (const std::exception& ex) {
            Cerr << "Cannot parse json: " << ex.what() << Endl;
            return;
        }

        TString key = json["key"].GetStringSafe();
        int v = json["value"].GetIntegerSafe();
        output.Add({key, v});
    }
};

class TStatefulDo
    : public IStatefulDoFn<TKV<TString, int>, void, i64>
{
public:
    void Do(const TInputRow& input, TOutput<TOutputRow>&, TState& state)
    {
        state += input.Value();
    }
};

void ConstructPipeline(NRoren::TPipeline& pipeline, const TSimplePipelineConfig& config)
{
    using namespace NRoren;

    auto creator = [kvConfig = config.GetKvStateManagerConfig()] (ui64 /*shard*/, NSFStats::TSolomonContext context) {
        return NYT::New<TKvStateManager<TString, i64>>(kvConfig, context);
    };
    auto pState = MakeBigRtPState<TString, i64>(pipeline, creator);

    pipeline
        | ReadMessageBatch()
        | UnpackMessagesParDo()
        | MakeParDo<TExtractKeyFn>()
        | MakeStatefulParDo<TStatefulDo>(pState);
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
