#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/transforms/sum.h>
#include <yt/cpp/roren/yt/yt_read.h>
#include <yt/cpp/roren/yt/yt_write.h>
#include <yt/cpp/roren/yt/yt.h>

using namespace NYT;
using namespace NRoren;

// https://st.yandex-team.ru/YQL-11486

int main(int argc, const char** argv) {
    NYT::Initialize(argc, argv);

    TYtPipelineConfig config;
    config.SetCluster("freud");
    config.SetWorkingDir("//tmp");
    auto pipeline = MakeYtPipeline(config);

    pipeline
        | YtRead<TNode>("//home/ermolovd/yt-tutorial/staff_unsorted")
        | ParDo([] (const TNode& node) {
            TKV<TString, int> kv;
            kv.Key() = node["name"].AsString();
            kv.Value() = static_cast<int>(node["uid"].AsInt64());
            return kv;
        })
        | CombinePerKey(Sum<int>())
        | ParDo([] (const TKV<TString, int>& kv) {
            TNode result;
            result["name"] = kv.Key();
            result["total_uid"] = kv.Value();
            return result;
        })
        | YtWrite<TNode>(
            "//tmp/ermolovd-tt",
            TTableSchema()
                .AddColumn("name", NTi::String())
                .AddColumn("total_uid", NTi::Int64())
        );

    pipeline.Run();
    return 0;
}
