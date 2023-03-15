#include <mapreduce/yt/interface/client.h>

#include <yt/cpp/roren/interface/roren.h>
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
        | ParDo([] (const TNode& node) -> TKV<TString, TString> {
            TKV<TString, TString> kv;
            kv.Key() = node["name"].AsString();
            kv.Value() = node["login"].AsString();
            return kv;
        })
        | GroupByKey()
        | ParDo([] (const TKV<TString, TInputPtr<TString>>& kv) {
            TNode result;
            TNode allLogins;
            for (const auto& v : kv.Value()) {
                allLogins.Add(v);
            }
            result["name"] = kv.Key();
            result["logins"] = allLogins;
            return result;
        })
        | YtWrite<TNode>(
            "//tmp/ermolovd-tt",
            TTableSchema()
                .AddColumn("name", NTi::String())
                .AddColumn("logins", NTi::List(NTi::String()))
        );

    pipeline.Run();
    return 0;
}
