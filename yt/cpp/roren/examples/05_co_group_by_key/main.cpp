#include <mapreduce/yt/interface/client.h>

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/yt/yt_read.h>
#include <yt/cpp/roren/yt/yt_write.h>
#include <yt/cpp/roren/yt/yt.h>

using namespace NYT;
using namespace NRoren;

int main(int argc, const char** argv) {
    NYT::Initialize(argc, argv);

    TYtPipelineConfig config;
    config.SetCluster("freud");
    config.SetWorkingDir("//tmp");
    auto pipeline = MakeYtPipeline(config);

    auto readStaff = pipeline | YtRead<TNode>("//home/tutorial/staff_unsorted");
    auto readIsRobot = pipeline | YtRead<TNode>("//home/tutorial/is_robot_unsorted");

    static const auto InputStaffTag = TTypeTag<TKV<int, TString>>{"input-staff"};
    static const auto InputIsRobotTag = TTypeTag<TKV<int, bool>>{"input-is-robot"};

    auto inputStaff = readStaff
        | ParDo([] (const TNode& node) {
            TKV<int, TString> kv;
            kv.Key() = node["uid"].AsInt64();
            kv.Value() = node["login"].AsString();
            return kv;
        });

    auto inputIsRobot = readIsRobot
        | ParDo([] (const TNode& node) {
            TKV<int, bool> kv;
            kv.Key() = node["uid"].AsInt64();
            kv.Value() = node["is_robot"].AsBool();
            return kv;
        });

    auto multiPCollection = TMultiPCollection{InputStaffTag, inputStaff, InputIsRobotTag, inputIsRobot};

    auto output = multiPCollection | CoGroupByKey() | ParDo([] (const TCoGbkResult& gbk) {
        TNode result;
        result["uid"] = gbk.GetKey<int>();

        auto login = gbk.GetInput(InputStaffTag).Next();
        if (login != nullptr) {
            result["login"] = login->Value();
        } else {
            result["login"] = "unknown";
        }

        auto isRobot = gbk.GetInput(InputIsRobotTag).Next();
        if (isRobot != nullptr) {
            result["is_robot"] = isRobot->Value();
        } else {
            result["is_robot"] = false;
        }

        return result;
    });

    output | YtWrite<TNode>(
        "//tmp/nadya73-tt",
        TTableSchema()
            .AddColumn("uid", NTi::String())
            .AddColumn("login", NTi::String())
            .AddColumn("is_robot", NTi::Bool())
    );

    pipeline.Run();

    return 0;
}

