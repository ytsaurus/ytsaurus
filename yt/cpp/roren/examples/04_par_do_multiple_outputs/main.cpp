#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/yt/yt_read.h>
#include <yt/cpp/roren/yt/yt_write.h>
#include <yt/cpp/roren/yt/yt.h>

using namespace NYT;
using namespace NRoren;

// https://st.yandex-team.ru/YQL-11486

struct TUserLoginInfo
{
    TString Name;
    TString Login;

    Y_SAVELOAD_DEFINE(Name, Login);
};

struct TRobotLoginInfo
{
    TString Login;

    Y_SAVELOAD_DEFINE(Login)
};

const auto HumanTag = TTypeTag<TUserLoginInfo>("humans");
const auto RobotTag = TTypeTag<TRobotLoginInfo>("robots");

class TSplitRobots : public IDoFn<TUserLoginInfo, TMultiRow>
{
public:
    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {RobotTag, HumanTag};
    }

    void Do(const TUserLoginInfo& info, TMultiOutput& output) override
    {
        if (info.Name.StartsWith("robot-")) {
            output.GetOutput(RobotTag).Add(TRobotLoginInfo{.Login = info.Login});
        } else {
            output.GetOutput(HumanTag).Add(info);
        }
    }
};

int main(int argc, const char** argv) {
    NYT::Initialize(argc, argv);

    TYtPipelineConfig config;
    config.SetCluster("freud");
    config.SetWorkingDir("//tmp");
    auto pipeline = MakeYtPipeline(config);

    auto parsed = pipeline
        | YtRead<TNode>("//home/ermolovd/yt-tutorial/staff_unsorted")
        | ParDo([] (const TNode& node) -> TUserLoginInfo {
            TUserLoginInfo info;
            info.Name = node["name"].AsString();
            info.Login = node["login"].AsString();
            return info;
        });

    std::vector<TDynamicTypeTag> tags = {HumanTag, RobotTag};
    auto humansRobotsPack = parsed | MakeParDo<TSplitRobots>();
    const auto& [humans, robots] = humansRobotsPack.Unpack(HumanTag, RobotTag);

    auto humanSchema = TTableSchema()
        .AddColumn("name", NTi::String())
        .AddColumn("login", NTi::String());

    auto robotSchema = TTableSchema()
        .AddColumn("login", NTi::String());

    robots | ParDo([] (const TRobotLoginInfo& info) {
        TNode result;
        result["login"] = info.Login;
        return result;
    }) | YtWrite<TNode>("//tmp/ermolovd-robots", robotSchema);

    humans | ParDo([] (const TUserLoginInfo& info) {
        TNode result;
        result["name"] = info.Name;
        result["login"] = info.Login;
        return result;
    }) | YtWrite<TNode>("//tmp/ermolovd-humans", humanSchema);

    pipeline.Run();
    return 0;
}
