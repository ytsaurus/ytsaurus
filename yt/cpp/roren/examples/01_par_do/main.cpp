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

struct TUserEmailInfo
{
    TString Name;
    TString Email;

    Y_SAVELOAD_DEFINE(Name, Email);
};

int main(int argc, const char** argv) {
    NYT::Initialize(argc, argv);

    auto pipeline = MakeYtPipeline("freud", "//tmp");

    pipeline
        | YtRead<TNode>("//home/ermolovd/yt-tutorial/staff_unsorted")
        | ParDo([] (const TNode& node) -> TUserLoginInfo {
            TUserLoginInfo info;
            info.Name = node["name"].AsString();
            info.Login = node["login"].AsString();
            return info;
        })
        | ParDo([] (const TUserLoginInfo& info) {
            TUserEmailInfo result;
            result.Name = info.Name;
            result.Email = info.Login + "@yandex-team.ru";
            return result;
        })
        | ParDo([] (const TUserEmailInfo& info) {
            TNode result;
            result["name"] = info.Name;
            result["email"] = info.Email;
            return result;
        })
        | YtWrite(
            "//tmp/ermolovd-tt",
            TTableSchema()
                .AddColumn("name", NTi::String())
                .AddColumn("email", NTi::String())
        );

    pipeline.Run();
    return 0;
}
