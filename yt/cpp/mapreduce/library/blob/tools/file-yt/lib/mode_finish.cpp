#include "modes.h"

#include <yt/cpp/mapreduce/library/blob/tools/file-yt/protos/config.pb.h>

#include <yt/cpp/mapreduce/library/blob/api.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/guid.h>

#include <cstdlib>

static NFileYtTool::TFinishConfig ParseOptions(const int argc, const char* argv[]) {
    NFileYtTool::TFinishConfig c;
    auto p = NLastGetopt::TOpts::Default();
    p.SetTitle("do preprocessing on YT file table to enable download from it");
    p.AddLongOption('p', "yt-proxy")
     .Required()
     .RequiredArgument("YT_PROXY")
     .Handler1T<TString>([&c](const auto& v) {
         c.SetProxy(v);
     })
     .Help("YT cluster proxy");
    p.AddLongOption('t', "yt-table")
     .Required()
     .RequiredArgument("TABLE")
     .Handler1T<TString>([&c](const auto& v) {
         c.SetTable(v);
     })
     .Help("table with files");
    p.AddLongOption("tx")
     .Optional()
     .RequiredArgument("GUID")
     .Handler1T<TString>([&c](const auto& v) {
         c.SetTransactionID(v);
     })
     .Help("transaction ID to attach");
    p.AddLongOption("unique")
     .Optional()
     .NoArgument()
     .Handler0([&c] {
         c.SetUniqueFiles(true);
     })
     .Help("use this flag only when you are sure that files in the table are unique, otherwise"
           " operation will fail");
    p.SetFreeArgsNum(0);
    NLastGetopt::TOptsParseResult{&p, argc, argv};
    c.CheckInitialized();
    return c;
}

static int Main(const NFileYtTool::TFinishConfig& config) {
    NYT::SetLogger(NYT::CreateStdErrLogger(NYT::ILogger::INFO));
    NYT::IClientBasePtr client = NYT::CreateClient(config.GetProxy());
    if (config.HasTransactionID()) {
        const auto transactionId = [&config]{
            TGUID guid;
            Y_ENSURE(GetGuid(config.GetTransactionID(), guid));
            return guid;
        }();
        client = dynamic_cast<NYT::IClient*>(client.Get())->AttachTransaction(transactionId);
    }

    NYtBlob::Finish(config.GetTable(), client, config.GetUniqueFiles());

    return EXIT_SUCCESS;
}

static int Main(const int argc, const char* argv[]) {
    const auto c = ParseOptions(argc, argv);
    return Main(c);
}

int NFileYtTool::MainFinish(const int argc, const char* argv[]) {
    return ::Main(argc, argv);
}
