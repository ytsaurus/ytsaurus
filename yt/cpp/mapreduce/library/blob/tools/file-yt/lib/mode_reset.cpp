#include "modes.h"

#include <yt/cpp/mapreduce/library/blob/tools/file-yt/protos/config.pb.h>

#include <yt/cpp/mapreduce/library/blob/api.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/guid.h>

#include <cstdlib>

static NFileYtTool::TResetConfig ParseOptions(const int argc, const char* argv[]) {
    NFileYtTool::TResetConfig c;
    auto p = NLastGetopt::TOpts::Default();
    p.SetTitle("make table writable again (after `finish`)");
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
    p.SetFreeArgsNum(0);
    NLastGetopt::TOptsParseResult{&p, argc, argv};
    c.CheckInitialized();
    return c;
}

static int Main(const NFileYtTool::TResetConfig& config) {
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

    NYtBlob::Reset(config.GetTable(), client);

    return EXIT_SUCCESS;
}

static int Main(const int argc, const char* argv[]) {
    const auto c = ParseOptions(argc, argv);
    return Main(c);
}

int NFileYtTool::MainReset(const int argc, const char* argv[]) {
    return ::Main(argc, argv);
}

