#include "modes.h"

#include <mapreduce/yt/library/blob/tools/file-yt/protos/config.pb.h>

#include <mapreduce/yt/library/blob/api.h>

#include <mapreduce/yt/interface/client.h>

#include <mapreduce/yt/interface/logging/logger.h>
#include <mapreduce/yt/interface/logging/yt_log.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/guid.h>
#include <util/generic/xrange.h>

#include <cstdlib>

static NFileYtTool::TUploadConfig ParseOptions(const int argc, const char* argv[]) {
    NFileYtTool::TUploadConfig c;
    auto p = NLastGetopt::TOpts::Default();
    p.SetTitle("upload files to YT table");
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
    p.AddLongOption("path")
     .Required()
     .RequiredArgument("PATH")
     .Handler1T<TString>([&c](const auto& v) {
         *c.AddFilePaths() = v;
     })
     .Help("document to upload (can read from standard input if \"-\" provided)");
    p.AddLongOption("name")
     .Required()
     .RequiredArgument("NAME")
     .Handler1T<TString>([&c](const auto& v) {
         *c.AddFileNames() = v;
     })
     .Help("file name (or ID) in the table");
    p.AddLongOption("create-table")
     .NoArgument()
     .Handler0([&c]{
         c.SetCreateTableIfDoesntExists(true);
     })
     .Help("create table if it didn't exist");
    p.SetFreeArgsNum(0);
    NLastGetopt::TOptsParseResult{&p, argc, argv};
    Y_ENSURE(c.FilePathsSize() == c.FileNamesSize());
    c.CheckInitialized();
    return c;
}

static int Main(const NFileYtTool::TUploadConfig& config) {
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

    if (!client->Exists(config.GetTable())) {
        if (config.GetCreateTableIfDoesntExists()) {
            NYtBlob::CreateTable(config.GetTable(), client);
        } else {
            Y_FAIL("table '%s' doesn't exists", config.GetTable().data());
        }
    }

    for (const auto i : xrange(config.FilePathsSize())) {
        const auto& path = config.GetFilePaths(i);
        if (path == "-") {
            NYtBlob::Upload(
                Cin,
                config.GetFileNames(i),
                config.GetTable(),
                client);
        } else {
            NYtBlob::Upload(
                config.GetFilePaths(i),
                config.GetFileNames(i),
                config.GetTable(),
                client);
        }
    }

    return EXIT_SUCCESS;
}

static int Main(const int argc, const char* argv[]) {
    const auto c = ParseOptions(argc, argv);
    return Main(c);
}

int NFileYtTool::MainUpload(const int argc, const char* argv[]) {
    return ::Main(argc, argv);
}
