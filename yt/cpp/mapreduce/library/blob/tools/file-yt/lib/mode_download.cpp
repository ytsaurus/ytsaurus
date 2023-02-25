#include "modes.h"

#include <yt/cpp/mapreduce/library/blob/tools/file-yt/protos/config.pb.h>

#include <yt/cpp/mapreduce/library/blob/api.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/xrange.h>

#include <cstdlib>

static NFileYtTool::TDownloadConfig ParseOptions(const int argc, const char* argv[]) {
    NFileYtTool::TDownloadConfig c;
    auto p = NLastGetopt::TOpts::Default();
    p.SetTitle("download files from YT table");
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
     .Help("table to download from");
    p.AddLongOption("tx")
     .Optional()
     .RequiredArgument("GUID")
     .Handler1T<TString>([&c](const auto& v) {
         c.SetTransactionID(v);
     })
     .Help("transaction ID to attach");
    p.AddLongOption("name")
     .Required()
     .RequiredArgument("NAME")
     .Handler1T<TString>([&c](const auto& v) {
         *c.AddFileNames() = v;
     })
     .Help("file name (or ID) in the table");
    p.AddLongOption("path")
     .Optional()
     .RequiredArgument("FILE")
     .Handler1T<TString>([&c](const auto& v) {
         *c.AddFilePaths() = v;
     })
     .Help("document will be written to the FILE");
    p.AddLongOption("out-dir")
     .Optional()
     .RequiredArgument("DIR")
     .Handler1T<TString>([&c](const auto& v) {
         c.SetOutDirectory(v);
     })
     .Help("each document will written to DIR/NAME");
    p.AddLongOption("ignore-invalid")
     .NoArgument()
     .Handler0([&c] {
         c.SetIgnoreInvalid(true);
     })
     .Help("skip invalid documents");
    p.AddLongOption("download-to-memory-first")
     .NoArgument()
     .Handler0([&c] {
         c.SetDownloadToMemoryFirst(true);
     })
     .Help("instead of downloading file chunk to memory and instantly writing it the disk, will"
           " download entire file to the memory first and then write it to the disk");
    p.SetFreeArgsNum(0);
    NLastGetopt::TOptsParseResult{&p, argc, argv};
    c.CheckInitialized();
    Y_ENSURE(
        (c.HasOutDirectory() && c.GetFilePaths().empty()) ||
        (!c.HasOutDirectory() && c.FilePathsSize() == c.FileNamesSize()));
    return c;
}

static void Download(
    IOutputStream& out, const TString& name,
    const ::NYT::TYPath& table, NYT::IClientBasePtr client,
    const bool failProgramOnDownloadFailure) {

    const auto downloaded = NYtBlob::TryDownload(out, name, table, client);
    Y_VERIFY(!failProgramOnDownloadFailure || downloaded, "name=\"%s\"", name.data());
}

static void Download(
    const TString& path, const TString& name,
    const ::NYT::TYPath& table, NYT::IClientBasePtr client,
    const bool failProgramOnDownloadFailure,
    const bool downloadToMemoryFirst) {

    const auto downloaded = NYtBlob::TryDownload(path, name, table, client, downloadToMemoryFirst);
    Y_VERIFY(!failProgramOnDownloadFailure || downloaded, "name=\"%s\"", name.data());
}

static int Main(const NFileYtTool::TDownloadConfig& config) {
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

    if (config.HasOutDirectory()) {
        const TFsPath dir = config.GetOutDirectory();
        for (const auto& name : config.GetFileNames()) {
            const auto outPath = dir / name;
            Download(
                outPath, name, config.GetTable(), client,
                !config.GetIgnoreInvalid(), config.GetDownloadToMemoryFirst());
        }
    } else {
        for (const auto i : xrange(config.FilePathsSize())) {
            const auto& path = config.GetFilePaths(i);
            if (path == "-") {
                Download(
                    Cout, config.GetFileNames(i), config.GetTable(), client,
                    !config.GetIgnoreInvalid());
            } else {
                Download(
                    config.GetFilePaths(i), config.GetFileNames(i), config.GetTable(), client,
                    !config.GetIgnoreInvalid(), config.GetDownloadToMemoryFirst());
            }
        }
    }

    return EXIT_SUCCESS;
}

static int Main(const int argc, const char* argv[]) {
    const auto c = ParseOptions(argc, argv);
    return Main(c);
}

int NFileYtTool::MainDownload(const int argc, const char* argv[]) {
    return ::Main(argc, argv);
}
