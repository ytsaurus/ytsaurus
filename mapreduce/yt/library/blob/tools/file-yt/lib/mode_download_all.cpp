#include "modes.h"

#include <mapreduce/yt/library/blob/tools/file-yt/protos/config.pb.h>

#include <mapreduce/yt/library/blob/api.h>
#include <mapreduce/yt/library/blob/protos/info.pb.h>

#include <mapreduce/yt/interface/client.h>

#include <mapreduce/yt/interface/logging/logger.h>
#include <mapreduce/yt/interface/logging/yt_log.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/vector.h>
#include <util/random/entropy.h>
#include <util/random/fast.h>
#include <util/system/info.h>
#include <util/system/yassert.h>
#include <util/thread/pool.h>

#include <cstdlib>

static NFileYtTool::TDownloadAllConfig ParseOptions(const int argc, const char* argv[]) {
    NFileYtTool::TDownloadAllConfig c;
    auto p = NLastGetopt::TOpts::Default();
    p.SetTitle("download all files from YT table");
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
    p.AddLongOption("out-dir")
     .Required()
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
    p.AddLongOption('j', "download-threads")
     .DefaultValue(ToString((NSystemInfo::CachedNumberOfCpus() + 1) / 2))
     .RequiredArgument("INT")
     .Handler1T<TString>([&c](const auto& v) {
         c.SetDownloadThreadCount(FromString<ui32>(v));
     })
     .Help("number of threads to use for files download");
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
    return c;
}

static void Download(
    const TString& path, const TString& name,
    const ::NYT::TYPath& table, NYT::IClientBasePtr client,
    const bool failProgramOnDownloadFailure,
    const bool downloadToMemoryFirst) {

    const auto downloaded = NYtBlob::TryDownload(path, name, table, client, downloadToMemoryFirst);
    Y_VERIFY(!failProgramOnDownloadFailure || downloaded, "name=\"%s\"", name.data());
}

static int Main(
    const NFileYtTool::TDownloadAllConfig& config,
    NYT::IClientBasePtr client) {

    const auto q = CreateThreadPool(config.GetDownloadThreadCount(), 10);
    TFastRng<ui64> prng{Seed()};

    const TFsPath dir = config.GetOutDirectory();
    for (const auto& i : NYtBlob::List(config.GetTable(), client)) {
        const auto path = dir / i.GetName();
        const auto f = [path, i, &config, client] {
            Download(
                path, i.GetName(), config.GetTable(), client,
                !config.GetIgnoreInvalid(), config.GetDownloadToMemoryFirst());
        };
        for (bool added = false; !added;) {
            if (added = q->AddFunc(f)) {
                break;
            }
            Sleep(TDuration::MilliSeconds(prng.Uniform(200)));
        }
    }

    return EXIT_SUCCESS;
}

static int Main(const NFileYtTool::TDownloadAllConfig& config) {
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
    return Main(config, client);
}

static int Main(const int argc, const char* argv[]) {
    const auto c = ParseOptions(argc, argv);
    return Main(c);
}

int NFileYtTool::MainDownloadAll(const int argc, const char* argv[]) {
    return ::Main(argc, argv);
}
