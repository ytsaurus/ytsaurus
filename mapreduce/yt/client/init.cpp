#include <mapreduce/yt/interface/init.h>

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/wait_proxy.h>
#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/interface/operation.h>
#include <mapreduce/yt/io/job_reader.h>

#include <util/string/cast.h>
#include <util/folder/dirut.h>
#include <util/system/env.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

void WriteVersionToLog()
{
    LOG_INFO("Wrapper version: %s", ~TProcessState::Get()->ClientVersion);
}

static TNode SecureVaultContents; // safe

void InitializeSecureVault()
{
    SecureVaultContents = NodeFromYsonString(
        GetEnv("YT_SECURE_VAULT", "{}"));
}

}

////////////////////////////////////////////////////////////////////////////////

const TNode& GetJobSecureVault()
{
    return SecureVaultContents;
}

////////////////////////////////////////////////////////////////////////////////

void Initialize(int argc, const char* argv[], const TInitializeOptions& options)
{
    auto logLevelStr = to_lower(TConfig::Get()->LogLevel);
    ILogger::ELevel logLevel;

    if (!TryFromString(logLevelStr, logLevel)) {
        Cerr << "Invalid log level: " << TConfig::Get()->LogLevel << Endl;
        exit(1);
    }

    SetLogger(CreateStdErrLogger(logLevel));

    TProcessState::Get()->SetCommandLine(argc, argv);

    if (options.WaitProxy_) {
        NDetail::TWaitProxy::Get() = options.WaitProxy_;
    }

    const bool isInsideJob = !GetEnv("YT_JOB_ID").empty();
    if (!isInsideJob) {
        WriteVersionToLog();
        return;
    }

    TString jobType = argv[1];
    if (argc != 5 || jobType != "--yt-map" && jobType != "--yt-reduce") {
        // We are inside job but probably using old API
        // (i.e. both NYT::Initialize and NMR::Initialize are called).
        WriteVersionToLog();
        return;
    }

    InitializeSecureVault();

    TString jobName(argv[2]);
    size_t outputTableCount = FromString<size_t>(argv[3]);
    int hasState = FromString<int>(argv[4]);

    THolder<IInputStream> jobStateStream;
    if (hasState) {
        jobStateStream = new TIFStream("jobstate");
    } else {
        jobStateStream = new TBufferStream(0);
    }
    exit(TJobFactory::Get()->GetJobFunction(~jobName)(outputTableCount, *jobStateStream));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

