#include "init.h"

#include <mapreduce/yt/interface/init.h>

#include <mapreduce/yt/common/abortable_registry.h>
#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/wait_proxy.h>
#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/interface/operation.h>
#include <mapreduce/yt/io/job_reader.h>

#include <library/threading/future/async.h>
#include <library/sighandler/async_signals_handler.h>

#include <util/folder/dirut.h>
#include <util/generic/singleton.h>
#include <util/string/cast.h>
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

class TAbnormalTerminator
 {
 public:
     TAbnormalTerminator() = default;

     static void SetErrorTerminationHandler()
     {
         Instance().DefaultHandler_ = std::get_terminate();
         Instance().HandlerThread_.Reset(CreateMtpQueue(1));

         std::set_terminate(&TerminateHandler);
         SetAsyncSignalFunction(SIGINT, SignalHandler);
         SetAsyncSignalFunction(SIGTERM, SignalHandler);
     }

 private:
     static TAbnormalTerminator& Instance()
     {
         return *Singleton<TAbnormalTerminator>();
     }

     static void TerminateWithTimeout(const TDuration& timeout, const std::function<void(void)>& exitFunction)
     {
         auto result = NThreading::Async([&] {
                 NDetail::TAbortableRegistry::Get()->AbortAllAndBlockForever();
             },
             *Instance().HandlerThread_);
         Sleep(timeout);
         exitFunction();
     }

     static void SignalHandler(int signalNumber)
     {
         LOG_INFO("Signal %d received, aborting transactions. Waiting 5 seconds...", signalNumber);
         TerminateWithTimeout(
             TDuration::Seconds(5),
             [&] {
                 _exit(-signalNumber);
             });
     }

     static void TerminateHandler()
     {
         LOG_INFO("Terminate called, aborting transactions. Waiting 5 seconds...");
         TerminateWithTimeout(
             TDuration::Seconds(5),
             [&] {
                 if (Instance().DefaultHandler_) {
                     Instance().DefaultHandler_();
                 } else {
                     abort();
                 }
             });
     }

 private:
     THolder<IMtpQueue> HandlerThread_;
     std::terminate_handler DefaultHandler_ = nullptr;
 };

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

EInitStatus& GetInitStatus()
{
    static EInitStatus initStatus = IS_NOT_INITIALIZED;
    return initStatus;
}

} // namespace NDetail

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

    NDetail::GetInitStatus() = NDetail::IS_INITIALIZED;

    const bool isInsideJob = !GetEnv("YT_JOB_ID").empty();
    if (!isInsideJob) {
        if (FromString<bool>(GetEnv("YT_CLEANUP_ON_TERMINATION", "0")) || options.CleanupOnTermination_) {
            TAbnormalTerminator::SetErrorTerminationHandler();
        }
        if (options.WaitProxy_) {
            NDetail::TWaitProxy::Get() = options.WaitProxy_;
        }
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

