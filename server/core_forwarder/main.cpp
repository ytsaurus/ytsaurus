#include <yt/server/core_dump/core_processor_service_proxy.h>
#include <yt/server/core_dump/core_processor_service.pb.h>

#include <yt/server/program/program.h>

#include <yt/core/misc/fs.h>

#include <yt/core/bus/config.h>
#include <yt/core/bus/tcp_client.h>

#include <yt/core/rpc/bus_channel.h>

#include <util/system/file.h>
#include <util/stream/file.h>

#include <syslog.h>

namespace NYT {
namespace NCoreDump {

using namespace NFS;
using namespace NProto;
using namespace NConcurrency;
using namespace NBus;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TCoreForwarderProgram
    : public TYTProgram
{
public:
    TCoreForwarderProgram()
    {
        Opts_.SetFreeArgsNum(6, 7);
        Opts_.SetFreeArgTitle(0, "PID");
        Opts_.SetFreeArgTitle(1, "UID");
        Opts_.SetFreeArgTitle(2, "EXECUTABLE_NAME");
        Opts_.SetFreeArgTitle(3, "RLIMIT_CORE");
        Opts_.SetFreeArgTitle(4, "JOB_PROXY_SOCKET_DIRECTORY");
        Opts_.SetFreeArgTitle(5, "FALLBACK_PATH");
        Opts_.SetFreeArgTitle(6, "JOB_PROXY_SOCKET");

        openlog("ytserver-core-forwarder", LOG_PID | LOG_PERROR, LOG_USER);
    }

    ~TCoreForwarderProgram()
    {
        closelog();
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::CurrentThreadSetName("CoreForwarder");

        ParseFreeArgs(parseResult);

        if (RLimitCore_ == 0) {
            // Do nothing.
            syslog(LOG_INFO, "Doing nothing (RLimitCore: 0)");
            return;
        }

        TString jobProxySocketNameFile = JobProxySocketNameDirectory_ + "/" + ToString(UserId_);
        if (JobProxySocketPath_ || Exists(jobProxySocketNameFile)) {
            auto jobProxySocketName = JobProxySocketPath_
                ? JobProxySocketPath_.Get()
                : TUnbufferedFileInput(jobProxySocketNameFile).ReadLine();
            ForwardCore(jobProxySocketName);
        } else {
            WriteCoreToDisk();
        }
    }

    virtual void OnError(const TString& message) const noexcept override
    {
        syslog(LOG_ERR, "%s", message.c_str());
    }

    void ParseFreeArgs(const NLastGetopt::TOptsParseResult& parseResult)
    {
        auto args = parseResult.GetFreeArgs();

        ProcessId_ = FromString<int>(args[0]);
        UserId_ = FromString<int>(args[1]);
        ExecutableName_ = args[2];
        RLimitCore_ = FromString<ui64>(args[3]);
        JobProxySocketNameDirectory_ = args[4];
        FallbackPath_ = args[5];
        if (args.size() == 7) {
            JobProxySocketPath_ = args[6];
        }

        syslog(LOG_INFO,
            "Processing core dump (Pid: %d, Uid: %d, ExecutableName: %s, RLimitCore: %" PRId64 ", FallbackPath: %s)",
            ProcessId_,
            UserId_,
            ExecutableName_.c_str(),
            RLimitCore_,
            FallbackPath_.c_str());
    }

    void WriteCoreToDisk()
    {
        // We do not fully imitate the system core dump logic here. We only check if
        // core limit is not zero, and then write the whole core dump without truncating
        // it to first RLIMIT_CORE bytes.
        syslog(LOG_INFO, "Writing core to fallback path (FallbackPath: %s)", FallbackPath_.c_str());
        TFile coreFile(FallbackPath_, CreateNew | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput coreFileOutput(coreFile);
        i64 size = Cin.ReadAll(coreFileOutput);
        syslog(LOG_INFO, "Finished writing core to disk (Size: %" PRId64 ")", size);
    }

    void ForwardCore(const TString& socketName)
    {
        syslog(LOG_INFO, "Sending core to job proxy (SocketName: %s)", socketName.c_str());

        auto coreProcessorClient = CreateTcpBusClient(TTcpBusClientConfig::CreateUnixDomain(socketName));
        auto coreProcessorChannel = CreateBusChannel(coreProcessorClient);

        TCoreProcessorServiceProxy proxy(coreProcessorChannel);

        TString namedPipePath;

        // Ask job proxy if it needs such a core dump.
        {
            auto req = proxy.StartCoreDump();
            req->set_process_id(ProcessId_);
            req->set_executable_name(ExecutableName_);
            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();
            namedPipePath = rsp->named_pipe_path();
        }

        syslog(LOG_INFO, "Writing core to the named pipe (NamedPipePath: %s)", namedPipePath.c_str());

        TUnbufferedFileOutput namedPipeOutput(namedPipePath);
        i64 size = Cin.ReadAll(namedPipeOutput);
        syslog(LOG_INFO, "Finished writing core to the named pipe (Size: %" PRId64 ")", size);
    }

    int ProcessId_ = -1;
    int UserId_ = -1;

    TString ExecutableName_;

    ui64 RLimitCore_ = -1;

    TString JobProxySocketNameDirectory_;
    TString FallbackPath_;
    TNullable<TString> JobProxySocketPath_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::NCoreDump::TCoreForwarderProgram().Run(argc, argv);
}

