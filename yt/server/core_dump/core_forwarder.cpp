#include "core_forwarder.h"

#include "core_processor_service_proxy.h"

#include <yt/server/core_dump/core_processor_service.pb.h>

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

static constexpr i64 CoreDumpBlockSize = 16 * 1024 * 1024;
static constexpr i64 MaximumConcurrentRequestSize = 100 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TCoreForwarder::TCoreForwarder()
{
    openlog("ytserver-core-forwarder", LOG_PID | LOG_PERROR, LOG_USER);
}

TCoreForwarder::~TCoreForwarder()
{
    closelog();
}

void TCoreForwarder::ParseArgs(const std::vector<Stroka>& args)
{
    if (args.size() != 6) {
        THROW_ERROR_EXCEPTION("There should be exactly 6 arguments to ytserver --core-forwarder");
    }
    ProcessId_ = FromString<int>(args[0]);
    UserId_ = FromString<int>(args[1]);
    ExecutableName_ = args[2];
    RLimitCore_ = FromString<i64>(args[3]);
    JobProxySocketNameDirectory_ = args[4];
    FallbackPath_ = args[5];

    syslog(LOG_INFO,
        "Processing core dump (Uid: %d, Pid: %d, ExecutableName: %s, RLimitCore: %" PRId64 ", FallbackPath: %s)",
        UserId_,
        ProcessId_,
        ExecutableName_.data(),
        RLimitCore_,
        FallbackPath_.data());
}

void TCoreForwarder::WriteCoreToDisk()
{
    // We do not fully imitate the system core dump logic here. We only check if
    // core limit is not zero, and then write the whole core dump without truncating
    // it to first RLIMIT_CORE bytes.

    if (RLimitCore_ == 0) {
        // Do nothing.
        syslog(LOG_INFO, "Doing nothing (RLimitCore: 0)");
        return;
    } else {
        syslog(LOG_INFO, "Writing core to fallback path (FallbackPath: %s)", FallbackPath_.data());
        TFile coreFile(FallbackPath_, CreateNew | WrOnly | Seq | CloseOnExec);
        TFileOutput coreFileOutput(coreFile);
        i64 size = Cin.ReadAll(coreFileOutput);
        syslog(LOG_INFO, "Finished writing core to disk (Size: %" PRId64 ")", size);
    }
}

void TCoreForwarder::ForwardCore(const Stroka& socketName)
{
    syslog(LOG_INFO, "Sending core to job proxy (SocketName: %s)", socketName.data());
    auto coreProcessorClient = CreateTcpBusClient(TTcpBusClientConfig::CreateUnixDomain(socketName));
    auto coreProcessorChannel = CreateBusChannel(coreProcessorClient);
    TCoreProcessorServiceProxy proxy(coreProcessorChannel);

    Stroka namedPipePath;

    // Ask job proxy if it needs such a core dump.
    {
        auto req = proxy.StartCoreDump();
        req->set_process_id(ProcessId_);
        req->set_executable_name(ExecutableName_);
        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();
        namedPipePath = rsp->named_pipe_path();
    }

    syslog(LOG_INFO, "Writing core to the named pipe (NamedPipePath: %s)", namedPipePath.data());

    TFileOutput output(namedPipePath);
    Cin.ReadAll(output);
    syslog(LOG_INFO, "Finished writing core to the named pipe");
}

void TCoreForwarder::GuardedMain(const std::vector<Stroka>& args)
{
    ParseArgs(args);

    Stroka jobProxySocketNameFile = JobProxySocketNameDirectory_ + "/" + ToString(UserId_);
    if (Exists(jobProxySocketNameFile)) {
        auto jobProxySocketName = TFileInput(jobProxySocketNameFile).ReadLine();
        ForwardCore(jobProxySocketName);
    } else {
        WriteCoreToDisk();
    }
}

bool TCoreForwarder::Main(const std::vector<Stroka>& args)
{
    try {
        GuardedMain(args);
        return false;
    } catch (const std::exception& ex) {
        syslog(LOG_ERR, "%s", ex.what());
        return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT
