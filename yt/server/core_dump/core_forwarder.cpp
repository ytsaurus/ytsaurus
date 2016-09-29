#include "core_forwarder.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/fs.h>

#include <util/system/file.h>
#include <util/stream/file.h>

#include <syslog.h>

namespace NYT {
namespace NCoreDump {

using namespace NFS;

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
    RlimitCore_ = FromString<i64>(args[3]);
    JobProxyUdsNameDirectory_ = args[4];
    FallbackPath_ = args[5];

    syslog(LOG_INFO,
        "Processing core dump (Uid: %d, Pid: %d, ExecutableName: %s, RlimitCore: %" PRId64 ")",
        UserId_,
        ProcessId_,
        ExecutableName_.data(),
        RlimitCore_);
}

void TCoreForwarder::WriteCoreToDisk()
{
    // We do not fully imitate the system core dump logic here. We only check if
    // core limit is not zero, and then write the whole core dump without truncating
    // it to first RLIMIT_CORE bytes.

    if (RlimitCore_ == 0) {
        // Do nothing.
        syslog(LOG_INFO, "Doing nothing since RlimitCore = 0");
        return;
    } else {
        syslog(LOG_INFO, "Writing core to fallback path (FallbackPath: %s)", FallbackPath_.data());
        TFile coreFile(FallbackPath_, CreateNew | WrOnly | Seq | CloseOnExec);
        TFileOutput coreFileOutput(coreFile);
        i64 size = Cin.ReadAll(coreFileOutput);
        syslog(LOG_INFO, "Finished writing core to disk (Size: %" PRId64 ")", size);
    }
}

void TCoreForwarder::ForwardCore(const Stroka& udsName)
{
    syslog(LOG_INFO, "Sending core to job proxy (UnixDomainSocketName: %s)", udsName.data());
}

void TCoreForwarder::GuardedMain(const std::vector<Stroka>& args)
{
    ParseArgs(args);

    Stroka jobProxyUdsNameFile = JobProxyUdsNameDirectory_ + "/" + ToString(UserId_);
    if (Exists(jobProxyUdsNameFile)) {
        Stroka jobProxyUdsName = TFileInput(jobProxyUdsNameFile).ReadLine();
        ForwardCore(jobProxyUdsName);
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
