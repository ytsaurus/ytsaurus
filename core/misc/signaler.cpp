#include "signaler.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/proc.h>
#include <yt/core/misc/subprocess.h>

#include <yt/core/tools/registry.h>
#include <yt/core/tools/tools.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/serialize.h>

namespace NYT {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

// NB: Moving this into the header will break tool registration as the linker
// will optimize it out.
TSignalerArg::TSignalerArg()
{
    RegisterParameter("pids", Pids)
        .Default();
    RegisterParameter("signal_name", SignalName)
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

void TSignalerTool::operator()(const TSignalerArgPtr& arg) const
{
    SafeSetUid(0);
    return SendSignal(arg->Pids, arg->SignalName);
}

////////////////////////////////////////////////////////////////////////////////

void SendSignal(const std::vector<int>& pids, const TString& signalName)
{
    ValidateSignalName(signalName);
    auto sig = FindSignalIdBySignalName(signalName);
    for (int pid : pids) {
        kill(pid, *sig);
    }
}

TNullable<int> FindSignalIdBySignalName(const TString& signalName)
{
    static THashMap<TString, int> SignalNameToNumber = {
        { "SIGHUP",  SIGHUP },
        { "SIGINT",  SIGINT },
        { "SIGALRM", SIGALRM },
        { "SIGKILL", SIGKILL },
        { "SIGTERM", SIGTERM },
        { "SIGUSR1", SIGUSR1 },
        { "SIGUSR2", SIGUSR2 },
        { "SIGURG", SIGURG },
    };

    auto it = SignalNameToNumber.find(signalName);
    return it == SignalNameToNumber.end() ? Null : MakeNullable(it->second);
}

void ValidateSignalName(const TString& signalName)
{
    auto signal = FindSignalIdBySignalName(signalName);
    if (!signal) {
        THROW_ERROR_EXCEPTION("Unsupported signal name %Qv", signalName);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
