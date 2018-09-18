#include "job_signal.h"

#include <yt/core/misc/error.h>

#include <map>

namespace NYT {
namespace NJobProberClient {

////////////////////////////////////////////////////////////////////////////////

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
    if (!signal.HasValue()) {
        THROW_ERROR_EXCEPTION("Unsupported signal name %Qv", signalName);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProberClient
} // namespace NYT
