#include "job_signal.h"

#include <yt/core/misc/error.h>

#include <map>

namespace NYT::NJobProberClient {

////////////////////////////////////////////////////////////////////////////////

std::optional<int> FindSignalIdBySignalName(const TString& signalName)
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
    return it == SignalNameToNumber.end() ? std::nullopt : std::make_optional(it->second);
}

void ValidateSignalName(const TString& signalName)
{
    auto signal = FindSignalIdBySignalName(signalName);
    if (!signal) {
        THROW_ERROR_EXCEPTION("Unsupported signal name %Qv", signalName);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProberClient
