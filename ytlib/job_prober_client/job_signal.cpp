#include "job_signal.h"

#include <yt/core/misc/error.h>

#include <map>

namespace NYT {
namespace NJobProberClient {

TNullable<int> FindSignalIdBySignalName(const Stroka& signalName)
{
    static std::map<Stroka, int> signalNumbersByName = {
        { "SIGHUP",  SIGHUP },
        { "SIGINT",  SIGINT },
        { "SIGALRM", SIGALRM },
        { "SIGKILL", SIGKILL },
        { "SIGTERM", SIGTERM },
        { "SIGUSR1", SIGUSR1 },
        { "SIGUSR2", SIGUSR2 },
    };

    TNullable<int> result;

    auto it = signalNumbersByName.find(signalName);
    if (it != signalNumbersByName.end()) {
        result = it->second;
    }
    return result;
}

void ValidateSignalName(const Stroka& signalName)
{
    auto signal = FindSignalIdBySignalName(signalName);
    if (!signal.HasValue()) {
        THROW_ERROR_EXCEPTION("Unsupported signal name %Qv", signalName);
    }
}

} // namespace NJobProberClient
} // namespace NYT
