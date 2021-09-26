#include "logging.h"

#include <mutex>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

void EnableLogging(TLogHandler logHandler)
{
    static std::once_flag Initialized;
    std::call_once(Initialized, [] () {
        LogManager.Construct();
    });

    LogManager->EnableLogging(logHandler);
}
    
////////////////////////////////////////////////////////////////////////////////

void TLogManager::EnableLogging(TLogHandler logHandler)
{
    LogHandler_.store(logHandler);
}

// Checks (in a racy way) that logging is enabled.
bool TLogManager::IsLoggingEnabled()
{
    return LogHandler_.load() != nullptr;
}

// A special case of zero args.
void TLogManager::LogMessage(ELogEventSeverity severity, const char* message)
{
    LogMessage(severity, "%s", message);
}

////////////////////////////////////////////////////////////////////////////////

TExplicitlyConstructableSingleton<TLogManager> LogManager{};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
