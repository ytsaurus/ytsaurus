#include "logger.h"

#include <yt/core/logging/log.h>
#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TLogger
    : public NInterop::ILogger
{
private:
    NLogging::TLogger Logger;

public:
    TLogger(const NLogging::TLogger& logger)
        : Logger(logger)
    {}

    void Write(const NInterop::TLogEvent& e) override
    {
        NLogging::TLogEvent event;
        event.Category = Logger.GetCategory();
        event.Level = static_cast<NLogging::ELogLevel>(e.Level);
        event.Message = e.Message;
        // YT use CPU timestamp counters instead of unix timestamps
        event.Instant = NProfiling::InstantToCpuInstant(e.Timestamp);
        event.ThreadId = e.ThreadId;

        Logger.Write(std::move(event));
    }
};

////////////////////////////////////////////////////////////////////////////////

NInterop::ILoggerPtr CreateLogger(const NLogging::TLogger& logger)
{
    return std::make_shared<TLogger>(logger);
}

}   // namespace NYT
}   // namespace NClickHouse
