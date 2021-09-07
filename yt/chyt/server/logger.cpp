#include "logger.h"

#include "helpers.h"

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/profiling/timing.h>

namespace NYT::NClickHouseServer {

using namespace NLogging;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TLogChannel
    : public Poco::Channel
{
private:
    const TLogger& Logger;

public:
    TLogChannel(const TLogger& logger)
        : Logger(logger)
    { }

    void log(const Poco::Message& message) override
    {
        NLogging::TLogEvent event;
        event.Category = Logger.GetCategory();
        event.Level = GetLogLevel(message.getPriority());
        event.Message = TSharedRef::FromString(Format(
            "[%v] %v",
            GetOriginalLevelLetter(message.getPriority()),
            MaybeTruncateSubquery(TString(message.getText()))));
        event.Instant = GetCpuInstant();
        event.ThreadId = TThread::CurrentThreadId();

        Logger.Write(std::move(event));
    }

private:
    static ELogLevel GetLogLevel(Poco::Message::Priority priority)
    {
        switch (priority) {
            case Poco::Message::PRIO_FATAL:
            case Poco::Message::PRIO_CRITICAL:
                return ELogLevel::Fatal;
            // ClickHouse often puts user errors into error level, which we
            // do not like to see in our logs. Thus, we always put its messages to
            // Debug level.
            case Poco::Message::PRIO_ERROR:
            case Poco::Message::PRIO_WARNING:
            case Poco::Message::PRIO_NOTICE:
            case Poco::Message::PRIO_INFORMATION:
                return ELogLevel::Info;
            case Poco::Message::PRIO_DEBUG:
                return ELogLevel::Debug;
            case Poco::Message::PRIO_TRACE:
            case Poco::Message::PRIO_TEST:
                return ELogLevel::Trace;
        }
    }

    static char GetOriginalLevelLetter(Poco::Message::Priority priority)
    {
        constexpr const char* Letters = "?FCEWNIDT";

        return Letters[priority];
    }
};

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Channel> CreateLogChannel(const TLogger& logger)
{
    return new TLogChannel(logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
