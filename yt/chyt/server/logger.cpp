#include "logger.h"

#include "helpers.h"

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/profiling/timing.h>

#include <util/system/thread.h>

namespace NYT::NClickHouseServer {

using namespace NLogging;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TLogChannel
    : public DBPoco::Channel
{
private:
    const TLogger& Logger;

public:
    TLogChannel(const TLogger& logger)
        : Logger(logger)
    { }

    void log(const DBPoco::Message& message) override
    {
        NLogging::TLogEvent event;
        event.Category = Logger.GetCategory();
        event.Level = GetLogLevel(message.getPriority());
        event.MessageRef = TSharedRef::FromString(Format(
            "[%v] %v",
            GetOriginalLevelLetter(message.getPriority()),
            TString(message.getText())));
        event.Instant = GetCpuInstant();
        event.ThreadId = TThread::CurrentThreadId();

        Logger.Write(std::move(event));
    }

private:
    static ELogLevel GetLogLevel(DBPoco::Message::Priority priority)
    {
        switch (priority) {
            case DBPoco::Message::PRIO_FATAL:
            case DBPoco::Message::PRIO_CRITICAL:
                return ELogLevel::Fatal;
            // ClickHouse often puts user errors into error level, which we
            // do not like to see in our logs. Thus, we always put its messages to
            // Debug level.
            case DBPoco::Message::PRIO_ERROR:
            case DBPoco::Message::PRIO_WARNING:
            case DBPoco::Message::PRIO_NOTICE:
            case DBPoco::Message::PRIO_INFORMATION:
                return ELogLevel::Info;
            case DBPoco::Message::PRIO_DEBUG:
                return ELogLevel::Debug;
            case DBPoco::Message::PRIO_TRACE:
            case DBPoco::Message::PRIO_TEST:
                return ELogLevel::Trace;
        }
    }

    static char GetOriginalLevelLetter(DBPoco::Message::Priority priority)
    {
        constexpr const char* Letters = "?FCEWNIDTT";

        YT_VERIFY(priority > 0 && priority < strlen(Letters));
        return Letters[priority];
    }
};

////////////////////////////////////////////////////////////////////////////////

DBPoco::AutoPtr<DBPoco::Channel> CreateLogChannel(const TLogger& logger)
{
    return new TLogChannel(logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
