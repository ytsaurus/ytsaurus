#include "logger.h"

#include "helpers.h"

#include <yt/core/logging/log.h>
#include <yt/core/profiling/timing.h>

#include <util/system/thread.h>

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
        event.Message = TSharedRef::FromString(MaybeTruncateSubquery(TString(message.getText())));
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
            case Poco::Message::PRIO_ERROR:
                return ELogLevel::Error;
            case Poco::Message::PRIO_WARNING:
                return ELogLevel::Warning;
            case Poco::Message::PRIO_NOTICE:
            case Poco::Message::PRIO_INFORMATION:
                return ELogLevel::Info;
            case Poco::Message::PRIO_DEBUG:
                return ELogLevel::Debug;
            case Poco::Message::PRIO_TRACE:
                return ELogLevel::Trace;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Channel> CreateLogChannel(const TLogger& logger)
{
    return new TLogChannel(logger);
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
