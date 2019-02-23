#include "logger.h"

#include "logger.h"

#include <yt/core/logging/log.h>
#include <yt/core/profiling/timing.h>

#include <util/system/thread.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TLogChannel
    : public Poco::Channel
{
private:
    const ILoggerPtr Logger;

public:
    TLogChannel(ILoggerPtr logger)
        : Logger(std::move(logger))
    {}

    void log(const Poco::Message& message) override
    {
        TLogEvent event;
        event.Level = GetLogLevel(message.getPriority());
        event.Message = message.getText();
        event.Timestamp = TInstant::Now();
        event.ThreadId = TThread::CurrentThreadId();

        Logger->Write(event);
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

Poco::AutoPtr<Poco::Channel> WrapToLogChannel(ILoggerPtr logger)
{
    return new TLogChannel(std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

class TLogger
    : public ILogger
{
private:
    NLogging::TLogger Logger;

public:
    TLogger(const NLogging::TLogger& logger)
        : Logger(logger)
    {}

    void Write(const TLogEvent& e) override
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

ILoggerPtr CreateLogger(const NLogging::TLogger& logger)
{
    return std::make_shared<TLogger>(logger);
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
