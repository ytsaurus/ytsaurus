#include "logger.h"

#include <util/system/thread.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TLogChannel
    : public Poco::Channel
{
private:
    const NInterop::ILoggerPtr Logger;

public:
    TLogChannel(NInterop::ILoggerPtr logger)
        : Logger(std::move(logger))
    {}

    void log(const Poco::Message& message) override
    {
        NInterop::TLogEvent event;
        event.Level = GetLogLevel(message.getPriority());
        event.Message = message.getText();
        event.Timestamp = TInstant::Now();
        event.ThreadId = TThread::CurrentThreadId();

        Logger->Write(event);
    }

private:
    static NInterop::ELogLevel GetLogLevel(Poco::Message::Priority priority)
    {
        switch (priority) {
            case Poco::Message::PRIO_FATAL:
            case Poco::Message::PRIO_CRITICAL:
                return NInterop::ELogLevel::Fatal;
            case Poco::Message::PRIO_ERROR:
                return NInterop::ELogLevel::Error;
            case Poco::Message::PRIO_WARNING:
                return NInterop::ELogLevel::Warning;
            case Poco::Message::PRIO_NOTICE:
            case Poco::Message::PRIO_INFORMATION:
                return NInterop::ELogLevel::Info;
            case Poco::Message::PRIO_DEBUG:
                return NInterop::ELogLevel::Debug;
            case Poco::Message::PRIO_TRACE:
                return NInterop::ELogLevel::Trace;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Channel> WrapToLogChannel(NInterop::ILoggerPtr logger)
{
    return new TLogChannel(std::move(logger));
}

}   // namespace NClickHouse
}   // namespace NYT
