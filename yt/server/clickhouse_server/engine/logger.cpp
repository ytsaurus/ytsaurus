#include "logger.h"

#include <yt/server/clickhouse_server/native/logger.h>

#include <util/system/thread.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

class TLogChannel
    : public Poco::Channel
{
private:
    const NNative::ILoggerPtr Logger;

public:
    TLogChannel(NNative::ILoggerPtr logger)
        : Logger(std::move(logger))
    {}

    void log(const Poco::Message& message) override
    {
        NNative::TLogEvent event;
        event.Level = GetLogLevel(message.getPriority());
        event.Message = message.getText();
        event.Timestamp = TInstant::Now();
        event.ThreadId = TThread::CurrentThreadId();

        Logger->Write(event);
    }

private:
    static NNative::ELogLevel GetLogLevel(Poco::Message::Priority priority)
    {
        switch (priority) {
            case Poco::Message::PRIO_FATAL:
            case Poco::Message::PRIO_CRITICAL:
                return NNative::ELogLevel::Fatal;
            case Poco::Message::PRIO_ERROR:
                return NNative::ELogLevel::Error;
            case Poco::Message::PRIO_WARNING:
                return NNative::ELogLevel::Warning;
            case Poco::Message::PRIO_NOTICE:
            case Poco::Message::PRIO_INFORMATION:
                return NNative::ELogLevel::Info;
            case Poco::Message::PRIO_DEBUG:
                return NNative::ELogLevel::Debug;
            case Poco::Message::PRIO_TRACE:
                return NNative::ELogLevel::Trace;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Channel> WrapToLogChannel(NNative::ILoggerPtr logger)
{
    return new TLogChannel(std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
