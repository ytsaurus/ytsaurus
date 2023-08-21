#include "event_log.h"

namespace NYT::NScheduler {

using namespace NYson;
using namespace NEventLog;

////////////////////////////////////////////////////////////////////////////////

TFluentLogEvent TEventLogHostBase::LogEventFluently(ELogEventType eventType)
{
    return LogEventFluently(GetEventLogger(), eventType);
}

TFluentLogEvent TEventLogHostBase::LogEventFluently(const NLogging::TLogger* eventLogger, ELogEventType eventType)
{
    return LogEventFluently(eventLogger, eventType, TInstant::Now());
}

TFluentLogEvent TEventLogHostBase::LogEventFluently(const NLogging::TLogger* eventLogger, ELogEventType eventType, TInstant now)
{
    return LogEventFluently(eventLogger, GetEventLogConsumer(), eventType, now);
}

TFluentLogEvent TEventLogHostBase::LogEventFluently(
    const NLogging::TLogger* eventLogger,
    NYson::IYsonConsumer* eventLogConsumer,
    ELogEventType eventType,
    TInstant now)
{
    std::vector<IYsonConsumer*> consumers;
    if (eventLogConsumer) {
        consumers.push_back(eventLogConsumer);
    }

    std::vector<std::unique_ptr<IYsonConsumer>> ownedConsumers;
    if (eventLogger) {
        ownedConsumers.push_back(std::make_unique<TFluentLogEventConsumer>(eventLogger));
    }

    YT_VERIFY(!consumers.empty() || !ownedConsumers.empty());

    auto teeConsumer = std::make_unique<TTeeYsonConsumer>(
        std::move(consumers),
        std::move(ownedConsumers));
    return TFluentLogEvent(std::move(teeConsumer))
        .Item("timestamp").Value(now)
        .Item("event_type").Value(eventType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

