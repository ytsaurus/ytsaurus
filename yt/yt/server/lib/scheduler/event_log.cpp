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
    auto consumer = std::make_unique<TFluentLogEventConsumer>(eventLogger, eventLogConsumer);
    return TFluentLogEvent(std::move(consumer))
        .Item("timestamp").Value(now)
        .Item("event_type").Value(eventType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

