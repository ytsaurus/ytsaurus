#include "event_log.h"

namespace NYT::NScheduler {

using namespace NYson;
using namespace NEventLog;

////////////////////////////////////////////////////////////////////////////////

TFluentLogEvent TEventLogHostBase::LogEventFluently(ELogEventType eventType)
{
    return LogEventFluently(eventType, TInstant::Now());
}

TFluentLogEvent TEventLogHostBase::LogEventFluently(ELogEventType eventType, TInstant now)
{
    return LogEventFluently(eventType, GetEventLogConsumer(), GetEventLogger(), now);
}

TFluentLogEvent TEventLogHostBase::LogEventFluently(
    ELogEventType eventType,
    NYson::IYsonConsumer* eventLogConsumer,
    const NLogging::TLogger* eventLogger,
    TInstant now)
{
    auto consumer = std::make_unique<TFluentLogEventConsumer>(eventLogConsumer, eventLogger);
    return TFluentLogEvent(std::move(consumer))
        .Item("timestamp").Value(now)
        .Item("event_type").Value(eventType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

