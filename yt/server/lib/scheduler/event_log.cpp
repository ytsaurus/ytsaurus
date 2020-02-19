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
    auto consumer = std::make_unique<TFluentLogEventConsumer>(GetEventLogConsumer(), GetEventLogger());
    return TFluentLogEvent(std::move(consumer))
        .Item("timestamp").Value(now)
        .Item("event_type").Value(eventType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

