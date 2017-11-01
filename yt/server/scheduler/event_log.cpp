#include "event_log.h"

namespace NYT {
namespace NScheduler {

using namespace NYson;
using namespace NEventLog;

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

TFluentLogEvent TEventLogHostBase::LogEventFluently(ELogEventType eventType)
{
    return LogEventFluently(eventType, TInstant::Now());
}

TFluentLogEvent TEventLogHostBase::LogEventFluently(ELogEventType eventType, TInstant now)
{
    return EventLogger_.LogEventFluently(GetEventLogConsumer())
        .Item("timestamp").Value(now)
        .Item("event_type").Value(eventType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

