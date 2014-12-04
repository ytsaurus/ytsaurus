#include "stdafx.h"
#include "event_log.h"

namespace NYT {
namespace NScheduler {

using namespace NYson;

////////////////////////////////////////////////////////////////////

TFluentEventLogger::TFluentEventLogger()
    : Consumer_(nullptr)
    , Counter_(0)
{ }

TFluentEventLogger::~TFluentEventLogger()
{
    YCHECK(!Consumer_);
}

TFluentLogEvent TFluentEventLogger::LogEventFluently(IYsonConsumer* consumer)
{
    YCHECK(consumer);
    YCHECK(!Consumer_);
    Consumer_ = consumer;
    return TFluentLogEvent(this);
}

void TFluentEventLogger::Acquire()
{
    if (++Counter_ == 1) {
        Consumer_->OnBeginMap();
    }
}

void TFluentEventLogger::Release()
{
    if (--Counter_ == 0) {
        Consumer_->OnEndMap();
        Consumer_ = nullptr;
    }
}

////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

