#include "event_log.h"

namespace NYT {
namespace NEventLog {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NEventLog
} // namespace NYT

