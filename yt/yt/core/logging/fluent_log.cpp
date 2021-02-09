#include "fluent_log.h"

namespace NYT::NLogging {

using namespace NYson;
using namespace NYTree;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TOneShotFluentLogEvent LogStructuredEventFluently(const TLogger& logger, ELogLevel level)
{
    return TOneShotFluentLogEvent(
        New<TFluentYsonWriterState>(EYsonFormat::Binary, EYsonType::MapFragment),
        logger,
        level);
}

TOneShotFluentLogEvent LogStructuredEventFluentlyToNowhere()
{
    return TOneShotFluentLogEvent(
        New<TFluentYsonWriterState>(EYsonFormat::Binary, EYsonType::MapFragment),
        NullLogger,
        ELogLevel{});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
