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
    static const TLogger NullLogger;
    return TOneShotFluentLogEvent(
        New<TFluentYsonWriterState>(EYsonFormat::Binary, EYsonType::MapFragment),
        NullLogger,
        ELogLevel::Debug);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
