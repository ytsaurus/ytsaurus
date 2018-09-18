#include "fluent_log.h"

namespace NYT {
namespace NLogging {

using namespace NYson;
using namespace NYTree;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TOneShotFluentLogEvent LogStructuredEventFluently(const TLogger& logger, ELogLevel level)
{
    return TOneShotFluentLogEvent(New<TFluentYsonWriterState>(EYsonFormat::Binary, EYsonType::MapFragment), logger, level);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
