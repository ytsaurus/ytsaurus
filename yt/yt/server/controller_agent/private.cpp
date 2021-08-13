#include "private.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger ControllerLogger("Controller");
const NLogging::TLogger ControllerAgentLogger("ControllerAgent");
const NLogging::TLogger ControllerEventLogger("ControllerEventLog");
const NLogging::TLogger ControllerFeatureStructuredLogger("ControllerFeatureStructuredLog");

const NProfiling::TProfiler ControllerAgentProfiler("/controller_agent");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
