#include "private.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

const TString IntermediatePath = "<intermediate>";

const NLogging::TLogger ControllerLogger("Controller");
const NLogging::TLogger ControllerAgentLogger("ControllerAgent");

const NProfiling::TProfiler ControllerAgentProfiler("/controller_agent");

const TDuration PrepareYieldPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

const double ApproximateSizesBoostFactor = 1.3;
const double JobSizeBoostFactor = 2.0;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
