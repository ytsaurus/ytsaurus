#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger OperationLogger("Operation");
const NLogging::TLogger MasterConnectorLogger("MasterConnector");
const NProfiling::TProfiler ControllerAgentProfiler("/controller_agent");

const TDuration PrepareYieldPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

const double ApproximateSizesBoostFactor = 1.3;
const double JobSizeBoostFactor = 2.0;

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
