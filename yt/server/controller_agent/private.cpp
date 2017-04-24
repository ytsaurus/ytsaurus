#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

const NLogging::TLogger OperationLogger("Operation");
const NLogging::TLogger ControllersMasterConnectorLogger("ControllersMasterConnector");

const TDuration PrepareYieldPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////

const double ApproximateSizesBoostFactor = 1.3;
const double JobSizeBoostFactor = 2.0;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
