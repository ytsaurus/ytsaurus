#include "private.h"
#include "public.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger ExecNodeLogger("ExecNode");
const NProfiling::TProfiler ExecNodeProfiler("/exec_agent");

const int TmpfsRemoveAttemptCount = 5;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

