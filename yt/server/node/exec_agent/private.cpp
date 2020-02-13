#include "private.h"
#include "public.h"

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger ExecAgentLogger("ExecAgent");
const NProfiling::TProfiler ExecAgentProfiler("/exec_agent");

const int TmpfsRemoveAttemptCount = 5;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent

