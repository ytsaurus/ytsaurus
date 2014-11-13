#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

NLog::TLogger ExecAgentLogger("ExecAgent");
NProfiling::TProfiler ExecAgentProfiler("/exec_agent");

const Stroka SandboxDirectoryName("sandbox");

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

