#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

NLog::TLogger ExecAgentLogger("ExecAgent");
NProfiling::TProfiler ExecAgentProfiler("/exec_agent");

const int EmptyUserId = -1;

const Stroka SandboxDirectoryName("sandbox");

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

