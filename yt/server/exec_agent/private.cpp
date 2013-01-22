#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

NLog::TLogger ExecAgentLogger("ExecAgent");
NProfiling::TProfiler ExecAgentProfiler("/exec_agent");

const Stroka SandboxName("sandbox");
const Stroka ProxyConfigFileName("config.yson");

const int EmptyUserId = -1;

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

