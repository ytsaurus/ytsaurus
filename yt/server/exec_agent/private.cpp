#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

NLog::TLogger ExecAgentLogger("ExecAgent");
NProfiling::TProfiler ExecAgentProfiler("/exec_agent");

const Stroka SandboxName("sandbox");
const Stroka ProxyConfigFileName("config.yson");

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

