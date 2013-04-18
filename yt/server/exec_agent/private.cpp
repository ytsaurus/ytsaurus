#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

NLog::TLogger ExecAgentLogger("ExecAgent");
NProfiling::TProfiler ExecAgentProfiler("/exec_agent");

const int EmptyUserId = -1;

const Stroka SandboxDirectoryName("sandbox");

const Stroka ProxyConfigFileName("config.yson");

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

