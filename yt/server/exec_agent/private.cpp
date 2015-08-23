#include "stdafx.h"
#include "private.h"
#include "public.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

const NLogging::TLogger ExecAgentLogger("ExecAgent");
const NProfiling::TProfiler ExecAgentProfiler("/exec_agent");

const TEnumIndexedVector<Stroka, ESandboxIndex> SandboxDirectoryNames{"sandbox", "udf"};

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

