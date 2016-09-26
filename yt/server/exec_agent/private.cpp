#include "private.h"
#include "public.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

const NLogging::TLogger ExecAgentLogger("ExecAgent");
const NProfiling::TProfiler ExecAgentProfiler("/exec_agent");

const TEnumIndexedVector<Stroka, ESandboxKind> SandboxDirectoryNames{"sandbox", "udf", "home", "pipes"};
const int TmpfsRemoveAttemptCount = 5;

////////////////////////////////////////////////////////////////////

Stroka GetJobProxyUnixDomainName(const Stroka& nodeTag, int slotIndex)
{
    return Format("%v-job-proxy-%v", nodeTag, slotIndex);
}

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

