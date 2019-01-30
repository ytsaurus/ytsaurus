#include "private.h"
#include "public.h"

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger ExecAgentLogger("ExecAgent");
const NProfiling::TProfiler ExecAgentProfiler("/exec_agent");

const int TmpfsRemoveAttemptCount = 5;

////////////////////////////////////////////////////////////////////////////////

TString GetJobProxyUnixDomainName(const TString& nodeTag, int slotIndex)
{
    return Format("%v-job-proxy-%v", nodeTag, slotIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent

