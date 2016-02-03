#include "private.h"
#include "public.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

const NLogging::TLogger ExecAgentLogger("ExecAgent");
const NProfiling::TProfiler ExecAgentProfiler("/exec_agent");

const TEnumIndexedVector<Stroka, ESandboxKind> SandboxDirectoryNames{"sandbox", "udf"};
const Stroka TmpfsDirName("tmpfs");

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

