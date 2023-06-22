#include "helpers.h"

#include <yt/yt/core/misc/fs.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

TString GetSandboxRelPath(ESandboxKind sandboxKind)
{
    const auto& sandboxName = SandboxDirectoryNames[sandboxKind];
    YT_ASSERT(sandboxName);

    return sandboxName;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
