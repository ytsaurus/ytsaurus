#include "helpers.h"

#include <yt/yt/core/misc/fs.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

TString GetRootFsUserDirectory()
{
    return "user";
}

TString GetSandboxRelPath(ESandboxKind sandboxKind)
{
    const auto& sandboxName = SandboxDirectoryNames[sandboxKind];
    YT_ASSERT(sandboxName);

    if (sandboxKind == ESandboxKind::User || sandboxKind == ESandboxKind::Tmp) {
        return NFS::CombinePaths(GetRootFsUserDirectory(), sandboxName);
    } else {
        return sandboxName;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
