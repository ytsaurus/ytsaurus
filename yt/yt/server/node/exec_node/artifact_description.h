#pragma once

#include "artifact.h"
#include "private.h"

#include <yt/yt/server/lib/exec_node/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TArtifactDescription
{
    ESandboxKind SandboxKind;
    TString Name;
    bool Executable;
    bool BypassArtifactCache;
    bool CopyFile;
    TArtifactKey Key;
    TArtifactPtr Artifact;
    bool AccessedViaBind = false;
    bool AccessedViaVirtualSandbox = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
