#include "artifact_description.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

bool TArtifactDescription::IsStaticDescriptionEqualTo(const TArtifactDescription& other) const
{
    return SandboxKind == other.SandboxKind &&
        Name == other.Name &&
        Executable == other.Executable &&
        BypassArtifactCache == other.BypassArtifactCache &&
        CopyFile == other.CopyFile &&
        Key == other.Key;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
