#include "public.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

const TEnumIndexedArray<ESandboxKind, TString> SandboxDirectoryNames{
    {ESandboxKind::User, "sandbox"},
    {ESandboxKind::Udf, "udf"},
    {ESandboxKind::Home, "home"},
    {ESandboxKind::Pipes, "pipes"},
    {ESandboxKind::Tmp, "tmp"},
    {ESandboxKind::Cores, "cores"},
    {ESandboxKind::Logs, "logs"},
    {ESandboxKind::PortoPlace, "place"},
    {ESandboxKind::RootVolumeOverlay, "overlay"},
};

const std::string EmptyCpuSet("");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
