#include "public.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

const TEnumIndexedArray<ESandboxKind, std::string> SandboxDirectoryNames{
    {ESandboxKind::User, "sandbox"},
    {ESandboxKind::Udf, "udf"},
    {ESandboxKind::Home, "home"},
    {ESandboxKind::Pipes, "pipes"},
    {ESandboxKind::Tmp, "tmp"},
    {ESandboxKind::Cores, "cores"},
    {ESandboxKind::Logs, "logs"},
    {ESandboxKind::PortoPlace, "porto_place"},
    {ESandboxKind::Private, "private"},
    {ESandboxKind::Empty, "empty"},
};

const std::string EmptyCpuSet("");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
