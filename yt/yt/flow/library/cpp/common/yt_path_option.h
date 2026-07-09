#pragma once

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Attached to a path-typed TYsonStruct field via `.AddOption(...)` to declare how
//! the entity owning the field touches that YT path. Read back at spec-validation
//! time via `IYsonStructParameter::FindOption<EYTPathOwnership>()` to detect
//! conflicting writers across a pipeline.
enum class EYTPathOwnership
{
    ReadOnly,
    SharedWrite,
    ExclusiveWrite,
};

//! A single normalized (cluster, path) ownership claim produced while walking a pipeline spec.
struct TYTPathClaim
{
    std::string Cluster;
    NYPath::TYPath Path;
    EYTPathOwnership Ownership;
    TString ParameterKey;
    TString Origin;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
