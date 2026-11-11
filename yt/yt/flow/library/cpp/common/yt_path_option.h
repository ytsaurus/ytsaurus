#pragma once

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/public.h>

#include <functional>
#include <optional>

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

//! Attached instead of #EYTPathOwnership to a field of a struct shared by entities that own the path differently:
//! resolves the ownership by the final type of the parameters being collected. Null means no claim.
struct TYTPathOwnershipResolver
{
    std::function<std::optional<EYTPathOwnership>(const NYTree::TYsonStruct& parameters)> Resolve;
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
