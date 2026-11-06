#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSpecPatchChange
    : public NYTree::TYsonStructLite
{
    std::string Path;
    NYTree::INodePtr Value;

    REGISTER_YSON_STRUCT_LITE(TDynamicSpecPatchChange);

    static void Register(TRegistrar registrar);
};

struct TDynamicSpecPatch
    : public NYTree::TYsonStruct
{
    TVersion Version;
    std::vector<TDynamicSpecPatchChange> Changes;
    std::optional<std::string> Comment;
    TInstant Timestamp;

    REGISTER_YSON_STRUCT(TDynamicSpecPatch);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSpecPatch);

DEFINE_ENUM(EDynamicSpecPatchAction,
    (Set)
    (UpdateComment)
    (CancelPatch)
    (Reset)
);

struct TDynamicSpecPatchRequest
{
    EDynamicSpecPatchAction Operation{};
    std::string Path;
    NYTree::INodePtr Value;
    std::vector<TDynamicSpecPatchChange> Changes;
    std::optional<TVersion> PatchVersion;
    std::optional<std::string> Comment;
    std::optional<TVersion> ExpectedVersion;
};

//! A detached candidate; publication requires CAS against the original version.
TVersionedDynamicPipelineSpecPtr ApplyDynamicSpecPatch(
    const TVersionedDynamicPipelineSpecPtr& current,
    const TDynamicSpecPatchRequest& patchRequest,
    const IVersionProviderPtr& versionProvider);

TVersionedDynamicPipelineSpecPtr UpdateDynamicSpecBase(
    const TVersionedDynamicPipelineSpecPtr& current,
    const NYTree::INodePtr& base,
    const IVersionProviderPtr& versionProvider);

TVersionedDynamicPipelineSpecPtr UpdateDynamicSpecTargetState(
    const TVersionedDynamicPipelineSpecPtr& current,
    EPipelineState targetState,
    const IVersionProviderPtr& versionProvider);

TVersionedDynamicPipelineSpecPtr NormalizeDynamicSpecState(const TVersionedDynamicPipelineSpecPtr& current);

NYTree::INodePtr ApplyDynamicSpecPatches(
    const NYTree::INodePtr& base,
    const std::vector<TDynamicSpecPatchPtr>& patches);

NYTree::INodePtr BuildDynamicSpecPatchesSnapshot(
    const TVersionedDynamicPipelineSpecPtr& current,
    std::optional<TVersion> patchId = std::nullopt);
NYTree::INodePtr BuildDynamicSpecBlameSnapshot(const TVersionedDynamicPipelineSpecPtr& current);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
