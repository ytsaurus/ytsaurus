#include "dynamic_spec.h"
#include "spec.h"

#include <yt/yt/client/api/flow_client.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <algorithm>

namespace NYT::NFlow {

using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TDynamicSpecPatchChange::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("value", &TThis::Value);
}

void TDynamicSpecPatch::Register(TRegistrar registrar)
{
    registrar.Parameter("patch_id", &TThis::Version);
    registrar.Parameter("changes", &TThis::Changes);
    registrar.Parameter("comment", &TThis::Comment)
        .Default();
    registrar.Parameter("timestamp", &TThis::Timestamp);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
void ValidateNesting(const T& value)
{
    auto serialized = NYson::ConvertToYsonStringNestingLimited(value, DefaultYsonParserNestingLevelLimit);
    ValidateYson(serialized, DefaultYsonParserNestingLevelLimit);
}

std::vector<std::string> ParsePath(const std::string& path)
{
    std::vector<std::string> result;
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        THROW_ERROR_EXCEPTION_IF(std::ssize(result) >= DefaultYsonParserNestingLevelLimit,
            "DynamicSpec path nesting limit exceeded");
        result.push_back(tokenizer.GetLiteralValue());
        tokenizer.Advance();
    }
    return result;
}

std::string BuildPath(const std::vector<std::string>& tokens, int count)
{
    std::string result;
    for (int index = 0; index < count; ++index) {
        result = YPathJoin(result, tokens[index]);
    }
    return result;
}

void ForEachChange(const TDynamicSpecPatchPtr& patch, const auto& callback)
{
    for (const auto& change : patch->Changes) {
        callback(change.Path, change.Value);
    }
}

void ApplyPatch(const INodePtr& root, const std::string& path, const INodePtr& value)
{
    auto tokens = ParsePath(path);
    YT_VERIFY(!tokens.empty());
    auto parent = root->AsMap();
    for (int index = 0; index + 1 < std::ssize(tokens); ++index) {
        auto child = parent->FindChild(tokens[index]);
        if (!child || child->GetType() != ENodeType::Map) {
            child = GetEphemeralNodeFactory()->CreateMap();
            parent->RemoveChild(tokens[index]);
            parent->AddChild(tokens[index], child);
        }
        parent = child->AsMap();
    }
    parent->RemoveChild(tokens.back());
    parent->AddChild(tokens.back(), NYTree::CloneNode(value));
}

TDynamicPipelineSpecPtr BuildEffectiveSpec(const TVersionedDynamicPipelineSpec& state)
{
    if (state.Patches.empty()) {
        return state.GetBaseSpec();
    }
    auto node = ApplyDynamicSpecPatches(ConvertToNode(state.GetBaseSpec()), state.Patches);
    auto spec = ConvertTo<TDynamicPipelineSpecPtr>(node);
    ValidateDynamicPipelineSpec(spec);
    return spec;
}

void RefreshEffectiveSpec(const TVersionedDynamicPipelineSpecPtr& state)
{
    state->SetEffectiveSpec(BuildEffectiveSpec(*state));
}

// List element edits pin the resulting list to keep index-based patches stable.
void LiftListPatch(
    const TVersionedDynamicPipelineSpecPtr& state,
    TDynamicSpecPatchChange* change,
    std::vector<std::string>* tokens)
{
    auto effective = ConvertToNode(state->GetValue());
    auto node = effective;
    for (int index = 0; index < std::ssize(*tokens); ++index) {
        if (!node) {
            break;
        }
        if (node->GetType() == ENodeType::List) {
            auto listPath = BuildPath(*tokens, index);
            auto suffixTokens = std::vector<std::string>(tokens->begin() + index, tokens->end());
            auto relativePath = BuildPath(suffixTokens, std::ssize(suffixTokens));
            auto value = NYTree::CloneNode(node);
            SetNodeByYPath(value, relativePath, change->Value, /*force*/ true);
            ValidateNesting(value);
            change->Path = listPath;
            change->Value = value;
            tokens->resize(index);
            return;
        }
        THROW_ERROR_EXCEPTION_UNLESS(node->GetType() == ENodeType::Map,
            "Cannot address a child of scalar DynamicSpec parameter at %v",
            BuildPath(*tokens, index));
        node = node->AsMap()->FindChild((*tokens)[index]);
    }
}

void ValidateVersion(const TVersionedDynamicPipelineSpecPtr& state, const std::optional<TVersion>& expected)
{
    THROW_ERROR_EXCEPTION_IF(expected && *expected != state->GetVersion(),
        EErrorCode::SpecVersionMismatch,
        "Spec version mismatch: expected %v, actual %v",
        expected.value_or(TVersion(0)),
        state->GetVersion());
}

void BumpState(const TVersionedDynamicPipelineSpecPtr& state, const IVersionProviderPtr& provider)
{
    state->Bump(provider);
}

void ValidatePersistedDynamicSpecSize(const TVersionedDynamicPipelineSpecPtr& state)
{
    auto serialized = ConvertToYsonString(state);
    THROW_ERROR_EXCEPTION_IF(std::ssize(serialized.AsStringBuf()) > NTableClient::MaxAnyValueLength,
        "DynamicSpec exceeds the YT dynamic table value size limit")
        .With("max_value_size", NTableClient::MaxAnyValueLength);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

const TDynamicPipelineSpecPtr& TVersionedDynamicPipelineSpec::GetValue() const
{
    std::lock_guard guard(EffectiveSpecMutex_);
    if (!EffectiveSpec_) {
        EffectiveSpec_ = BuildEffectiveSpec(*this);
    }
    return EffectiveSpec_;
}

TVersionedDynamicPipelineSpecPtr NormalizeDynamicSpecState(const TVersionedDynamicPipelineSpecPtr& current)
{
    return CloneYsonStruct(current);
}

INodePtr ApplyDynamicSpecPatches(
    const INodePtr& base,
    const std::vector<TDynamicSpecPatchPtr>& patches)
{
    auto node = NYTree::CloneNode(base);
    for (const auto& patch : patches) {
        ForEachChange(patch, [&] (const auto& path, const auto& value) {
            ApplyPatch(node, path, value);
        });
    }
    ValidateNesting(node);
    return node;
}

TVersionedDynamicPipelineSpecPtr ApplyDynamicSpecPatch(
    const TVersionedDynamicPipelineSpecPtr& current,
    const TDynamicSpecPatchRequest& originalPatch,
    const IVersionProviderPtr& versionProvider)
{
    THROW_ERROR_EXCEPTION_UNLESS(originalPatch.ExpectedVersion,
        "expected_version is required for a DynamicSpec patch operation");
    ValidateVersion(current, originalPatch.ExpectedVersion);
    if (originalPatch.Operation == EDynamicSpecPatchAction::UpdateComment ||
        (originalPatch.Operation == EDynamicSpecPatchAction::Set && (!originalPatch.PatchVersion || originalPatch.Comment)))
    {
        NApi::ValidatePipelineDynamicSpecPatchComment(originalPatch.Comment);
    }
    auto state = NormalizeDynamicSpecState(current);
    auto patchRequest = originalPatch;
    bool changed = true;
    switch (patchRequest.Operation) {
        case EDynamicSpecPatchAction::Set: {
            auto changes = patchRequest.Changes;
            if (changes.empty()) {
                THROW_ERROR_EXCEPTION_UNLESS(patchRequest.Value, "Spec must be specified");
                TDynamicSpecPatchChange change;
                change.Path = patchRequest.Path;
                change.Value = patchRequest.Value;
                changes.push_back(std::move(change));
            } else {
                THROW_ERROR_EXCEPTION_IF(!patchRequest.Path.empty() || patchRequest.Value,
                    "Specify either set_path and set_value or set_changes");
            }
            for (auto& change : changes) {
                auto tokens = ParsePath(change.Path);
                THROW_ERROR_EXCEPTION_IF(tokens.empty() || tokens.front() == "target_state",
                    "DynamicSpec patches cannot change target_state");
                THROW_ERROR_EXCEPTION_UNLESS(change.Value, "Spec must be specified");
                ValidateNesting(change.Value);
                LiftListPatch(state, &change, &tokens);
                change.Path = BuildPath(tokens, std::ssize(tokens));
            }
            std::vector<std::vector<std::string>> paths;
            paths.reserve(changes.size());
            for (const auto& change : changes) {
                paths.push_back(ParsePath(change.Path));
            }
            std::sort(paths.begin(), paths.end());
            for (int index = 1; index < std::ssize(paths); ++index) {
                const auto& previous = paths[index - 1];
                const auto& current = paths[index];
                THROW_ERROR_EXCEPTION_IF(previous.size() <= current.size() &&
                        std::equal(previous.begin(), previous.end(), current.begin()),
                    "DynamicSpec changes in one patch must address distinct paths")
                    .With("first_path", BuildPath(previous, std::ssize(previous)))
                    .With("second_path", BuildPath(current, std::ssize(current)));
            }
            auto existing = patchRequest.PatchVersion
                ? std::find_if(state->Patches.begin(), state->Patches.end(), [&] (const auto& record) {
                      return record->Version == *patchRequest.PatchVersion;
                  })
                : state->Patches.end();
            THROW_ERROR_EXCEPTION_IF(patchRequest.PatchVersion && existing == state->Patches.end(),
                "DynamicSpec patch %v does not exist",
                *patchRequest.PatchVersion);
            std::optional<std::string> existingComment;
            if (existing != state->Patches.end()) {
                existingComment = (*existing)->Comment;
                state->Patches.erase(existing);
            }
            BumpState(state, versionProvider);
            auto patchId = patchRequest.PatchVersion.value_or(state->GetVersion());
            auto record = New<TDynamicSpecPatch>();
            record->Version = patchId;
            record->Changes = std::move(changes);
            record->Comment = patchRequest.Comment ? patchRequest.Comment : existingComment;
            record->Timestamp = state->GetLastUpdate();
            state->Patches.push_back(record);
            break;
        }
        case EDynamicSpecPatchAction::UpdateComment: {
            THROW_ERROR_EXCEPTION_UNLESS(patchRequest.PatchVersion, "Patch ID must be specified");
            auto it = std::find_if(state->Patches.begin(), state->Patches.end(), [&] (const auto& record) {
                return record->Version == *patchRequest.PatchVersion;
            });
            THROW_ERROR_EXCEPTION_IF(it == state->Patches.end(),
                "DynamicSpec patch %v has no remaining changes",
                *patchRequest.PatchVersion);
            changed = (*it)->Comment != patchRequest.Comment;
            if (changed) {
                BumpState(state, versionProvider);
                (*it)->Comment = patchRequest.Comment;
            }
            break;
        }
        case EDynamicSpecPatchAction::CancelPatch:
            THROW_ERROR_EXCEPTION_UNLESS(patchRequest.PatchVersion, "Patch version must be specified");
            changed = std::erase_if(state->Patches, [&] (const auto& patch) {
                return patch->Version == *patchRequest.PatchVersion;
            }) != 0;
            // clang-format off
            THROW_ERROR_EXCEPTION_UNLESS(changed, "DynamicSpec patch %v does not exist",
                *patchRequest.PatchVersion);
            // clang-format on
            break;
        case EDynamicSpecPatchAction::Reset:
            changed = !state->Patches.empty();
            state->Patches.clear();
            break;
    }
    ValidateNesting(state);
    if (changed && state->GetVersion() == current->GetVersion()) {
        BumpState(state, versionProvider);
    }
    constexpr int MaxPatchCount = 100;
    THROW_ERROR_EXCEPTION_IF(std::ssize(state->Patches) > MaxPatchCount,
        "Too many DynamicSpec patches: save their changes as the base dynamic spec or cancel unnecessary patches")
        .With("max_patch_count", MaxPatchCount);
    RefreshEffectiveSpec(state);
    ValidatePersistedDynamicSpecSize(state);
    ValidateNesting(state);
    return state;
}

TVersionedDynamicPipelineSpecPtr UpdateDynamicSpecBase(
    const TVersionedDynamicPipelineSpecPtr& current,
    const INodePtr& base,
    const IVersionProviderPtr& versionProvider)
{
    auto state = NormalizeDynamicSpecState(current);
    THROW_ERROR_EXCEPTION_UNLESS(base && base->GetType() == ENodeType::Map, "DynamicSpec base must be a map");
    ValidateNesting(base);
    auto spec = ConvertTo<TDynamicPipelineSpecPtr>(base);
    ValidateDynamicPipelineSpec(spec);
    if (state->TrySetValue(std::move(spec), versionProvider)) {
        RefreshEffectiveSpec(state);
    }
    ValidatePersistedDynamicSpecSize(state);
    ValidateNesting(state);
    return state;
}

TVersionedDynamicPipelineSpecPtr UpdateDynamicSpecTargetState(
    const TVersionedDynamicPipelineSpecPtr& current,
    EPipelineState targetState,
    const IVersionProviderPtr& versionProvider)
{
    auto state = NormalizeDynamicSpecState(current);
    if (state->GetBaseSpec()->TargetState != targetState) {
        auto base = CloneYsonStruct(state->GetBaseSpec());
        base->TargetState = targetState;
        state->TrySetValue(std::move(base), versionProvider);
        RefreshEffectiveSpec(state);
    }
    ValidatePersistedDynamicSpecSize(state);
    ValidateNesting(state);
    return state;
}

INodePtr BuildDynamicSpecPatchesSnapshot(
    const TVersionedDynamicPipelineSpecPtr& current,
    std::optional<TVersion> patchId)
{
    auto state = NormalizeDynamicSpecState(current);
    std::vector<TDynamicSpecPatchPtr> patches;
    for (const auto& patch : state->Patches) {
        if (!patchId || patch->Version == *patchId) {
            patches.push_back(patch);
        }
    }
    // clang-format off
    THROW_ERROR_EXCEPTION_IF(patchId && patches.empty(), "DynamicSpec patch %v does not exist",
        *patchId);
    // clang-format on
    // clang-format off
    return ConvertToNode(BuildYsonStringFluently()
        .BeginMap()
            .Item("version").Value(state->GetVersion())
            .Item("patches").Value(patches)
        .EndMap());
    // clang-format on
}

INodePtr BuildDynamicSpecBlameSnapshot(const TVersionedDynamicPipelineSpecPtr& current)
{
    auto state = NormalizeDynamicSpecState(current);
    auto result = GetEphemeralNodeFactory()->CreateMap();
    for (const auto& patch : state->Patches) {
        ForEachChange(patch, [&] (const auto& changePath, const auto&) {
            std::vector<std::string> shadowedPaths;
            for (const auto& [path, value] : result->GetChildren()) {
                if (path == changePath || path.starts_with(changePath + "/")) {
                    shadowedPaths.push_back(path);
                }
            }
            for (const auto& path : shadowedPaths) {
                result->RemoveChild(path);
            }
            // clang-format off
            auto blame = BuildYsonStringFluently()
                .BeginMap()
                    .Item("patch_id").Value(patch->Version)
                    .Item("comment").Value(patch->Comment)
                    .Item("timestamp").Value(patch->Timestamp)
                .EndMap();
            // clang-format on
            result->AddChild(changePath, ConvertToNode(blame));
        });
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
