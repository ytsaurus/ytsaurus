#include "pools_config_parser.h"

#include "private.h"

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/scheduler/config.h>
#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NScheduler {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPoolsConfigParser::TPoolsConfigParser(
    THashMap<TString, TString> poolToParentMap,
    THashSet<TString> ephemeralPools,
    THashMap<TString, INodePtr> poolConfigPresets)
    : OldPoolToParentMap_(std::move(poolToParentMap))
    , EphemeralPools_(std::move(ephemeralPools))
    , PoolConfigPresets_(std::move(poolConfigPresets))
{ }

TError TPoolsConfigParser::TryParse(const INodePtr& rootNode)
{
    if (TryParse(rootNode, RootPoolName, /*isFifo*/ false)) {
        ProcessErasedPools();
    }
    return Error_;
}

const std::vector<TPoolsConfigParser::TUpdatePoolAction>& TPoolsConfigParser::GetOrderedUpdatePoolActions()
{
    return UpdatePoolActions;
}

bool TPoolsConfigParser::TryParse(const INodePtr& configNode, const TString& parentName, bool isFifo)
{
    auto nodeType = configNode->GetType();
    if (nodeType != ENodeType::Map) {
        Error_ = TError("Found node with type %v, but only map is allowed", nodeType);
        return false;
    }

    auto children = configNode->AsMap()->GetChildren();

    if (isFifo && !children.empty()) {
        Error_ = TError("Pool %Qv cannot have subpools since it is in FIFO mode", parentName);
        return false;
    }

    for (const auto& [childName, childNode] : children) {
        if (ParsedPoolNames_.contains(childName)) {
            Error_ = TError("Duplicate pool %Qv found in new configuration", childName);
            return false;
        }

        TUpdatePoolAction updatePoolAction;
        updatePoolAction.Name = childName;
        updatePoolAction.ParentName = parentName;
        try {
            auto poolConfigNode = ConvertToNode(childNode->Attributes());
            auto poolConfig = ConvertTo<TPoolConfigPtr>(poolConfigNode);
            if (poolConfig->ConfigPreset) {
                auto it = PoolConfigPresets_.find(*poolConfig->ConfigPreset);
                if (it == PoolConfigPresets_.end()) {
                    THROW_ERROR_EXCEPTION("Config preset %Qv is not found", *poolConfig->ConfigPreset);
                }

                const auto& presetNode = it->second;
                ValidatePoolPresetConfig(*poolConfig->ConfigPreset, presetNode);

                // Explicit config has higher priority than preset.
                poolConfigNode = PatchNode(presetNode, poolConfigNode);
                poolConfig = ConvertTo<TPoolConfigPtr>(poolConfigNode);
            }
            poolConfig->Validate(childName);

            updatePoolAction.PoolConfig = poolConfig;
            updatePoolAction.ObjectId = childNode->Attributes().Get<TGuid>("id");
        } catch (const std::exception& ex) {
            Error_ = TError("Parsing configuration of pool %Qv failed", childName)
                << ex;
            return false;
        }

        auto oldParentIt = OldPoolToParentMap_.find(childName);
        if (oldParentIt != OldPoolToParentMap_.end()) {
            if (parentName == oldParentIt->second) {
                updatePoolAction.Type = EUpdatePoolActionType::Keep;
            } else {
                updatePoolAction.Type = EUpdatePoolActionType::Move;
            }
        } else {
            updatePoolAction.Type = EUpdatePoolActionType::Create;
        }


        bool childIsFifo = updatePoolAction.PoolConfig->Mode == NVectorHdrf::ESchedulingMode::Fifo;
        UpdatePoolActions.push_back(std::move(updatePoolAction));
        YT_VERIFY(ParsedPoolNames_.insert(childName).second);

        if (!TryParse(childNode, childName, childIsFifo)) {
            return false;
        }
    }
    return true;
}

void TPoolsConfigParser::ProcessErasedPools()
{
    THashMap<TString, TString> erasingPoolToParent;
    for (const auto& [poolName, parent] : OldPoolToParentMap_) {
        if (!ParsedPoolNames_.contains(poolName) && !EphemeralPools_.contains(poolName)) {
            erasingPoolToParent.emplace(poolName, parent);
        }
    }
    THashMap<TString, int> parentReferenceCount;
    for (const auto& [poolName, parent] : erasingPoolToParent) {
        if (erasingPoolToParent.contains(parent)) {
            ++parentReferenceCount[parent];
        }
    }
    std::vector<TString> candidates;
    for (const auto& [poolName, _] : erasingPoolToParent) {
        if (!parentReferenceCount.contains(poolName)) {
            candidates.push_back(poolName);
        }
    }
    int eraseActionCount = 0;
    while (!candidates.empty()) {
        auto poolName = std::move(candidates.back());
        candidates.pop_back();

        TUpdatePoolAction eraseAction {
            .Name = poolName,
            .Type = EUpdatePoolActionType::Erase
        };
        UpdatePoolActions.push_back(eraseAction);
        ++eraseActionCount;

        const auto& parent = GetOrCrash(OldPoolToParentMap_, poolName);
        auto it = parentReferenceCount.find(parent);
        if (it != parentReferenceCount.end()) {
            --it->second;
            if (it->second == 0) {
                candidates.push_back(it->first);
                parentReferenceCount.erase(it);
            }
        }
    }
    YT_VERIFY(eraseActionCount == std::ssize(erasingPoolToParent));
}

void TPoolsConfigParser::ValidatePoolPresetConfig(const TString& presetName, const INodePtr& presetNode)
{
    auto presetConfig = New<TPoolPresetConfig>();

    try {
        presetConfig->Load(presetNode, /*validate*/ true);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Config of preset %Qv failed to load as TPoolPresetConfig",
            presetName)
            << ex;
    }

    auto unrecognized = presetConfig->GetRecursiveUnrecognized();
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        THROW_ERROR_EXCEPTION("Config of preset %Qv contains unrecognized options",
            presetName)
            << TErrorAttribute("unrecognized", unrecognized);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
