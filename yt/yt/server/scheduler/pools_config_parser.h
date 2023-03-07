#pragma once

#include "public.h"

#include <yt/server/lib/scheduler/helpers.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUpdatePoolActionType,
    (Keep)
    (Create)
    (Move)
    (Erase)
)

// The purpose of this class is not only to parse and validate config
// but also to generate ordered sequence of primitive actions on existent pool tree structure.
// This sequence must provide safe transition to new tree structure where all intermediate states are consistent.
// See https://wiki.yandex-team.ru/yt/internal/Update-pools-config-algorithm/ for details.
class TPoolsConfigParser
{
public:
    struct TUpdatePoolAction
    {
        TString Name = nullptr;
        TString ParentName = nullptr;
        TPoolConfigPtr PoolConfig = nullptr;
        EUpdatePoolActionType Type = EUpdatePoolActionType::Keep;
    };

    explicit TPoolsConfigParser(THashMap<TString, TString> poolToParentMap, THashSet<TString> ephemeralPools)
        : OldPoolToParentMap_(std::move(poolToParentMap))
        , EphemeralPools_(std::move(ephemeralPools))
    { }

    TError TryParse(const NYTree::INodePtr& rootNode)
    {
        if (TryParse(rootNode, RootPoolName, /* isFifo */ false)) {
            ProcessErasedPools();
        }
        return Error_;
    }

    const std::vector<TUpdatePoolAction>& GetOrderedUpdatePoolActions()
    {
        return UpdatePoolActions;
    }

private:
    const THashMap<TString, TString> OldPoolToParentMap_;
    const THashSet<TString> EphemeralPools_;

    THashSet<TString> ParsedPoolNames_;
    std::vector<TUpdatePoolAction> UpdatePoolActions;
    TError Error_;

    bool TryParse(const NYTree::INodePtr& configNode, const TString& parentName, bool isFifo)
    {
        auto nodeType = configNode->GetType();
        if (nodeType != NYTree::ENodeType::Map) {
            Error_ = TError("Found node with type %v, but only Map is allowed", nodeType);
            return false;
        }

        auto children = configNode->AsMap()->GetChildren();

        if (isFifo && !children.empty()) {
            Error_ = TError("Pool %Qv cannot have subpools since it is in fifo mode", parentName);
            return false;
        }

        for (const auto& [childName, childNode] : children) {
            Error_ = CheckPoolName(childName);
            if (!Error_.IsOK()) {
                return false;
            }

            if (ParsedPoolNames_.contains(childName)) {
                Error_ = TError("Duplicate poolId %v found in new configuration", childName);
                return false;
            }

            TUpdatePoolAction updatePoolAction;
            updatePoolAction.Name = childName;
            updatePoolAction.ParentName = parentName;
            try {
                updatePoolAction.PoolConfig = NYTree::ConvertTo<TPoolConfigPtr>(childNode->Attributes());
                updatePoolAction.PoolConfig->Validate();
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


            bool childIsFifo = updatePoolAction.PoolConfig->Mode == ESchedulingMode::Fifo;
            UpdatePoolActions.push_back(std::move(updatePoolAction));
            YT_VERIFY(ParsedPoolNames_.insert(childName).second);

            if (!TryParse(childNode, childName, childIsFifo)) {
                return false;
            }
        }
        return true;
    }

    void ProcessErasedPools()
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
            UpdatePoolActions.emplace_back(eraseAction);
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
        YT_VERIFY(eraseActionCount == erasingPoolToParent.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
