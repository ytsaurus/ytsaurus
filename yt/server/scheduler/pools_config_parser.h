#pragma once

#include "public.h"
#include "fair_share_tree.h"

#include <yt/core/ytree/convert.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPoolsConfigParser
{
public:
    struct TParsePoolResult
    {
        TString ParentId = nullptr;
        TPoolConfigPtr PoolConfig = nullptr;
        bool IsNew = false;
        bool ParentIsChanged = false;
    };

    TPoolsConfigParser(THashMap<TString, TString> poolToParentMap, TString rootElementId)
        : OldPoolToParentMap_(std::move(poolToParentMap))
        , RootElementId_(std::move(rootElementId))
    { }

    TError TryParse(const NYTree::INodePtr& rootNode)
    {
        TryParse(rootNode, RootElementId_, /* pathToRootChanged */ false);
        return Error_;
    }

    const THashMap<TString, TParsePoolResult>& GetPoolConfigMap()
    {
        return PoolConfigMap_;
    }

    const std::vector<TString>& GetNewPoolsByInOrderTraversal()
    {
        return NewPools_;
    }

private:
    THashMap<TString, TString> OldPoolToParentMap_;
    TString RootElementId_;

    THashMap <TString, TParsePoolResult> PoolConfigMap_;
    std::vector<TString> NewPools_;
    TError Error_;

    bool TryParse(const NYTree::INodePtr& configNode, const TString& parentId, bool pathToRootChanged)
    {
        auto nodeType = configNode->GetType();
        if (nodeType != NYTree::ENodeType::Map) {
            Error_ = TError("Found node with type %v, but only Map is allowed", nodeType);
            return false;
        }

        for (const auto& [childId, childNode] : configNode->AsMap()->GetChildren()) {
            if (childId == RootPoolName) {
                Error_ = TError("Use of root element id is forbidden");
                return false;
            }

            if (PoolConfigMap_.contains(childId)) {
                Error_ = TError("Duplicate poolId %v found in new configuration", childId);
                return false;
            }

            TParsePoolResult parsePoolResult;
            parsePoolResult.ParentId = parentId;
            try {
                parsePoolResult.PoolConfig = NYTree::ConvertTo<TPoolConfigPtr>(childNode->Attributes());
                parsePoolResult.PoolConfig->Validate();
            } catch (const std::exception& ex) {
                Error_ = TError("Parsing configuration of pool %Qv failed", childId)
                    << ex;
                return false;
            }

            auto oldParentIt = OldPoolToParentMap_.find(childId);
            if (oldParentIt != OldPoolToParentMap_.end()) {
                parsePoolResult.IsNew = false;

                if (parentId == oldParentIt->second) {
                    parsePoolResult.ParentIsChanged = false;
                } else {
                    if (pathToRootChanged) {
                        Error_ = TError("Path to pool %Qv changed in more than one place; make pool tree changes more gradually",
                            childId);
                        return false;
                    }
                    pathToRootChanged = true;
                    parsePoolResult.ParentIsChanged = true;
                }
            } else {
                parsePoolResult.IsNew = true;
                parsePoolResult.ParentIsChanged = false;
                NewPools_.push_back(childId);
            }

            if (parentId != RootElementId_) {
                auto it = PoolConfigMap_.find(parentId);
                YT_VERIFY(it != PoolConfigMap_.end());
                if (it->second.PoolConfig->Mode == ESchedulingMode::Fifo) {
                    Error_ = TError("Pool %Qv cannot have subpools since it is in fifo mode", parentId);
                    return false;
                }
            }

            PoolConfigMap_.emplace(childId, std::move(parsePoolResult));
            if (!TryParse(childNode, childId, pathToRootChanged)) {
                return false;
            }
        }
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
