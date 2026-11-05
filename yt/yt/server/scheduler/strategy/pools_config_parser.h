#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUpdatePoolActionType,
    (Keep)
    (Create)
    (Move)
    (Erase)
);

// The purpose of this class is not only to parse and validate config
// but also to generate ordered sequence of primitive actions on existent pool tree structure.
// This sequence must provide safe transition to new tree structure where all intermediate states are consistent.
// See https://wiki.yandex-team.ru/yt/internal/Update-pools-config-algorithm/ for details.
class TPoolsConfigParser
{
public:
    struct TUpdatePoolAction
    {
        std::string Name;
        std::string ParentName;
        TPoolConfigPtr PoolConfig;
        NObjectClient::TObjectId ObjectId;
        EUpdatePoolActionType Type = EUpdatePoolActionType::Keep;
    };

    TPoolsConfigParser(
        THashMap<std::string, std::string> poolToParentMap,
        THashSet<std::string> ephemeralPools,
        THashMap<std::string, NYTree::INodePtr> poolConfigPresets);

    TError TryParse(const NYTree::INodePtr& rootNode);

    const std::vector<TUpdatePoolAction>& GetOrderedUpdatePoolActions();

private:
    const THashMap<std::string, std::string> OldPoolToParentMap_;
    const THashSet<std::string> EphemeralPools_;
    const THashMap<std::string, NYTree::INodePtr> PoolConfigPresets_;

    THashSet<std::string> EphemeralPoolParents_;
    THashSet<std::string> ParsedPoolNames_;
    std::vector<TUpdatePoolAction> UpdatePoolActions_;
    TError Error_;

    bool TryParse(const NYTree::INodePtr& configNode, const std::string& parentName, bool isFifo);
    void ProcessErasedPools();
    void ValidatePoolPresetConfig(const std::string& presetName, const NYTree::INodePtr& presetNode);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
