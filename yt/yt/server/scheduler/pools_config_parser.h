#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

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

    TPoolsConfigParser(THashMap<TString, TString> poolToParentMap, THashSet<TString> ephemeralPools);

    TError TryParse(const NYTree::INodePtr& rootNode);

    const std::vector<TUpdatePoolAction>& GetOrderedUpdatePoolActions();

private:
    const THashMap<TString, TString> OldPoolToParentMap_;
    const THashSet<TString> EphemeralPools_;

    THashSet<TString> ParsedPoolNames_;
    std::vector<TUpdatePoolAction> UpdatePoolActions;
    TError Error_;

    bool TryParse(const NYTree::INodePtr& configNode, const TString& parentName, bool isFifo);
    void ProcessErasedPools();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
