#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

class TStrategyOperationState
    : public TRefCounted
{
public:
    using TTreeIdToPoolNameMap = THashMap<TString, TPoolName>;

    DEFINE_BYVAL_RO_PROPERTY(IOperationPtr, Host);
    DEFINE_BYVAL_RO_PROPERTY(TOperationControllerPtr, Controller);
    DEFINE_BYREF_RW_PROPERTY(TTreeIdToPoolNameMap, TreeIdToPoolNameMap);
    DEFINE_BYVAL_RW_PROPERTY(bool, Enabled);

public:
    TStrategyOperationState(
        IOperationPtr host,
        const TStrategyOperationControllerConfigPtr& config,
        const std::vector<IInvokerPtr>& nodeShardInvokers);

    void UpdateConfig(const TStrategyOperationControllerConfigPtr& config);

    TPoolName GetPoolNameByTreeId(const std::string& treeId) const;
};

DEFINE_REFCOUNTED_TYPE(TStrategyOperationState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
