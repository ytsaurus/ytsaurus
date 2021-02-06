#pragma once

#include "private.h"
#include "fair_share_tree_element.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeSnapshotImpl
    : public TRefCounted
{
    DEFINE_BYREF_RO_PROPERTY(TRootElementPtr, RootElement)
    DEFINE_BYREF_RO_PROPERTY(TNonOwningOperationElementMap, EnabledOperationMap)
    DEFINE_BYREF_RO_PROPERTY(TNonOwningOperationElementMap, DisabledOperationMap)
    DEFINE_BYREF_RO_PROPERTY(TNonOwningPoolMap, PoolMap)
    DEFINE_BYREF_RO_PROPERTY(TFairShareStrategyTreeConfigPtr, TreeConfig)
    DEFINE_BYREF_RO_PROPERTY(TFairShareStrategyOperationControllerConfigPtr, ControllerConfig)
    DEFINE_BYVAL_RO_PROPERTY(bool, CoreProfilingCompatibilityEnabled)
    DEFINE_BYREF_RO_PROPERTY(TTreeSchedulingSegmentsState, SchedulingSegmentsState)
        
public:
    TFairShareTreeSnapshotImpl(
        TRootElementPtr rootElement,
        TNonOwningOperationElementMap enabledOperationIdToElement,
        TNonOwningOperationElementMap disabledOperationIdToElement,
        TNonOwningPoolMap poolNameToElement,
        TFairShareStrategyTreeConfigPtr treeConfig,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        bool coreProfilingCompatibilityEnabled,
        TTreeSchedulingSegmentsState schedulingSegmentsState);

    TPool* FindPool(const TString& poolName) const;
    TOperationElement* FindEnabledOperationElement(TOperationId operationId) const;
    TOperationElement* FindDisabledOperationElement(TOperationId operationId) const;
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeSnapshotImpl)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
