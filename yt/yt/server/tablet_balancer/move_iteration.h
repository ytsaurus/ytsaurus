#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IMoveIteration
    : public TRefCounted
{
    virtual void StartIteration() const = 0;
    virtual void Prepare(const TTableRegistryPtr& tableRegistry) = 0;
    virtual void FinishIteration(int actionCount) const = 0;

    virtual TFuture<std::vector<TMoveDescriptor>> ReassignTablets(const IInvokerPtr& invoker) = 0;

    virtual void UpdateProfilingCounters(const TTable* table) = 0;

    virtual bool IsGroupBalancingEnabled() const = 0;
    virtual void LogDisabledBalancing() const = 0;

    virtual const TString& GetBundleName() const = 0;
    virtual const TString& GetGroupName() const = 0;
    virtual const TBundleStatePtr& GetBundle() const = 0;
    virtual const TTabletBalancerDynamicConfigPtr& GetDynamicConfig() const = 0;
    virtual const TTabletBalancingGroupConfigPtr& GetGroupConfig() const = 0;

    virtual EBalancingMode GetBalancingMode() const = 0;
    virtual TStringBuf GetActionSubtypeName() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IMoveIteration)

////////////////////////////////////////////////////////////////////////////////

IMoveIterationPtr CreateOrdinaryMoveIteration(
    TBundleStatePtr bundleState,
    TTabletBalancingGroupConfigPtr groupConfig,
    TTabletBalancerDynamicConfigPtr dynamicConfig);

IMoveIterationPtr CreateInMemoryMoveIteration(
    TBundleStatePtr bundleState,
    TTabletBalancingGroupConfigPtr groupConfig,
    TTabletBalancerDynamicConfigPtr dynamicConfig);

IMoveIterationPtr CreateParameterizedMoveIteration(
    TString groupName,
    TBundleStatePtr bundleState,
    TTableParameterizedMetricTrackerPtr metricTracker,
    TTabletBalancingGroupConfigPtr groupConfig,
    TTabletBalancerDynamicConfigPtr dynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
