#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IReshardIteration
    : public TRefCounted
{
    virtual void StartIteration() const = 0;
    virtual void Prepare(
        const TBundleStatePtr& bundleState,
        const TTabletBalancingGroupConfigPtr& groupConfig) = 0;
    virtual void FinishIteration(int actionCount) const = 0;

    virtual std::vector<TTablePtr> GetTablesToReshard(const TTabletCellBundlePtr& bundle) const = 0;
    virtual TFuture<std::vector<TReshardDescriptor>> MergeSplitTable(
        const TTablePtr& table,
        const IInvokerPtr& invoker) = 0;

    virtual void UpdateProfilingCounters(
        const TTable* table,
        TTableProfilingCounters& profilingCounters,
        const TReshardDescriptor& descriptor) = 0;

    virtual bool IsGroupBalancingEnabled(const TTabletBalancingGroupConfigPtr& /*groupConfig*/) const = 0;

    virtual bool IsTableBalancingEnabled(const TTablePtr& /*table*/) const = 0;

    virtual bool IsPickPivotKeysEnabled(const TBundleTabletBalancerConfigPtr& /*bundleConfig*/) const = 0;

    virtual const TString& GetBundleName() const = 0;
    virtual const TString& GetGroupName() const = 0;
    virtual const TTabletBalancerDynamicConfigPtr& GetDynamicConfig() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReshardIteration)

////////////////////////////////////////////////////////////////////////////////

IReshardIterationPtr CreateSizeReshardIteration(
    TString bundleName,
    TString groupName,
    TTabletBalancerDynamicConfigPtr dynamicConfig);

IReshardIterationPtr CreateParameterizedReshardIteration(
    TString bundleName,
    TString groupName,
    TTabletBalancerDynamicConfigPtr dynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
