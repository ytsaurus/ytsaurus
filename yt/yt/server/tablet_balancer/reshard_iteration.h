#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IReshardIteration
    : public TRefCounted
{
    virtual void StartIteration() const = 0;
    virtual void Prepare(const TTabletBalancingGroupConfigPtr& groupConfig) = 0;
    virtual void FinishIteration(int actionCount) const = 0;

    virtual std::vector<TTablePtr> GetTablesToReshard(const TTabletCellBundlePtr& bundle) const = 0;
    virtual TFuture<std::vector<TReshardDescriptor>> MergeSplitTable(
        const TTablePtr& table,
        const IInvokerPtr& invoker) = 0;

    virtual void UpdateProfilingCounters(
        const TTable* table,
        const TReshardDescriptor& descriptor) = 0;

    virtual bool IsGroupBalancingEnabled(const TTabletBalancingGroupConfigPtr& /*groupConfig*/) const = 0;

    virtual bool IsTableBalancingEnabled(const TTablePtr& /*table*/) const = 0;

    virtual bool IsPickPivotKeysEnabled() const = 0;

    virtual const std::string& GetBundleName() const = 0;
    virtual const TGroupName& GetGroupName() const = 0;
    virtual const TTabletCellBundlePtr GetBundle() const = 0;
    virtual const TTabletBalancerDynamicConfigPtr& GetDynamicConfig() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReshardIteration)

////////////////////////////////////////////////////////////////////////////////

IReshardIterationPtr CreateSizeReshardIteration(
    TBundleSnapshotPtr bundleSnapshot,
    TGroupName groupName,
    TTabletBalancerDynamicConfigPtr dynamicConfig);

IReshardIterationPtr CreateParameterizedReshardIteration(
    TBundleSnapshotPtr bundleSnapshot,
    TGroupName groupName,
    TTabletBalancerDynamicConfigPtr dynamicConfig);

IReshardIterationPtr CreateReplicaReshardIteration(
    TBundleSnapshotPtr bundleSnapshot,
    TGroupName groupName,
    TTabletBalancerDynamicConfigPtr dynamicConfig,
    TClusterName clusterName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
