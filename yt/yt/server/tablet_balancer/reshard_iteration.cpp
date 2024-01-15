#include "bundle_state.h"
#include "config.h"
#include "private.h"
#include "reshard_iteration.h"

#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/parameterized_balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NTabletBalancer {

using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TReshardIterationBase
    : public IReshardIteration
{
public:
    TReshardIterationBase(
        TString bundleName,
        TString groupName,
        TTabletBalancerDynamicConfigPtr dynamicConfig)
        : BundleName_(std::move(bundleName))
        , GroupName_(std::move(groupName))
        , DynamicConfig_(std::move(dynamicConfig))
    { }

    bool IsGroupBalancingEnabled(const TTabletBalancingGroupConfigPtr& /*groupConfig*/) override
    {
        return true;
    }

    bool IsTableBalancingEnabled(const TTablePtr& /*table*/) override
    {
        return true;
    }

    const TString& GetBundleName() const override
    {
        return BundleName_;
    }

    const TString& GetGroupName() const override
    {
        return GroupName_;
    }

    const TTabletBalancerDynamicConfigPtr& GetDynamicConfig() const override
    {
        return DynamicConfig_;
    }

protected:
    TString BundleName_;
    TString GroupName_;
    TTabletBalancerDynamicConfigPtr DynamicConfig_;
};

////////////////////////////////////////////////////////////////////////////////

class TSizeReshardIteration
    : public TReshardIterationBase
{
public:
    TSizeReshardIteration(
        TString bundleName,
        TString groupName,
        TTabletBalancerDynamicConfigPtr dynamicConfig)
        : TReshardIterationBase(
            std::move(bundleName),
            std::move(groupName),
            std::move(dynamicConfig))
    { }

    void StartIteration() override
    {
        YT_LOG_DEBUG("Balancing tablets via reshard started (BundleName: %v, Group: %v)",
            BundleName_,
            GroupName_);
    }

    void Prepare(
        const TBundleStatePtr& /*bundleState*/,
        const TTabletBalancingGroupConfigPtr& /*groupConfig*/) override
    { }

    std::vector<TTablePtr> GetTablesToReshard(const TTabletCellBundlePtr& bundle) override
    {
        std::vector<TTablePtr> tables;
        for (const auto& [id, table] : bundle->Tables) {
            if (TypeFromId(id) != EObjectType::Table) {
                continue;
            }

            if (table->GetBalancingGroup() != GroupName_) {
                continue;
            }

            tables.push_back(table);
        }
        return tables;
    }

    bool IsTableBalancingEnabled(const TTablePtr& table) override
    {
        auto parameterizedBalancingEnabled = table->IsParameterizedReshardBalancingEnabled(
            DynamicConfig_->EnableParameterizedReshardByDefault);
        if (parameterizedBalancingEnabled) {
            YT_LOG_DEBUG("Will not balance table via reshard by size because "
                "parameterized reshard is enabled for this table (BundleName: %v, Group: %v, TableId: %v)",
                BundleName_,
                GroupName_,
                table->Id);
        }
        return !parameterizedBalancingEnabled;
    }

    TFuture<std::vector<TReshardDescriptor>> MergeSplitTable(
        const TTablePtr& table,
        const IInvokerPtr& invoker) override
    {
        std::vector<TTabletPtr> tablets;
        for (const auto& tablet : table->Tablets) {
            if (IsTabletReshardable(tablet, /*ignoreConfig*/ false)) {
                tablets.push_back(tablet);
            }
        }

        if (tablets.empty()) {
            return MakeFuture(std::vector<TReshardDescriptor>{});
        }

        return BIND(
                MergeSplitTabletsOfTable,
                Passed(std::move(tablets)),
                DynamicConfig_->MinDesiredTabletSize,
                DynamicConfig_->PickReshardPivotKeys,
                Logger)
            .AsyncVia(invoker)
            .Run();
    }

    void UpdateProfilingCounters(
        const TTablePtr& /*table*/,
        const TTableProfilingCounters& profilingCounters,
        const TReshardDescriptor& descriptor) override
    {
        if (descriptor.TabletCount == 1) {
            profilingCounters.TabletMerges.Increment(1);
        } else if (std::ssize(descriptor.Tablets) == 1) {
            profilingCounters.TabletSplits.Increment(1);
        } else {
            profilingCounters.NonTrivialReshards.Increment(1);
        }
    }

    void FinishIteration(int actionCount) override
    {
        YT_LOG_DEBUG("Balancing tablets via reshard finished (BundleName: %v, Group: %v, ActionCount: %v)",
            BundleName_,
            GroupName_,
            actionCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TParameterizedReshardIteration
    : public TReshardIterationBase
{
public:
    TParameterizedReshardIteration(
        TString bundleName,
        TString groupName,
        TTabletBalancerDynamicConfigPtr dynamicConfig)
        : TReshardIterationBase(
            std::move(bundleName),
            std::move(groupName),
            std::move(dynamicConfig))
    { }

    void StartIteration() override
    {
        YT_LOG_DEBUG("Balancing tablets via parameterized reshard started (BundleName: %v, Group: %v)",
            BundleName_,
            GroupName_);
    }

    void Prepare(
        const TBundleStatePtr& bundleState,
        const TTabletBalancingGroupConfigPtr& groupConfig) override
    {
        Resharder_ = CreateParameterizedResharder(
            bundleState->GetBundle(),
            bundleState->PerformanceCountersKeys(),
            bundleState->GetPerformanceCountersTableSchema(),
            TParameterizedResharderConfig{
                .EnableReshardByDefault = DynamicConfig_->EnableParameterizedReshardByDefault,
                .Metric = DynamicConfig_->DefaultParameterizedMetric,
            }.MergeWith(groupConfig->Parameterized),
            GroupName_,
            Logger);
    }

    std::vector<TTablePtr> GetTablesToReshard(const TTabletCellBundlePtr& bundle) override
    {
        std::vector<TTablePtr> tables;
        for (const auto& [tableId, table] : bundle->Tables) {
            tables.push_back(table);
        }
        return tables;
    }

    bool IsGroupBalancingEnabled(const TTabletBalancingGroupConfigPtr& groupConfig) override
    {
        auto enable = groupConfig->Parameterized->EnableReshard.value_or(
            DynamicConfig_->EnableParameterizedReshardByDefault);
        if (!enable) {
            YT_LOG_DEBUG("Balancing tablets via parameterized reshard is disabled (BundleName: %v, Group: %v)",
                BundleName_,
                GroupName_);
        }
        return enable;
    }

    TFuture<std::vector<TReshardDescriptor>> MergeSplitTable(
        const TTablePtr& table,
        const IInvokerPtr& invoker) override
    {
        return BIND(&IParameterizedResharder::BuildTableActionDescriptors, Resharder_, table)
            .AsyncVia(invoker)
            .Run();
    }

    void UpdateProfilingCounters(
        const TTablePtr& table,
        const TTableProfilingCounters& profilingCounters,
        const TReshardDescriptor& descriptor) override
    {
        if (descriptor.TabletCount == 1) {
            profilingCounters.ParameterizedReshardMerges.Increment(1);
        } else if (std::ssize(descriptor.Tablets) == 1) {
            profilingCounters.ParameterizedReshardSplits.Increment(1);
        } else {
            YT_LOG_ALERT(
                "Non-trivial reshards are forbidden in parameterized balancing, "
                "but for some reason they appeared (Bundle: %v, Group: %v, "
                "TableId: %v, TablePath: %v, TabletCount: %v, Tablets: %v, CorrelationId: %v)",
                BundleName_,
                GroupName_,
                table->Id,
                table->Path,
                descriptor.TabletCount,
                descriptor.Tablets,
                descriptor.CorrelationId);
        }
    }

    void FinishIteration(int actionCount) override
    {
        YT_LOG_DEBUG("Balancing tablets via parameterized reshard finished (BundleName: %v, Group: %v, ActionCount: %v)",
            BundleName_,
            GroupName_,
            actionCount);
    }

private:
    IParameterizedResharderPtr Resharder_;
};

////////////////////////////////////////////////////////////////////////////////

IReshardIterationPtr CreateSizeReshardIteration(
    TString bundleName,
    TString groupName,
    TTabletBalancerDynamicConfigPtr dynamicConfig)
{
    return New<TSizeReshardIteration>(
        std::move(bundleName),
        std::move(groupName),
        std::move(dynamicConfig));
}

IReshardIterationPtr CreateParameterizedReshardIteration(
    TString bundleName,
    TString groupName,
    TTabletBalancerDynamicConfigPtr dynamicConfig)
{
    return New<TParameterizedReshardIteration>(
        std::move(bundleName),
        std::move(groupName),
        std::move(dynamicConfig));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
