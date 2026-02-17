#include "bundle_state.h"
#include "config.h"
#include "private.h"
#include "table_registry.h"
#include "reshard_iteration.h"

#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/parameterized_balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NTabletBalancer {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TReshardIterationBase
    : public IReshardIteration
{
public:
    TReshardIterationBase(
        TBundleSnapshotPtr bundleSnapshot,
        TGroupName groupName,
        TTabletBalancerDynamicConfigPtr dynamicConfig)
        : BundleName_(bundleSnapshot->Bundle->Name)
        , GroupName_(std::move(groupName))
        , BundleSnapshot_(std::move(bundleSnapshot))
        , DynamicConfig_(std::move(dynamicConfig))
    { }

    bool IsGroupBalancingEnabled(const TTabletBalancingGroupConfigPtr& /*groupConfig*/) const override
    {
        return true;
    }

    bool IsTableBalancingEnabled(const TTablePtr& /*table*/) const override
    {
        return true;
    }

    bool IsPickPivotKeysEnabled() const override
    {
        return DynamicConfig_->PickReshardPivotKeys && BundleSnapshot_->Bundle->Config->EnablePickPivotKeys;
    }

    const std::string& GetBundleName() const override
    {
        return BundleName_;
    }

    const TTabletCellBundlePtr GetBundle() const override
    {
        return BundleSnapshot_->Bundle;
    }

    const TGroupName& GetGroupName() const override
    {
        return GroupName_;
    }

    const TTabletBalancerDynamicConfigPtr& GetDynamicConfig() const override
    {
        return DynamicConfig_;
    }

    TTableProfilingCounters& GetProfilingCounters(const TTable* table)
    {
        return BundleSnapshot_->TableRegistry->GetProfilingCounters(table, GroupName_);
    }

protected:
    const std::string BundleName_;
    const TGroupName GroupName_;
    const TBundleSnapshotPtr BundleSnapshot_;
    const TTabletBalancerDynamicConfigPtr DynamicConfig_;
};

////////////////////////////////////////////////////////////////////////////////

class TSizeReshardIteration
    : public TReshardIterationBase
{
public:
    TSizeReshardIteration(
        TBundleSnapshotPtr bundleSnapshot,
        TGroupName groupName,
        TTabletBalancerDynamicConfigPtr dynamicConfig)
        : TReshardIterationBase(
            std::move(bundleSnapshot),
            std::move(groupName),
            std::move(dynamicConfig))
    { }

    void StartIteration() const override
    {
        YT_LOG_INFO("Balancing tablets via reshard started (BundleName: %v, Group: %v)",
            BundleName_,
            GroupName_);
    }

    void Prepare(const TTabletBalancingGroupConfigPtr& /*groupConfig*/) override
    { }

    std::vector<TTablePtr> GetTablesToReshard(const TTabletCellBundlePtr& bundle) const override
    {
        std::vector<TTablePtr> tables;
        for (const auto& [id, table] : bundle->Tables) {
            if (TypeFromId(id) != EObjectType::Table) {
                continue;
            }

            if (table->GetBalancingGroup() != GroupName_) {
                continue;
            }

            if (!table->Sorted) {
                continue;
            }

            tables.push_back(table);
        }
        return tables;
    }

    bool IsGroupBalancingEnabled(const TTabletBalancingGroupConfigPtr& groupConfig) const override
    {
        auto enableReplicaBalancing = !groupConfig->Parameterized->ReplicaClusters.empty();
        if (enableReplicaBalancing) {
            YT_LOG_INFO("Balancing tablets via reshard by size is disabled, "
                "the group will be replica balanced (BundleName: %v, Group: %v)",
                BundleName_,
                GroupName_);
        }
        return !enableReplicaBalancing;
    }

    bool IsTableBalancingEnabled(const TTablePtr& table) const override
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
            if (IsTabletReshardable(tablet)) {
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
            IsPickPivotKeysEnabled(),
            Logger())
            .AsyncVia(invoker)
            .Run();
    }

    void UpdateProfilingCounters(
        const TTable* table,
        const TReshardDescriptor& descriptor) override
    {
        if (descriptor.TabletCount == 1) {
            GetProfilingCounters(table).TabletMerges.Increment(1);
        } else if (std::ssize(descriptor.Tablets) == 1) {
            GetProfilingCounters(table).TabletSplits.Increment(1);
        } else {
            GetProfilingCounters(table).NonTrivialReshards.Increment(1);
        }
    }

    void FinishIteration(int actionCount) const override
    {
        YT_LOG_INFO("Balancing tablets via reshard finished (BundleName: %v, Group: %v, ActionCount: %v)",
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
        TBundleSnapshotPtr bundleSnapshot,
        TGroupName groupName,
        TTabletBalancerDynamicConfigPtr dynamicConfig)
        : TReshardIterationBase(
            std::move(bundleSnapshot),
            std::move(groupName),
            std::move(dynamicConfig))
    { }

    void StartIteration() const override
    {
        YT_LOG_INFO("Balancing tablets via parameterized reshard started (BundleName: %v, Group: %v)",
            BundleName_,
            GroupName_);
    }

    void Prepare(const TTabletBalancingGroupConfigPtr& groupConfig) override
    {
        Resharder_ = CreateParameterizedResharder(
            BundleSnapshot_->Bundle,
            BundleSnapshot_->PerformanceCountersKeys,
            TParameterizedResharderConfig{
                .EnableReshardByDefault = DynamicConfig_->EnableParameterizedReshardByDefault,
                .Metric = DynamicConfig_->DefaultParameterizedMetric,
            }.MergeWith(groupConfig->Parameterized),
            GroupName_,
            Logger());
    }

    std::vector<TTablePtr> GetTablesToReshard(const TTabletCellBundlePtr& bundle) const override
    {
        std::vector<TTablePtr> tables;
        for (const auto& [tableId, table] : bundle->Tables) {
            tables.push_back(table);
        }
        return tables;
    }

    bool IsGroupBalancingEnabled(const TTabletBalancingGroupConfigPtr& groupConfig) const override
    {
        if (!groupConfig->Parameterized->ReplicaClusters.empty()) {
            YT_LOG_INFO("Balancing tablets via parameterized reshard is disabled, "
                "the group will be replica balanced (BundleName: %v, Group: %v)",
                BundleName_,
                GroupName_);
        }

        auto enable = groupConfig->Parameterized->EnableReshard.value_or(
            DynamicConfig_->EnableParameterizedReshardByDefault);
        if (!enable) {
            YT_LOG_INFO("Balancing tablets via parameterized reshard is disabled (BundleName: %v, Group: %v)",
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
        const TTable* table,
        const TReshardDescriptor& descriptor) override
    {
        if (descriptor.TabletCount == 1) {
            GetProfilingCounters(table).ParameterizedReshardMerges.Increment(1);
        } else if (std::ssize(descriptor.Tablets) == 1) {
            GetProfilingCounters(table).ParameterizedReshardSplits.Increment(1);
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

    void FinishIteration(int actionCount) const override
    {
        YT_LOG_INFO("Balancing tablets via parameterized reshard finished (BundleName: %v, Group: %v, ActionCount: %v)",
            BundleName_,
            GroupName_,
            actionCount);
    }

private:
    IParameterizedResharderPtr Resharder_;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicaReshardIteration
    : public TSizeReshardIteration
{
public:
    TReplicaReshardIteration(
        TBundleSnapshotPtr bundleSnapshot,
        TGroupName groupName,
        TTabletBalancerDynamicConfigPtr dynamicConfig,
        TClusterName clusterName)
        : TSizeReshardIteration(
            std::move(bundleSnapshot),
            std::move(groupName),
            std::move(dynamicConfig))
        , SelfClusterName_(std::move(clusterName))
    { }

    void StartIteration() const override
    {
        YT_LOG_INFO("Balancing tablets via replica reshard started (BundleName: %v, Group: %v)",
            BundleName_,
            GroupName_);
    }

    void Prepare(const TTabletBalancingGroupConfigPtr& groupConfig) override
    {
        YT_VERIFY(TableToReferenceTable_.empty());
        YT_VERIFY(SelfReferenceTableIds_.empty());
        YT_VERIFY(!groupConfig->Parameterized->ReplicaClusters.empty());

        if (BundleSnapshot_->ReplicaBalancingFetchFailed) {
            YT_LOG_INFO("Balancing tablets via replica reshard is not possible because "
                "last statistics fetch failed (BundleName: %v, Group: %v)",
                BundleName_,
                GroupName_);
            THROW_ERROR_EXCEPTION(
                NTabletBalancer::EErrorCode::StatisticsFetchFailed,
                "Not all statistics for replica reshard balancing were fetched");
        }

        for (const auto& [id, table] : BundleSnapshot_->Bundle->Tables) {
            if (TypeFromId(id) != EObjectType::Table) {
                continue;
            }

            auto groupName = table->GetBalancingGroup();
            if (groupName != GroupName_) {
                continue;
            }

            if (!table->Sorted) {
                continue;
            }

            if (!table->IsReplicaReshardBalancingEnabled()) {
                continue;
            }

            if (!table->ReplicaMode) {
                YT_LOG_DEBUG("Cannot reshard table because replica mode was not fetched (TableId: %v)", id);
                continue;
            }

            if (auto response = FindReferenceTable(table); response.AreAllReplicasValid) {
                if (response.AlienReferenceTable) {
                    EmplaceOrCrash(TableToReferenceTable_, id, response.AlienReferenceTable);
                } else {
                    SelfReferenceTableIds_.insert(id);
                }
            }
        }
    }

    std::vector<TTablePtr> GetTablesToReshard(const TTabletCellBundlePtr& bundle) const override
    {
        std::vector<TTablePtr> tables;
        for (const auto& [id, table] : bundle->Tables) {
            if (SelfReferenceTableIds_.contains(id) || TableToReferenceTable_.contains(id)) {
                tables.push_back(table);
            }
        }
        return tables;
    }

    bool IsGroupBalancingEnabled(const TTabletBalancingGroupConfigPtr& groupConfig) const override
    {
        return !groupConfig->Parameterized->ReplicaClusters.empty();
    }

    bool IsTableBalancingEnabled(const TTablePtr& /*table*/) const override
    {
        return true;
    }

    TFuture<std::vector<TReshardDescriptor>> MergeSplitTable(
        const TTablePtr& table,
        const IInvokerPtr& invoker) override
    {
        auto referenceTable = TableToReferenceTable_.find(table->Id);
        if (referenceTable == TableToReferenceTable_.end()) {
            // The table is reference itself, so balance it as usual.
            YT_VERIFY(SelfReferenceTableIds_.contains(table->Id));
            return TSizeReshardIteration::MergeSplitTable(table, invoker);
        }

        return BIND(
            MergeSplitReplicaTable,
            table,
            referenceTable->second,
            DynamicConfig_->ActionManager->MaxTabletCountPerAction,
            Logger()
                .WithTag("BundleName: %v", BundleName_)
                .WithTag("TableId: %v", table->Id))
            .AsyncVia(invoker)
            .Run();
    }

    void UpdateProfilingCounters(
        const TTable* table,
        const TReshardDescriptor& descriptor) override
    {
        if (descriptor.TabletCount == 1) {
            GetProfilingCounters(table).ReplicaMerges.Increment(1);
        } else if (std::ssize(descriptor.Tablets) == 1) {
            GetProfilingCounters(table).ReplicaSplits.Increment(1);
        } else {
            GetProfilingCounters(table).ReplicaNonTrivialReshards.Increment(1);
        }
    }

    void FinishIteration(int actionCount) const override
    {
        YT_LOG_INFO("Balancing tablets via replica reshard finished (BundleName: %v, Group: %v, ActionCount: %v)",
            BundleName_,
            GroupName_,
            actionCount);
    }

private:
    const TClusterName SelfClusterName_;

    THashMap<TTableId, TAlienTablePtr> TableToReferenceTable_;
    THashSet<TTableId> SelfReferenceTableIds_;

    struct TReferenceTableSearchResponse
    {
        TAlienTablePtr AlienReferenceTable;
        bool AreAllReplicasValid = true;
    };

    //! Returns nullptr if table is reference itself.
    TReferenceTableSearchResponse FindReferenceTable(const TTablePtr& table) const
    {
        struct TTableKey
        {
            NTabletClient::ETableReplicaMode Mode = ETableReplicaMode::Sync;
            TClusterName Cluster;
            TTableId Id;
        };

        std::vector<TTableKey> replicas;
        replicas.emplace_back(TTableKey{
            .Mode = *table->ReplicaMode,
            .Cluster = SelfClusterName_,
            .Id = table->Id,
        });

        THashMap<TTableId, TAlienTablePtr> alienTables;
        for (const auto& [cluster, minorTablePaths] : table->GetReplicaBalancingMinorTables(SelfClusterName_)) {
            if (BundleSnapshot_->BannedReplicaClusters.contains(cluster)) {
                YT_LOG_DEBUG("Skipping cluster because statistics of the banned replica were not fetched "
                    "(BundleName: %v, Group: %v, Cluster: %v)",
                    BundleName_,
                    GroupName_,
                    cluster);
                continue;
            }

            for (const auto& minorTablePath : minorTablePaths) {
                auto it = BundleSnapshot_->AlienTablePaths.find(TBundleSnapshot::TAlienTableTag(cluster, minorTablePath));
                THROW_ERROR_EXCEPTION_IF(it == BundleSnapshot_->AlienTablePaths.end(),
                    "Not all tables was resolved. Table id for table %v was not found. Check that table path is correct",
                    minorTablePath);


                auto minorTableIt = BundleSnapshot_->AlienTables.find(it->second);
                if (minorTableIt == BundleSnapshot_->AlienTables.end()) {
                    YT_LOG_DEBUG("Alien table attributes or statistics was not fetched successfully "
                        "(MajorTableId: %v, MinorTableId: %v)",
                        table->Id,
                        it->second);
                    return TReferenceTableSearchResponse{.AreAllReplicasValid = false};
                }

                const auto& minorTable = minorTableIt->second;
                EmplaceOrCrash(alienTables, minorTable->Id, minorTable);

                if (!minorTable->ReplicaMode) {
                    YT_LOG_DEBUG("Cannot find replica mode for minor table (MajorTableId: %v, MinorTableId: %v)",
                        table->Id,
                        minorTable->Id);
                    return TReferenceTableSearchResponse{.AreAllReplicasValid = false};
                }

                replicas.push_back(TTableKey{
                    .Mode = *minorTable->ReplicaMode,
                    .Cluster = cluster,
                    .Id = minorTable->Id,
                });
            }
        }

        //! Sorting by sync mode, then by cluster name, then by table id to determine major table.
        auto referenceTableId = std::min_element(replicas.begin(), replicas.end(), [] (const auto& lhs, const auto& rhs) {
            if (lhs.Mode != rhs.Mode) {
                return (lhs.Mode == NTabletClient::ETableReplicaMode::Sync) > (rhs.Mode == NTabletClient::ETableReplicaMode::Sync);
            } else if (lhs.Cluster != rhs.Cluster) {
                return lhs.Cluster < rhs.Cluster;
            }
            return lhs.Id < rhs.Id;
        })->Id;

        TReferenceTableSearchResponse response;
        if (referenceTableId != table->Id) {
            response.AlienReferenceTable = GetOrCrash(alienTables, referenceTableId);

            THROW_ERROR_EXCEPTION_IF(table->PivotKeys.empty(),
                NTabletBalancer::EErrorCode::StatisticsFetchFailed,
                "Pivot keys for table %v were not fetched successfully",
                table->Id);

            THROW_ERROR_EXCEPTION_IF(response.AlienReferenceTable->PivotKeys.empty(),
                NTabletBalancer::EErrorCode::StatisticsFetchFailed,
                "Pivot keys for reference table %v were not fetched successfully",
                referenceTableId);
        }
        return response;
    }
};

////////////////////////////////////////////////////////////////////////////////

IReshardIterationPtr CreateSizeReshardIteration(
    TBundleSnapshotPtr bundleSnapshot,
    TGroupName groupName,
    TTabletBalancerDynamicConfigPtr dynamicConfig)
{
    return New<TSizeReshardIteration>(
        std::move(bundleSnapshot),
        std::move(groupName),
        std::move(dynamicConfig));
}

IReshardIterationPtr CreateParameterizedReshardIteration(
    TBundleSnapshotPtr bundleSnapshot,
    TGroupName groupName,
    TTabletBalancerDynamicConfigPtr dynamicConfig)
{
    return New<TParameterizedReshardIteration>(
        std::move(bundleSnapshot),
        std::move(groupName),
        std::move(dynamicConfig));
}

IReshardIterationPtr CreateReplicaReshardIteration(
    TBundleSnapshotPtr bundleSnapshot,
    TGroupName groupName,
    TTabletBalancerDynamicConfigPtr dynamicConfig,
    TClusterName selfClusterName)
{
    return New<TReplicaReshardIteration>(
        std::move(bundleSnapshot),
        std::move(groupName),
        std::move(dynamicConfig),
        std::move(selfClusterName));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
