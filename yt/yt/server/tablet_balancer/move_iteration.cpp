#include "bundle_state.h"
#include "config.h"
#include "helpers.h"
#include "move_iteration.h"
#include "private.h"
#include "table_registry.h"

#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/parameterized_balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTabletBalancer {

using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TMoveIterationBase
    : public IMoveIteration
{
public:
    TMoveIterationBase(
        std::string groupName,
        TBundleSnapshotPtr bundleSnapshot,
        TTabletBalancingGroupConfigPtr groupConfig,
        TTabletBalancerDynamicConfigPtr dynamicConfig)
        : BundleName_(bundleSnapshot->Bundle->Name)
        , GroupName_(std::move(groupName))
        , BundleSnapshot_(std::move(bundleSnapshot))
        , GroupConfig_(std::move(groupConfig))
        , DynamicConfig_(std::move(dynamicConfig))
    { }

    void StartIteration() const override
    {
        YT_TLOG_INFO("Balancing tablets via move started")
            .With("BundleName", BundleName_)
            .With("Group", GroupName_)
            .With("MoveBalancingType", GetActionSubtypeName());
    }

    void LogDisabledBalancing() const override
    {
        YT_TLOG_INFO("Balancing tablets via move is disabled")
            .With("BundleName", BundleName_)
            .With("Group", GroupName_)
            .With("MoveBalancingType", GetActionSubtypeName());
    }

    void FinishIteration(int actionCount) const override
    {
        YT_TLOG_INFO("Balancing tablets via move finished")
            .With("BundleName", BundleName_)
            .With("Group", GroupName_)
            .With("MoveBalancingType", GetActionSubtypeName())
            .With("ActionCount", actionCount);
    }

    const std::string& GetBundleName() const override
    {
        return BundleName_;
    }

    const std::string& GetGroupName() const override
    {
        return GroupName_;
    }

    const TTabletCellBundlePtr& GetBundle() const override
    {
        return BundleSnapshot_->Bundle;
    }

    const TTabletBalancerDynamicConfigPtr& GetDynamicConfig() const override
    {
        return DynamicConfig_;
    }

    const TTabletBalancingGroupConfigPtr& GetGroupConfig() const override
    {
        return GroupConfig_;
    }

    TGuard<NYT::NThreading::TSpinLock> StartApplyingActions() const override
    {
        return Guard(BundleSnapshot_->PublishedObjectLock);
    }

    void ApplyMoveAction(const TTabletPtr& tablet, TTabletCellId cellId) const override
    {
        YT_ASSERT_SPINLOCK_AFFINITY(BundleSnapshot_->PublishedObjectLock);
        ApplyMoveTabletAction(tablet, cellId);
    }

    void Prepare() override
    { }

    TTableProfilingCounters& GetProfilingCounters(const TTable* table)
    {
        return BundleSnapshot_->TableRegistry->GetProfilingCounters(table, GroupName_);
    }

protected:
    const std::string BundleName_;
    const TGroupName GroupName_;
    const TBundleSnapshotPtr BundleSnapshot_;
    const TTabletBalancingGroupConfigPtr GroupConfig_;
    const TTabletBalancerDynamicConfigPtr DynamicConfig_;

    std::vector<TMoveDescriptor> AnnotateSmoothMovementDescriptors(
        std::vector<TMoveDescriptor> descriptors)
    {
        auto [hasTrue, hasFalse] = EvaluateFeatureFlag(
            &TFeatureFlagConfig::EnableSmoothMovement,
            DynamicConfig_,
            GroupConfig_,
            BundleSnapshot_->Bundle->Config);

        auto [orderedHasTrue, orderedHasFalse] = EvaluateFeatureFlag(
            &TFeatureFlagConfig::EnableSmoothMovementForOrdered,
            DynamicConfig_,
            GroupConfig_,
            BundleSnapshot_->Bundle->Config);

        if (hasFalse) {
            return descriptors;
        }

        for (auto& descriptor : descriptors) {
            const auto& tablet = GetOrCrash(BundleSnapshot_->Bundle->Tablets, descriptor.TabletId);
            const auto* table = tablet->Table;

            // Support smooth movement for frozen tablets: YT-17388.
            if (tablet->State != ETabletState::Mounted) {
                continue;
            }

            bool smooth = table->TableConfig->EnableSmoothMovement.value_or(hasTrue);

            bool flagForOrdered = !orderedHasFalse && table->TableConfig->EnableSmoothMovementForOrdered.value_or(orderedHasTrue);

            if (!table->Sorted) {
                smooth &= flagForOrdered;
            }

            if (TypeFromId(table->Id) == EObjectType::ReplicationLogTable) {
                smooth &= flagForOrdered;
            } else if (TypeFromId(table->Id) != EObjectType::Table) {
                continue;
            }

            descriptor.Smooth = smooth;
        }

        return descriptors;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TOrdinaryMoveIteration
    : public TMoveIterationBase
{
public:
    TOrdinaryMoveIteration(
        TBundleSnapshotPtr bundleSnapshot,
        TTabletBalancingGroupConfigPtr groupConfig,
        TTabletBalancerDynamicConfigPtr dynamicConfig)
        : TMoveIterationBase(
            LegacyOrdinaryGroupName,
            std::move(bundleSnapshot),
            std::move(groupConfig),
            std::move(dynamicConfig))
    { }

    bool IsGroupBalancingEnabled() const override
    {
        return BundleSnapshot_->Bundle->Config->EnableCellBalancer;
    }

    EBalancingMode GetBalancingMode() const override
    {
        return EBalancingMode::OrdinaryMove;
    }

    TFuture<std::vector<TMoveDescriptor>> ReassignTablets(const IInvokerPtr& invoker) override
    {
        return BIND(
            ReassignOrdinaryTablets,
            BundleSnapshot_->Bundle,
            Logger())
            .AsyncVia(invoker)
            .Run()
            .Apply(BIND(
                &TOrdinaryMoveIteration::AnnotateSmoothMovementDescriptors,
                MakeStrong(this)));
    }

    TStringBuf GetActionSubtypeName() const override
    {
        static const TStringBuf subtypeName = "ordinary";
        return subtypeName;
    }

    void UpdateProfilingCounters(const TTable* table) override
    {
        GetProfilingCounters(table).OrdinaryMoves.Increment(1);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TInMemoryMoveIteration
    : public TMoveIterationBase
{
public:
    TInMemoryMoveIteration(
        TBundleSnapshotPtr bundleSnapshot,
        TTabletBalancingGroupConfigPtr groupConfig,
        TTabletBalancerDynamicConfigPtr dynamicConfig)
        : TMoveIterationBase(
            LegacyInMemoryGroupName,
            std::move(bundleSnapshot),
            std::move(groupConfig),
            std::move(dynamicConfig))
    { }

    bool IsGroupBalancingEnabled() const override
    {
        return BundleSnapshot_->Bundle->Config->EnableInMemoryCellBalancer;
    }

    EBalancingMode GetBalancingMode() const override
    {
        return EBalancingMode::InMemoryMove;
    }

    TFuture<std::vector<TMoveDescriptor>> ReassignTablets(const IInvokerPtr& invoker) override
    {
        return BIND(
            ReassignInMemoryTablets,
            BundleSnapshot_->Bundle,
            Logger())
            .AsyncVia(invoker)
            .Run()
            .Apply(BIND(
                &TInMemoryMoveIteration::AnnotateSmoothMovementDescriptors,
                MakeStrong(this)));
    }

    TStringBuf GetActionSubtypeName() const override
    {
        static const TStringBuf subtypeName = "in-memory";
        return subtypeName;
    }

    void UpdateProfilingCounters(const TTable* table) override
    {
        GetProfilingCounters(table).InMemoryMoves.Increment(1);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TParameterizedMoveIterationBase
    : public TMoveIterationBase
{
public:
    TParameterizedMoveIterationBase(
        std::string groupName,
        TBundleSnapshotPtr bundleSnapshot,
        TTableParameterizedMetricTrackerPtr metricTracker,
        TTabletBalancingGroupConfigPtr groupConfig,
        TTabletBalancerDynamicConfigPtr dynamicConfig,
        IThreadPoolPtr workerPool)
        : TMoveIterationBase(
            std::move(groupName),
            std::move(bundleSnapshot),
            std::move(groupConfig),
            std::move(dynamicConfig))
        , MetricTracker_(std::move(metricTracker))
        , WorkerPool_(std::move(workerPool))
    { }

    bool IsGroupBalancingEnabled() const override
    {
        return true;
    }

protected:
    const TTableParameterizedMetricTrackerPtr MetricTracker_;
    const IThreadPoolPtr WorkerPool_;

    TParameterizedReassignSolverConfig GetReassignSolverConfig()
    {
        return TParameterizedReassignSolverConfig{
            .MaxMoveActionCount = DynamicConfig_->MaxParameterizedMoveActionCount,
            .NodeDeviationThreshold = DynamicConfig_->ParameterizedNodeDeviationThreshold,
            .CellDeviationThreshold = DynamicConfig_->ParameterizedCellDeviationThreshold,
            .MinRelativeMetricImprovement = DynamicConfig_->ParameterizedMinRelativeMetricImprovement,
            .Metric = DynamicConfig_->DefaultParameterizedMetric,
            .Factors = DynamicConfig_->ParameterizedFactors,
        }.MergeWith(
            GroupConfig_->Parameterized,
            std::min(DynamicConfig_->MaxActionsPerGroup, DynamicConfig_->MaxParameterizedMoveActionHardLimit));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TParameterizedMoveIteration
    : public TParameterizedMoveIterationBase
{
public:
    using TParameterizedMoveIterationBase::TParameterizedMoveIterationBase;

    EBalancingMode GetBalancingMode() const override
    {
        return EBalancingMode::ParameterizedMove;
    }

    TFuture<std::vector<TMoveDescriptor>> ReassignTablets(const IInvokerPtr& invoker) override
    {
        return BIND(
            ReassignTabletsParameterized,
            BundleSnapshot_->Bundle,
            BundleSnapshot_->PerformanceCountersKeys,
            GetReassignSolverConfig(),
            GroupName_,
            MetricTracker_,
            WorkerPool_,
            Logger())
            .AsyncVia(invoker)
            .Run()
            .Apply(BIND(
                &TParameterizedMoveIteration::AnnotateSmoothMovementDescriptors,
                MakeStrong(this)));
    }

    TStringBuf GetActionSubtypeName() const override
    {
        static const TStringBuf subtypeName = "parameterized";
        return subtypeName;
    }

    bool IsGroupBalancingEnabled() const override
    {
        return GroupConfig_->Parameterized->ReplicaClusters.empty();
    }

    void UpdateProfilingCounters(const TTable* table) override
    {
        GetProfilingCounters(table).ParameterizedMoves.Increment(1);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReplicaMoveIteration
    : public TParameterizedMoveIterationBase
{
public:
    TReplicaMoveIteration(
        std::string groupName,
        TBundleSnapshotPtr bundleSnapshot,
        TTableParameterizedMetricTrackerPtr metricTracker,
        TTabletBalancingGroupConfigPtr groupConfig,
        TTabletBalancerDynamicConfigPtr dynamicConfig,
        IThreadPoolPtr workerPool,
        std::string selfClusterName)
        : TParameterizedMoveIterationBase(
            std::move(groupName),
            std::move(bundleSnapshot),
            std::move(metricTracker),
            std::move(groupConfig),
            std::move(dynamicConfig),
            std::move(workerPool))
        , SelfClusterName_(std::move(selfClusterName))
    { }

    EBalancingMode GetBalancingMode() const override
    {
        return EBalancingMode::ReplicaMove;
    }

    void Prepare() override
    {
        if (BundleSnapshot_->ReplicaBalancingFetchFailed) {
            YT_TLOG_INFO("Balancing tablets via replica move is not possible because last statistics fetch failed")
                .With("BundleName", BundleName_)
                .With("Group", GroupName_);
            THROW_ERROR_EXCEPTION(
                NTabletBalancer::EErrorCode::StatisticsFetchFailed,
                "Not all statistics for replica move balancing were fetched");
        }

        for (const auto& [id, table] : BundleSnapshot_->Bundle->Tables) {
            if (table->GetBalancingGroup() != GroupName_) {
                continue;
            }

            if (!table->IsReplicaMoveBalancingEnabled()) {
                continue;
            }

            THROW_ERROR_EXCEPTION_IF(table->PivotKeys.empty(),
                NTabletBalancer::EErrorCode::StatisticsFetchFailed,
                "Pivot keys for table %v were not fetched successfully",
                id);

            YT_VERIFY(std::ssize(table->PivotKeys) == std::ssize(table->Tablets));

            for (const auto& [cluster, minorTablePaths] : table->GetReplicaBalancingMinorTables(SelfClusterName_)) {
                if (BundleSnapshot_->BannedReplicaClusters.contains(cluster)) {
                    YT_TLOG_DEBUG("Skipping cluster because statistics of the banned replica were not fetched")
                        .With("BundleName", BundleName_)
                        .With("Group", GroupName_)
                        .With("Cluster", cluster);
                    continue;
                }

                // TODO(alexelexa): Detect unavaliable clusters and skip it.

                for (const auto& minorTablePath : minorTablePaths) {
                    auto it = BundleSnapshot_->AlienTablePaths.find(TBundleSnapshot::TAlienTableTag(cluster, minorTablePath));
                    THROW_ERROR_EXCEPTION_IF(it == BundleSnapshot_->AlienTablePaths.end(),
                        "Not all tables was resolved. Table id for table %v on cluster %Qv was not found. "
                        "Check that table path is correct",
                        minorTablePath,
                        cluster);

                    auto tableIt = BundleSnapshot_->AlienTables.find(it->second);
                    if (tableIt == BundleSnapshot_->AlienTables.end()) {
                        THROW_ERROR_EXCEPTION(
                            "Not all statistics was fetched successfully. Attributes or statistics of table %v on cluster %Qv was not found",
                            minorTablePath,
                            cluster)
                            .With("table_id", it->second);
                    }
                }
            }
        }

        YT_TLOG_INFO("Preparations for balancing tablets via move finished")
            .With("BundleName", BundleName_)
            .With("Group", GroupName_)
            .With("MoveBalancingType", GetActionSubtypeName());
    }

    TFuture<std::vector<TMoveDescriptor>> ReassignTablets(const IInvokerPtr& invoker) override
    {
        return BIND(
            ReassignTabletsReplica,
            BundleSnapshot_->Bundle,
            BundleSnapshot_->PerformanceCountersKeys,
            GetReassignSolverConfig(),
            GroupName_,
            MetricTracker_,
            WorkerPool_,
            Logger())
            .AsyncVia(invoker)
            .Run()
            .Apply(BIND(
                &TReplicaMoveIteration::AnnotateSmoothMovementDescriptors,
                MakeStrong(this)));
    }

    TStringBuf GetActionSubtypeName() const override
    {
        static const TStringBuf subtypeName = "replica";
        return subtypeName;
    }

    bool IsGroupBalancingEnabled() const override
    {
        return !GroupConfig_->Parameterized->ReplicaClusters.empty();
    }

    void UpdateProfilingCounters(const TTable* table) override
    {
        GetProfilingCounters(table).ReplicaMoves.Increment(1);
    }

private:
    const std::string SelfClusterName_;
};

////////////////////////////////////////////////////////////////////////////////

IMoveIterationPtr CreateOrdinaryMoveIteration(
    TBundleSnapshotPtr bundleSnapshot,
    TTabletBalancingGroupConfigPtr groupConfig,
    TTabletBalancerDynamicConfigPtr dynamicConfig)
{
    return New<TOrdinaryMoveIteration>(
        std::move(bundleSnapshot),
        std::move(groupConfig),
        std::move(dynamicConfig));
}

IMoveIterationPtr CreateInMemoryMoveIteration(
    TBundleSnapshotPtr bundleSnapshot,
    TTabletBalancingGroupConfigPtr groupConfig,
    TTabletBalancerDynamicConfigPtr dynamicConfig)
{
    return New<TInMemoryMoveIteration>(
        std::move(bundleSnapshot),
        std::move(groupConfig),
        std::move(dynamicConfig));
}

IMoveIterationPtr CreateParameterizedMoveIteration(
    std::string groupName,
    TBundleSnapshotPtr bundleSnapshot,
    TTableParameterizedMetricTrackerPtr metricTracker,
    TTabletBalancingGroupConfigPtr groupConfig,
    TTabletBalancerDynamicConfigPtr dynamicConfig,
    IThreadPoolPtr workerPool)
{
    return New<TParameterizedMoveIteration>(
        std::move(groupName),
        std::move(bundleSnapshot),
        std::move(metricTracker),
        std::move(groupConfig),
        std::move(dynamicConfig),
        std::move(workerPool));
}

IMoveIterationPtr CreateReplicaMoveIteration(
    std::string groupName,
    TBundleSnapshotPtr bundleSnapshot,
    TTableParameterizedMetricTrackerPtr metricTracker,
    TTabletBalancingGroupConfigPtr groupConfig,
    TTabletBalancerDynamicConfigPtr dynamicConfig,
    IThreadPoolPtr workerPool,
    std::string selfClusterName)
{
    return New<TReplicaMoveIteration>(
        std::move(groupName),
        std::move(bundleSnapshot),
        std::move(metricTracker),
        std::move(groupConfig),
        std::move(dynamicConfig),
        std::move(workerPool),
        std::move(selfClusterName));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
