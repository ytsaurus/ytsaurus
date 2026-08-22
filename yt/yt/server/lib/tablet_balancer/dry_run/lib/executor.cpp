#include "executor.h"
#include "helpers.h"
#include "holders.h"

#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/parameterized_balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/server/lib/tablet_node/performance_counters.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletBalancer::NDryRun {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, Logger, "TabletBalancer");

////////////////////////////////////////////////////////////////////////////////

const std::vector<std::string> DefaultPerformanceCountersKeys{
    #define XX(name, Name) #name,
    ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX
};

////////////////////////////////////////////////////////////////////////////////

void PrintDescriptors(const std::vector<TMoveDescriptor>& descriptors)
{
    for (const auto& descriptor : descriptors) {
        YT_TLOG_INFO("Move descriptor")
            .With("CellId", descriptor.TabletCellId)
            .With("TabletId", descriptor.TabletId);
    }
}

void PrintDescriptors(const std::vector<TReshardDescriptor>& descriptors)
{
    for (const auto& descriptor : descriptors) {
        YT_TLOG_INFO("Reshard descriptor")
            .With("Tablets", descriptor.Tablets)
            .With("TabletCount", descriptor.TabletCount);
    }
}

void PrintDescriptors(const TTabletActionBatch& descriptors)
{
    PrintDescriptors(descriptors.MoveDescriptors);
    PrintDescriptors(descriptors.ReshardDescriptors);
}

void ApplyMoveDescriptors(
    const TTabletCellBundlePtr& bundle,
    const std::vector<TMoveDescriptor>& descriptors)
{
    for (const auto& descriptor : descriptors) {
        auto tablet = FindTabletInBundle(bundle, descriptor.TabletId);
        ApplyMoveTabletAction(tablet, descriptor.TabletCellId);
    }
}

std::vector<TReshardDescriptor> ReshardBundleParameterized(
    const TTabletCellBundlePtr& bundle,
    const std::string& parameterizedConfig,
    const TGroupName& group)
{
    auto commonParameterizedConfig = ConvertTo<TParameterizedBalancingConfigPtr>(TYsonString(parameterizedConfig));
    auto groupConfig = GetOrCrash(bundle->Config->Groups, group)->Parameterized;

    auto enable = groupConfig->EnableReshard.value_or(false);
    if (!enable) {
        YT_TLOG_DEBUG("Balancing tablets via parameterized reshard is disabled")
            .With("BundleName", bundle->Name)
            .With("Group", group);
        return {};
    }

    auto config = TParameterizedResharderConfig()
        .MergeWith(commonParameterizedConfig)
        .MergeWith(groupConfig);
    auto resharder = CreateParameterizedResharder(
        bundle,
        DefaultPerformanceCountersKeys,
        config,
        group,
        Logger());

    std::vector<TReshardDescriptor> descriptors;
    for (const auto& [id, table] : bundle->Tables) {
        YT_TLOG_DEBUG("Performing table parameterized reshard")
            .With("TableId", id);
        auto tableDescriptors = resharder->BuildTableActionDescriptors(table);
        descriptors.insert(descriptors.end(), tableDescriptors.begin(), tableDescriptors.end());
    }

    return descriptors;
}

std::vector<TReshardDescriptor> ReshardBundle(const TTabletCellBundlePtr& bundle)
{
    std::vector<TTablePtr> tables;
    for (const auto& [id, table] : bundle->Tables) {
        if (TypeFromId(id) != EObjectType::Table) {
            continue;
        }

        tables.push_back(table);
    }

    SortBy(tables, [&] (const TTablePtr& table) {
        return table->Id;
    });

    std::vector<TReshardDescriptor> descriptors;

    for (const auto& table : tables) {
        std::vector<TTabletPtr> tablets;
        for (const auto& tablet : table->Tablets) {
            if (IsTabletReshardable(tablet)) {
                tablets.push_back(tablet);
            }
        }

        if (tablets.empty()) {
            YT_TLOG_DEBUG("Table skipped since it has 0 mounted tablets")
                .With("TableId", table->Id);
            continue;
        }

        YT_TLOG_DEBUG("Resharding table")
            .With("TableId", table->Id);
        auto tableDescriptors = MergeSplitTabletsOfTable(
            std::move(tablets),
            /*minDesiredTabletSize*/ 0,
            /*pickPivotKeys*/ true,
            Logger());

        descriptors.insert(descriptors.end(), tableDescriptors.begin(), tableDescriptors.end());
    }

    return descriptors;
}

void ValidateBundle(const TTabletCellBundlePtr& bundle)
{
    YT_TLOG_ERROR_IF(bundle->TabletCells.empty(), "Bundle has no cells");
    YT_TLOG_ERROR_IF(bundle->Tables.empty(), "Bundle has no tables");
    YT_TLOG_ERROR_IF(bundle->NodeStatistics.empty(), "Bundle has no nodes");

    YT_TLOG_DEBUG_UNLESS(bundle->TabletCells.empty(), "Reporting cell count")
        .With("CellCount", bundle->TabletCells.size());

    YT_TLOG_DEBUG_UNLESS(bundle->Tables.empty(), "Reporting table count")
        .With("TableCount", bundle->Tables.size());

    YT_TLOG_DEBUG_UNLESS(bundle->NodeStatistics.empty(), "Reporting node count")
        .With("NodeCount", bundle->NodeStatistics.size());

    for (const auto& [id, table] : bundle->Tables) {
        YT_TLOG_ERROR_IF(table->Tablets.empty(), "Table has no tablets")
            .With("TableId", id);

        YT_TLOG_DEBUG_UNLESS(table->Tablets.empty(), "Reporting tablet count")
            .With("TableId", id)
            .With("TabletCount", table->Tablets.size());
    }
}

TTabletActionBatch Balance(
    EBalancingMode mode,
    const TTabletCellBundlePtr& bundle,
    const TGroupName& group,
    const std::string& parameterizedConfig)
{
    switch (mode) {
        case EBalancingMode::InMemoryMove: {
            return TTabletActionBatch{
                .MoveDescriptors = ReassignInMemoryTablets(
                    bundle,
                    Logger())
                };
        }

        case EBalancingMode::ParameterizedMove: {
            auto commonParameterizedConfig = ConvertTo<TParameterizedBalancingConfigPtr>(TYsonString(parameterizedConfig));
            auto groupConfig = GetOrCrash(bundle->Config->Groups, group)->Parameterized;
            auto config = TParameterizedReassignSolverConfig()
                .MergeWith(commonParameterizedConfig)
                .MergeWith(groupConfig);

            return TTabletActionBatch{
                .MoveDescriptors = ReassignTabletsParameterized(
                    bundle,
                    DefaultPerformanceCountersKeys,
                    config,
                    group,
                    /*metricTracker*/ nullptr,
                    CreateThreadPool(
                        4,
                        "Worker"),
                    Logger())
            };
        }

        case EBalancingMode::Reshard: {
            return TTabletActionBatch{.ReshardDescriptors = ReshardBundle(bundle)};
        }

        case EBalancingMode::ParameterizedReshard: {
            return TTabletActionBatch{.ReshardDescriptors = ReshardBundleParameterized(bundle, parameterizedConfig, group)};
        }

        case EBalancingMode::OrdinaryMove: {
            return TTabletActionBatch{
                .MoveDescriptors = ReassignOrdinaryTablets(
                    bundle,
                    Logger())
            };
        }

        case EBalancingMode::ReplicaMove: {
            // TODO(alexelexa): Call replica balancing properly.
            // Right now, there is no way to do it without fetching performance counters and schema from statistics table.
            YT_ABORT();
        }
    }
}

TTabletActionBatch BalanceAndPrintDescriptors(
    EBalancingMode mode,
    const TTabletCellBundlePtr& bundle,
    const TGroupName& group,
    const std::string& parameterizedConfig)
{
    ValidateBundle(bundle);
    YT_TLOG_INFO("Balancing iteration started");
    auto descriptors = Balance(mode, bundle, group, parameterizedConfig);
    YT_TLOG_INFO("Balancing iteration finished");
    PrintDescriptors(descriptors);
    return descriptors;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer::NDryRun
