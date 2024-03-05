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

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletBalancer::NDryRun {

using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("TabletBalancer");

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString> DefaultPerformanceCountersKeys{
    #define XX(name, Name) #name,
    ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX
};

////////////////////////////////////////////////////////////////////////////////

void PrintDescriptors(const std::vector<TMoveDescriptor>& descriptors)
{
    for (const auto& descriptor : descriptors) {
        YT_LOG_INFO("TMoveDescriptor(CellId: %v, TabletId: %v)",
            descriptor.TabletCellId,
            descriptor.TabletId);
    }
}

void PrintDescriptors(const std::vector<TReshardDescriptor>& descriptors)
{
    for (const auto& descriptor : descriptors) {
        YT_LOG_INFO("TReshardDescriptor(Tablets: %v,  TabletCount: %v)",
            descriptor.Tablets,
            descriptor.TabletCount);
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
    const TString& parameterizedConfig,
    const TString& group)
{
    auto commonParameterizedConfig = ConvertTo<TParameterizedBalancingConfigPtr>(TYsonString(parameterizedConfig));
    auto groupConfig = GetOrCrash(bundle->Config->Groups, group)->Parameterized;

    auto enable = groupConfig->EnableReshard.value_or(false);
    if (!enable) {
        YT_LOG_DEBUG("Balancing tablets via parameterized reshard is disabled (BundleName: %v, Group: %v)",
            bundle->Name,
            group);
        return {};
    }

    auto config = TParameterizedResharderConfig()
        .MergeWith(commonParameterizedConfig)
        .MergeWith(groupConfig);
    auto resharder = CreateParameterizedResharder(
        bundle,
        DefaultPerformanceCountersKeys,
        /*performanceCountersTableSchema*/ nullptr,
        config,
        group,
        Logger);

    std::vector<TReshardDescriptor> descriptors;
    for (const auto& [id, table] : bundle->Tables) {
        YT_LOG_DEBUG("Performing table parameterized reshard (TableId: %v)", id);
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
            if (IsTabletReshardable(tablet, /*ignoreConfig*/ false)) {
                tablets.push_back(tablet);
            }
        }

        if (tablets.empty()) {
            YT_LOG_DEBUG("Table skipped since it has 0 mounted tablets (TableId: %v)", table->Id);
            continue;
        }

        YT_LOG_DEBUG("Resharding table (TableId: %v)", table->Id);
        auto tableDescriptors = MergeSplitTabletsOfTable(
            std::move(tablets),
            /*minDesiredTabletSize*/ 0,
            /*pickPivotKeys*/ true,
            Logger);

        descriptors.insert(descriptors.end(), tableDescriptors.begin(), tableDescriptors.end());
    }

    return descriptors;
}

void ValidateBundle(const TTabletCellBundlePtr& bundle)
{
    YT_LOG_ERROR_IF(bundle->TabletCells.empty(), "Bundle has no cells");
    YT_LOG_ERROR_IF(bundle->Tables.empty(), "Bundle has no tables");
    YT_LOG_ERROR_IF(bundle->NodeStatistics.empty(), "Bundle has no nodes");

    YT_LOG_DEBUG_UNLESS(bundle->TabletCells.empty(),
        "Reporting cell count (CellCount: %v)",
        bundle->TabletCells.size());

    YT_LOG_DEBUG_UNLESS(bundle->Tables.empty(),
        "Reporting table count (TableCount: %v)",
        bundle->Tables.size());

    YT_LOG_DEBUG_UNLESS(bundle->NodeStatistics.empty(),
        "Reporting node count (NodeCount: %v)",
        bundle->NodeStatistics.size());

    for (const auto& [id, table] : bundle->Tables) {
        YT_LOG_ERROR_IF(table->Tablets.empty(), "Table has no tablets (TableId: %v)", id);

        YT_LOG_DEBUG_UNLESS(table->Tablets.empty(),
            "Reporting tablet count (TableId: %v, TabletCount: %v)",
            id,
            table->Tablets.size());
    }
}

TTabletActionBatch Balance(
    EBalancingMode mode,
    const TTabletCellBundlePtr& bundle,
    const TString& group,
    const TString& parameterizedConfig)
{
    switch (mode) {
        case EBalancingMode::InMemoryMove: {
            return TTabletActionBatch{
                .MoveDescriptors = ReassignInMemoryTablets(
                    bundle,
                    /*movableTables*/ std::nullopt,
                    /*ignoreTableWiseConfig*/ false,
                    Logger)
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
                    /*performanceCountersTableSchema*/ nullptr,
                    config,
                    group,
                    /*metricTracker*/ nullptr,
                    Logger)
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
                    /*movableTables*/ std::nullopt,
                    Logger)
                };
        }
    }
}

TTabletActionBatch BalanceAndPrintDescriptors(
    EBalancingMode mode,
    const TTabletCellBundlePtr& bundle,
    const TString& group,
    const TString& parameterizedConfig)
{
    ValidateBundle(bundle);
    YT_LOG_INFO("Balancing iteration started");
    auto descriptors = Balance(mode, bundle, group, parameterizedConfig);
    YT_LOG_INFO("Balancing iteration finished");
    PrintDescriptors(descriptors);
    return descriptors;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer::NDryRun
