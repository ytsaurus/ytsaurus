#include "table_registry.h"

#include "private.h"

#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

void TTableRegistry::AddTable(const TTablePtr& table)
{
    if (auto it = Tables_.find(table->Id); it != Tables_.end()) {
        YT_VERIFY(it->second->Bundle != table->Bundle);

        UnlinkTableFromOldBundle(it->second);
        EraseOrCrash(it->second->Bundle->Tables, table->Id);

        it->second = table;
    } else {
        EmplaceOrCrash(Tables_, table->Id, table);
    }
}

void TTableRegistry::RemoveTable(const TTableId& tableId)
{
    auto it = Tables_.find(tableId);
    YT_VERIFY(it != Tables_.end());

    UnlinkTableFromOldBundle(it->second);
    Tables_.erase(it);

    ProfilingCounters_.erase(tableId);
}

void TTableRegistry::RemoveBundle(const TTabletCellBundlePtr& bundle)
{
    bundle->TabletCells.clear();
    for (const auto& [tableId, table] : bundle->Tables) {
        table->Tablets.clear();
        RemoveTable(tableId);
    }
    bundle->Tables.clear();
}

void TTableRegistry::AddAlienTablePath(const TClusterName& cluster, const NYPath::TYPath& path, TTableId tableId)
{
    EmplaceOrCrash(AlienTablePaths_, TAlienTableTag{cluster, path}, tableId);
}

void TTableRegistry::AddAlienTable(const TAlienTablePtr& table, const std::vector<TTableId>& majorTableIds)
{
    EmplaceOrCrash(AlienTables_, table->Id, table);
    TablesWithAlienTable_.insert(majorTableIds.begin(), majorTableIds.end());
}

void TTableRegistry::DropAllAlienTables()
{
    for (const auto& tableId : TablesWithAlienTable_) {
        const auto& table = GetOrCrash(Tables_, tableId);
        table->AlienTables.clear();
    }

    AlienTables_.clear();
    AlienTablePaths_.clear();
    TablesWithAlienTable_.clear();
}

void TTableRegistry::UnlinkTableFromOldBundle(const TTablePtr& table)
{
    for (const auto& tablet : table->Tablets) {
        UnlinkTabletFromCell(tablet);
    }
    table->Tablets.clear();
}

void TTableRegistry::UnlinkTabletFromCell(const TTabletPtr& tablet)
{
    if (auto cell = tablet->Cell.Lock()) {
        EraseOrCrash(cell->Tablets, tablet->Id);
    }
}

TTableProfilingCounters& TTableRegistry::GetProfilingCounters(
    const TTable* table,
    const TString& groupName)
{
    auto it = ProfilingCounters_.find(table->Id);
    if (it == ProfilingCounters_.end()) {
        auto profilingCounters = InitializeProfilingCounters(table, groupName);
        return EmplaceOrCrash(
            ProfilingCounters_,
            table->Id,
            std::move(profilingCounters))->second;
    }

    if (it->second.GroupName != groupName || it->second.BundleName != table->Bundle->Name) {
        it->second = InitializeProfilingCounters(table, groupName);
    }

    return it->second;
}

TTableProfilingCounters TTableRegistry::InitializeProfilingCounters(
    const TTable* table,
    const TString& groupName) const
{
    TTableProfilingCounters profilingCounters{.BundleName = table->Bundle->Name, .GroupName = groupName};
    auto profiler = TabletBalancerProfiler()
        .WithSparse()
        .WithTag("tablet_cell_bundle", profilingCounters.BundleName)
        .WithTag("group", groupName)
        .WithTag("table_path", table->Path);

    profilingCounters.InMemoryMoves = profiler.Counter("/tablet_balancer/in_memory_moves");
    profilingCounters.OrdinaryMoves = profiler.Counter("/tablet_balancer/ordinary_moves");

    profilingCounters.TabletMerges = profiler.Counter("/tablet_balancer/tablet_merges");
    profilingCounters.TabletSplits = profiler.Counter("/tablet_balancer/tablet_splits");
    profilingCounters.NonTrivialReshards = profiler.Counter("/tablet_balancer/non_trivial_reshards");

    profilingCounters.ParameterizedMoves = profiler.Counter("/tablet_balancer/parameterized_moves");

    profilingCounters.ReplicaMoves = profiler.Counter("/tablet_balancer/parameterized_replica_moves");

    profilingCounters.ParameterizedReshardMerges = profiler.Counter(
        "/tablet_balancer/parameterized_reshard_merges");
    profilingCounters.ParameterizedReshardSplits = profiler.Counter(
        "/tablet_balancer/parameterized_reshard_splits");

    profilingCounters.ReplicaMerges = profiler.Counter("/tablet_balancer/replica_merges");
    profilingCounters.ReplicaSplits = profiler.Counter("/tablet_balancer/replica_splits");
    profilingCounters.ReplicaNonTrivialReshards = profiler.Counter(
        "/tablet_balancer/replica_non_trivial_reshards");

    return profilingCounters;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
