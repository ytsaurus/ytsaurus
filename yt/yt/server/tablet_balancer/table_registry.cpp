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
    auto guard = ReaderGuard(ProfilingCountersLock_);
    auto it = ProfilingCounters_.find(table->Id);
    if (it == ProfilingCounters_.end()) {
        auto profilingCounters = InitializeProfilingCounters(table, groupName);

        guard.Release();
        auto writerGuard = WriterGuard(ProfilingCountersLock_);

        // Cannot EmplaceOrCrash due to a race between releasing read guard and aquiring write guard.
        return ProfilingCounters_.emplace(
            table->Id,
            std::move(profilingCounters)).first->second;
    }

    if (it->second.GroupName != groupName) {
        guard.Release();
        auto writerGuard = WriterGuard(ProfilingCountersLock_);
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
