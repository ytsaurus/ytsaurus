#include "tablet_sync_replica_cache.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ObsoleteEntriesSweepPeriod = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

TTabletSyncReplicaCache::TTabletSyncReplicaCache()
{
    ScheduleSweepObsoleteEntries();
}

std::vector<TTableReplicaIdList> TTabletSyncReplicaCache::Filter(
    THashMap<TCellId, std::vector<TTabletId>>* cellIdToTabletIds,
    TInstant cachedSyncReplicasDeadline)
{
    std::vector<TTableReplicaIdList> syncReplicaIdLists;

    auto readerGuard = ReaderGuard(Lock);

    for (auto& [cellId, tabletIds] : *cellIdToTabletIds) {
        std::vector<TTabletId> noncachedTabletIds;

        for (auto tabletId : tabletIds) {
            auto it = TabletIdToSyncReplicaIds_.find(tabletId);
            if (it == TabletIdToSyncReplicaIds_.end()) {
                noncachedTabletIds.push_back(tabletId);
                continue;
            }

            auto tabletGuard = Guard(it->second.Lock);

            if (it->second.CachedSyncReplicasAt >= cachedSyncReplicasDeadline) {
                syncReplicaIdLists.push_back(it->second.SyncReplicaIds);
            } else {
                noncachedTabletIds.push_back(tabletId);
            }
        }

        tabletIds = std::move(noncachedTabletIds);
    }

    return syncReplicaIdLists;
}

void TTabletSyncReplicaCache::Put(
    TInstant cachedSyncReplicasAt,
    THashMap<TTabletId, TTableReplicaIdList> tabletIdToSyncReplicaIds)
{
    std::vector<std::pair<TTabletId, TTableReplicaIdList>> newTabletIdToSyncReplicaIds;
    {
        auto readerGuard = ReaderGuard(Lock);

        for (auto&& [tabletId, syncReplicaIds] : tabletIdToSyncReplicaIds) {
            auto tabletIt = TabletIdToSyncReplicaIds_.find(tabletId);
            if (tabletIt == TabletIdToSyncReplicaIds_.end()) {
                newTabletIdToSyncReplicaIds.emplace_back(tabletId, std::move(syncReplicaIds));
            } else {
                auto tabletGuard = Guard(tabletIt->second.Lock);

                tabletIt->second.CachedSyncReplicasAt = cachedSyncReplicasAt;
                tabletIt->second.SyncReplicaIds = std::move(syncReplicaIds);
            }
        }
    }

    if (newTabletIdToSyncReplicaIds.empty()) {
        return;
    }

    auto writerGuard = WriterGuard(Lock);

    for (auto&& [tabletId, syncReplicaIds] : newTabletIdToSyncReplicaIds) {
        auto [tabletIt, emplaced] = TabletIdToSyncReplicaIds_.try_emplace(tabletId);
        if (!emplaced) {
            continue;
        }

        tabletIt->second.CachedSyncReplicasAt = cachedSyncReplicasAt;
        tabletIt->second.SyncReplicaIds = std::move(syncReplicaIds);
    }
}

void TTabletSyncReplicaCache::ScheduleSweepObsoleteEntries()
{
    TDelayedExecutor::Submit(
        BIND(
            &TTabletSyncReplicaCache::DoSweepObsoleteEntries,
            MakeWeak(this)),
        ObsoleteEntriesSweepPeriod);
}

void TTabletSyncReplicaCache::DoSweepObsoleteEntries()
{
    auto cachedSyncReplicasDeadline = TInstant::Now() - TDuration::Minutes(1);

    std::vector<TTabletId> obsoleteTabletIds;
    {
        auto readerGuard = ReaderGuard(Lock);

        for (const auto& [tabletId, syncReplicaIds] : TabletIdToSyncReplicaIds_) {
            auto tabletGuard = Guard(syncReplicaIds.Lock);

            if (syncReplicaIds.CachedSyncReplicasAt < cachedSyncReplicasDeadline) {
                obsoleteTabletIds.push_back(tabletId);
            }
        }
    }

    if (!obsoleteTabletIds.empty()) {
        auto writerGuard = WriterGuard(Lock);
        for (auto tabletId : obsoleteTabletIds) {
            TabletIdToSyncReplicaIds_.erase(tabletId);
        }
    }

    ScheduleSweepObsoleteEntries();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
