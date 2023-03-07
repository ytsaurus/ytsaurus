#include "peer_block_table.h"
#include "private.h"
#include "config.h"

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/profiling/timing.h>

#include <util/generic/algorithm.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = P2PLogger;

////////////////////////////////////////////////////////////////////////////////

TBlockPeerData::TBlockPeerData(int entryCountLimit)
    : EntryCountLimit_(entryCountLimit)
{ }

TBlockPeerData::TNodeIdList TBlockPeerData::GetPeers()
{
    TBlockPeerData::TNodeIdList result;
    {
        auto guard = Guard(Lock_);
        result.reserve(Entries_.size());
        DoSweep([&] (const auto& entry) { result.push_back(entry.NodeId); });
    }
    SortUnique(result);
    return result;
}

void TBlockPeerData::AddPeer(TNodeId nodeId, TInstant expirationTime)
{
    auto guard = Guard(Lock_);
    Entries_.push_back({nodeId, expirationTime});
}

bool TBlockPeerData::Sweep()
{
    auto guard = Guard(Lock_);
    DoSweep([&] (const auto& /*entry*/) { });
    return !Entries_.empty();
}

template <class F>
void TBlockPeerData::DoSweep(F&& func)
{
    auto now = NProfiling::GetInstant();

    auto sourceIt = Entries_.begin();
    auto destIt = sourceIt;
    while (sourceIt != Entries_.end()) {
        auto& entry = *sourceIt;
        if (entry.ExpirationTime < now) {
            ++sourceIt;
        } else {
            func(entry);
            *destIt++ = *sourceIt++;
        }
    }
    Entries_.erase(destIt, Entries_.end());

    // x2 is for proper amortization.
    if (Entries_.size() > 2 * EntryCountLimit_) {
        SortUniqueBy(
            Entries_,
            [] (const auto& entry) { return entry.NodeId; });
        Sort(
            Entries_,
            [] (const auto& lhs, const auto& rhs) { return lhs.ExpirationTime > rhs.ExpirationTime; });
        // NB: SortUniqueBy could have already pruned the vector.
        if (Entries_.size() > EntryCountLimit_) {
            Entries_.erase(Entries_.begin() + EntryCountLimit_, Entries_.end());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TPeerBlockTable::TPeerBlockTable(
    TPeerBlockTableConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , SweepExecutor_(New<TPeriodicExecutor>(
        bootstrap->GetStorageHeavyInvoker(),
        BIND(&TPeerBlockTable::OnSweep, MakeWeak(this)),
        Config_->SweepPeriod))
{
    SweepExecutor_->Start();
}

TBlockPeerDataPtr TPeerBlockTable::FindOrCreatePeerData(const TBlockId& blockId, bool insert)
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        TReaderGuard guard(Lock_);
        auto it = BlockIdToData_.find(blockId);
        if (it != BlockIdToData_.end()) {
            return it->second;
        }
    }

    if (!insert) {
        return nullptr;
    }

    {
        TWriterGuard guard(Lock_);
        auto it = BlockIdToData_.find(blockId);
        if (it == BlockIdToData_.end()) {
            it = BlockIdToData_.emplace(blockId, New<TBlockPeerData>(Config_->MaxPeersPerBlock)).first;
        }
        return it->second;
    }
}

void TPeerBlockTable::OnSweep()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Starting expired peers sweep");

    std::vector<std::pair<TBlockId, TBlockPeerDataPtr>> snapshot;
    {
        TReaderGuard guard(Lock_);
        snapshot.reserve(BlockIdToData_.size());
        for (const auto& pair : BlockIdToData_) {
            snapshot.push_back(pair);
        }
    }

    YT_LOG_DEBUG("Peer table snapshot captured (BlockCount: %v)",
        snapshot.size());

    std::vector<TBlockId> blockIdsToEvict;
    for (const auto& [blockId, data] : snapshot) {
        if (!data->Sweep()) {
            blockIdsToEvict.push_back(blockId);
        }
    }

    if (!blockIdsToEvict.empty()) {
        TWriterGuard guard(Lock_);
        for (auto blockId : blockIdsToEvict) {
            BlockIdToData_.erase(blockId);
        }
    }

    YT_LOG_DEBUG("Finished expired peers sweep (EvictedBlockCount: %v)",
        blockIdsToEvict.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
