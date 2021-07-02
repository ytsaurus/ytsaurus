#include "block_peer_table.h"
#include "bootstrap.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/profiling/timing.h>

#include <util/generic/algorithm.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = P2PLogger;

////////////////////////////////////////////////////////////////////////////////

TCachedPeerList::TCachedPeerList(int entryCountLimit)
    : EntryCountLimit_(entryCountLimit)
{ }

TCachedPeerList::TNodeIdList TCachedPeerList::GetPeers()
{
    TCachedPeerList::TNodeIdList result;
    {
        auto guard = Guard(Lock_);
        result.reserve(Entries_.size());
        DoSweep([&] (const auto& entry) { result.push_back(entry.NodeId); });
    }
    SortUnique(result);
    return result;
}

void TCachedPeerList::AddPeer(TNodeId nodeId, TInstant expirationTime)
{
    auto guard = Guard(Lock_);
    Entries_.push_back({nodeId, expirationTime});
}

bool TCachedPeerList::Sweep()
{
    auto guard = Guard(Lock_);
    DoSweep([&] (const auto& /*entry*/) { });
    return !Entries_.empty();
}

template <class F>
void TCachedPeerList::DoSweep(F&& func)
{
    auto now = NProfiling::GetInstant();

    auto sourceIt = Entries_.begin();
    auto destIt = sourceIt;
    while (sourceIt != Entries_.end()) {
        auto& entry = *sourceIt;
        if (entry.ExpirationDeadline < now) {
            ++sourceIt;
        } else {
            func(entry);
            *destIt++ = *sourceIt++;
        }
    }
    Entries_.erase(destIt, Entries_.end());

    // x2 is for proper amortization.
    if (std::ssize(Entries_) > 2 * EntryCountLimit_) {
        SortUniqueBy(
            Entries_,
            [] (const auto& entry) { return entry.NodeId; });
        Sort(
            Entries_,
            [] (const auto& lhs, const auto& rhs) { return lhs.ExpirationDeadline > rhs.ExpirationDeadline; });
        // NB: SortUniqueBy could have already pruned the vector.
        if (std::ssize(Entries_) > EntryCountLimit_) {
            Entries_.erase(Entries_.begin() + EntryCountLimit_, Entries_.end());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TBlockPeerTable::TBlockPeerTable(IBootstrap* bootstrap)
    : Config_(bootstrap->GetConfig()->DataNode->BlockPeerTable)
    , SweepExecutor_(New<TPeriodicExecutor>(
        bootstrap->GetStorageHeavyInvoker(),
        BIND(&TBlockPeerTable::OnSweep, MakeWeak(this)),
        Config_->SweepPeriod))
{
    SweepExecutor_->Start();
}

TCachedPeerListPtr TBlockPeerTable::FindPeerList(const TBlockId& blockId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(Lock_);
    auto it = BlockIdToPeerList_.find(blockId);
    return it == BlockIdToPeerList_.end() ? nullptr : it->second;
}

TCachedPeerListPtr TBlockPeerTable::FindPeerList(TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return FindPeerList(TBlockId(chunkId, AllBlocksIndex));
}

TCachedPeerListPtr TBlockPeerTable::GetOrCreatePeerList(const TBlockId& blockId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Fast path.
    if (auto peerList = FindPeerList(blockId)) {
        return peerList;
    }

    // Slow path.
    {
        auto guard = WriterGuard(Lock_);
        auto it = BlockIdToPeerList_.find(blockId);
        if (it == BlockIdToPeerList_.end()) {
            it = BlockIdToPeerList_.emplace(blockId, New<TCachedPeerList>(Config_->MaxPeersPerBlock)).first;
        }
        return it->second;
    }
}

TCachedPeerListPtr TBlockPeerTable::GetOrCreatePeerList(TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetOrCreatePeerList(TBlockId(chunkId, AllBlocksIndex));
}

void TBlockPeerTable::OnSweep()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Starting expired peers sweep");

    std::vector<std::pair<TBlockId, TCachedPeerListPtr>> snapshot;
    {
        auto guard = ReaderGuard(Lock_);
        snapshot.reserve(BlockIdToPeerList_.size());
        for (const auto& pair : BlockIdToPeerList_) {
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
        auto guard = WriterGuard(Lock_);
        for (auto blockId : blockIdsToEvict) {
            BlockIdToPeerList_.erase(blockId);
        }
    }

    YT_LOG_DEBUG("Finished expired peers sweep (EvictedBlockCount: %v)",
        blockIdsToEvict.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
