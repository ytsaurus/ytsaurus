#include "block_peer_table.h"
#include "private.h"
#include "config.h"

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>

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
    if (Entries_.size() > 2 * EntryCountLimit_) {
        SortUniqueBy(
            Entries_,
            [] (const auto& entry) { return entry.NodeId; });
        Sort(
            Entries_,
            [] (const auto& lhs, const auto& rhs) { return lhs.ExpirationDeadline > rhs.ExpirationDeadline; });
        // NB: SortUniqueBy could have already pruned the vector.
        if (Entries_.size() > EntryCountLimit_) {
            Entries_.erase(Entries_.begin() + EntryCountLimit_, Entries_.end());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TBlockPeerTable::TBlockPeerTable(TBootstrap* bootstrap)
    : Config_(bootstrap->GetConfig()->DataNode->BlockPeerTable)
    , SweepExecutor_(New<TPeriodicExecutor>(
        bootstrap->GetStorageHeavyInvoker(),
        BIND(&TBlockPeerTable::OnSweep, MakeWeak(this)),
        Config_->SweepPeriod))
{
    SweepExecutor_->Start();
}

TCachedPeerListPtr TBlockPeerTable::FindOrCreatePeerList(const TBlockId& blockId, bool insert)
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        auto guard = ReaderGuard(Lock_);
        auto it = BlockIdToPeerList_.find(blockId);
        if (it != BlockIdToPeerList_.end()) {
            return it->second;
        }
    }

    if (!insert) {
        return nullptr;
    }

    {
        auto guard = WriterGuard(Lock_);
        auto it = BlockIdToPeerList_.find(blockId);
        if (it == BlockIdToPeerList_.end()) {
            it = BlockIdToPeerList_.emplace(blockId, New<TCachedPeerList>(Config_->MaxPeersPerBlock)).first;
        }
        return it->second;
    }
}

TCachedPeerListPtr TBlockPeerTable::FindOrCreatePeerList(TChunkId chunkId, bool insert)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return FindOrCreatePeerList(TBlockId(chunkId, AllBlocksIndex), insert);
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
