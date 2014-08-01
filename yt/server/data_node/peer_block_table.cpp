#include "stdafx.h"
#include "private.h"
#include "peer_block_table.h"
#include "config.h"

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TPeerBlockTable::TPeerBlockTable(TPeerBlockTableConfigPtr config)
    : Config_(config)
{ }

const std::vector<TPeerInfo>& TPeerBlockTable::GetPeers(const TBlockId& blockId)
{
    SweepAllExpiredPeers();

    auto it = Table_.find(blockId);
    if (it == Table_.end()) {
        static std::vector<TPeerInfo> empty;
        return empty;
    } else {
        SweepExpiredPeers(it->second);
        return it->second;
    }
}

void TPeerBlockTable::UpdatePeer(const TBlockId& blockId, const TPeerInfo& peer)
{
    LOG_DEBUG("Updating peer (BlockId: %v, Address: %v, ExpirationTime: %v)",
        blockId,
        peer.Descriptor.GetDefaultAddress(),
        peer.ExpirationTime);

    SweepAllExpiredPeers();

    auto& peers = GetMutablePeers(blockId);
    SweepExpiredPeers(peers); // In case when all expired peers were not swept

    for (auto it = peers.begin(); it != peers.end(); ++it) {
        if (it->Descriptor.GetDefaultAddress() == peer.Descriptor.GetDefaultAddress()) {
            peers.erase(it);
            break;
        }
    }

    {
        auto it = peers.begin();
        while (it != peers.end() && it->ExpirationTime > peer.ExpirationTime) {
            ++it;
        }

        peers.insert(it, peer);
    }

    if (peers.size() > Config_->MaxPeersPerBlock) {
        peers.erase(peers.begin() + Config_->MaxPeersPerBlock, peers.end());
    }
}

void TPeerBlockTable::SweepAllExpiredPeers()
{
    if (TInstant::Now() < LastSwept_ + Config_->SweepPeriod) {
        return;
    }

    auto it = Table_.begin();
    while (it != Table_.end()) {
        auto jt = it;
        ++jt;
        SweepExpiredPeers(it->second);
        if (it->second.empty()) {
            Table_.erase(it);
        }
        it = jt;
    }

    LastSwept_ = TInstant::Now();

    LOG_DEBUG("All expired peers were swept");
}

void TPeerBlockTable::SweepExpiredPeers(std::vector<TPeerInfo>& peers)
{
    auto now = TInstant::Now();

    auto it = peers.end();
    while (it != peers.begin() && (it - 1)->ExpirationTime < now) {
        --it;
    }

    peers.erase(it, peers.end());
}

std::vector<TPeerInfo>& TPeerBlockTable::GetMutablePeers(const TBlockId& blockId)
{
    auto it = Table_.find(blockId);
    if (it != Table_.end())
        return it->second;
    auto pair = Table_.insert(std::make_pair(blockId, std::vector<TPeerInfo>()));
    YCHECK(pair.second);
    return pair.first->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
