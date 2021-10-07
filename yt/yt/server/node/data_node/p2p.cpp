#include "p2p.h"

#include "private.h"

namespace NYT::NDataNode {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

// static const auto& Logger = P2PLogger;

////////////////////////////////////////////////////////////////////////////////

TP2PBlockCache::TP2PBlockCache(const TP2PConfigPtr& config)
    : Config_(config)
{ }

bool TP2PBlockCache::FinishSessionIteration(TNodeId node, TGuid sessionId, i64 iteration)
{
    Y_UNUSED(node, sessionId, iteration);
    return true;
}

TFuture<void> TP2PBlockCache::WaitSessionIteration(TGuid sessionId, i64 iteration)
{
    Y_UNUSED(sessionId, iteration);
    return VoidFuture;
}

void TP2PBlockCache::HoldBlocks(
    TGuid sessionId,
    TChunkId chunkId,
    const std::vector<int>& blockIndices,
    const std::vector<NChunkClient::TBlock>& blocks)
{
    Y_UNUSED(sessionId, chunkId, blockIndices, blocks);
}

void TP2PBlockCache::DropBlocks(TGuid sessionId, TChunkId chunkId)
{
    Y_UNUSED(sessionId, chunkId);
}

std::vector<NChunkClient::TBlock> TP2PBlockCache::LookupBlocks(TChunkId chunkId, const std::vector<int>& blockIndices)
{
    Y_UNUSED(chunkId, blockIndices);
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TP2PManager::TP2PManager(const TP2PConfigPtr& config)
    : Config_(config)
{ }

std::vector<TP2PSuggestion> TP2PManager::OnBlockRead(
    TChunkId chunkId,
    const std::vector<int>& blockIndices,
    std::vector<NChunkClient::TBlock>* blocks)
{
    Y_UNUSED(chunkId, blockIndices, blocks);
    return {};
}

std::vector<TP2PSuggestion> TP2PManager::OnBlockProbe(TChunkId chunkId, const std::vector<int>& blockIndices)
{
    Y_UNUSED(chunkId, blockIndices);
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
