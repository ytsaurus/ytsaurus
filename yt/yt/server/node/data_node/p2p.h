#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/ytlib/chunk_client/block.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TP2PBlockCache
    : public TRefCounted
{
public:
    TP2PBlockCache(const TP2PConfigPtr& config);

    bool FinishSessionIteration(TNodeId node, TGuid sessionId, i64 iteration);
    TFuture<void> WaitSessionIteration(TGuid sessionId, i64 iteration);

    void HoldBlocks(
        TGuid sessionId,
        TChunkId chunkId,
        const std::vector<int>& blockIndices,
        const std::vector<NChunkClient::TBlock>& blocks);

    void DropBlocks(TGuid sessionId, TChunkId chunkId);

    std::vector<NChunkClient::TBlock> LookupBlocks(TChunkId chunkId, const std::vector<int>& blockIndices);

private:
    YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);
    TP2PConfigPtr Config_;
};

DEFINE_REFCOUNTED_TYPE(TP2PBlockCache)

////////////////////////////////////////////////////////////////////////////////

struct TP2PSuggestion
{
    int BlockIndex;
    TCompactVector<TNodeId, 8> Peers;

    TGuid P2PSessionId;
    ui64 P2PIteration;
};

////////////////////////////////////////////////////////////////////////////////

class TP2PManager
    : public TRefCounted
{
public:
    TP2PManager(const TP2PConfigPtr& config);

    std::vector<TP2PSuggestion> OnBlockRead(
        TChunkId chunkId,
        const std::vector<int>& blockIndices,
        std::vector<NChunkClient::TBlock>* blocks);

    std::vector<TP2PSuggestion> OnBlockProbe(TChunkId chunkId, const std::vector<int>& blockIndices);

private:
    TP2PConfigPtr Config_;
};

DEFINE_REFCOUNTED_TYPE(TP2PManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
