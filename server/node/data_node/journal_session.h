#pragma once

#include "session_detail.h"
#include "chunk.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/server/lib/hydra/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalSession
    : public TSessionBase
{
public:
    using TSessionBase::TSessionBase;

private:
    TJournalChunkPtr Chunk_;
    NHydra::IChangelogPtr Changelog_;
    TChunkUpdateGuard ChunkUpdateGuard_;
    TFuture<void> LastAppendResult_ = VoidFuture;


    virtual TFuture<void> DoStart() override;
    virtual TFuture<void> DoPutBlocks(
        int startBlockIndex,
        const std::vector<NChunkClient::TBlock>& blocks,
        bool enableCaching) override;
    virtual TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> DoSendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target) override;
    virtual TFuture<void> DoFlushBlocks(int blockIndex) override;
    virtual void DoCancel(const TError& error) override;
    virtual TFuture<NChunkClient::NProto::TChunkInfo> DoFinish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount) override;

    void OnFinished();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

