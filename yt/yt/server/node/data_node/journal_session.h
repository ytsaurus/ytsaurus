#pragma once

#include "session_detail.h"
#include "chunk.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalSession
    : public TSessionBase
{
public:
    using TSessionBase::TSessionBase;

private:
    TJournalChunkPtr Chunk_;
    NHydra::IFileChangelogPtr Changelog_;
    TChunkUpdateGuard ChunkUpdateGuard_;
    TFuture<void> LastAppendResult_ = VoidFuture;
    i64 LastDataSize_ = 0;

    TFuture<void> DoStart() override;
    TFuture<void> DoPutBlocks(
        int startBlockIndex,
        const std::vector<NChunkClient::TBlock>& blocks,
        bool enableCaching) override;
    TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> DoSendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target) override;
    TFuture<NIO::TIOCounters> DoFlushBlocks(int blockIndex) override;
    void DoCancel(const TError& error) override;
    TFuture<NChunkClient::NProto::TChunkInfo> DoFinish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount) override;

    void OnFinished();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

