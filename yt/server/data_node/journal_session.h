#pragma once

#include "public.h"
#include "session_detail.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/hydra/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalSession
    : public TSessionBase
{
public:
    using TSessionBase::TSessionBase;

    virtual NChunkClient::NProto::TChunkInfo GetChunkInfo() const override;

private:
    TJournalChunkPtr Chunk_;
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
    virtual TFuture<IChunkPtr> DoFinish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount) override;

    void OnFinished();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

