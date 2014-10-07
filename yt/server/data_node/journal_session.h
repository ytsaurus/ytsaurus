#pragma once

#include "public.h"
#include "session_detail.h"

#include <server/hydra/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalSession
    : public TSessionBase
{
public:
    TJournalSession(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap,
        const TChunkId& chunkId,
        const TSessionOptions& options,
        TLocationPtr location);

    virtual NChunkClient::NProto::TChunkInfo GetChunkInfo() const override;

private:
    TJournalChunkPtr Chunk_;
    TAsyncError LastAppendResult_;


    virtual void DoStart() override;

    virtual TAsyncError DoPutBlocks(
        int startBlockIndex,
        const std::vector<TSharedRef>& blocks,
        bool enableCaching) override;

    virtual TAsyncError DoSendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target) override;

    virtual TAsyncError DoFlushBlocks(int blockIndex) override;

    virtual void DoCancel() override;

    virtual TFuture<TErrorOr<IChunkPtr>> DoFinish(
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const TNullable<int>& blockCount) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

