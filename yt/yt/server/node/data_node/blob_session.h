#pragma once

#include "location.h"
#include "session_detail.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/block.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBlobSessionSlotState,
    (Empty)
    (Received)
    (Written)
);

class TBlobSession
    : public TSessionBase
{
public:
    TBlobSession(
        TDataNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap,
        TSessionId sessionId,
        const TSessionOptions& options,
        TStoreLocationPtr location,
        NConcurrency::TLease lease);

private:
    const TBlobWritePipelinePtr Pipeline_;

    using ESlotState = EBlobSessionSlotState;

    struct TSlot
    {
        ESlotState State = ESlotState::Empty;
        NChunkClient::TBlock Block;
        TPromise<void> WrittenPromise = NewPromise<void>();
        TMemoryUsageTrackerGuard MemoryTrackerGuard;
        TPendingIOGuard PendingIOGuard;
    };

    TError Error_;

    std::vector<TSlot> Window_;
    int WindowStartBlockIndex_ = 0;
    int WindowIndex_ = 0;
    i64 Size_ = 0;
    int BlockCount_ = 0;

    virtual TFuture<void> DoStart() override;
    void OnStarted(const TError& error);

    virtual TFuture<void> DoPutBlocks(
        int startBlockIndex,
        const std::vector<NChunkClient::TBlock>& blocks,
        bool enableCaching) override;
    void OnBlocksWritten(
        int beginBlockIndex,
        int endBlockIndex,
        const TError& error);

    virtual TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> DoSendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& targetDescriptor) override;

    virtual TFuture<void> DoFlushBlocks(int blockIndex) override;
    void OnBlockFlushed(int blockIndex);

    virtual void DoCancel(const TError& error) override;

    virtual TFuture<NChunkClient::NProto::TChunkInfo> DoFinish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount) override;
    NChunkClient::NProto::TChunkInfo OnFinished(const TError& error);

    void Abort();
    void OnAborted(const TError& error);

    bool IsInWindow(int blockIndex);
    void ValidateBlockIsInWindow(int blockIndex);
    TSlot& GetSlot(int blockIndex);
    void ReleaseBlocks(int flushedBlockIndex);
    NChunkClient::TBlock GetBlock(int blockIndex);
    void MarkAllSlotsFailed(const TError& error);
    void ReleaseSpace();
    void SetFailed(const TError& error, bool fatal);
    void OnSlotCanceled(int blockIndex, const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TBlobSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

