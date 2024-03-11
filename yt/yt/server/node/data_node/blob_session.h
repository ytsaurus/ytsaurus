#pragma once

#include "location.h"
#include "session_detail.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBlobSessionSlotState,
    (Empty)
    (Received)
    (Written)
    (Released)
);

class TBlobSession
    : public TSessionBase
{
public:
    TBlobSession(
        TDataNodeConfigPtr config,
        IBootstrap* bootstrap,
        TSessionId sessionId,
        const TSessionOptions& options,
        TStoreLocationPtr location,
        NConcurrency::TLease lease,
        TLockedChunkGuard lockedChunkGuard);

private:
    const TBlobWritePipelinePtr Pipeline_;

    using ESlotState = EBlobSessionSlotState;

    struct TSlot
    {
        ESlotState State = ESlotState::Empty;
        NChunkClient::TBlock Block;

        TPromise<void> ReceivedPromise = NewPromise<void>();
        TPromise<void> WrittenPromise = NewPromise<void>();

        // This guard accounts memory usage before the block was written.
        TPendingIOGuard PendingIOGuard;

        // This guard accounts memory usage after the block was written, but before block release.
        TMemoryUsageTrackerGuard MemoryUsageGuard;
    };

    TError Error_;

    std::vector<TSlot> Window_;
    int WindowStartBlockIndex_ = 0;
    int WindowIndex_ = 0;
    i64 Size_ = 0;
    int BlockCount_ = 0;

    TFuture<void> DoStart() override;
    void OnStarted(const TError& error);

    TFuture<NIO::TIOCounters> DoPutBlocks(
        int startBlockIndex,
        const std::vector<NChunkClient::TBlock>& blocks,
        bool enableCaching) override;
    TFuture<NIO::TIOCounters> DoPerformPutBlocks(
        int startBlockIndex,
        const std::vector<NChunkClient::TBlock>& blocks,
        std::vector<TMemoryUsageTrackerGuard> memoryTrackerGuards,
        bool enableCaching);
    void OnBlocksWritten(
        int beginBlockIndex,
        int endBlockIndex,
        const TError& error);

    TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> DoSendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& targetDescriptor) override;

    TFuture<NIO::TIOCounters> DoFlushBlocks(int blockIndex) override;
    NIO::TIOCounters OnBlockFlushed(int blockIndex);

    void DoCancel(const TError& error) override;

    TFuture<NChunkClient::NProto::TChunkInfo> DoFinish(
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

