#pragma once

#include "location.h"
#include "session_detail.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

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
        TLockedChunkGuard lockedChunkGuard,
        NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions);

    i64 GetMemoryUsage() const override;
    i64 GetTotalSize() const override;
    i64 GetBlockCount() const override;
    i64 GetWindowSize() const override;
    i64 GetIntermediateEmptyBlockCount() const override;

private:
    const TBlobWritePipelinePtr Pipeline_;

    using ESlotState = EBlobSessionSlotState;

    struct TSlot
    {
        ESlotState State = ESlotState::Empty;
        NChunkClient::TBlock Block;

        TPromise<void> ReceivedPromise = NewPromise<void>();
        TPromise<void> WrittenPromise = NewPromise<void>();

        TLocationMemoryGuard LocationMemoryGuard;
    };

    TError Error_;

    std::vector<TSlot> Window_;
    int WindowStartBlockIndex_ = 0;
    int WindowIndex_ = 0;

    std::atomic<i64> TotalByteSize_ = 0;
    std::atomic<i64> BlockCount_ = 0;
    std::atomic<i64> MemoryUsage_ = 0;

    std::atomic<i64> WindowSize_ = 0;
    std::atomic<i64> IntermediateEmptyBlockCount_ = 0;

    i64 MaxCumulativeBlockSize_ = 0;
    TLocationMemoryGuard PendingBlockLocationMemoryGuard_;
    TMemoryUsageTrackerGuard PendingBlockMemoryGuard_;

    TFuture<void> DoStart() override;
    void OnStarted(const TError& error);

    TFuture<NIO::TIOCounters> DoPutBlocks(
        int startBlockIndex,
        std::vector<NChunkClient::TBlock> blocks,
        i64 cumulativeBlockSize,
        bool enableCaching) override;
    TFuture<NIO::TIOCounters> DoPerformPutBlocks(
        int startBlockIndex,
        std::vector<NChunkClient::TBlock> blocks,
        bool useCumulativeBlockSize,
        bool enableCaching);
    void OnBlocksWritten(
        int beginBlockIndex,
        int endBlockIndex,
        const TError& error);

    TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> DoSendBlocks(
        int startBlockIndex,
        int blockCount,
        i64 cumulativeBlockSize,
        const NNodeTrackerClient::TNodeDescriptor& targetDescriptor) override;

    TFuture<TFlushBlocksResult> DoFlushBlocks(int blockIndex) override;
    TFlushBlocksResult OnBlockFlushed(int blockIndex);

    void DoCancel(const TError& error) override;

    TFuture<TFinishResult> DoFinish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount) override;
    TFinishResult OnFinished(const TError& error);

    void Abort();
    void OnAborted(const TError& error);

    bool IsInWindow(int blockIndex);
    void ValidateBlockIsInWindow(int blockIndex);
    TSlot& GetSlot(int blockIndex);
    void ReleaseBlockSlot(TSlot& slot);
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

