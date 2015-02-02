#pragma once

#include "public.h"
#include "session_detail.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/throughput_throttler.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

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
        NCellNode::TBootstrap* bootstrap,
        const TChunkId& chunkId,
        const TSessionOptions& options,
        TLocationPtr location,
        TLease lease);

    NChunkClient::NProto::TChunkInfo GetChunkInfo() const override;

private:
    using ESlotState = EBlobSessionSlotState;

    struct TSlot
    {
        ESlotState State = ESlotState::Empty;
        TSharedRef Block;
        TPromise<void> WrittenPromise = NewPromise<void>();
    };

    // Thread affinity: WriterThread
    TError Error_;
    NChunkClient::TFileWriterPtr Writer_;

    // Thread affinity: ControlThread
    std::vector<TSlot> Window_;
    int WindowStartBlockIndex_ = 0;
    int WindowIndex_ = 0;
    i64 Size_ = 0;
    int BlockCount_ = 0;


    virtual TFuture<void> DoStart() override;
    void DoOpenWriter();
   
    virtual TFuture<void> DoPutBlocks(
        int startBlockIndex,
        const std::vector<TSharedRef>& blocks,
        bool enableCaching) override;

    virtual TFuture<void> DoSendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target) override;

    virtual TFuture<void> DoFlushBlocks(int blockIndex) override;

    virtual void DoCancel() override;

    virtual TFuture<IChunkPtr> DoFinish(
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const TNullable<int>& blockCount) override;

    bool IsInWindow(int blockIndex);
    void ValidateBlockIsInWindow(int blockIndex);
    TSlot& GetSlot(int blockIndex);
    void ReleaseBlocks(int flushedBlockIndex);
    TSharedRef GetBlock(int blockIndex);
    void MarkAllSlotsWritten(const TError& error);

    TFuture<void> AbortWriter();
    void DoAbortWriter();
    void OnWriterAborted(const TError& error);

    TFuture<void> CloseWriter(const NChunkClient::NProto::TChunkMeta& chunkMeta);
    void DoCloseWriter(const NChunkClient::NProto::TChunkMeta& chunkMeta);
    IChunkPtr OnWriterClosed(const TError& error);

    void DoWriteBlock(const TSharedRef& block, int blockIndex);
    void OnBlockWritten(int blockIndex, const TError& error);

    void OnBlockFlushed(int blockIndex, const TError& error);

    void ReleaseSpace();

    void SetFailed(const TError& error);

};

DEFINE_REFCOUNTED_TYPE(TBlobSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

