#pragma once

#include "public.h"
#include "session_detail.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/throughput_throttler.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <core/logging/tagged_logger.h>

#include <core/profiling/profiler.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TBlobSession
    : public TSession
{
public:
    TBlobSession(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap,
        const TChunkId& chunkId,
        EWriteSessionType type,
        bool syncOnClose,
        TLocationPtr location);

    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override;

    virtual void Start(TLeaseManager::TLease lease) override;

    virtual TAsyncError PutBlocks(
        int startBlockIndex,
        const std::vector<TSharedRef>& blocks,
        bool enableCaching) override;

    virtual TAsyncError SendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target) override;

    virtual TAsyncError FlushBlocks(int blockIndex) override;

    virtual void Cancel(const TError& error) override;

    virtual TFuture<TErrorOr<IChunkPtr>> Finish(
        const NChunkClient::NProto::TChunkMeta& chunkMeta) override;

private:
    DECLARE_ENUM(ESlotState,
        (Empty)
        (Received)
        (Written)
    );

    struct TSlot
    {
        TSlot()
            : State(ESlotState::Empty)
            , IsWritten(NewPromise())
        { }

        ESlotState State;
        TSharedRef Block;
        TPromise<void> IsWritten;
    };

    typedef std::vector<TSlot> TWindow;

    TError Error_;
    TWindow Window_;
    int WindowStartIndex_;
    int WriteIndex_;
    i64 Size_;
    NChunkClient::TFileWriterPtr Writer_;


    bool IsInWindow(int blockIndex);
    void ValidateBlockIsInWindow(int blockIndex);
    TSlot& GetSlot(int blockIndex);
    void ReleaseBlocks(int flushedBlockIndex);
    TSharedRef GetBlock(int blockIndex);
    void MarkAllSlotsWritten();

    void OpenFile();
    void DoOpenWriter();

    TAsyncError AbortWriter();
    TError DoAbortWriter();
    TError OnWriterAborted(TError error);

    TAsyncError CloseWriter(const NChunkClient::NProto::TChunkMeta& chunkMeta);
    TError DoCloseWriter(const NChunkClient::NProto::TChunkMeta& chunkMeta);
    TErrorOr<IChunkPtr> OnWriterClosed(TError error);

    void EnqueueWrites();
    TError DoWriteBlock(const TSharedRef& block, int blockIndex);
    void OnBlockWritten(int blockIndex, TError error);

    TError OnBlockFlushed(int blockIndex);

    void ReleaseSpace();

    void OnIOError(const TError& error);

};

DEFINE_REFCOUNTED_TYPE(TBlobSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

