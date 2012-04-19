#pragma once

#include "public.h"
#include "private.h"
#include "async_writer.h"
#include <ytlib/chunk_server/chunk_service.pb.h>

#include <ytlib/misc/configurable.h>
#include <ytlib/misc/metric.h>
#include <ytlib/misc/semaphore.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/chunk_holder/chunk_holder_service_proxy.h>
#include <ytlib/chunk_server/chunk_ypath_proxy.h>

#include <util/generic/deque.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TRemoteWriter
    : public IAsyncWriter
{
public:
    /*!
     * \note Thread affinity: ClientThread.
     */
    TRemoteWriter(
        const TRemoteWriterConfigPtr& config, 
        const TChunkId& chunkId,
        const std::vector<Stroka>& addresses);

    /*!
     * \note Thread affinity: ClientThread.
     */
    void Open();

    /*!
     * \note Thread affinity: ClientThread.
     */
    virtual TAsyncError AsyncWriteBlocks(const std::vector<TSharedRef>& blocks);

    /*!
     * \note Thread affinity: ClientThread.
     */
    virtual TAsyncError AsyncClose(
        const std::vector<TSharedRef>& lastBlocks,
        const NChunkHolder::NProto::TChunkMeta& chunkMeta);

    ~TRemoteWriter();

    /*!
     * \note Thread affinity: any.
     */
    Stroka GetDebugInfo();

    const NChunkHolder::NProto::TChunkInfo& GetChunkInfo() const;
    const std::vector<Stroka> GetHolders() const;
    const TChunkId& GetChunkId() const;

private:
    //! A group is a bunch of blocks that is sent in a single RPC request.
    class TGroup;
    typedef TIntrusivePtr<TGroup> TGroupPtr;

    struct THolder;
    typedef TIntrusivePtr<THolder> THolderPtr;
    
    typedef ydeque<TGroupPtr> TWindow;

    typedef NChunkHolder::TChunkHolderServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TRemoteWriterConfigPtr Config;
    TChunkId ChunkId;
    std::vector<Stroka> Addresses;

    TAsyncStreamState State;

    bool IsOpen;
    bool IsInitComplete;

    //! This flag is raised whenever #Close is invoked.
    //! All access to this flag happens from #WriterThread.
    bool IsCloseRequested;
    NChunkHolder::NProto::TChunkMeta ChunkMeta;

    TWindow Window;
    TAsyncSemaphore WindowSlots;

    std::vector<THolderPtr> Holders;

    //! Number of holders that are still alive.
    int AliveHolderCount;

    //! A new group of blocks that is currently being filled in by the client.
    //! All access to this field happens from client thread.
    TGroupPtr CurrentGroup;

    //! Number of blocks that are already added via #AddBlock. 
    int BlockCount;

    //! Returned from holder in Finish.
    NChunkHolder::NProto::TChunkInfo ChunkInfo;

    TMetric StartChunkTiming;
    TMetric PutBlocksTiming;
    TMetric SendBlocksTiming;
    TMetric FlushBlockTiming;
    TMetric FinishChunkTiming;

    NLog::TTaggedLogger Logger;

    void DoClose(
        const std::vector<TSharedRef>& lastBlocks,
        TVoid);

    void AddGroup(TGroupPtr group);

    void RegisterReadyEvent(TFuture<TVoid> windowReady);

    void OnHolderFailed(THolderPtr holder);

    void ShiftWindow();

    TProxy::TInvFlushBlock FlushBlock(THolderPtr holder, int blockIndex);

    void OnBlockFlushed(THolderPtr holder, int blockIndex, TProxy::TRspFlushBlock::TPtr rsp);

    void OnWindowShifted(int blockIndex);

    TProxy::TInvStartChunk StartChunk(THolderPtr holder);

    void OnChunkStarted(THolderPtr holder, TProxy::TRspStartChunk::TPtr rsp);

    void OnSessionStarted();

    void CloseSession();

    TProxy::TInvFinishChunk FinishChunk(THolderPtr holder);

    void OnChunkFinished(THolderPtr holder, TProxy::TRspFinishChunk::TPtr rsp);

    void OnSessionFinished();

    void PingSession(THolderPtr holder);
    void SchedulePing(THolderPtr holder);
    void CancelPing(THolderPtr holder);
    void CancelAllPings();

    template <class TResponse>
    void CheckResponse(
        THolderPtr holder, 
        TCallback<void(TIntrusivePtr<TResponse>)> onSuccess,
        TMetric* metric,
        TIntrusivePtr<TResponse> rsp);

    void AddBlocks(const std::vector<TSharedRef>& blocks);
    void DoWriteBlocks(const std::vector<TSharedRef>& blocks, TVoid);

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

