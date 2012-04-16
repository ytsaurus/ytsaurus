#pragma once

#include "common.h"
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
    typedef TIntrusivePtr<TRemoteWriter> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        //! Maximum window size (in bytes).
        int WindowSize;
        
        //! Maximum group size (in bytes).
        int GroupSize;
        
        //! RPC requests timeout.
        /*!
         *  This timeout is especially useful for PutBlocks calls to ensure that
         *  uploading is not stalled.
         */
        TDuration HolderRpcTimeout;

        //! Maximum allowed period of time without RPC requests to holders.
        /*!
         *  If the writer remains inactive for the given period, it sends #TChunkHolderProxy::PingSession.
         */
        TDuration SessionPingInterval;

        TConfig()
        {
            Register("window_size", WindowSize)
                .Default(4 * 1024 * 1024)
                .GreaterThan(0);
            Register("group_size", GroupSize)
                .Default(1024 * 1024)
                .GreaterThan(0);
            Register("holder_rpc_timeout", HolderRpcTimeout)
                .Default(TDuration::Seconds(30));
            Register("session_ping_interval", SessionPingInterval)
                .Default(TDuration::Seconds(10));
        }

        virtual void DoValidate() const
        {
            if (WindowSize < GroupSize) {
                ythrow yexception() << "\"window_size\" cannot be less than \"group_size\"";
            }
        }
    };

    /*!
     * \note Thread affinity: ClientThread.
     */
    TRemoteWriter(
        TConfig* config, 
        const TChunkId& chunkId,
        const yvector<Stroka>& addresses);

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

    const NChunkHolder::NProto::TChunkMeta& GetChunkMeta() const;
    const std::vector<Stroka> GetHolders() const;

private:
    //! A group is a bunch of blocks that is sent in a single RPC request.
    class TGroup;
    typedef TIntrusivePtr<TGroup> TGroupPtr;

    struct THolder;
    typedef TIntrusivePtr<THolder> THolderPtr;
    
    typedef ydeque<TGroupPtr> TWindow;

    typedef NChunkHolder::TChunkHolderServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TConfig::TPtr Config;
    TChunkId ChunkId;
    yvector<Stroka> Addresses;

    TAsyncStreamState State;

    bool IsOpen;
    bool IsInitComplete;

    //! This flag is raised whenever #Close is invoked.
    //! All access to this flag happens from #WriterThread.
    bool IsCloseRequested;
    NChunkHolder::NProto::TChunkMeta ChunkMeta;

    TWindow Window;
    TAsyncSemaphore WindowSlots;

    yvector<THolderPtr> Holders;

    //! Number of holders that are still alive.
    int AliveHolderCount;

    //! A new group of blocks that is currently being filled in by the client.
    //! All access to this field happens from client thread.
    TGroupPtr CurrentGroup;

    //! Number of blocks that are already added via #AddBlock. 
    int BlockCount;

    //! Returned from holder in Finish.
    NChunkHolder::NProto::TInfo ChunkInfo;

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

    void RegisterReadyEvent(TFuture<TVoid>::TPtr windowReady);

    void OnHolderFailed(THolderPtr holder);

    void ShiftWindow();

    TProxy::TInvFlushBlock::TPtr FlushBlock(THolderPtr holder, int blockIndex);

    void OnBlockFlushed(THolderPtr holder, int blockIndex, TProxy::TRspFlushBlock::TPtr rsp);

    void OnWindowShifted(int blockIndex);

    TProxy::TInvStartChunk::TPtr StartChunk(THolderPtr holder);

    void OnChunkStarted(THolderPtr holder, TProxy::TRspStartChunk::TPtr rsp);

    void OnSessionStarted();

    void CloseSession();

    TProxy::TInvFinishChunk::TPtr FinishChunk(THolderPtr holder);

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

