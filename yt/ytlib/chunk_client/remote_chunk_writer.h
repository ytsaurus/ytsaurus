#pragma once

#include "chunk_writer.h"

#include "../misc/lazy_ptr.h"
#include "../misc/config.h"
#include "../misc/semaphore.h"
#include "../rpc/client.h"
#include "../chunk_client/common.h"
#include "../chunk_holder/chunk_holder_rpc.h"
#include "../actions/action_queue.h"

#include <util/generic/deque.h>

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

class TRemoteChunkWriter
    : public IChunkWriter
{
public:
    typedef TIntrusivePtr<TRemoteChunkWriter> TPtr;

    struct TConfig
    {
        //! Maximum number of blocks that may be concurrently present in the window.
        int WindowSize;
        
        //! Maximum group size (in bytes).
        int GroupSize;
        
        //! RPC requests timeout.
        /*!
         *  This timeout is especially useful for PutBlocks calls to ensure that
         *  uploading is not stalled.
         */
        TDuration RpcTimeout;

        //! Timeout specifying a maxmimum allowed period of time without RPC request to ChunkHolder
        /*!
         * If no activity occured during this period -- PingSession call will be send
         */
        TDuration SessionTimeout;

        TConfig()
            : WindowSize(16)
            , GroupSize(1024 * 1024)
            , RpcTimeout(TDuration::Seconds(30))
            , SessionTimeout(TDuration::Seconds(10))
        { }

        // ToDo: move to implementation
        void Read(TJsonObject* config)
        {
            TryRead(config, L"WindowSize", &WindowSize);
            TryRead(config, L"GroupSize", &GroupSize);
            //ToDo: make timeout configurable
        }
    };

    // Client thread
    TRemoteChunkWriter(
        const TConfig& config, 
        const TChunkId& chunkId,
        const yvector<Stroka>& addresses);

    EResult AsyncWriteBlock(const TSharedRef& data, TAsyncResult<TVoid>::TPtr* ready);

    TAsyncResult<EResult>::TPtr AsyncClose();

    void Cancel();

    ~TRemoteChunkWriter();

    static Stroka GetDebugInfo();

private:

    //! A group is a bunch of blocks that is sent in a single RPC request.
    class TGroup;
    typedef TIntrusivePtr<TGroup> TGroupPtr;

    struct TNode;
    typedef TIntrusivePtr<TNode> TNodePtr;
    
    typedef ydeque<TGroupPtr> TWindow;

    typedef NChunkHolder::TChunkHolderProxy TProxy;

    USE_RPC_PROXY_METHOD(TProxy, StartChunk);
    USE_RPC_PROXY_METHOD(TProxy, FinishChunk);
    USE_RPC_PROXY_METHOD(TProxy, PutBlocks);
    USE_RPC_PROXY_METHOD(TProxy, SendBlocks);
    USE_RPC_PROXY_METHOD(TProxy, FlushBlock);
    USE_RPC_PROXY_METHOD(TProxy, PingSession);

private:
    //! Manages all internal upload functionality, 
    //! sends out RPC requests, and handles responses.
    static TLazyPtr<TActionQueue> WriterThread;

    TChunkId ChunkId;
    const TConfig Config;

    DECLARE_ENUM(EWriterState,
        (Initializing)
        (Writing)
        (Closed)
        (Canceled)
    );

    //! Set in #WriterThread, read from client and writer threads
    EWriterState State;

    //! This flag is raised whenever #Close is invoked.
    //! All access to this flag happens from #WriterThread.
    bool IsCloseRequested;

    // Result of write session, set when session is completed.
    // Is returned from #AsyncClose
    TAsyncResult<EResult>::TPtr Result;

    TWindow Window;
    TSemaphore WindowSlots;

    yvector<TNodePtr> Nodes;

    //! Number of nodes that are still alive.
    int AliveNodeCount;

    //! A new group of blocks that is currently being filled in by the client.
    //! All access to this field happens from client thread.
    TGroupPtr CurrentGroup;

    //! Number of blocks that are already added via #AddBlock. 
    int BlockCount;

    TAsyncResult<TVoid>::TPtr WindowReady;

    static NRpc::TChannelCache ChannelCache;

    /* ToDo: implement metrics

    TMetric StartChunkTiming;
    TMetric PutBlocksTiming;
    TMetric SendBlocksTiming;
    TMetric FlushBlockTiming;
    TMetric FinishChunkTiming;*/

private:
    //! Invoked from #Close via #WriterThread.
    //! Sets #IsCloseRequested.
    void DoClose();
    
    //! Invoked from #Cancel via #WriterThread.
    void DoCancel();

    void AddGroup(TGroupPtr group);
    void RegisterReadyEvent(TAsyncResult<TVoid>::TPtr windowReady);

    void OnNodeDied(int node);
    void ReleaseSlots(int count);

    void ShiftWindow();
    TInvFlushBlock::TPtr FlushBlock(int node, int blockIndex);
    void OnFlushedBlock(int node, int blockIndex);
    void OnWindowShifted(int blockIndex);

    void InitializeNodes(const yvector<Stroka>& addresses);
    void StartSession();
    TInvStartChunk::TPtr StartChunk(int node);
    void OnStartedChunk(int node);
    void OnSessionStarted();

    void CloseSession();
    TInvFinishChunk::TPtr FinishChunk(int node);
    void OnFinishedChunk(int node);
    void OnFinishedSession();

    void PingSession(int node);
    void SchedulePing(int node);
    void CancelPing(int node);
    void CancelAllPings();

    template<class TResponse>
    void CheckResponse(typename TResponse::TPtr rsp, int node, IAction::TPtr onSuccess);
};

///////////////////////////////////////////////////////////////////////////////

}

