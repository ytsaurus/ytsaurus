#pragma once

#include "common.h"
#include "async_writer.h"

#include "../misc/config.h"
#include "../misc/metric.h"
#include "../misc/semaphore.h"
#include "../misc/thread_affinity.h"

#include "../chunk_holder/chunk_holder_rpc.h"
#include "../actions/action_queue.h"

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
        : TConfigBase
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
        TDuration HolderRpcTimeout;

        //! Timeout specifying a maximum allowed period of time without RPC request to ChunkHolder.
        /*!
         * If no activity occurred during this period, PingSession call will be sent.
         */
        TDuration SessionPingInterval;

        TConfig()
        {
            Register("window_size", WindowSize).Default(16).GreaterThan(0);
            Register("group_size", GroupSize).Default(1024 * 1024).GreaterThan(0);
            Register("holder_rpc_timeout", HolderRpcTimeout).Default(TDuration::Seconds(30));
            Register("session_ping_interval", SessionPingInterval).Default(TDuration::Seconds(10));

            SetDefaults();
        }

        void Read(TJsonObject* config);
    };

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

    /*!
     * \note Thread affinity: ClientThread.
     */
    TRemoteWriter(
        const TConfig& config, 
        const TChunkId& chunkId,
        const yvector<Stroka>& addresses);

    /*!
     * \note Thread affinity: ClientThread.
     */
    TAsyncStreamState::TAsyncResult::TPtr 
    AsyncWriteBlock(const TSharedRef& data);

    /*!
     * \note Thread affinity: ClientThread.
     */
    TAsyncStreamState::TAsyncResult::TPtr
    AsyncClose(const TSharedRef& masterMeta);


    /*!
     * \note Thread affinity: any.
     */
    void Cancel(const Stroka& errorMessage);

    ~TRemoteWriter();

    /*!
     * \note Thread affinity: any.
     */
    Stroka GetDebugInfo();

    TChunkId GetChunkId() const;

private:

    //! A group is a bunch of blocks that is sent in a single RPC request.
    class TGroup;
    typedef TIntrusivePtr<TGroup> TGroupPtr;

    struct TNode;
    typedef TIntrusivePtr<TNode> TNodePtr;
    
    typedef ydeque<TGroupPtr> TWindow;

    typedef NChunkHolder::TChunkHolderProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    USE_RPC_PROXY_METHOD(TProxy, StartChunk);
    USE_RPC_PROXY_METHOD(TProxy, FinishChunk);
    USE_RPC_PROXY_METHOD(TProxy, PutBlocks);
    USE_RPC_PROXY_METHOD(TProxy, SendBlocks);
    USE_RPC_PROXY_METHOD(TProxy, FlushBlock);
    USE_RPC_PROXY_METHOD(TProxy, PingSession);

    TChunkId ChunkId;
    const TConfig Config;

    TAsyncStreamState State;

    bool IsInitComplete;

    //! This flag is raised whenever #Close is invoked.
    //! All access to this flag happens from #WriterThread.
    bool IsCloseRequested;
    TSharedRef MasterMeta;

    // ToDo: replace by cyclic buffer
    TWindow Window;
    TAsyncSemaphore WindowSlots;

    yvector<TNodePtr> Nodes;

    //! Number of nodes that are still alive.
    int AliveNodeCount;

    //! A new group of blocks that is currently being filled in by the client.
    //! All access to this field happens from client thread.
    TGroupPtr CurrentGroup;

    //! Number of blocks that are already added via #AddBlock. 
    int BlockCount;

    TMetric StartChunkTiming;
    TMetric PutBlocksTiming;
    TMetric SendBlocksTiming;
    TMetric FlushBlockTiming;
    TMetric FinishChunkTiming;

    /*!
     * Invoked from #Close.
     * Sets #IsCloseRequested.
     */
    void DoClose(const TSharedRef& masterMeta);
    
    /*!
     * Invoked from #Cancel
     */
    void DoCancel(const Stroka& errorMessage);

    void AddGroup(TGroupPtr group);

    void RegisterReadyEvent(TFuture<TVoid>::TPtr windowReady);

    void OnNodeDied(int node);

    void ReleaseSlots(int count);

    void ShiftWindow();

    TInvFlushBlock::TPtr FlushBlock(int node, int blockIndex);

    void OnBlockFlushed(int node, int blockIndex);

    void OnWindowShifted(int blockIndex);

    void InitializeNodes(const yvector<Stroka>& addresses);

    void StartSession();

    TInvStartChunk::TPtr StartChunk(int node);

    void OnChunkStarted(int node);

    void OnSessionStarted();

    void CloseSession();

    TInvFinishChunk::TPtr FinishChunk(int node);

    void OnChunkFinished(int node);

    void OnSessionFinished();

    void PingSession(int node);
    void SchedulePing(int node);
    void CancelPing(int node);
    void Shutdown();
    void CancelAllPings();

    template<class TResponse>
    void CheckResponse(
        typename TResponse::TPtr rsp, 
        int node, 
        IAction::TPtr onSuccess,
        TMetric* metric);

    void AddBlock(TVoid, const TSharedRef& data);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

