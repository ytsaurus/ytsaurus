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

        //! Timeout specifying a maxmimum allowed period of time without RPC request to ChunkHolder.
        /*!
         * If no activity occured during this period, PingSession call will be sent.
         */
        TDuration SessionTimeout;

        TConfig()
            : WindowSize(16)
            , GroupSize(1024 * 1024)
            , RpcTimeout(TDuration::Seconds(30))
            , SessionTimeout(TDuration::Seconds(10))
        { }

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

    USE_RPC_PROXY_METHOD(TProxy, StartChunk);
    USE_RPC_PROXY_METHOD(TProxy, FinishChunk);
    USE_RPC_PROXY_METHOD(TProxy, PutBlocks);
    USE_RPC_PROXY_METHOD(TProxy, SendBlocks);
    USE_RPC_PROXY_METHOD(TProxy, FlushBlock);
    USE_RPC_PROXY_METHOD(TProxy, PingSession);

private:
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

private:
    /*!
     * Invoked from #Close.
     * \note Thread affinity: WriterThread
     * Sets #IsCloseRequested.
     */
    void DoClose(const TSharedRef& masterMeta);
    
    /*!
     * Invoked from #Cancel
     * \note Thread affinity: WriterThread.
     */
    void DoCancel(const Stroka& errorMessage);

    /*!
     * \note Thread affinity: WriterThread
     */
    void AddGroup(TGroupPtr group);

    /*!
     * \note Thread affinity: WriterThread
     */
    void RegisterReadyEvent(TFuture<TVoid>::TPtr windowReady);

    /*!
     * \note Thread affinity: WriterThread
     */
    void OnNodeDied(int node);

    /*!
     * \note Thread affinity: WriterThread
     */
    void ReleaseSlots(int count);

    /*!
     * \note Thread affinity: WriterThread
     */
    void ShiftWindow();

    /*!
     * \note Thread affinity: WriterThread
     */
    TInvFlushBlock::TPtr FlushBlock(int node, int blockIndex);

    /*!
     * \note Thread affinity: WriterThread
     */
    void OnBlockFlushed(int node, int blockIndex);

    /*!
     * \note Thread affinity: WriterThread
     */

    void OnWindowShifted(int blockIndex);
    /*!
     * \note Thread affinity: ClientThread
     */
    void InitializeNodes(const yvector<Stroka>& addresses);

    /*!
     * \note Thread affinity: ClientThread
     */
    void StartSession();

    /*!
     * \note Thread affinity: ClientThread
     */
    TInvStartChunk::TPtr StartChunk(int node);

    /*!
     * \note Thread affinity: WriterThread
     */
    void OnChunkStarted(int node);

    /*!
     * \note Thread affinity: WriterThread
     */
    void OnSessionStarted();

    /*!
     * \note Thread affinity: WriterThread
     */
    void CloseSession();

    /*!
     * \note Thread affinity: WriterThread
     */
    TInvFinishChunk::TPtr FinishChunk(int node);

    /*!
     * \note Thread affinity: WriterThread
     */
    void OnChunkFinished(int node);

    /*!
     * \note Thread affinity: WriterThread
     */
    void OnSessionFinished();

    /*!
     * \note Thread affinity: WriterThread
     */
    void PingSession(int node);

    /*!
     * \note Thread affinity: WriterThread
     */
    void SchedulePing(int node);

    /*!
     * \note Thread affinity: WriterThread
     */
    void CancelPing(int node);

     /*!
     * \note Thread affinity: WriterThread
     */
    void Shutdown();

    /*!
     * \note Thread affinity: WriterThread
     */
    void CancelAllPings();

    /*!
     * \note Thread affinity: WriterThread
     */
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

