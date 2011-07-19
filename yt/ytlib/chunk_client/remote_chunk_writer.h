#pragma once

#include "chunk_writer.h"
#include "../misc/lazy_ptr.h"
#include "../rpc/client.h"
#include "../chunk_holder/common.h"
#include "../chunk_holder/chunk_holder_rpc.h"
#include "../actions/action_queue.h"

#include <util/generic/deque.h>

namespace NYT
{

// TODO: move to client.h
// TODO: rename to USE_RPC_PROXY_METHOD
#define USE_RPC_METHOD(TProxy, MethodName) \
    typedef TProxy::TReq##MethodName TReq##MethodName; \
    typedef TProxy::TRsp##MethodName TRsp##MethodName; \
    typedef TProxy::TInv##MethodName TInv##MethodName;
    

///////////////////////////////////////////////////////////////////////////////

class TRemoteChunkWriter
    : public IChunkWriter
{
public:
    typedef TIntrusivePtr<TRemoteChunkWriter> TPtr;
    typedef NChunkHolder::TChunkId TChunkId;

    struct TConfig
    {
        // TODO: -> WindowSize
        //! Number of blocks in window.
        int WinSize;
        // TODO: consider removign
        int MinRepFactor;
        //! Maximum group size (in bytes).
        int GroupSize;
    };

    // Client thread
    TRemoteChunkWriter(TConfig config, yvector<Stroka> nodes);

    void AddBlock(TBlob *buffer);

    void Close();

    TChunkId GetChunkId();

    ~TRemoteChunkWriter();

    // TODO: maybe make instance
    // TODO: -> GetDebugInfo()
    static Stroka GetTimingInfo();

private:
    struct TNode :
        public TRefCountedBase
    {
        DECLARE_ENUM(ENodeState,
            (Starting)
            (Alive)
            (Closed)
            (Dead)
        );

        ENodeState State;
        // TODO: maybe keep proxy instead of address
        const Stroka Address;

        TNode(Stroka address)
            : State(ENodeState::Starting)
            , Address(address)
        { }

        bool IsAlive() const
        {
            return State == ENodeState::Alive;
        }
    };

    //! Represents a block of chunk.
    struct TBlock;

    //! A group is a bunch of blocks that is sent in a single RPC request.
    class TGroup;

    typedef TIntrusivePtr<TGroup> TGroupPtr;
    // TODO: -> TWindow
    typedef ydeque<TGroupPtr> TGroupBuffer;
    typedef NChunkHolder::TChunkHolderProxy TProxy;

    USE_RPC_METHOD(TProxy, StartChunk);
    USE_RPC_METHOD(TProxy, FinishChunk);
    USE_RPC_METHOD(TProxy, PutBlocks);
    USE_RPC_METHOD(TProxy, SendBlocks);
    USE_RPC_METHOD(TProxy, FlushBlock);

private:
    //! Manages all internal upload functionality, 
    //! sends out RPC requests, and handles responses.
    static TLazyPtr<TActionQueue> WriterThread;

    //! Caches node channels.
    NRpc::TChannelCache ChannelCache;

    // TODO: -> TChunkId
    const Stroka Id;

    const TConfig Config;

    DECLARE_ENUM(ESessionState,
        (Starting)
        (Ready)
        (Failed)
    );

    ESessionState State;

    //! This flag is raised whenever Close is invoked.
    //! All access to this flag happens from #WriterThread.
    bool Finishing;
    // TODO: -> TAsyncResult<TVoid>::TPtr IsFinished;
    Event FinishedEvent;

    // TODO: turn into a semaphore
    TAtomic WindowSlots;
    Event WindowReady;

    // TODO: typedef yvector< TIntrusivePtr<TNode> > TNodes;
    yvector< TIntrusivePtr<TNode> > Nodes;
    // TODO: -> Window
    TGroupBuffer Groups;

    //! Number of nodes that are still alive.
    int AliveNodes;

    //! A new group of blocks that is currently being filled in by the client.
    //! All access to this field happens from client thread.
    TGroupPtr NewGroup;
    // TODO: unsigned -> int
    //! Number of blocks that are already added via #AddBlock. 
    unsigned BlockCount;
    // TODO: -> TBlockOffset
    //! The current offset inside the chunk that is being uploaded.
    size_t BlockOffset;

    // TODO: killmeplz!!!!
    unsigned Pending;

private:
    // writer thread
    void AddGroup(TGroupPtr group);
    void FinishSession();
    void Work();

    void NodeDied(unsigned idx);
    void CheckStateAndThrow(); // client thread
    void ShiftWindow();

    void SetFinishFlag();

    TInvStartChunk::TPtr StartSession(i32 node);
    void StartSessionSuccess(i32 node);
    void StartSessionComplete();

    TInvFinishChunk::TPtr FinishSession(i32 node);
    void FinishSessionSuccess(i32 node);
    void FinishSessionComplete();

    void PutBlocks(i32 node, TGroupPtr group);
    void PutBlocksSuccess(i32 node, TGroupPtr group);

    void SendBlocks(i32 node, i32 dst, TGroupPtr group);
    void SendBlocksSuccess(i32 node, i32 dst, TGroupPtr group);

    TInvFlushBlock::TPtr FlushBlocks(i32 node, TGroupPtr group);
    void FlushBlocksSuccess(i32 node, TGroupPtr group);

    template<class TResponse>
    void CheckResponse(typename TResponse::TPtr rsp, i32 node, IAction::TPtr action);

};

///////////////////////////////////////////////////////////////////////////////

}

