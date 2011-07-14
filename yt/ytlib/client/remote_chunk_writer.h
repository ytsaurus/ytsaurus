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
        int WinSize;
        int MinRepFactor;
        int GroupSize;
    };

    // Client thread
    TRemoteChunkWriter(TConfig config, yvector<Stroka> nodes);
    void AddBlock(TBlob *buffer);
    void Finish();
    TChunkId GetChunkId();

    ~TRemoteChunkWriter();

    static Stroka GetTimingInfo();

private:
    struct TNode : public TRefCountedBase
    {

        DECLARE_ENUM(ENodeState,
            (Starting)
            (Alive)
            (Closed)
            (Dead)
        );
        ENodeState State;

        const Stroka Address;

        bool IsAlive()
        {
            return State == ENodeState::Alive;
        }

        TNode(Stroka address)
            : State(ENodeState::Starting)
            , Address(address)
        {}
    };

    struct TBlock;
    class TGroup;
    typedef TIntrusivePtr<TGroup> TGroupPtr;
    typedef ydeque<TGroupPtr> TGroupBuffer;
    typedef TChunkHolderProxy TProxy;

    USE_RPC_METHOD(TProxy, StartChunk);
    USE_RPC_METHOD(TProxy, FinishChunk);
    USE_RPC_METHOD(TProxy, PutBlocks);
    USE_RPC_METHOD(TProxy, SendBlocks);
    USE_RPC_METHOD(TProxy, FlushBlock);

private:
    static TLazyPtr<TActionQueue> WriterThread;
    NRpc::TChannelCache ChannelCache;

    const Stroka Id;
    const TConfig Config;

    DECLARE_ENUM(ESessionState,
        (Starting)
        (Ready)
        (Failed)
    );
    ESessionState State;

    bool Finishing;

    TAtomic WindowSlots; // Number of free slots for blocks in sliding window
    Event WindowReady;
    Event FinishedEvent;

    yvector< TIntrusivePtr<TNode> > Nodes;
    TGroupBuffer Groups;

    int AliveNodes;

    TGroupPtr NewGroup;
    unsigned BlockCount;
    size_t BlockOffset;

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

