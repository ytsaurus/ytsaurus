#pragma once

#include "../rpc/client.h"
#include "../misc/common.h"
#include "../misc/common.h"
#include "../misc/lazy_ptr.h"
#include "../holder/chunk_holder_rpc.h"
#include "../holder/chunk.h"
#include "../actions/action_queue.h"

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/deque.h>
#include <util/system/event.h>

namespace NYT
{

#define USE_RPC_METHOD(TProxy, MethodName) \
    typedef TProxy::TReq##MethodName TReq##MethodName; \
    typedef TProxy::TRsp##MethodName TRsp##MethodName; \
    typedef TProxy::TInv##MethodName TInv##MethodName;
    
///////////////////////////////////////////////////////////////////////////////

struct TChunkWriterConfig
{
    unsigned WinSize;
    unsigned MinRepFactor;
    size_t GroupSize;
};

///////////////////////////////////////////////////////////////////////////////

class TChunkWriter
    : public TRefCountedBase
{
    struct TNode : public TRefCountedBase
    {
        enum ENodeState {
            Starting,
            Alive,
            Closed,
            Dead
        } State;

        const Stroka Address;

        bool IsAlive()
        {
            return State == Alive;
        }

        TNode(Stroka address)
            : State(Starting)
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
    USE_RPC_METHOD(TProxy, FlushBlocks);

private:
    static TLazyPtr<TActionQueue> WriterThread;
    NRpc::TChannelCache ChannelCache;

    const Stroka Id;
    const TChunkWriterConfig Config;

    enum ESessionState {
        Starting,
        Ready,
        Failed
    } State;
    bool Finishing;

    TAtomic WindowSlots; // Number of free slots for blocks in sliding window
    Event WindowReady;
    Event FinishedEvent;

    yvector< TIntrusivePtr<TNode> > Nodes;
    TGroupBuffer Groups;

    unsigned AliveNodes;

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

    TInvFlushBlocks::TPtr FlushBlocks(i32 node, TGroupPtr group);
    void FlushBlocksSuccess(i32 node, TGroupPtr group);

    template<class TResponse>
    void CheckResponse(typename TResponse::TPtr rsp, i32 node, IAction::TPtr action);

public:
    typedef TIntrusivePtr<TChunkWriter> TPtr;

    // Client thread
    TChunkWriter(TChunkWriterConfig config, yvector<Stroka> nodes);
    void AddBlock(TBlob &buffer);
    void Finish();
    TChunkId GetChunkId();

    ~TChunkWriter();

    static Stroka GetTimingInfo();
};

///////////////////////////////////////////////////////////////////////////////

}
