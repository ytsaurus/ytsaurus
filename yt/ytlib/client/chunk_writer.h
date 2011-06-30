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
   /* template<class TReq, class TRsp>
    class TNodeCallback;

    template<class TReq, class TRsp>
    class TGroupMethodCall;

    template<class TReq, class TRsp>
    class TSessionMethodCall;

    class TStartSessionCall;
    class TFinishSessionCall;
    class TPutBlocksCall;
    class TSendBlocksCall;
    class TFlushBlocksCall; */

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
    struct TGroup;
    typedef TIntrusivePtr<TGroup> TGroupPtr;
    typedef ydeque<TGroupPtr> TGroupBuffer;
    typedef TChunkHolderProxy TProxy;
private:
    static TLazyPtr<TActionQueue> WriterThread;
    NRpc::TChannelCache ChannelCache;

    const Stroka Id;
    const TChunkWriterConfig Config;

    enum ESessionState {
        Starting,
        Ready,
        Finishing,
        Failed
    } State;

    TAtomic WinSlots;
    Event WinReady;
    Event FinEv;

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

    void NodeDied(unsigned idx);
    void CheckStateAndThrow();
    void ProcessBlocks();

    void SetFinishFlag();
    void StartSession(i32 node);
    void StartSessionCallback(TProxy::TRspStartChunk::TPtr rsp, i32 node);
    void FinishSession(i32 node);
    void PutBlocks(i32 node, TGroupPtr group);
    void SendBlocks(i32 node, i32 dst, TGroupPtr group);
    void FlushBlocks(i32 node, TGroupPtr group);

    template<class TResponsePtr>
    bool CheckResponse(TResponsePtr rsp, i32 node);

    template<class TResponsePtr>
    bool CheckSessionResponse(TResponsePtr rsp, i32 node);

    template<class TResponsePtr>
    bool CheckGroupResponse(TResponsePtr rsp, i32 node, TGroupPtr group);

public:
    typedef TIntrusivePtr<TChunkWriter> TPtr;

    // writer thread
    void Work();

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
