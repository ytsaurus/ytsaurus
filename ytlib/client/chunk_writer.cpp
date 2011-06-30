#include "chunk_writer.h"
#include "chunk_holder.pb.h"

#include "../misc/serialize.h"
#include "../misc/metric.h"
#include "../logging/log.h"
#include "../actions/action_util.h"

#include <util/random/random.h>
#include <util/generic/yexception.h>
#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>
#include <util/stream/str.h>

#define CHAINING

namespace NYT
{
static NLog::TLogger Logger("ChunkWriter");

struct TChunkWriter::TBlock 
    : public TRefCountedBase
{
    TBlob Buffer;
    size_t Offset;

    TBlock(TBlob &b, size_t offset)
        : Offset(offset)
    {
        Buffer.swap(b);
    }

    size_t Size()
    {
        return Buffer.size();
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TChunkWriter::TGroup : public TRefCountedBase
{
    enum ENodeGroupState {
        No,
        InMem,
        Flushed
    };

    yvector<ENodeGroupState> States;
    yvector< TIntrusivePtr<TBlock> > Blocks;
    unsigned StartId;
    unsigned Pending;
    size_t Size;
    TChunkWriter* Session;

    TGroup(unsigned numNodes, unsigned blockId, TChunkWriter* s)
        : States(numNodes)
        , StartId(blockId)
        , Pending(0)
        , Size(0)
        , Session(s)
    { }

    void AddBlock(TBlock *b)
    {
        Blocks.push_back(b);
        Size += b->Size();
    }

    void ReleaseSlots()
    {
        for (unsigned i = 0; i < Blocks.size(); ++i)
            AtomicIncrement(Session->WinSlots);
        Session->WinReady.Signal();
    }

    unsigned GetEndId()
    {
        return StartId + Blocks.size() - 1;
    }

    void Process();
    void Flush();
    void Send(unsigned src);
    unsigned GetNearestNodeIdx();
};

///////////////////////////////////////////////////////////////////////////////
/*
template<class TReq, class TRsp>
class TChunkWriter::TNodeCallback
    : public TMethodCallback<TReq, TRsp>
{
public:
    static TMetric TimeStat;

protected:
    TIntrusivePtr<TChunkWriter> Session;
    unsigned NodeIdx;
    TSimpleTimer Timer;

protected:
    virtual void SuccessCallback() throw() = 0;

public:
    TNodeCallback(TChunkWriter *s, unsigned idx)
        : TMethodCallback<TReq, TRsp>(TChunkWriter::WriterThread.Get())
        , Session(s)
        , NodeIdx(idx)
    {}

    void Do() throw()
    {
        TimeStat.AddValue(Timer.Get().MilliSeconds());
        if (!TMethodCallback<TReq, TRsp>::Context->Result)
            SuccessCallback();
        else {
            Session->NodeDied(NodeIdx);
        }
        TimeStat.AddValue(Timer.Get().SecondsFloat());
    }

    TNode* GetNode()
    {
        return ~Session->Nodes[NodeIdx];
    }
};

template<class TReq, class TRsp>
TMetric TChunkWriter::TNodeCallback<TReq, TRsp>::TimeStat;

///////////////////////////////////////////////////////////////////////////////

template<class TReq, class TRsp>
class TChunkWriter::TSessionMethodCall
    : public TNodeCallback<TReq, TRsp>
{
public:
    TSessionMethodCall(TChunkWriter *s, unsigned idx)
        : TNodeCallback<TReq, TRsp>(s, idx)
    {
        TNodeCallback<TReq, TRsp>::Session->Pending++;
    }

    void Do() throw()
    {
        TNodeCallback<TReq, TRsp>::Session->Pending--;
        TNodeCallback<TReq, TRsp>::Do();
        if (!TNodeCallback<TReq, TRsp>::Session->Pending--)
            TNodeCallback<TReq, TRsp>::Session->Work();
    }
};

///////////////////////////////////////////////////////////////////////////////

template<class TReq, class TRsp>
class TChunkWriter::TGroupMethodCall
    : public TNodeCallback<TReq, TRsp>
{
protected:
    TGroup* Group;

public:
    TGroupMethodCall(TChunkWriter* s, unsigned idx, TGroup* g)
        : TNodeCallback<TReq, TRsp>(s, idx)
        , Group(g)
    {
        Group->Pending++;
    }

    void Do() throw()
    {
        Group->Pending--;
        TNodeCallback<TReq, TRsp>::Do();
    }
};

///////////////////////////////////////////////////////////////////////////////

class TChunkWriter::TStartSessionCall
    : public TSessionMethodCall<TReqStartChunk, TRspStartChunk>
{
protected:
    void SuccessCallback() throw()
    {
        GetNode()->State = TNode::Alive;
        LOG_DEBUG("Session %s, node %d start success",
            Session->Id.c_str(), NodeIdx);
    }

public:
    TStartSessionCall(TChunkWriter *s, unsigned idx)
        : TSessionMethodCall<TReqStartChunk, TRspStartChunk>(s, idx)
    {
        Req->SetChunkId(ProtoGuidFromGuid(GetGuid(Session->Id)));
        GetNode()->Chunkholder->StartChunk(RPC_PARAMS(this));
        LOG_DEBUG("Session %s, node %d start req",
            Session->Id.c_str(), NodeIdx);
    }
};

///////////////////////////////////////////////////////////////////////////////

class TChunkWriter::TFinishSessionCall
    : public TSessionMethodCall<TReqFinishChunk, TRspFinishChunk>
{
protected:
    void SuccessCallback() throw()
    {
        GetNode()->State = TNode::Closed;
        LOG_DEBUG("Session %s, node %d finish success",
            Session->Id.c_str(), NodeIdx);
    }

public:
    TFinishSessionCall(TChunkWriter *s, unsigned idx)
        : TSessionMethodCall<TReqFinishChunk, TRspFinishChunk>(s, idx)
    {
        Req->SetChunkId(ProtoGuidFromGuid(GetGuid(Session->Id)));
        GetNode()->Chunkholder->FinishChunk(RPC_PARAMS(this));
        LOG_DEBUG("Session %s, node %d finish req",
            Session->Id.c_str(), NodeIdx);
    }
};

///////////////////////////////////////////////////////////////////////////////

class TChunkWriter::TPutBlocksCall
    : public TGroupMethodCall<TReqPutBlocks, TRspPutBlocks>
{
protected:
    void SuccessCallback() throw()
    {
        Group->States[NodeIdx] = TGroup::InMem;
        LOG_DEBUG("Session %s, group %d, node %d put success",
            Session->Id.c_str(), Group->StartId, NodeIdx);
        if (!Group->Pending && !Session->Pending)
            Group->Process();
    }

public:
    TPutBlocksCall(TChunkWriter *s, unsigned idx, TGroup *g)
        : TGroupMethodCall<TReqPutBlocks, TRspPutBlocks>(s, idx, g)
    {
        LOG_DEBUG("Session %s, group %d, node %d put req",
            Session->Id.c_str(), Group->StartId, NodeIdx);
        Req->SetChunkId(ProtoGuidFromGuid(GetGuid(Session->Id)));
        Req->SetStartBlockIndex(Group->StartId);
        Req->SetBlockCount(Group->Blocks.size());
        Req->SetStartOffset(Group->Blocks.front()->Offset);

        for (unsigned i = 0; i < Group->Blocks.size(); ++i)
            Context->RequestAttachments.push_back(Group->Blocks[i]->Buffer);

        GetNode()->Chunkholder->PutBlocks(RPC_PARAMS(this));
    }
};

///////////////////////////////////////////////////////////////////////////////

class TChunkWriter::TSendBlocksCall
    : public TGroupMethodCall<TReqSendBlocks, TRspSendBlocks>
{
    unsigned DstIdx;

protected:
    void SuccessCallback() throw()
    {
        Group->States[DstIdx] = TGroup::InMem;
        LOG_DEBUG("Session %s, group %d, node %d, send to %d success",
            Session->Id.c_str(), Group->StartId, NodeIdx, DstIdx);
        if (!Group->Pending && !Session->Pending)
            Group->Process();
    }

public:
    TSendBlocksCall(TChunkWriter *s, unsigned src, unsigned dst, TGroup *g)
        : TGroupMethodCall<TReqSendBlocks, TRspSendBlocks>(s, src, g)
        , DstIdx(dst)
    {
        Req->SetChunkId(ProtoGuidFromGuid(GetGuid(Session->Id)));
        Req->SetStartBlockIndex(Group->StartId);
        Req->SetEndBlockIndex(Group->GetEndId());
        Req->SetDestination(Session->Nodes[DstIdx]->Address);
        GetNode()->Chunkholder->SendBlocks(RPC_PARAMS(this));
        LOG_DEBUG("Session %s, group %d, node %d, send to %d req",
            Session->Id.c_str(), Group->StartId, NodeIdx, DstIdx);
    }
};

///////////////////////////////////////////////////////////////////////////////

class TChunkWriter::TFlushBlocksCall
    : public TGroupMethodCall<TReqFlushBlocks, TRspFlushBlocks>
{
protected:
    void SuccessCallback() throw()
    {
        LOG_DEBUG("Session %s, group %d, node %d, flush success",
            Session->Id.c_str(), Group->StartId, NodeIdx);
        Group->States[NodeIdx] = TGroup::Flushed;
        if (!Group->Pending && !Session->Pending)
            Session->Work();

    }

public:
    TFlushBlocksCall(TChunkWriter *s, unsigned idx, TGroup *g)
        : TGroupMethodCall<TReqFlushBlocks, TRspFlushBlocks>(s, idx, g)
    {
        Req->SetChunkId(ProtoGuidFromGuid(GetGuid(Session->Id)));
        Req->SetStartBlockIndex(Group->StartId);
        Req->SetEndBlockIndex(Group->GetEndId());
        GetNode()->Chunkholder->FlushBlocks(RPC_PARAMS(this));
        LOG_DEBUG("Session %s, group %d, node %d, flush req",
            Session->Id.c_str(), Group->StartId, NodeIdx);
    }
};
*/
///////////////////////////////////////////////////////////////////////////////

void TChunkWriter::TGroup::Flush()
{
    for (unsigned i = 0; i < States.size(); ++i) {
        if (Session->Nodes[i]->IsAlive() && States[i] != Flushed) {
            Session->FlushBlocks(i, this);
        }
    }
}

void TChunkWriter::TGroup::Send(unsigned src)
{
    for (unsigned i = 0; i < States.size(); ++i) {
        if (Session->Nodes[i]->IsAlive() && States[i] == No) {
            Session->SendBlocks(src, i, this);
#ifdef CHAINING
            break;
#endif
        }
    }
}

unsigned TChunkWriter::TGroup::GetNearestNodeIdx()
{
    int aliveCount = int(RandomNumber<float>() * Session->AliveNodes) + 1;
    int i = 0;
    while (aliveCount) {
        if (Session->Nodes[i]->IsAlive())
            aliveCount--;
        i++;
    }
    return i - 1;
}

void TChunkWriter::TGroup::Process()
{
    int nodeWithBlock = -1;
    bool existsEmpty = false;
    LOG_DEBUG("Session %s, processing group %d",
        Session->Id.c_str(), StartId);

    for (unsigned i = 0; i < States.size(); ++i) {
        if (Session->Nodes[i]->IsAlive()) {
            switch (States[i]) {
            case InMem:
                nodeWithBlock = i;
                break;

            case No:
                existsEmpty = true;
                break;

            case Flushed:
                //Nothing to do here
                break;
            }
        }
    }

    if (!existsEmpty) {
        Flush();
    } else if (nodeWithBlock < 0) {
#ifdef CHAINING
        unsigned idx = 0;
        while (!Session->Nodes[idx]->IsAlive())
            ++idx;
#else
        unsigned idx = GetNearestNodeIdx();
#endif
        Session->PutBlocks(idx, this);
    } else {
        Send(nodeWithBlock);
    }
}

///////////////////////////////////////////////////////////////////////////////
TLazyPtr<TActionQueue> TChunkWriter::WriterThread; 

TChunkWriter::TChunkWriter(TChunkWriterConfig config, yvector<Stroka> nodes)
    : Id(CreateGuidAsString())
    , Config(config)
    , State(Starting)
    , WinSlots(config.WinSize)
    , WinReady(Event::rAuto)
    , FinEv(Event::rAuto)
    , AliveNodes(nodes.size())
    , NewGroup(new TGroup(AliveNodes, 0, this))
    , BlockCount(0)
    , BlockOffset(0)
    , Pending(0)
{
    YASSERT(Config.MinRepFactor <= AliveNodes);
    LOG_DEBUG("New session %s", Id.c_str());

    yvector<Stroka>::const_iterator it = nodes.begin();
    for (; it != nodes.end(); ++it) {
        Nodes.push_back(new TNode(*it));
    }

    for (unsigned i = 0; i < Nodes.size(); ++i) {
        StartSession(i);
    }
}

TChunkWriter::~TChunkWriter()
{
    LOG_DEBUG("Session %s destructor", Id.c_str());
    YASSERT((State == Finishing && Groups.empty()) || State == Failed);
}

TChunkId TChunkWriter::GetChunkId()
{
    return GetGuid(Id);
}

void TChunkWriter::ProcessBlocks()
{
    TGroupBuffer::iterator it;
    for (it = Groups.begin(); it != Groups.end(); ++it) {
        TIntrusivePtr<TGroup> group = *it;
        if (!group->Pending)
            group->Process();
        else
            LOG_DEBUG("Session %s, group %d pending %d",
                Id.c_str(), group->StartId, group->Pending);
    }

    // Shifting window
    while (!Groups.empty()) {
        TIntrusivePtr<TGroup> &g = Groups.front();
        if (!g->Pending) {
            LOG_DEBUG("Session %s, shifted out group %d",
                Id.c_str(), g->StartId);
            g->ReleaseSlots();
            Groups.pop_front();
        } else
            break;
    }
}

void TChunkWriter::SetFinishFlag()
{
    LOG_DEBUG("Session %s, set finish flag", Id.c_str());
    State = Finishing;
}

void TChunkWriter::Work()
{
    if (Pending) {
        LOG_DEBUG("Session %s, session-level requests pending %d",
            Id.c_str(), Pending);
        return;
    }

    switch (State) {
    case Finishing:
        ProcessBlocks();
        if (Groups.empty())
            FinishSession();
        break;

    case Starting:
        State = Ready;

    case Ready:
        ProcessBlocks();
        break;

    case Failed:
        //Waits to be detached
        break;
    }
}

void TChunkWriter::FinishSession()
{
    for (unsigned i = 0; i < Nodes.size(); ++i) {
        if (Nodes[i]->IsAlive())
            FinishSession(i);
    }
    LOG_DEBUG("Finishing session %s, %d requests pending",
        Id.c_str(), Pending);

    if (!Pending) {
        // Successfully finished
        FinEv.Signal();
    }
}

void TChunkWriter::AddBlock(TBlob &buffer)
{
    CheckStateAndThrow();
    
    while (!WinSlots)
        WinReady.Wait();
    AtomicDecrement(WinSlots);

    LOG_DEBUG("Session %s, client adds new block", Id.c_str());

    TBlock *b = new TBlock(buffer, BlockOffset);
    NewGroup->AddBlock(b);
    BlockOffset += b->Size();
    ++BlockCount;

    if (NewGroup->Size >= Config.GroupSize) {
        WriterThread->Invoke(FromMethod(
            &TChunkWriter::AddGroup, this, NewGroup));
        TIntrusivePtr<TGroup> g(new TGroup(Nodes.size(), BlockCount, this));
        NewGroup.Swap(g);
    }
}

void TChunkWriter::CheckStateAndThrow()
{
    if (State == Failed)
        ythrow yexception() << "Chunk write session failed!";
}

void TChunkWriter::AddGroup(TIntrusivePtr<TGroup> group)
{
    YASSERT(State != Finishing);

    if (State == Failed) {
        group->ReleaseSlots();
    } else {
        LOG_DEBUG("Session %s, added group %d", Id.c_str(), group->StartId);
        Groups.push_back(group);
    }
    Work();
}

void TChunkWriter::Finish()
{
    LOG_DEBUG("Session %s, client thread finishing", Id.c_str());
    if (NewGroup->Size)
        WriterThread->Invoke(FromMethod(
            &TChunkWriter::AddGroup, this, NewGroup));

    // Set "Finishing" state through queue to ensure that flag will be set
    // after all block appends
    WriterThread->Invoke(FromMethod(
        &TChunkWriter::SetFinishFlag, this));
    FinEv.Wait();
    CheckStateAndThrow();
    LOG_DEBUG("Session %s, client thread complete.", Id.c_str());
}

void TChunkWriter::NodeDied(unsigned idx)
{
    if (Nodes[idx]->State != TNode::Dead) {
        Nodes[idx]->State = TNode::Dead;
        --AliveNodes;
        LOG_DEBUG("Session %s, node %d died. Alive nodes = %d", Id.c_str(), idx, AliveNodes);
        if (State != Failed && AliveNodes < Config.MinRepFactor) {
            State = Failed;
            FinEv.Signal();
            LOG_DEBUG("Write session %s failed", ~Id);
            AtomicIncrement(WinSlots);
            WinReady.Signal();
        }
    }
}

template<class TResponsePtr>
bool TChunkWriter::CheckResponse(TResponsePtr rsp, i32 node)
{
    if (rsp->IsOK())
        return true;
    
    if (rsp->IsServiceError())
        return false;
    
    // Rpc error: node died or overloaded
    NodeDied(node);

    // TODO: compiler warning; function does not return any value
    // consider defining NodeDied as non-returnable (like abort())
    return false;
}

template<class TResponsePtr>
bool TChunkWriter::CheckSessionResponse(TResponsePtr rsp, i32 node)
{
    --Pending;
    return CheckResponse(rsp, node);
}

template<class TResponsePtr>
bool TChunkWriter::CheckGroupResponse(TResponsePtr rsp, i32 node, TGroupPtr group)
{
    --group->Pending;
    return CheckResponse(rsp, node);
}

void TChunkWriter::StartSession(i32 node)
{
    NRpc::TChannel::TPtr channel = ChannelCache.GetChannel(Nodes[node]->Address);
    TProxy::TReqStartChunk::TPtr req = TProxy(channel).StartChunk();
    req->SetChunkId(ProtoGuidFromGuid(GetGuid(Id)));
    ++Pending;
    req->Invoke()->Subscribe(FromMethod(
        &TChunkWriter::StartSessionCallback,
        TPtr(this),
        node)->Via(~WriterThread));
    LOG_DEBUG("Session %s, node %d start req", ~Id, node);
}

void TChunkWriter::StartSessionCallback(TProxy::TRspStartChunk::TPtr rsp, i32 node)
{
    if (CheckSessionResponse(rsp, node)) {
        Nodes[node]->State = TNode::Alive;
        LOG_DEBUG("Session %s, node %d started successfully", ~Id, node);
    }
}

void TChunkWriter::FinishSession(i32 node)
{
    NRpc::TChannel::TPtr channel = ChannelCache.GetChannel(Nodes[node]->Address);
/*    TProxy::TReqPutBlocks::TPtr req = TProxy(channel).StartChunk();
    req->SetChunkId(ProtoGuidFromGuid(GetGuid(Id)));
    ++Pending;
    req->Invoke()->Subscribe(
        FromMethod(&TChunkWriter::StartSessionCallback, TPtr(this), node)->Via(~WriterThread));
    LOG_DEBUG("Session %s, node %d start req", ~Id, node); */
}

void TChunkWriter::PutBlocks(i32 node, TGroupPtr group)
{
}

void TChunkWriter::SendBlocks(i32 node, i32 dst, TGroupPtr group)
{
}

void TChunkWriter::FlushBlocks(i32 node, TGroupPtr group)
{
}

Stroka TChunkWriter::GetTimingInfo()
{
    TStringStream ss;
    // ToDo: implement measures
    
 /*   ss << "PutBlocks: mean " << TPutBlocksCall::TimeStat.GetMean() << "ms, std " << 
        TPutBlocksCall::TimeStat.GetStd() << "ms, calls " << TPutBlocksCall::TimeStat.GetNum() << Endl;
    ss << "SendBlocks: mean " << TSendBlocksCall::TimeStat.GetMean() << "ms, std " << 
        TSendBlocksCall::TimeStat.GetStd() << "ms, calls " << TSendBlocksCall::TimeStat.GetNum() << Endl;
    ss << "FlushBlocks: mean " << TFlushBlocksCall::TimeStat.GetMean() << "ms, std " << 
        TFlushBlocksCall::TimeStat.GetStd() << "ms, calls " << TFlushBlocksCall::TimeStat.GetNum() << Endl;
    ss << "StartSession: mean " << TStartSessionCall::TimeStat.GetMean() << "ms, std " << 
        TStartSessionCall::TimeStat.GetStd() << "ms, calls " << TStartSessionCall::TimeStat.GetNum() << Endl;
    ss << "FinishSession: mean " << TFinishSessionCall::TimeStat.GetMean() << "ms, std " << 
        TFinishSessionCall::TimeStat.GetStd() << "ms, calls " << TFinishSessionCall::TimeStat.GetNum() << Endl;
*/
    return ss;
}

}
