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

namespace NYT
{
static NLog::TLogger Logger("ChunkWriter");

struct TChunkWriter::TBlock 
    : public TRefCountedBase
{
    TSharedRef Buffer;
    size_t Offset;

    TBlock(TBlob &data, size_t offset)
        : Buffer(data)
        , Offset(offset)
    { }

    size_t Size()
    {
        return TRef(Buffer).Size();
    }
};

///////////////////////////////////////////////////////////////////////////////

class TChunkWriter::TGroup : public TRefCountedBase
{
public:

    DECLARE_ENUM(ENodeGroupState,
        (No)
        (InMem)
        (Flushed)
    );

    yvector<ENodeGroupState> States;
    yvector< TIntrusivePtr<TBlock> > Blocks;
    unsigned StartId;
    size_t Size;

private:
    TChunkWriter::TPtr Session;

    void Flush();
    void Send(unsigned src);

public:
    TGroup(unsigned numNodes, unsigned blockId, TChunkWriter::TPtr s)
        : States(numNodes)
        , StartId(blockId)
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
            AtomicIncrement(Session->WindowSlots);
        Session->WindowReady.Signal();
    }

    unsigned GetEndId()
    {
        return StartId + Blocks.size() - 1;
    }

    bool IsFlushed()
    {
        for (unsigned i = 0; i < Session->Nodes.size(); ++i) {
            if (Session->Nodes[i]->IsAlive() && States[i] != ENodeGroupState::Flushed) {
                return false;
            }
        }
        return true;
    }

    void Process();
};

void TChunkWriter::TGroup::Flush()
{    
    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter(~WriterThread);
    for (unsigned i = 0; i < Session->Nodes.size(); ++i) {
        if (Session->Nodes[i]->IsAlive() && States[i] != ENodeGroupState::Flushed) {
            IAction::TPtr onSuccess = FromMethod(&TChunkWriter::FlushBlocksSuccess, Session, i, TGroupPtr(this));
            IParamAction<TRspFlushBlocks::TPtr>::TPtr onResponse = 
                FromMethod(&TChunkWriter::CheckResponse<TRspFlushBlocks>, Session, i, onSuccess);
            awaiter->Await(Session->FlushBlocks(i, TGroupPtr(this)), onResponse);
        }
    }
    awaiter->Complete(FromMethod(&TChunkWriter::ShiftWindow, Session));
}

void TChunkWriter::TGroup::Send(unsigned src)
{
    for (unsigned i = 0; i < States.size(); ++i) {
        if (Session->Nodes[i]->IsAlive() && States[i] == ENodeGroupState::No) {
            Session->SendBlocks(src, i, this);
            break;
        }
    }
}

void TChunkWriter::TGroup::Process()
{
    int nodeWithBlock = -1;
    bool existsEmpty = false;
    LOG_DEBUG("Session %s, processing group %d",
        ~Session->Id, StartId);

    for (unsigned i = 0; i < States.size(); ++i) {
        if (Session->Nodes[i]->IsAlive()) {
            switch (States[i]) {
                case ENodeGroupState::InMem:
                    nodeWithBlock = i;
                    break;

                case ENodeGroupState::No:
                    existsEmpty = true;
                    break;

                case ENodeGroupState::Flushed:
                    //Nothing to do here
                    break;
            }
        }
    }

    if (!existsEmpty) {
        Flush();
    } else if (nodeWithBlock < 0) {
        unsigned idx = 0;
        while (!Session->Nodes[idx]->IsAlive())
            ++idx;
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
    , State(ESessionState::Starting)
    , WindowSlots(config.WinSize)
    , WindowReady(Event::rAuto)
    , FinishedEvent(Event::rAuto)
    , AliveNodes(nodes.size())
    , NewGroup(new TGroup(AliveNodes, 0, this))
    , BlockCount(0)
    , BlockOffset(0)
    , Pending(0)
{
    YASSERT(Config.MinRepFactor <= AliveNodes);
    LOG_DEBUG("New session %s", ~Id);

    yvector<Stroka>::const_iterator it = nodes.begin();
    for (; it != nodes.end(); ++it) {
        Nodes.push_back(new TNode(*it));
    }
    
    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter(~WriterThread);
    for (unsigned i = 0; i < Nodes.size(); ++i) {
        IAction::TPtr onSuccess = FromMethod(&TChunkWriter::StartSessionSuccess, TPtr(this), i);

        IParamAction<TRspStartChunk::TPtr>::TPtr onResponse = 
            FromMethod(&TChunkWriter::CheckResponse<TRspStartChunk>, TPtr(this), i, onSuccess);
        awaiter->Await(StartSession(i), onResponse);
    }
    awaiter->Complete(FromMethod(&TChunkWriter::StartSessionComplete, TPtr(this)));
}

TChunkWriter::~TChunkWriter()
{
    //LOG_DEBUG("Session %s destructor", Id.c_str());
    YASSERT((Finishing && Groups.empty()) || State == ESessionState::Failed);
}

TChunkId TChunkWriter::GetChunkId()
{
    return GetGuid(Id);
}

void TChunkWriter::ShiftWindow()
{
    while (!Groups.empty()) {
        TGroupPtr g = Groups.front();
        if (g->IsFlushed()) {
            LOG_DEBUG("Session %s, shifted out group %d",
                ~Id, g->StartId);
            g->ReleaseSlots();
            Groups.pop_front();
        } else
            return;
    }

    if (Finishing)
        FinishSession();
}

void TChunkWriter::SetFinishFlag()
{
    LOG_DEBUG("Session %s, set finish flag", ~Id);
    Finishing = true;
}

void TChunkWriter::FinishSession()
{
    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter(~WriterThread);
    for (unsigned i = 0; i < Nodes.size(); ++i) {
        if (Nodes[i]->IsAlive()) {
            IAction::TPtr onSuccess = 
                FromMethod(&TChunkWriter::FinishSessionSuccess, TPtr(this), i);
            IParamAction<TRspFinishChunk::TPtr>::TPtr onResponse = 
                FromMethod(&TChunkWriter::CheckResponse<TRspFinishChunk>, TPtr(this), i, onSuccess);
            awaiter->Await(FinishSession(i), onResponse);    ;
       }
    }
    awaiter->Complete(FromMethod(&TChunkWriter::FinishSessionComplete, TPtr(this)));
    LOG_DEBUG("Finishing session %s", ~Id);
}

void TChunkWriter::AddBlock(TBlob &buffer)
{
    CheckStateAndThrow();
    
    while (!WindowSlots)
        WindowReady.Wait();
    AtomicDecrement(WindowSlots);

    LOG_DEBUG("Session %s, client adds new block", ~Id);

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
    if (State == ESessionState::Failed)
        ythrow yexception() << "Chunk write session failed!";
}

void TChunkWriter::AddGroup(TGroupPtr group)
{
    // ToDo: throw exception here
    YASSERT(!Finishing);

    if (State == ESessionState::Failed) {
        group->ReleaseSlots();
    } else {
        LOG_DEBUG("Session %s, added group %d", ~Id, group->StartId);
        Groups.push_back(group);
        if (State != ESessionState::Starting)
            group->Process();
    }
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
    FinishedEvent.Wait();
    CheckStateAndThrow();
    LOG_DEBUG("Session %s, client thread complete.", Id.c_str());
}

void TChunkWriter::NodeDied(unsigned idx)
{
    if (Nodes[idx]->State != TNode::ENodeState::Dead) {
        Nodes[idx]->State = TNode::ENodeState::Dead;
        --AliveNodes;
        LOG_DEBUG("Session %s, node %d died. Alive nodes = %d", Id.c_str(), idx, AliveNodes);
        if (State != ESessionState::Failed && AliveNodes < Config.MinRepFactor) {
            State = ESessionState::Failed;
            FinishedEvent.Signal();
            LOG_DEBUG("Write session %s failed", ~Id);
            AtomicIncrement(WindowSlots);
            WindowReady.Signal();
        }
    }
}

template<class TResponse>
void TChunkWriter::CheckResponse(typename TResponse::TPtr rsp, i32 node, IAction::TPtr action)
{
    if (rsp->IsOK()) {
        action->Do();
    } else if (rsp->IsServiceError()) {
        if (rsp->GetErrorCode() == NRpc::EErrorCode::ServiceError)
            LOG_FATAL("Node %d returned soft error", node)
        else
            LOG_DEBUG("Node %d returned unknown service error", node)
    } else
        NodeDied(node);
}

TChunkWriter::TInvStartChunk::TPtr TChunkWriter::StartSession(i32 node)
{
    NRpc::TChannel::TPtr channel = ChannelCache.GetChannel(Nodes[node]->Address);
    TProxy::TReqStartChunk::TPtr req = TProxy(channel).StartChunk();
    req->SetChunkId(ProtoGuidFromGuid(GetGuid(Id)));
    LOG_DEBUG("Session %s, node %d start request", ~Id, node);
    return req->Invoke();
}

void TChunkWriter::StartSessionSuccess(i32 node)
{
    Nodes[node]->State = TNode::ENodeState::Alive;
    LOG_DEBUG("Session %s, node %d started successfully", ~Id, node);
}

void TChunkWriter::StartSessionComplete()
{
    if (State == ESessionState::Starting) {
        State = ESessionState::Ready;
        TGroupBuffer::iterator it;
        for (it = Groups.begin(); it != Groups.end(); ++it) {
            TGroupPtr group = *it;
            group->Process();
        }
    }
}

TChunkWriter::TInvFinishChunk::TPtr TChunkWriter::FinishSession(i32 node)
{
    NRpc::TChannel::TPtr channel = ChannelCache.GetChannel(Nodes[node]->Address);
    TReqFinishChunk::TPtr req = TProxy(channel).FinishChunk();
    req->SetChunkId(ProtoGuidFromGuid(GetGuid(Id)));
    LOG_DEBUG("Session %s, node %d finish request", ~Id, node);
    return req->Invoke();
}

void TChunkWriter::FinishSessionSuccess(i32 node)
{
    Nodes[node]->State = TNode::ENodeState::Closed;
    LOG_DEBUG("Session %s, node %d finished successfully", ~Id, node);
}

void TChunkWriter::FinishSessionComplete()
{
    FinishedEvent.Signal();
}

void TChunkWriter::PutBlocks(i32 node, TGroupPtr group)
{
    LOG_DEBUG("Session %s, group %d, node %d put request",
        ~Id, group->StartId, node);
    NRpc::TChannel::TPtr channel = ChannelCache.GetChannel(Nodes[node]->Address);
    TReqPutBlocks::TPtr req = TProxy(channel).PutBlocks();
    req->SetChunkId(ProtoGuidFromGuid(GetGuid(Id)));
    req->SetStartBlockIndex(group->StartId);
    req->SetStartOffset(group->Blocks.front()->Offset);

    for (unsigned i = 0; i < group->Blocks.size(); ++i)
        req->Attachments().push_back(group->Blocks[i]->Buffer);
    
    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter(~WriterThread);
    IAction::TPtr onSuccess = 
        FromMethod(&TChunkWriter::PutBlocksSuccess, TPtr(this), node, group);
    IParamAction<TRspPutBlocks::TPtr>::TPtr onResponse = 
        FromMethod(&TChunkWriter::CheckResponse<TRspPutBlocks>, TPtr(this), node, onSuccess);
    awaiter->Await(req->Invoke(), onResponse);
    awaiter->Complete(FromMethod(&TGroup::Process, group));
}

void TChunkWriter::PutBlocksSuccess(i32 node, TGroupPtr group)
{
    group->States[node] = TGroup::ENodeGroupState::InMem;
    LOG_DEBUG("Session %s, group %d, node %d put success",
        ~Id, group->StartId, node);
}

void TChunkWriter::SendBlocks(i32 node, i32 dst, TGroupPtr group)
{
    LOG_DEBUG("Session %s, group %d, node %d, send to %d request",
        ~Id, group->StartId, node, dst);
    NRpc::TChannel::TPtr channel = ChannelCache.GetChannel(Nodes[node]->Address);
    TProxy::TReqSendBlocks::TPtr req = TProxy(channel).SendBlocks();
    req->SetChunkId(ProtoGuidFromGuid(GetGuid(Id)));
    req->SetStartBlockIndex(group->StartId);
    req->SetEndBlockIndex(group->GetEndId());
    req->SetDestination(Nodes[dst]->Address);

    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter(~WriterThread);
    IAction::TPtr onSuccess = 
        FromMethod(&TChunkWriter::SendBlocksSuccess, TPtr(this), node, dst, group);
    IParamAction<TRspSendBlocks::TPtr>::TPtr onResponse = 
        FromMethod(&TChunkWriter::CheckResponse<TRspSendBlocks>, TPtr(this), node, onSuccess);
    awaiter->Await(req->Invoke(), onResponse);
    awaiter->Complete(FromMethod(&TGroup::Process, group));
}

void TChunkWriter::SendBlocksSuccess(i32 node, i32 dst, TGroupPtr group)
{
    group->States[dst] = TGroup::ENodeGroupState::InMem;
    LOG_DEBUG("Session %s, group %d, node %d, send to %d success",
        ~Id, group->StartId, node, dst);
}

TChunkWriter::TInvFlushBlocks::TPtr TChunkWriter::FlushBlocks(i32 node, TGroupPtr group)
{
    LOG_DEBUG("Session %s, group %d, node %d, flush request",
        ~Id, group->StartId, node);
    NRpc::TChannel::TPtr channel = ChannelCache.GetChannel(Nodes[node]->Address);
    TProxy::TReqFlushBlocks::TPtr req = TProxy(channel).FlushBlocks();
    req->SetChunkId(ProtoGuidFromGuid(GetGuid(Id)));
    req->SetStartBlockIndex(group->StartId);
    req->SetEndBlockIndex(group->GetEndId());
    return req->Invoke();
}

void TChunkWriter::FlushBlocksSuccess(i32 node, TGroupPtr group)
{
    group->States[node] = TGroup::ENodeGroupState::Flushed;
    LOG_DEBUG("Session %s, group %d, node %d, flush success",
        ~Id, group->StartId, node);
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
