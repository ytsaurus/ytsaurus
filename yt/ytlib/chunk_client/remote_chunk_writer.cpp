#include "remote_chunk_writer.h"
#include "chunk_holder.pb.h"

#include "../misc/serialize.h"
#include "../misc/metric.h"
#include "../misc/assert.h"
#include "../logging/log.h"
#include "../actions/action_util.h"
#include "../actions/parallel_awaiter.h"

#include <util/random/random.h>
#include <util/generic/yexception.h>
#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>
#include <util/stream/str.h>

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkWriter");

///////////////////////////////////////////////////////////////////////////////

struct TRemoteChunkWriter::TNode 
    : public TRefCountedBase
{
    bool IsAlive;
    const Stroka Address;
    TProxy Proxy;

    TNode(Stroka address, NRpc::TChannel::TPtr channel)
        : IsAlive(true)
        , Address(address)
        , Proxy(channel)
    { }
};

///////////////////////////////////////////////////////////////////////////////

class TRemoteChunkWriter::TGroup 
    : public TRefCountedBase
{
public:
    TGroup(
        int nodeCount, 
        int startBlockIndex, 
        TBlockOffset startOffset, 
        TRemoteChunkWriter::TPtr writer);

    void AddBlock(const TSharedRef& block);
    void Process();

    bool IsFlushed() const;
    i64 GetSize() const;
    int GetEndBlockIndex() const;
    int GetBlockCount() const;

private:
    DECLARE_ENUM(EGroupState,
        (Empty)
        (InMemory)
        (Flushed)
    );
    yvector<EGroupState> States;

    yvector<TSharedRef> Blocks;
    TBlockOffset StartOffset;
    int StartBlockIndex;

    i64 Size;

    TRemoteChunkWriter::TPtr Writer;

    void PutGroup();
    TInvPutBlocks::TPtr PutBlocks(int node);
    void OnPutBlocks(int node);

    void SendGroup(int srcNode);
    TInvSendBlocks::TPtr SendBlocks(int srcNode, int dstNode);
    void OnSentBlocks(int srcNode, int dstNode);

    void FlushGroup();
    TInvFlushBlock::TPtr FlushBlock(int node);
    void OnFlushedBlock(int node);
};

///////////////////////////////////////////////////////////////////////////////

TRemoteChunkWriter::TGroup::TGroup(
    int nodeCount, 
    int startBlockIndex, 
    TBlockOffset startOffset, 
    TRemoteChunkWriter::TPtr writer)
    : States(nodeCount, EGroupState::Empty)
    , StartOffset(startOffset)
    , StartBlockIndex(startBlockIndex)
    , Size(0)
    , Writer(writer)
{ }

void TRemoteChunkWriter::TGroup::AddBlock(const TSharedRef& block)
{
    Blocks.push_back(block);
    Size += block.Size();
}

int TRemoteChunkWriter::TGroup::GetEndBlockIndex() const
{
    return StartBlockIndex + Blocks.ysize() - 1;
}

i64 TRemoteChunkWriter::TGroup::GetSize() const
{
    return Size;
}

int TRemoteChunkWriter::TGroup::GetBlockCount() const
{
    return Blocks.ysize();
}

bool TRemoteChunkWriter::TGroup::IsFlushed() const
{
    int nodeCount = States.ysize();
    for (int node = 0; node < nodeCount; ++node) {
        if (Writer->Nodes[node]->IsAlive && 
            States[node] != EGroupState::Flushed) 
        {
            return false;
        }
    }
    return true;
}

void TRemoteChunkWriter::TGroup::PutGroup()
{
    int node = 0;
    while (!Writer->Nodes[node]->IsAlive) {
        ++node;
        YASSERT(node < Writer->Nodes.ysize());
    }

    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter(~Writer->WriterThread);
    IAction::TPtr onSuccess = FromMethod(
        &TGroup::OnPutBlocks, 
        TGroupPtr(this), 
        node);
    IParamAction<TRspPutBlocks::TPtr>::TPtr onResponse = FromMethod(
        &TRemoteChunkWriter::CheckResponse<TRspPutBlocks>, 
        Writer, 
        node, 
        onSuccess);
    awaiter->Await(PutBlocks(node), onResponse);
    awaiter->Complete(FromMethod(
        &TRemoteChunkWriter::TGroup::Process, 
        TGroupPtr(this)));
}

TRemoteChunkWriter::TInvPutBlocks::TPtr 
TRemoteChunkWriter::TGroup::PutBlocks(int node)
{
    LOG_DEBUG("Chunk %s, blocks %d-%d, node %s put request",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[node]->Address);

    TReqPutBlocks::TPtr req = Writer->Nodes[node]->Proxy.PutBlocks();
    req->SetChunkId(Writer->ChunkId.ToProto());
    req->SetStartBlockIndex(StartBlockIndex);
    req->SetStartOffset(StartOffset);

    for (int i = 0; i < Blocks.ysize(); ++i)
        req->Attachments().push_back(Blocks[i]);

    return req->Invoke(Writer->Config.RpcTimeout);
}

void TRemoteChunkWriter::TGroup::OnPutBlocks(int node)
{
    States[node] = EGroupState::InMemory;
    LOG_DEBUG("Chunk %s, blocks %d-%d, node %s put success",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[node]->Address);
}

void TRemoteChunkWriter::TGroup::SendGroup(int srcNode)
{
    int nodeCount = States.ysize();
    for (int node = 0; node < nodeCount; ++node) {
        if (Writer->Nodes[node]->IsAlive && States[node] == EGroupState::Empty) {
            TParallelAwaiter::TPtr awaiter = 
                new TParallelAwaiter(~TRemoteChunkWriter::WriterThread);
            IAction::TPtr onSuccess = FromMethod(
                &TGroup::OnSentBlocks, 
                TGroupPtr(this), 
                srcNode, 
                node);
            IParamAction<TRspSendBlocks::TPtr>::TPtr onResponse = FromMethod(
                &TRemoteChunkWriter::CheckResponse<TRspSendBlocks>, 
                Writer, 
                srcNode, 
                onSuccess);
            awaiter->Await(SendBlocks(srcNode, node), onResponse);
            awaiter->Complete(FromMethod(&TGroup::Process, TGroupPtr(this)));

            break;
        }
    }
}

TRemoteChunkWriter::TInvSendBlocks::TPtr 
TRemoteChunkWriter::TGroup::SendBlocks(int srcNode, int dstNode)
{
    LOG_DEBUG("Chunk %s, blocks %d-%d, node %s, send to %s request",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[srcNode]->Address,
        ~Writer->Nodes[dstNode]->Address);

    TProxy::TReqSendBlocks::TPtr req = Writer->Nodes[srcNode]->Proxy.SendBlocks();
    req->SetChunkId(Writer->ChunkId.ToProto());
    req->SetStartBlockIndex(StartBlockIndex);
    req->SetEndBlockIndex(GetEndBlockIndex());
    req->SetDestination(Writer->Nodes[dstNode]->Address);
    return req->Invoke(Writer->Config.RpcTimeout);
}

void TRemoteChunkWriter::TGroup::OnSentBlocks(int srcNode, int dstNode)
{
    States[dstNode] = TGroup::EGroupState::InMemory;
    LOG_DEBUG("Chunk %s, blocks %d-%d, node %s, send to %s success",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[srcNode]->Address,
        ~Writer->Nodes[dstNode]->Address);
}

void TRemoteChunkWriter::TGroup::FlushGroup()
{
    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter(~Writer->WriterThread);
    int nodeCount = States.ysize();
    for (int node = 0; node < nodeCount; ++node) {
        if (Writer->Nodes[node]->IsAlive && States[node] != EGroupState::Flushed) {
            IAction::TPtr onSuccess = FromMethod(
                &TGroup::OnFlushedBlock, 
                TGroupPtr(this), 
                node);
            IParamAction<TRspFlushBlock::TPtr>::TPtr onResponse = FromMethod(
                &TRemoteChunkWriter::CheckResponse<TRspFlushBlock>, 
                Writer, 
                node, 
                onSuccess);
            awaiter->Await(FlushBlock(node), onResponse);
        }
    }
    awaiter->Complete(FromMethod(
        &TRemoteChunkWriter::ShiftWindow, 
        Writer));    
}

TRemoteChunkWriter::TInvFlushBlock::TPtr 
TRemoteChunkWriter::TGroup::FlushBlock(int node)
{
    LOG_DEBUG("ChunkId %s, blocks %d-%d, node %s flush request",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[node]->Address);

    TProxy::TReqFlushBlock::TPtr req = Writer->Nodes[node]->Proxy.FlushBlock();
    req->SetChunkId(Writer->ChunkId.ToProto());
    req->SetBlockIndex(GetEndBlockIndex());
    return req->Invoke(Writer->Config.RpcTimeout);
}

void TRemoteChunkWriter::TGroup::OnFlushedBlock(int node)
{
    States[node] = EGroupState::Flushed;
    LOG_DEBUG("ChunkId %s, blocks %d-%d, node %s flush success",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[node]->Address);
}

void TRemoteChunkWriter::TGroup::Process()
{
    LOG_DEBUG("Chunk %s, processing blocks %d-%d",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex());

    int nodeWithBlocks = -1;
    bool emptyNodeExists = false;

    int nodeCount = States.ysize();
    for (int node = 0; node < nodeCount; ++node) {
        if (Writer->Nodes[node]->IsAlive) {
            switch (States[node]) {
                case EGroupState::InMemory:
                    nodeWithBlocks = node;
                    break;

                case EGroupState::Empty:
                    emptyNodeExists = true;
                    break;

                case EGroupState::Flushed:
                    //Nothing to do here
                    break;

                default:
                    YASSERT(false);
                    break;
            }
        }
    }

    if (!emptyNodeExists) {
        FlushGroup();
    } else if (nodeWithBlocks < 0) {
        PutGroup();
    } else {
        SendGroup(nodeWithBlocks);
    }
}

///////////////////////////////////////////////////////////////////////////////

TLazyPtr<TActionQueue> TRemoteChunkWriter::WriterThread; 

TRemoteChunkWriter::TRemoteChunkWriter(
    const TRemoteChunkWriter::TConfig& config, 
    const TChunkId& chunkId,
    const yvector<Stroka>& nodes)
    : ChunkId(chunkId) 
    , Config(config)
    , State(EWriterState::Initializing)
    , IsFinishRequested(false)
    , IsFinished(new TAsyncResult<TVoid>())
    , WindowSlots(config.WindowSize)
    , AliveNodes(nodes.ysize())
    , CurrentGroup(new TGroup(AliveNodes, 0, 0, this))
    , BlockCount(0)
    , BlockOffset(0)
{
    LOG_DEBUG("Start writing chunk %s", ~ChunkId.ToString());
    YVERIFY(AliveNodes > 0);

    NRpc::TChannelCache channelCache;
    yvector<Stroka>::const_iterator it = nodes.begin();
    for (; it != nodes.end(); ++it) {
        Nodes.push_back(new TNode(*it, channelCache.GetChannel(*it)));
    }

    StartSession();
}

TRemoteChunkWriter::~TRemoteChunkWriter()
{
    YASSERT((IsFinishRequested && Window.empty()) || State == EWriterState::Failed);
}

void TRemoteChunkWriter::ShiftWindow()
{
    while (!Window.empty()) {
        TGroupPtr group = Window.front();
        if (!group->IsFlushed())
            return;

        LOG_DEBUG("Chunk %s, blocks up to %d shifted out from window",
            ~ChunkId.ToString(), 
            group->GetEndBlockIndex());

        for (int i = 0; i < group->GetBlockCount(); ++i)
            YVERIFY(WindowSlots.Release());

        Window.pop_front();
    }

    if (IsFinishRequested) {
        FinishSession();
    }
}

void TRemoteChunkWriter::RequestFinalization()
{
    LOG_DEBUG("Chunk %s, finish requested", ~ChunkId.ToString());
    IsFinishRequested = true;
    if (Window.empty())
        FinishSession();
}

void TRemoteChunkWriter::AddGroup(TGroupPtr group)
{
    YASSERT(!IsFinishRequested);

    if (State == EWriterState::Failed) {
        // Release client thread if it is blocked inside AddBlock.
        for (int i = 0; i < group->GetBlockCount(); ++i) {
            WindowSlots.Release();
        }
    } else {
        LOG_DEBUG("Chunk %s, added blocks up to %d", 
            ~ChunkId.ToString(), 
            group->GetEndBlockIndex());

        Window.push_back(group);
        if (State != EWriterState::Initializing)
            group->Process();
    }
}

void TRemoteChunkWriter::OnNodeDied(int node)
{
    if (Nodes[node]->IsAlive) {
        Nodes[node]->IsAlive = false;
        --AliveNodes;

        LOG_INFO("Chunk %s, node %s died. %d alive nodes left", 
            ~ChunkId.ToString(), 
            ~Nodes[node]->Address,
            AliveNodes);

        if (State != EWriterState::Failed && AliveNodes == 0) {
            State = EWriterState::Failed;
            IsFinished->Set(TVoid());
            LOG_WARNING("Chunk %s writing failed", ~ChunkId.ToString());
            // Release client thread if it is blocked inside AddBlock.
            WindowSlots.Release();
        }
    }
}

template<class TResponse>
void TRemoteChunkWriter::CheckResponse(typename TResponse::TPtr rsp, int node, IAction::TPtr onSuccess)
{
    if (rsp->IsOK()) {
        onSuccess->Do();
    } else if (rsp->IsServiceError()) {
        // For now assume it means errors in client logic.
        // ToDo: proper error handling, e.g lease expiration.
        LOG_FATAL("Chunk %s, node %s returned soft error %s", 
            ~ChunkId.ToString(),
            ~Nodes[node]->Address, 
            ~rsp->GetErrorCode().ToString());
    } else {
        // Node probably died or overloaded.
        // ToDo: consider more detailed error handling for timeouts.
        LOG_WARNING("Chunk %s, node %s returned rpc error %s", 
            ~ChunkId.ToString(),
            ~Nodes[node]->Address, 
            ~rsp->GetErrorCode().ToString());
        OnNodeDied(node);
    }
}

void TRemoteChunkWriter::StartSession()
{
    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter(~WriterThread);
    for (int node = 0; node < Nodes.ysize(); ++node) {
        IAction::TPtr onSuccess = FromMethod(
            &TRemoteChunkWriter::OnStartedChunk, 
            TPtr(this), 
            node);
        IParamAction<TRspStartChunk::TPtr>::TPtr onResponse = FromMethod(
            &TRemoteChunkWriter::CheckResponse<TRspStartChunk>, 
            TPtr(this), 
            node, 
            onSuccess);
        awaiter->Await(StartChunk(node), onResponse);
    }
    awaiter->Complete(FromMethod(&TRemoteChunkWriter::OnStartedSession, TPtr(this)));
}

TRemoteChunkWriter::TInvStartChunk::TPtr TRemoteChunkWriter::StartChunk(int node)
{
    LOG_DEBUG("Chunk %s, node %s start request", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);

    TProxy::TReqStartChunk::TPtr req = Nodes[node]->Proxy.StartChunk();
    req->SetChunkId(ChunkId.ToProto());
    req->SetWindowSize(Config.WindowSize);
    return req->Invoke(Config.RpcTimeout);
}

void TRemoteChunkWriter::OnStartedChunk(int node)
{
    LOG_DEBUG("Chunk %s, node %s started successfully", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);
}

void TRemoteChunkWriter::OnStartedSession()
{
    if (State == EWriterState::Initializing) {
        State = EWriterState::Writing;
        TWindow::iterator it;
        for (it = Window.begin(); it != Window.end(); ++it) {
            TGroupPtr group = *it;
            group->Process();
        }
    }
}

void TRemoteChunkWriter::FinishSession()
{
    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter(~WriterThread);
    for (int node = 0; node < Nodes.ysize(); ++node) {
        if (Nodes[node]->IsAlive) {
            IAction::TPtr onSuccess = FromMethod(
                &TRemoteChunkWriter::OnFinishedChunk, 
                TPtr(this), 
                node);
            IParamAction<TRspFinishChunk::TPtr>::TPtr onResponse = FromMethod(
                &TRemoteChunkWriter::CheckResponse<TRspFinishChunk>, 
                TPtr(this), 
                node, 
                onSuccess);
            awaiter->Await(FinishChunk(node), onResponse);
        }
    }
    awaiter->Complete(FromMethod(&TRemoteChunkWriter::OnFinishedSession, TPtr(this)));
    LOG_DEBUG("Chunk %s finished writing ", ~ChunkId.ToString());
}

TRemoteChunkWriter::TInvFinishChunk::TPtr TRemoteChunkWriter::FinishChunk(int node)
{
    TReqFinishChunk::TPtr req = Nodes[node]->Proxy.FinishChunk();
    req->SetChunkId(ChunkId.ToProto());
    LOG_DEBUG("Chunk %s, node %s finish request", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);
    return req->Invoke(Config.RpcTimeout);
}

void TRemoteChunkWriter::OnFinishedChunk(int node)
{
    LOG_DEBUG("Chunk %s, node %s finished successfully", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);
}

void TRemoteChunkWriter::OnFinishedSession()
{
    IsFinished->Set(TVoid());
}

void TRemoteChunkWriter::CheckStateAndThrow()
{
    // ToDo: more info in exception.
    if (State == EWriterState::Failed)
        ythrow yexception() << "Chunk write session failed!";
}

void TRemoteChunkWriter::AddBlock(const TSharedRef& data)
{
    CheckStateAndThrow();

    WindowSlots.Acquire();

    LOG_DEBUG("Chunk %s, client adds new block", ~ChunkId.ToString());

    CurrentGroup->AddBlock(data);
    BlockOffset += data.Size();
    ++BlockCount;

    if (CurrentGroup->GetSize() >= Config.GroupSize) {
        WriterThread->Invoke(FromMethod(
            &TRemoteChunkWriter::AddGroup, 
            TPtr(this), 
            CurrentGroup));
        TGroupPtr group = new TGroup(Nodes.ysize(), BlockCount, BlockOffset, this);
        CurrentGroup.Swap(group);
    }
}

void TRemoteChunkWriter::Close()
{
    LOG_DEBUG("Chunk %s, client thread closing writer", ~ChunkId.ToString());

    if (CurrentGroup->GetSize() != 0) {
        WriterThread->Invoke(FromMethod(
            &TRemoteChunkWriter::AddGroup, 
            TPtr(this), 
            CurrentGroup));
    }

    // Set IsFinishRequested via queue to ensure that the flag will be set
    // after all block appends.
    WriterThread->Invoke(FromMethod(
        &TRemoteChunkWriter::RequestFinalization, 
        TPtr(this)));
    IsFinished->Get();

    CheckStateAndThrow();
    LOG_DEBUG("Chunk %s, client thread complete.", ~ChunkId.ToString());
}

Stroka TRemoteChunkWriter::GetDebugInfo()
{
    TStringStream ss;
    // ToDo: implement measures
    
 /*   ss << "PutBlocks: mean " << TPutBlocksCall::TimeStat.GetMean() << "ms, std " << 
        TPutBlocksCall::TimeStat.GetStd() << "ms, calls " << TPutBlocksCall::TimeStat.GetNum() << Endl;
    ss << "SendBlocks: mean " << TSendBlocksCall::TimeStat.GetMean() << "ms, std " << 
        TSendBlocksCall::TimeStat.GetStd() << "ms, calls " << TSendBlocksCall::TimeStat.GetNum() << Endl;
    ss << "FlushBlocks: mean " << TFlushBlocksCall::TimeStat.GetMean() << "ms, std " << 
        TFlushBlocksCall::TimeStat.GetStd() << "ms, calls " << TFlushBlocksCall::TimeStat.GetNum() << Endl;
    ss << "StartChunk: mean " << TStartChunkCall::TimeStat.GetMean() << "ms, std " << 
        TStartChunkCall::TimeStat.GetStd() << "ms, calls " << TStartChunkCall::TimeStat.GetNum() << Endl;
    ss << "FinishChunk: mean " << TFinishChunkCall::TimeStat.GetMean() << "ms, std " << 
        TFinishChunkCall::TimeStat.GetStd() << "ms, calls " << TFinishChunkCall::TimeStat.GetNum() << Endl;
*/
    return ss;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

