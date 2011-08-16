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

    TNode(Stroka address, NRpc::IChannel::TPtr channel)
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
        TRemoteChunkWriter::TPtr writer);

    void AddBlock(const TSharedRef& block);
    void Process();

    bool IsWritten() const;
    i64 GetSize() const;
    int GetEndBlockIndex() const;
    int GetBlockCount() const;

    bool IsFlushing;

private:
    yvector<bool> IsSent;

    yvector<TSharedRef> Blocks;
    int StartBlockIndex;

    i64 Size;

    TRemoteChunkWriter::TPtr Writer;

    void PutGroup();
    TInvPutBlocks::TPtr PutBlocks(int node);
    void OnPutBlocks(int node);

    void SendGroup(int srcNode);
    TInvSendBlocks::TPtr SendBlocks(int srcNode, int dstNode);
    void OnSentBlocks(int srcNode, int dstNode);
};

///////////////////////////////////////////////////////////////////////////////

TRemoteChunkWriter::TGroup::TGroup(
    int nodeCount, 
    int startBlockIndex, 
    TRemoteChunkWriter::TPtr writer)
    : IsFlushing(false)
    , IsSent(nodeCount, false)
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

bool TRemoteChunkWriter::TGroup::IsWritten() const
{
    for (int node = 0; node < IsSent.ysize(); ++node) {
        if (Writer->Nodes[node]->IsAlive && !IsSent[node]) {
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

    for (int i = 0; i < Blocks.ysize(); ++i) {
        req->Attachments().push_back(Blocks[i]);
    }

    return req->Invoke(Writer->Config.RpcTimeout);
}

void TRemoteChunkWriter::TGroup::OnPutBlocks(int node)
{
    IsSent[node] = true;
    LOG_DEBUG("Chunk %s, blocks %d-%d, node %s put success",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[node]->Address);
}

void TRemoteChunkWriter::TGroup::SendGroup(int srcNode)
{
    int nodeCount = IsSent.ysize();
    for (int node = 0; node < nodeCount; ++node) {
        if (Writer->Nodes[node]->IsAlive && !IsSent[node]) {
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
    req->SetBlockCount(Blocks.ysize());
    req->SetAddress(Writer->Nodes[dstNode]->Address);
    return req->Invoke(Writer->Config.RpcTimeout);
}

void TRemoteChunkWriter::TGroup::OnSentBlocks(int srcNode, int dstNode)
{
    IsSent[dstNode] = true;
    LOG_DEBUG("Chunk %s, blocks %d-%d, node %s, send to %s success",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[srcNode]->Address,
        ~Writer->Nodes[dstNode]->Address);
}

void TRemoteChunkWriter::TGroup::Process()
{
    LOG_DEBUG("Chunk %s, processing blocks %d-%d",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex());

    int nodeWithBlocks = -1;
    bool emptyNodeExists = false;

    int nodeCount = IsSent.ysize();
    for (int node = 0; node < nodeCount; ++node) {
        if (Writer->Nodes[node]->IsAlive) {
            if (IsSent[node]) {
                nodeWithBlocks = node;
            } else {
                emptyNodeExists = true;
            }
        }
    }

    if (!emptyNodeExists) {
        Writer->ShiftWindow();
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
    , IsFinished(new TAsyncResult<EResult>())
    , WindowSlots(config.WindowSize)
    , AliveNodes(nodes.ysize())
    , CurrentGroup(new TGroup(AliveNodes, 0, this))
    , BlockCount(0)
    , WindowReady(NULL)
{
    LOG_DEBUG("Start writing chunk %s", ~ChunkId.ToString());
    YVERIFY(AliveNodes > 0);

    NRpc::TChannelCache channelCache;
    yvector<Stroka>::const_iterator it = nodes.begin();
    for (; it != nodes.end(); ++it) {
        Nodes.push_back(new TNode(*it, ~channelCache.GetChannel(*it)));
    }

    StartSession();
}

TRemoteChunkWriter::~TRemoteChunkWriter()
{
    YASSERT((IsFinishRequested && Window.empty()) || State == EWriterState::Terminated);
}

void TRemoteChunkWriter::ShiftWindow()
{ 
    YASSERT(!Window.empty());

    int lastFlushableBlock = -1;
    for (TWindow::const_iterator it = Window.begin(); it != Window.end(); ++it) {
        TGroupPtr group = *it;
        if (!group->IsFlushing) {
            if (group->IsWritten()) {
                lastFlushableBlock = group->GetEndBlockIndex();
                group->IsFlushing = true;
            } else {
                break;
            }
        }
    }

    if (lastFlushableBlock < 0) {
        return;
    }

    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter(~WriterThread);
    for (int node = 0; node < Nodes.ysize(); ++node) {
        if (Nodes[node]->IsAlive) {
            IAction::TPtr onSuccess = FromMethod(
                &TRemoteChunkWriter::OnFlushedBlock, 
                TPtr(this), 
                node,
                lastFlushableBlock);
            IParamAction<TRspFlushBlock::TPtr>::TPtr onResponse = FromMethod(
                &TRemoteChunkWriter::CheckResponse<TRspFlushBlock>, 
                TPtr(this), 
                node, 
                onSuccess);
            awaiter->Await(FlushBlock(node, lastFlushableBlock), onResponse);
        }
    }
    awaiter->Complete(FromMethod(
        &TRemoteChunkWriter::OnShiftedWindow, 
        TPtr(this),
        lastFlushableBlock));
}

TRemoteChunkWriter::TInvFlushBlock::TPtr 
TRemoteChunkWriter::FlushBlock(int node, int blockIndex)
{
    LOG_DEBUG("ChunkId %s, blocks up to %d, node %s flush request",
        ~ChunkId.ToString(), 
        blockIndex,
        ~Nodes[node]->Address);

    TProxy::TReqFlushBlock::TPtr req = Nodes[node]->Proxy.FlushBlock();
    req->SetChunkId(ChunkId.ToProto());
    req->SetBlockIndex(blockIndex);
    return req->Invoke(Config.RpcTimeout);
}

void TRemoteChunkWriter::OnFlushedBlock(int node, int blockIndex)
{
    LOG_DEBUG("ChunkId %s, blocks up to %d, node %s flush success",
        ~ChunkId.ToString(), 
        blockIndex,
        ~Nodes[node]->Address);
}

void TRemoteChunkWriter::OnShiftedWindow(int lastFlushedBlock)
{
    while (!Window.empty()) {
        TGroupPtr group = Window.front();
        if (group->GetEndBlockIndex() > lastFlushedBlock)
            return;

        LOG_DEBUG("Chunk %s, blocks up to %d shifted out from window",
            ~ChunkId.ToString(), 
            group->GetEndBlockIndex());

        ReleaseSlots(group->GetBlockCount());

        Window.pop_front();
    }

    if (IsFinishRequested) {
        FinishSession();
    }
}

void TRemoteChunkWriter::ReleaseSlots(int count)
{
    for (int i = 0; i < count; ++i) {
        YVERIFY(WindowSlots.Release());
    }

    if (WindowReady != NULL) {
        TAsyncResult<TVoid>::TPtr* ready = WindowReady;
        WindowReady = NULL;
        (*ready)->Set(TVoid());
    }
}

void TRemoteChunkWriter::RequestFinalization()
{
    LOG_DEBUG("Chunk %s, finish requested", ~ChunkId.ToString());
    IsFinishRequested = true;
    if (Window.empty())
        FinishSession();
}

void TRemoteChunkWriter::Terminate()
{
    LOG_DEBUG("Chunk %s, writer terminated by client", ~ChunkId.ToString());
    State = EWriterState::Terminated;
}

void TRemoteChunkWriter::AddGroup(TGroupPtr group)
{
    YASSERT(!IsFinishRequested);

    if (State == EWriterState::Terminated) {
        // Release client thread if it is blocked inside AddBlock.
        ReleaseSlots(group->GetBlockCount());
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

        if (State != EWriterState::Terminated && AliveNodes == 0) {
            State = EWriterState::Terminated;
            IsFinished->Set(EResult::Failed);
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
    IsFinished->Set(EResult::OK);
}

void TRemoteChunkWriter::RegisterReadyEvent(TAsyncResult<TVoid>::TPtr* windowReady)
{
    YASSERT(WindowReady == NULL);
    if (WindowSlots.GetCount() > 0) {
        (*windowReady)->Set(TVoid());
    } else {
        WindowReady = windowReady;
    }
}

IChunkWriter::EResult TRemoteChunkWriter::AsyncAddBlock(const TSharedRef& data, TAsyncResult<TVoid>::TPtr* ready)
{
    if (State == EWriterState::Terminated) {
        return EResult::Failed;
    }

    if (WindowSlots.TryAcquire()) {
        LOG_DEBUG("Chunk %s, client adds new block", ~ChunkId.ToString());
        CurrentGroup->AddBlock(data);
        ++BlockCount;

        if (CurrentGroup->GetSize() >= Config.GroupSize) {
            WriterThread->Invoke(FromMethod(
                &TRemoteChunkWriter::AddGroup, 
                TPtr(this),
                CurrentGroup));
            TGroupPtr group = new TGroup(Nodes.ysize(), BlockCount, this);
            CurrentGroup.Swap(group);
        }

        return EResult::OK;
    } else {
        LOG_DEBUG("Chunk %s, window is full", ~ChunkId.ToString());
        WriterThread->Invoke(FromMethod(
            &TRemoteChunkWriter::RegisterReadyEvent,
            TPtr(this),
            ready));
        return EResult::TryLater;
    }
}

void TRemoteChunkWriter::AddBlock(const TSharedRef& data)
{
    while (true) {
        TAsyncResult<TVoid>::TPtr ready = new TAsyncResult<TVoid>();
        EResult result = AsyncAddBlock(data, &ready);

        switch (result) {
        case EResult::OK:
            return;

        case EResult::Failed:
            ythrow yexception() << 
                Sprintf("Chunk %s write session terminated!", ~ChunkId.ToString());

        case EResult::TryLater:
            ready->Get();
            break;

        default:
            YASSERT(false);
        }
    }
}

TAsyncResult<IChunkWriter::EResult>::TPtr TRemoteChunkWriter::AsyncClose()
{
    LOG_DEBUG("Chunk %s, client thread closing writer", ~ChunkId.ToString());

    if (CurrentGroup->GetSize() != 0) {
        WriterThread->Invoke(FromMethod(
            &TRemoteChunkWriter::AddGroup,
            TPtr(this), 
            CurrentGroup));
    }

    // Remove cyclic reference
    CurrentGroup.Drop();

    // Set IsFinishRequested via queue to ensure that the flag will be set
    // after all block appends.
    WriterThread->Invoke(FromMethod(
        &TRemoteChunkWriter::RequestFinalization, 
        TPtr(this)));

    return IsFinished;
}

void TRemoteChunkWriter::Close()
{
    switch (AsyncClose()->Get()) {
    case EResult::OK:
        LOG_DEBUG("Chunk %s, client thread complete.", ~ChunkId.ToString());
        return;

    case EResult::Failed:
        ythrow yexception() << 
            Sprintf("Chunk %s write session terminated!", ~ChunkId.ToString());

    default:
        YASSERT(false);
    }
}

void TRemoteChunkWriter::Cancel()
{
    LOG_DEBUG("Chunk %s, client cancels writing.", ~ChunkId.ToString());
    // Remove cyclic reference
    CurrentGroup.Drop();
    WriterThread->Invoke(FromMethod(
        &TRemoteChunkWriter::Terminate, 
        TPtr(this)));
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

