#include "remote_chunk_writer.h"
#include "chunk_holder.pb.h"

#include "../misc/serialize.h"
#include "../misc/metric.h"
#include "../misc/assert.h"
#include "../misc/string.h"
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
    int GetStartBlockIndex() const;
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

int TRemoteChunkWriter::TGroup::GetStartBlockIndex() const
{
    return StartBlockIndex;
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

    auto awaiter = New<TParallelAwaiter>(~Writer->WriterThread);
    auto onSuccess = FromMethod(
        &TGroup::OnPutBlocks, 
        TGroupPtr(this), 
        node);
    auto onResponse = FromMethod(
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
    auto req = Writer->Nodes[node]->Proxy.PutBlocks();
    req->SetChunkId(Writer->ChunkId.ToProto());
    req->SetStartBlockIndex(StartBlockIndex);

    for (int i = 0; i < Blocks.ysize(); ++i) {
        req->Attachments().push_back(Blocks[i]);
    }

    LOG_DEBUG("Putting blocks (ChunkId: %s, Blocks: %d-%d, Address: %s)",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[node]->Address);

    return req->Invoke(Writer->Config.RpcTimeout);
}

void TRemoteChunkWriter::TGroup::OnPutBlocks(int node)
{
    IsSent[node] = true;

    LOG_DEBUG("Blocks are put (ChunkId: %s, Blocks, %d-%d, Address: %s)",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[node]->Address);
}

void TRemoteChunkWriter::TGroup::SendGroup(int srcNode)
{
    for (int node = 0; node < IsSent.ysize(); ++node) {
        if (Writer->Nodes[node]->IsAlive && !IsSent[node]) {
            auto awaiter = New<TParallelAwaiter>(~TRemoteChunkWriter::WriterThread);
            auto onSuccess = FromMethod(
                &TGroup::OnSentBlocks, 
                TGroupPtr(this), 
                srcNode, 
                node);
            auto onResponse = FromMethod(
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
    LOG_DEBUG("Sending blocks (ChunkId: %s, Blocks: %d-%d, SrcAddress: %s, DstAddress: %s)",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[srcNode]->Address,
        ~Writer->Nodes[dstNode]->Address);

    auto req = Writer->Nodes[srcNode]->Proxy.SendBlocks();
    req->SetChunkId(Writer->ChunkId.ToProto());
    req->SetStartBlockIndex(StartBlockIndex);
    req->SetBlockCount(Blocks.ysize());
    req->SetAddress(Writer->Nodes[dstNode]->Address);
    return req->Invoke(Writer->Config.RpcTimeout);
}

void TRemoteChunkWriter::TGroup::OnSentBlocks(int srcNode, int dstNode)
{
    LOG_DEBUG("Blocks are sent (ChunkId: %s, Blocks: %d-%d, SrcAddress: %s, DstAddress: %s)",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[srcNode]->Address,
        ~Writer->Nodes[dstNode]->Address);

    IsSent[dstNode] = true;
}

void TRemoteChunkWriter::TGroup::Process()
{
    LOG_DEBUG("Processing group (ChunkId: %s, Blocks: %d-%d)",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex());

    int nodeWithBlocks = -1;
    bool emptyNodeFound = false;
    for (int node = 0; node < IsSent.ysize(); ++node) {
        if (Writer->Nodes[node]->IsAlive) {
            if (IsSent[node]) {
                nodeWithBlocks = node;
            } else {
                emptyNodeFound = true;
            }
        }
    }

    if (!emptyNodeFound || Writer->State == EWriterState::Canceled) {
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
    const yvector<Stroka>& addresses)
    : ChunkId(chunkId) 
    , Config(config)
    , State(EWriterState::Initializing)
    , IsCloseRequested(false)
    , Result(New< TAsyncResult<EResult> >())
    , WindowSlots(config.WindowSize)
    , AliveNodeCount(addresses.ysize())
    , CurrentGroup(New<TGroup>(AliveNodeCount, 0, this))
    , BlockCount(0)
    , WindowReady(NULL)
{
    YASSERT(AliveNodeCount > 0);

    LOG_DEBUG("Writer created (ChunkId: %s, Addresses: [%s])",
        ~ChunkId.ToString(),
        ~JoinToString(addresses));

    InitializeNodes(addresses);

    StartSession();
}

void TRemoteChunkWriter::InitializeNodes(const yvector<Stroka>& addresses)
{
    for (auto it = addresses.begin(); it != addresses.end(); ++it) {
        Nodes.push_back(New<TNode>(*it, ~ChannelCache.GetChannel(*it)));
    }
}

TRemoteChunkWriter::~TRemoteChunkWriter()
{
    YASSERT(
        State == EWriterState::Closed && Window.empty() ||
        State == EWriterState::Canceled);
}

void TRemoteChunkWriter::ShiftWindow()
{
    int lastFlushableBlock = -1;
    for (auto it = Window.begin(); it != Window.end(); ++it) {
        auto group = *it;
        if (!group->IsFlushing) {
            if (group->IsWritten() || State == EWriterState::Canceled) {
                lastFlushableBlock = group->GetEndBlockIndex();
                group->IsFlushing = true;
            } else {
                break;
            }
        }
    }

    if (lastFlushableBlock < 0)
        return;

    auto awaiter = New<TParallelAwaiter>(~WriterThread);
    for (int node = 0; node < Nodes.ysize(); ++node) {
        if (Nodes[node]->IsAlive && State != EWriterState::Canceled) {
            auto onSuccess = FromMethod(
                &TRemoteChunkWriter::OnFlushedBlock, 
                TPtr(this), 
                node,
                lastFlushableBlock);
            auto onResponse = FromMethod(
                &TRemoteChunkWriter::CheckResponse<TRspFlushBlock>, 
                TPtr(this), 
                node, 
                onSuccess);
            awaiter->Await(FlushBlock(node, lastFlushableBlock), onResponse);
        }
    }

    awaiter->Complete(FromMethod(
        &TRemoteChunkWriter::OnWindowShifted, 
        TPtr(this),
        lastFlushableBlock));
}

TRemoteChunkWriter::TInvFlushBlock::TPtr 
TRemoteChunkWriter::FlushBlock(int node, int blockIndex)
{
    LOG_DEBUG("Flushing blocks (ChunkId: %s, BlockIndex: %d, Address: %s)",
        ~ChunkId.ToString(), 
        blockIndex,
        ~Nodes[node]->Address);

    auto req = Nodes[node]->Proxy.FlushBlock();
    req->SetChunkId(ChunkId.ToProto());
    req->SetBlockIndex(blockIndex);
    return req->Invoke(Config.RpcTimeout);
}

void TRemoteChunkWriter::OnFlushedBlock(int node, int blockIndex)
{
    LOG_DEBUG("Blocks flushed (ChunkId: %s, BlockIndex: %d, Address: %s)",
        ~ChunkId.ToString(), 
        blockIndex,
        ~Nodes[node]->Address);
}

void TRemoteChunkWriter::OnWindowShifted(int lastFlushedBlock)
{
    while (!Window.empty()) {
        auto group = Window.front();
        if (group->GetEndBlockIndex() > lastFlushedBlock)
            return;

        LOG_DEBUG("Window shifted (ChunkId: %s, BlockIndex: %d)",
            ~ChunkId.ToString(), 
            group->GetEndBlockIndex());

        ReleaseSlots(group->GetBlockCount());

        Window.pop_front();
    }

    if (State == EWriterState::Writing && IsCloseRequested) {
        CloseSession();
    }
}

void TRemoteChunkWriter::ReleaseSlots(int count)
{
    LOG_DEBUG("Window slots released (ChunkId: %s, SlotCount: %d)",
        ~ChunkId.ToString(), 
        count);

    for (int i = 0; i < count; ++i) {
        YVERIFY(WindowSlots.Release());
    }

    if (~WindowReady != NULL) {
        WindowReady->Set(TVoid());
        WindowReady.Drop();
    }
}

void TRemoteChunkWriter::DoClose()
{
    if (IsCloseRequested)
        return;

    if (State == EWriterState::Closed ||
        State == EWriterState::Canceled)
        return;

    LOG_DEBUG("Writer close requested (ChunkId: %s)",
        ~ChunkId.ToString());

    IsCloseRequested = true;
    if (Window.empty()) {
        CloseSession();
    }
}

void TRemoteChunkWriter::DoCancel()
{
    if (State == EWriterState::Closed ||
        State == EWriterState::Canceled)
        return;

    Result->Set(EResult::Failed);
    State = EWriterState::Canceled;

    LOG_DEBUG("Writer canceled (ChunkId: %s)",
        ~ChunkId.ToString());
}

void TRemoteChunkWriter::AddGroup(TGroupPtr group)
{
    YASSERT(!IsCloseRequested);

    if (State == EWriterState::Canceled) {
        // TODO: fix comment
        // Release client thread if it is blocked inside AddBlock.
        ReleaseSlots(group->GetBlockCount());
        return;
    } 

    LOG_DEBUG("Group added (ChunkId: %s, Blocks: %d-%d)", 
        ~ChunkId.ToString(), 
        group->GetStartBlockIndex(),
        group->GetEndBlockIndex());

    Window.push_back(group);

    if (State != EWriterState::Initializing) {
        group->Process();
    }
}

void TRemoteChunkWriter::OnNodeDied(int node)
{
    if (!Nodes[node]->IsAlive)
        return;

    Nodes[node]->IsAlive = false;
    --AliveNodeCount;

    LOG_INFO("Node died (ChunkId: %s, Address: %s, AliveNodeCount: %d)", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address,
        AliveNodeCount);

    if (State != EWriterState::Canceled && AliveNodeCount == 0) {
        State = EWriterState::Canceled;
        Result->Set(EResult::Failed);
        LOG_WARNING("No alive nodes left, chunk writing failed (ChunkId: %s)",
            ~ChunkId.ToString());
    }
}

template<class TResponse>
void TRemoteChunkWriter::CheckResponse(
    typename TResponse::TPtr rsp,
    int node,
    IAction::TPtr onSuccess)
{
    if (rsp->IsOK()) {
        onSuccess->Do();
        return;
    } 

    // TODO: retry?
    LOG_ERROR("Error reported by node (ChunkId: %s, Address: %s, ErrorCode: %s)", 
        ~ChunkId.ToString(),
        ~Nodes[node]->Address, 
        ~rsp->GetErrorCode().ToString());

    OnNodeDied(node);
}

void TRemoteChunkWriter::StartSession()
{
    auto awaiter = New<TParallelAwaiter>(~WriterThread);
    for (int node = 0; node < Nodes.ysize(); ++node) {
        auto onSuccess = FromMethod(
            &TRemoteChunkWriter::OnStartedChunk, 
            TPtr(this), 
            node);
        auto onResponse = FromMethod(
            &TRemoteChunkWriter::CheckResponse<TRspStartChunk>, 
            TPtr(this), 
            node, 
            onSuccess);
        awaiter->Await(StartChunk(node), onResponse);
    }
    awaiter->Complete(FromMethod(&TRemoteChunkWriter::OnSessionStarted, TPtr(this)));
}

TRemoteChunkWriter::TInvStartChunk::TPtr TRemoteChunkWriter::StartChunk(int node)
{
    LOG_DEBUG("Starting chunk (ChunkId: %s, Address: %s)", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);

    auto req = Nodes[node]->Proxy.StartChunk();
    req->SetChunkId(ChunkId.ToProto());
    req->SetWindowSize(Config.WindowSize);
    return req->Invoke(Config.RpcTimeout);
}

void TRemoteChunkWriter::OnStartedChunk(int node)
{
    LOG_DEBUG("Chunk started (ChunkId: %s, Address: %s)", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);
}

void TRemoteChunkWriter::OnSessionStarted()
{
    // Check if the session is not canceled yet.
    if (State == EWriterState::Canceled)
        return;

    LOG_DEBUG("Writer ready (ChunkId: %s)", 
        ~ChunkId.ToString());

    State = EWriterState::Writing;
    for (auto it = Window.begin(); it != Window.end(); ++it) {
        (*it)->Process();
    }

    // Possible for an empty chunk.
    if (Window.empty() && IsCloseRequested) {
        CloseSession();
    }
}

void TRemoteChunkWriter::CloseSession()
{
    YASSERT(IsCloseRequested);
    YASSERT(State == EWriterState::Writing);

    LOG_DEBUG("Closing writer (ChunkId: %s)",
        ~ChunkId.ToString());

    auto awaiter = New<TParallelAwaiter>(~WriterThread);
    for (int node = 0; node < Nodes.ysize(); ++node) {
        if (Nodes[node]->IsAlive) {
            auto onSuccess = FromMethod(
                &TRemoteChunkWriter::OnFinishedChunk, 
                TPtr(this), 
                node);
            auto onResponse = FromMethod(
                &TRemoteChunkWriter::CheckResponse<TRspFinishChunk>, 
                TPtr(this), 
                node, 
                onSuccess);
            awaiter->Await(FinishChunk(node), onResponse);
        }
    }
    awaiter->Complete(FromMethod(&TRemoteChunkWriter::OnFinishedSession, TPtr(this)));
}

TRemoteChunkWriter::TInvFinishChunk::TPtr TRemoteChunkWriter::FinishChunk(int node)
{
    LOG_DEBUG("Finishing chunk (ChunkId: %s, Address: %s)",
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);
    auto req = Nodes[node]->Proxy.FinishChunk();
    req->SetChunkId(ChunkId.ToProto());
    return req->Invoke(Config.RpcTimeout);
}

void TRemoteChunkWriter::OnFinishedChunk(int node)
{
    LOG_DEBUG("Chunk finished (ChunkId: %s, Address: %s)",
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);
}

void TRemoteChunkWriter::OnFinishedSession()
{
    if (State != EWriterState::Writing)
        return;

    State = EWriterState::Closed;
    Result->Set(EResult::OK);

    LOG_DEBUG("Writer closed (ChunkId: %s)",
        ~ChunkId.ToString());
}

void TRemoteChunkWriter::RegisterReadyEvent(TAsyncResult<TVoid>::TPtr windowReady)
{
    YASSERT(~WindowReady == NULL);
    if (WindowSlots.GetFreeSlotsCount() > 0 ||
        State == EWriterState::Canceled ||
        State == EWriterState::Closed)
    {
        windowReady->Set(TVoid());
    } else {
        WindowReady = windowReady;
    }
}

IChunkWriter::EResult TRemoteChunkWriter::AsyncWriteBlock(const TSharedRef& data, TAsyncResult<TVoid>::TPtr* ready)
{
    YASSERT(ready != NULL);

    // Check that the current group is still valid.
    // If no, then the client had already canceled or closed the writer.
    YASSERT(~CurrentGroup != NULL);

    // Make a quick check and fail immediately if the writer is already closed or canceled.
    // We still can make a round-trip to the writer thread in case
    // the writer is closing or canceling and the window is full.
    // Fortunately, RegisterReadyEvent (which gets invoked in the writers thread)
    // is capable of handling this case.
    if (State == EWriterState::Canceled ||
        State == EWriterState::Closed)
        return EResult::Failed;

    if (WindowSlots.TryAcquire()) {
        LOG_DEBUG("Block added (ChunkId: %s, BlockIndex: %d)",
            ~ChunkId.ToString(),
            BlockCount);

        CurrentGroup->AddBlock(data);
        ++BlockCount;

        if (CurrentGroup->GetSize() >= Config.GroupSize) {
            WriterThread->Invoke(FromMethod(
                &TRemoteChunkWriter::AddGroup, 
                TPtr(this),
                CurrentGroup));

            CurrentGroup.Drop();
            CurrentGroup = New<TGroup>(Nodes.ysize(), BlockCount, this);
        }

        return EResult::OK;
    } else {
        LOG_DEBUG("Window is full (ChunkId: %s)",
            ~ChunkId.ToString());

        *ready = New< TAsyncResult<TVoid> >();
        WriterThread->Invoke(FromMethod(
            &TRemoteChunkWriter::RegisterReadyEvent,
            TPtr(this),
            *ready));

        return EResult::TryLater;
    }
}

TAsyncResult<IChunkWriter::EResult>::TPtr TRemoteChunkWriter::AsyncClose()
{
    // Make a quick check and return immediately if the writer is already closed or canceled.
    if (State == EWriterState::Closed ||
        State == EWriterState::Canceled)
        return Result;

    LOG_DEBUG("Requesting close (ChunkId: %s)",
        ~ChunkId.ToString());

    if (~CurrentGroup != NULL) {
        if (CurrentGroup->GetSize() > 0) {
            WriterThread->Invoke(FromMethod(
                &TRemoteChunkWriter::AddGroup,
                TPtr(this), 
                CurrentGroup));
        }

        // Drop the cyclic reference.
        CurrentGroup.Drop();
    }

    // Set IsCloseRequested via queue to ensure proper serialization
    // (i.e. the flag will be set when all appended blocks are processed).
    WriterThread->Invoke(FromMethod(
        &TRemoteChunkWriter::DoClose, 
        TPtr(this)));

    return Result;
}

void TRemoteChunkWriter::Cancel()
{
    // Just a quick check.
    if (State == EWriterState::Canceled ||
        State == EWriterState::Closed)
        return;

    LOG_DEBUG("Requesting cancel (ChunkId: %s)",
        ~ChunkId.ToString());

    // Drop the cyclic reference.
    CurrentGroup.Drop();

    WriterThread->Invoke(FromMethod(
        &TRemoteChunkWriter::DoCancel, 
        TPtr(this)));
}

Stroka TRemoteChunkWriter::GetDebugInfo()
{
    TStringStream ss;
    // ToDo: implement metrics
    
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

NRpc::TChannelCache TRemoteChunkWriter::ChannelCache;

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
