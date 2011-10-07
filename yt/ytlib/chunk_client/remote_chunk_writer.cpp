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
NRpc::TChannelCache TRemoteChunkWriter::ChannelCache;

///////////////////////////////////////////////////////////////////////////////

void TRemoteChunkWriter::TConfig::Read(TJsonObject *config)
{
    TryRead(config, L"WindowSize", &WindowSize);
    TryRead(config, L"GroupSize", &GroupSize);
    //ToDo: make timeout configurable
}

///////////////////////////////////////////////////////////////////////////////

struct TRemoteChunkWriter::TNode 
    : public TRefCountedBase
{
    bool IsAlive;
    const Stroka Address;
    TProxy Proxy;
    TDelayedInvoker::TCookie Cookie;

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

    /*!
     * \note Thread affinity: ClientThread.
     */
    void AddBlock(const TSharedRef& block);

    /*!
     * \note Thread affinity: ClientThread.
     */
    void Process();

    /*!
     * \note Thread affinity: WriterThread.
     */
    bool IsWritten() const;

    /*!
     * \note Thread affinity: ClientThread.
     */
    i64 GetSize() const;

    /*!
     * \note Thread affinity: any.
     */
    int GetStartBlockIndex() const;

    /*!
     * \note Thread affinity: any.
     */
    int GetEndBlockIndex() const;

    /*!
     * \note Thread affinity: any.
     */
    int GetBlockCount() const;

    /*!
     * \note Thread affinity: WriterThread.
     */
    bool IsFlushing() const;

    /*!
     * \note Thread affinity: WriterThread.
     */
    void SetFlushing();

private:
    bool IsFlushing_;
    yvector<bool> IsSent;

    yvector<TSharedRef> Blocks;
    int StartBlockIndex;

    i64 Size;

    TRemoteChunkWriter::TPtr Writer;

    /*!
     * \note Thread affinity: WriterThread.
     */
    void PutGroup();

    /*!
     * \note Thread affinity: WriterThread.
     */
    TInvPutBlocks::TPtr PutBlocks(int node);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void OnPutBlocks(int node);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void SendGroup(int srcNode);

    /*!
     * \note Thread affinity: WriterThread.
     */
    TInvSendBlocks::TPtr SendBlocks(int srcNode, int dstNode);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void CheckSendResponse(
        TRemoteChunkWriter::TRspSendBlocks::TPtr rsp, 
        int srcNode, 
        int dstNode);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void OnSentBlocks(int srcNode, int dstNode);
};

///////////////////////////////////////////////////////////////////////////////

TRemoteChunkWriter::TGroup::TGroup(
    int nodeCount, 
    int startBlockIndex, 
    TRemoteChunkWriter::TPtr writer)
    : IsFlushing_(false)
    , IsSent(nodeCount, false)
    , StartBlockIndex(startBlockIndex)
    , Size(0)
    , Writer(writer)
{ }

void TRemoteChunkWriter::TGroup::AddBlock(const TSharedRef& block)
{
    VERIFY_THREAD_AFFINITY(Writer->ClientThread);

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
    VERIFY_THREAD_AFFINITY(Writer->ClientThread);

    return Size;
}

int TRemoteChunkWriter::TGroup::GetBlockCount() const
{
    return Blocks.ysize();
}

bool TRemoteChunkWriter::TGroup::IsWritten() const
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    for (int node = 0; node < IsSent.ysize(); ++node) {
        if (Writer->Nodes[node]->IsAlive && !IsSent[node]) {
            return false;
        }
    }
    return true;
}

void TRemoteChunkWriter::TGroup::PutGroup()
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    int node = 0;
    while (!Writer->Nodes[node]->IsAlive) {
        ++node;
        YASSERT(node < Writer->Nodes.ysize());
    }

    auto awaiter = New<TParallelAwaiter>(Writer->WriterThread->GetInvoker());
    auto onSuccess = FromMethod(
        &TGroup::OnPutBlocks, 
        TGroupPtr(this), 
        node);
    auto onResponse = FromMethod(
        &TRemoteChunkWriter::CheckResponse<TRspPutBlocks>, 
        Writer, 
        node, 
        onSuccess,
        &Writer->PutBlocksTiming);
    awaiter->Await(PutBlocks(node), onResponse);
    awaiter->Complete(FromMethod(
        &TRemoteChunkWriter::TGroup::Process, 
        TGroupPtr(this)));
}

TRemoteChunkWriter::TInvPutBlocks::TPtr 
TRemoteChunkWriter::TGroup::PutBlocks(int node)
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    auto req = Writer->Nodes[node]->Proxy.PutBlocks();
    req->SetChunkId(Writer->ChunkId.ToProto());
    req->SetStartBlockIndex(StartBlockIndex);
    req->Attachments().insert(req->Attachments().begin(), Blocks.begin(), Blocks.end());

    LOG_DEBUG("Putting blocks (ChunkId: %s, Blocks: %d-%d, Address: %s)",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[node]->Address);

    return req->Invoke(Writer->Config.RpcTimeout);
}

void TRemoteChunkWriter::TGroup::OnPutBlocks(int node)
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    IsSent[node] = true;

    LOG_DEBUG("Blocks are put (ChunkId: %s, Blocks, %d-%d, Address: %s)",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[node]->Address);

    Writer->SchedulePing(node);
}

void TRemoteChunkWriter::TGroup::SendGroup(int srcNode)
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    for (int node = 0; node < IsSent.ysize(); ++node) {
        if (Writer->Nodes[node]->IsAlive && !IsSent[node]) {
            auto awaiter = New<TParallelAwaiter>(TRemoteChunkWriter::WriterThread->GetInvoker());
            auto onResponse = FromMethod(
                &TGroup::CheckSendResponse,
                TGroupPtr(this),
                srcNode,
                node);
            awaiter->Await(SendBlocks(srcNode, node), onResponse);
            awaiter->Complete(FromMethod(&TGroup::Process, TGroupPtr(this)));
            break;
        }
    }
}

TRemoteChunkWriter::TInvSendBlocks::TPtr 
TRemoteChunkWriter::TGroup::SendBlocks(int srcNode, int dstNode)
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

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

void TRemoteChunkWriter::TGroup::CheckSendResponse(
    TRemoteChunkWriter::TRspSendBlocks::TPtr rsp, 
    int srcNode, 
    int dstNode)
{
    if (rsp->GetErrorCode() == TProxy::EErrorCode::RemoteCallFailed) {
        Writer->OnNodeDied(dstNode);
        return;
    }

    auto onSuccess = FromMethod(
        &TGroup::OnSentBlocks, 
        TGroupPtr(this), 
        srcNode, 
        dstNode);

    Writer->CheckResponse<TRemoteChunkWriter::TRspSendBlocks>(
        rsp, 
        srcNode, 
        onSuccess,
        &Writer->SendBlocksTiming);
}

void TRemoteChunkWriter::TGroup::OnSentBlocks(int srcNode, int dstNode)
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    LOG_DEBUG("Blocks are sent (ChunkId: %s, Blocks: %d-%d, SrcAddress: %s, DstAddress: %s)",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[srcNode]->Address,
        ~Writer->Nodes[dstNode]->Address);

    IsSent[dstNode] = true;

    Writer->SchedulePing(srcNode);
    Writer->SchedulePing(dstNode);
}

bool TRemoteChunkWriter::TGroup::IsFlushing() const
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    return IsFlushing_;
}

void TRemoteChunkWriter::TGroup::SetFlushing()
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    IsFlushing_ = true;
}

void TRemoteChunkWriter::TGroup::Process()
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    if (Writer->State == EWriterState::Canceled ||
        // This can be if the last response came from the node considered dead
        Writer->State == EWriterState::Closed)
    {
        return;
    }

    YASSERT(Writer->State == EWriterState::Writing);

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
    , Result(New< TFuture<EResult> >())
    , WindowSlots(config.WindowSize)
    , AliveNodeCount(addresses.ysize())
    , CurrentGroup(New<TGroup>(AliveNodeCount, 0, this))
    , BlockCount(0)
    , WindowReady(NULL)
    , StartChunkTiming(0, 1000, 20)
    , PutBlocksTiming(0, 1000, 20)
    , SendBlocksTiming(0, 1000, 20)
    , FlushBlockTiming(0, 1000, 20)
    , FinishChunkTiming(0, 1000, 20)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    YASSERT(AliveNodeCount > 0);

    LOG_DEBUG("Writer created (ChunkId: %s, Addresses: [%s])",
        ~ChunkId.ToString(),
        ~JoinToString(addresses));

    InitializeNodes(addresses);

    StartSession();
}

void TRemoteChunkWriter::InitializeNodes(const yvector<Stroka>& addresses)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    FOREACH(const auto& address, addresses) {
        Nodes.push_back(New<TNode>(address, ~ChannelCache.GetChannel(address)));
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
    VERIFY_THREAD_AFFINITY(WriterThread);

    int lastFlushableBlock = -1;
    for (auto it = Window.begin(); it != Window.end(); ++it) {
        auto group = *it;
        if (!group->IsFlushing()) {
            if (group->IsWritten() || State == EWriterState::Canceled) {
                lastFlushableBlock = group->GetEndBlockIndex();
                group->SetFlushing();
            } else {
                break;
            }
        }
    }

    if (lastFlushableBlock < 0)
        return;

    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
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
                onSuccess,
                &FlushBlockTiming);
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
    VERIFY_THREAD_AFFINITY(WriterThread);

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
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Blocks flushed (ChunkId: %s, BlockIndex: %d, Address: %s)",
        ~ChunkId.ToString(), 
        blockIndex,
        ~Nodes[node]->Address);

    SchedulePing(node);
}

void TRemoteChunkWriter::OnWindowShifted(int lastFlushedBlock)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (Window.empty()) {
        // This happens when FlushBlocks responses are disordered
        // (i.e. a bigger BlockIndex is flushed before a smaller one)
        // and prevents repeated calling CloseSession
        return;
    }

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
    VERIFY_THREAD_AFFINITY(WriterThread);

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
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (IsCloseRequested)
        return;

    if (State == EWriterState::Closed ||
        State == EWriterState::Canceled)
        return;

    LOG_DEBUG("Writer close requested (ChunkId: %s)",
        ~ChunkId.ToString());

    IsCloseRequested = true;
    if (Window.empty() && State == EWriterState::Writing) {
        CloseSession();
    }
}

void TRemoteChunkWriter::Shutdown()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Some groups may still be pending.
    // Drop the references to ensure proper resource disposal.
    Window.clear();

    Result->Set(EResult::Failed);
    State = EWriterState::Canceled;
    CancelAllPings();

    if (~WindowReady != NULL) {
        WindowReady->Set(TVoid());
        WindowReady.Drop();
    }
}

void TRemoteChunkWriter::DoCancel()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (State == EWriterState::Closed ||
        State == EWriterState::Canceled)
        return;

    Shutdown();

    LOG_DEBUG("Writer canceled (ChunkId: %s)",
        ~ChunkId.ToString());
}

void TRemoteChunkWriter::AddGroup(TGroupPtr group)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

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

    if (State == EWriterState::Writing) {
        group->Process();
    }
}

void TRemoteChunkWriter::OnNodeDied(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Nodes[node]->IsAlive)
        return;

    if (State == EWriterState::Canceled || 
        State == EWriterState::Closed)
    {
        return;
    }

    Nodes[node]->IsAlive = false;
    --AliveNodeCount;

    LOG_INFO("Node is considered dead (ChunkId: %s, Address: %s, AliveNodeCount: %d)", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address,
        AliveNodeCount);

    if (State != EWriterState::Canceled && AliveNodeCount == 0) {
        YASSERT(State != EWriterState::Closed);

        Shutdown();

        LOG_WARNING("No alive nodes left, chunk writing failed (ChunkId: %s)",
            ~ChunkId.ToString());
    }
}

template<class TResponse>
void TRemoteChunkWriter::CheckResponse(
    typename TResponse::TPtr rsp,
    int node,
    IAction::TPtr onSuccess, 
    TMetric* metric)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (rsp->IsOK()) {
        metric->AddDelta(rsp->GetInvokeInstant());
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
    VERIFY_THREAD_AFFINITY(ClientThread);

    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    for (int node = 0; node < Nodes.ysize(); ++node) {
        auto onSuccess = FromMethod(
            &TRemoteChunkWriter::OnStartedChunk, 
            TPtr(this), 
            node);
        auto onResponse = FromMethod(
            &TRemoteChunkWriter::CheckResponse<TRspStartChunk>, 
            TPtr(this),
            node,
            onSuccess,
            &StartChunkTiming);
        awaiter->Await(StartChunk(node), onResponse);
    }
    awaiter->Complete(FromMethod(&TRemoteChunkWriter::OnSessionStarted, TPtr(this)));
}

TRemoteChunkWriter::TInvStartChunk::TPtr TRemoteChunkWriter::StartChunk(int node)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

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
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Chunk started (ChunkId: %s, Address: %s)", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);

    SchedulePing(node);
}

void TRemoteChunkWriter::OnSessionStarted()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Check if the session is not canceled yet.
    if (State == EWriterState::Canceled)
        return;

    LOG_DEBUG("Writer ready (ChunkId: %s)", 
        ~ChunkId.ToString());

    State = EWriterState::Writing;
    FOREACH(auto& group, Window) {
        group->Process();
    }

    // Possible for an empty chunk.
    if (Window.empty() && IsCloseRequested) {
        CloseSession();
    }
}

void TRemoteChunkWriter::CloseSession()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    YASSERT(IsCloseRequested);
    YASSERT(State == EWriterState::Writing);

    LOG_DEBUG("Closing writer (ChunkId: %s)",
        ~ChunkId.ToString());

    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
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
                onSuccess, 
                &FinishChunkTiming);
            awaiter->Await(FinishChunk(node), onResponse);
        }
    }
    awaiter->Complete(FromMethod(&TRemoteChunkWriter::OnFinishedSession, TPtr(this)));
}

TRemoteChunkWriter::TInvFinishChunk::TPtr TRemoteChunkWriter::FinishChunk(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Finishing chunk (ChunkId: %s, Address: %s)",
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);

    auto req = Nodes[node]->Proxy.FinishChunk();
    req->SetChunkId(ChunkId.ToProto());
    return req->Invoke(Config.RpcTimeout);
}

void TRemoteChunkWriter::OnFinishedChunk(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Chunk finished (ChunkId: %s, Address: %s)",
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);
}

void TRemoteChunkWriter::OnFinishedSession()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (State != EWriterState::Writing)
        return;

    // Check that we're not holding any references to groups.
    YASSERT(~CurrentGroup == NULL);
    YASSERT(Window.empty());


    State = EWriterState::Closed;
    CancelAllPings();
    Result->Set(EResult::OK);

    LOG_DEBUG("Writer closed (ChunkId: %s)",
        ~ChunkId.ToString());
}

void TRemoteChunkWriter::PingSession(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Pinging session (ChunkId: %s, Address: %s)",
        ~ChunkId.ToString(),
        ~Nodes[node]->Address);

    auto req = Nodes[node]->Proxy.PingSession();
    req->SetChunkId(ChunkId.ToProto());
    req->Invoke(Config.RpcTimeout);

    SchedulePing(node);
}

void TRemoteChunkWriter::SchedulePing(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (State == EWriterState::Closed ||
        State == EWriterState::Canceled)
    {
        return;
    }

    auto cookie = Nodes[node]->Cookie;
    if (cookie != TDelayedInvoker::TCookie()) {
        TDelayedInvoker::Get()->Cancel(cookie);
    }
    Nodes[node]->Cookie = TDelayedInvoker::Get()->Submit(
        FromMethod(
            &TRemoteChunkWriter::PingSession,
            TPtr(this),
            node)
        ->Via(WriterThread->GetInvoker()),
        Config.SessionTimeout);
}

void TRemoteChunkWriter::CancelPing(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto& cookie = Nodes[node]->Cookie;
    if (cookie != TDelayedInvoker::TCookie()) {
        TDelayedInvoker::Get()->Cancel(cookie);
        cookie = TDelayedInvoker::TCookie();
    }
}

void TRemoteChunkWriter::CancelAllPings()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    for (int node = 0; node < Nodes.ysize(); ++node) {
        CancelPing(node);
    }
}

void TRemoteChunkWriter::RegisterReadyEvent(TFuture<TVoid>::TPtr windowReady)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YASSERT(~WindowReady == NULL);

    if (WindowSlots.GetFreeSlotCount() > 0 ||
        State == EWriterState::Canceled ||
        State == EWriterState::Closed)
    {
        windowReady->Set(TVoid());
    } else {
        WindowReady = windowReady;
    }
}

IChunkWriter::EResult TRemoteChunkWriter::AsyncWriteBlock(
    const TSharedRef& data,
    TFuture<TVoid>::TPtr* ready)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
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
            WriterThread->GetInvoker()->Invoke(FromMethod(
                &TRemoteChunkWriter::AddGroup, 
                TPtr(this),
                CurrentGroup));

            // Construct a new (empty) group.
            CurrentGroup = New<TGroup>(Nodes.ysize(), BlockCount, this);
        }

        return EResult::OK;
    } else {
        LOG_DEBUG("Window is full (ChunkId: %s)",
            ~ChunkId.ToString());

        *ready = New< TFuture<TVoid> >();
        WriterThread->GetInvoker()->Invoke(FromMethod(
            &TRemoteChunkWriter::RegisterReadyEvent,
            TPtr(this),
            *ready));

        return EResult::TryLater;
    }
}

TFuture<IChunkWriter::EResult>::TPtr TRemoteChunkWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    // Make a quick check and return immediately if the writer is already closed or canceled.
    if (State == EWriterState::Closed ||
        State == EWriterState::Canceled)
        return Result;

    LOG_DEBUG("Requesting close (ChunkId: %s)",
        ~ChunkId.ToString());

    if (~CurrentGroup != NULL) {
        if (CurrentGroup->GetSize() > 0) {
            WriterThread->GetInvoker()->Invoke(FromMethod(
                &TRemoteChunkWriter::AddGroup,
                TPtr(this), 
                CurrentGroup));
        }

        // Drop the cyclic reference.
        CurrentGroup.Drop();
    }

    // Set IsCloseRequested via queue to ensure proper serialization
    // (i.e. the flag will be set when all appended blocks are processed).
    WriterThread->GetInvoker()->Invoke(FromMethod(
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

    WriterThread->GetInvoker()->Invoke(FromMethod(
        &TRemoteChunkWriter::DoCancel,
        TPtr(this)));
}

Stroka TRemoteChunkWriter::GetDebugInfo()
{
    return Sprintf(
        "ChunkId: %s; "
        "StartChunk: (%s); "
        "FinishChunk timing: (%s); "
        "PutBlocks timing: (%s); "
        "SendBlocks timing: (%s); "
        "FlushBlocks timing: (%s); ",
        ~ChunkId.ToString(), 
        ~StartChunkTiming.GetDebugInfo(),
        ~FinishChunkTiming.GetDebugInfo(),
        ~PutBlocksTiming.GetDebugInfo(),
        ~SendBlocksTiming.GetDebugInfo(),
        ~FlushBlockTiming.GetDebugInfo());
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
