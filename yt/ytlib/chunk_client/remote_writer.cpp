#include "stdafx.h"
#include "remote_writer.h"
#include "holder_channel_cache.h"
#include "writer_thread.h"
#include "chunk_holder_service_rpc.pb.h"

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

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NChunkHolder::NProto;
using namespace NChunkServer::NProto;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkClientLogger;

///////////////////////////////////////////////////////////////////////////////

void TRemoteWriter::TConfig::Read(TJsonObject *config)
{
    TryRead(config, L"WindowSize", &WindowSize);
    TryRead(config, L"GroupSize", &GroupSize);
    //ToDo: make timeout configurable
}

///////////////////////////////////////////////////////////////////////////////

struct TRemoteWriter::TNode 
    : public TRefCountedBase
{
    bool IsAlive;
    const Stroka Address;
    TProxy Proxy;
    TDelayedInvoker::TCookie Cookie;

    TNode(const Stroka& address, IChannel* channel, TDuration timeout)
        : IsAlive(true)
        , Address(address)
        , Proxy(channel)
    {
        Proxy.SetTimeout(timeout);
    }
};

///////////////////////////////////////////////////////////////////////////////

class TRemoteWriter::TGroup 
    : public TRefCountedBase
{
public:
    TGroup(
        int nodeCount, 
        int startBlockIndex, 
        TRemoteWriter::TPtr writer);

    void AddBlock(const TSharedRef& block);
    void Process();
    bool IsWritten() const;
    int GetSize() const;

    /*!
     * \note Thread affinity: any.
     */
    int GetStartBlockIndex() const;

    /*!
     * \note Thread affinity: any.
     */
    int GetEndBlockIndex() const;

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

    int Size;

    TRemoteWriter::TPtr Writer;

    /*!
     * \note Thread affinity: WriterThread.
     */
    void PutGroup();

    /*!
     * \note Thread affinity: WriterThread.
     */
    TProxy::TInvPutBlocks::TPtr PutBlocks(int node);

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
    TProxy::TInvSendBlocks::TPtr SendBlocks(int srcNode, int dstNode);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void CheckSendResponse(
        TRemoteWriter::TProxy::TRspSendBlocks::TPtr rsp,
        int srcNode, 
        int dstNode);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void OnSentBlocks(int srcNode, int dstNode);
};

///////////////////////////////////////////////////////////////////////////////

TRemoteWriter::TGroup::TGroup(
    int nodeCount, 
    int startBlockIndex, 
    TRemoteWriter::TPtr writer)
    : IsFlushing_(false)
    , IsSent(nodeCount, false)
    , StartBlockIndex(startBlockIndex)
    , Size(0)
    , Writer(writer)
{ }

void TRemoteWriter::TGroup::AddBlock(const TSharedRef& block)
{
    Blocks.push_back(block);
    Size += block.Size();
}

int TRemoteWriter::TGroup::GetStartBlockIndex() const
{
    return StartBlockIndex;
}

int TRemoteWriter::TGroup::GetEndBlockIndex() const
{
    return StartBlockIndex + Blocks.ysize() - 1;
}

int TRemoteWriter::TGroup::GetSize() const
{
    return Size;
}

bool TRemoteWriter::TGroup::IsWritten() const
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    for (int node = 0; node < IsSent.ysize(); ++node) {
        if (Writer->Nodes[node]->IsAlive && !IsSent[node]) {
            return false;
        }
    }
    return true;
}

void TRemoteWriter::TGroup::PutGroup()
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    int node = 0;
    while (!Writer->Nodes[node]->IsAlive) {
        ++node;
        YASSERT(node < Writer->Nodes.ysize());
    }

    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    auto onSuccess = FromMethod(
        &TGroup::OnPutBlocks, 
        TGroupPtr(this), 
        node);
    auto onResponse = FromMethod(
        &TRemoteWriter::CheckResponse<TProxy::TRspPutBlocks>,
        Writer, 
        node, 
        onSuccess,
        &Writer->PutBlocksTiming);
    awaiter->Await(PutBlocks(node), onResponse);
    awaiter->Complete(FromMethod(
        &TRemoteWriter::TGroup::Process, 
        TGroupPtr(this)));
}

TRemoteWriter::TProxy::TInvPutBlocks::TPtr
TRemoteWriter::TGroup::PutBlocks(int node)
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    auto req = Writer->Nodes[node]->Proxy.PutBlocks();
    req->set_chunk_id(Writer->ChunkId.ToProto());
    req->set_start_block_index(StartBlockIndex);
    req->Attachments().insert(req->Attachments().begin(), Blocks.begin(), Blocks.end());

    LOG_DEBUG("Putting blocks (ChunkId: %s, Blocks: %d-%d, Address: %s)",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[node]->Address);

    return req->Invoke();
}

void TRemoteWriter::TGroup::OnPutBlocks(int node)
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

void TRemoteWriter::TGroup::SendGroup(int srcNode)
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    for (int node = 0; node < IsSent.ysize(); ++node) {
        if (Writer->Nodes[node]->IsAlive && !IsSent[node]) {
            auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
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

TRemoteWriter::TProxy::TInvSendBlocks::TPtr
TRemoteWriter::TGroup::SendBlocks(int srcNode, int dstNode)
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    LOG_DEBUG("Sending blocks (ChunkId: %s, Blocks: %d-%d, SrcAddress: %s, DstAddress: %s)",
        ~Writer->ChunkId.ToString(), 
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~Writer->Nodes[srcNode]->Address,
        ~Writer->Nodes[dstNode]->Address);

    auto req = Writer->Nodes[srcNode]->Proxy.SendBlocks();
    req->set_chunk_id(Writer->ChunkId.ToProto());
    req->set_start_block_index(StartBlockIndex);
    req->set_block_count(Blocks.ysize());
    req->set_address(Writer->Nodes[dstNode]->Address);
    return req->Invoke();
}

void TRemoteWriter::TGroup::CheckSendResponse(
    TRemoteWriter::TProxy::TRspSendBlocks::TPtr rsp,
    int srcNode, 
    int dstNode)
{
    if (rsp->GetErrorCode() == EErrorCode::PutBlocksFailed) {
        Writer->OnNodeDied(dstNode);
        return;
    }

    auto onSuccess = FromMethod(
        &TGroup::OnSentBlocks, 
        TGroupPtr(this), 
        srcNode, 
        dstNode);

    Writer->CheckResponse<TRemoteWriter::TProxy::TRspSendBlocks>(
        rsp, 
        srcNode, 
        onSuccess,
        &Writer->SendBlocksTiming);
}

void TRemoteWriter::TGroup::OnSentBlocks(int srcNode, int dstNode)
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

bool TRemoteWriter::TGroup::IsFlushing() const
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    return IsFlushing_;
}

void TRemoteWriter::TGroup::SetFlushing()
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    IsFlushing_ = true;
}

void TRemoteWriter::TGroup::Process()
{
    VERIFY_THREAD_AFFINITY(Writer->WriterThread);

    if (!Writer->State.IsActive()) {
        return;
    }

    YASSERT(Writer->IsInitComplete);

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

    if (!emptyNodeFound) {
        Writer->ShiftWindow();
    } else if (nodeWithBlocks < 0) {
        PutGroup();
    } else {
        SendGroup(nodeWithBlocks);
    }
}

///////////////////////////////////////////////////////////////////////////////

TRemoteWriter::TRemoteWriter(
    TRemoteWriter::TConfig* config, 
    const TChunkId& chunkId,
    const yvector<Stroka>& addresses)
    : ChunkId(chunkId) 
    , Config(config)
    , IsInitComplete(false)
    , IsCloseRequested(false)
    , WindowSlots(config->WindowSize)
    , AliveNodeCount(addresses.ysize())
    , CurrentGroup(New<TGroup>(AliveNodeCount, 0, this))
    , BlockCount(0)
    , StartChunkTiming(0, 1000, 20)
    , PutBlocksTiming(0, 1000, 20)
    , SendBlocksTiming(0, 1000, 20)
    , FlushBlockTiming(0, 1000, 20)
    , FinishChunkTiming(0, 1000, 20)
{
    YASSERT(AliveNodeCount > 0);

    LOG_DEBUG("Writer created (ChunkId: %s, Addresses: [%s])",
        ~ChunkId.ToString(),
        ~JoinToString(addresses));

    InitializeNodes(addresses);

    StartSession();
}

void TRemoteWriter::InitializeNodes(const yvector<Stroka>& addresses)
{
    FOREACH(const auto& address, addresses) {
        auto node = New<TNode>(
            address,
            ~HolderChannelCache->GetChannel(address),
            Config->HolderRpcTimeout);
        Nodes.push_back(node);
    }
}

TRemoteWriter::~TRemoteWriter()
{
    YASSERT(!State.IsActive());
}

void TRemoteWriter::ShiftWindow()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!State.IsActive()) {
        YASSERT(Window.empty());
        return;
    }

    int lastFlushableBlock = -1;
    for (auto it = Window.begin(); it != Window.end(); ++it) {
        auto group = *it;
        if (!group->IsFlushing()) {
            if (group->IsWritten()) {
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
        if (Nodes[node]->IsAlive) {
            auto onSuccess = FromMethod(
                &TRemoteWriter::OnBlockFlushed, 
                TPtr(this), 
                node,
                lastFlushableBlock);
            auto onResponse = FromMethod(
                &TRemoteWriter::CheckResponse<TProxy::TRspFlushBlock>,
                TPtr(this), 
                node, 
                onSuccess,
                &FlushBlockTiming);
            awaiter->Await(FlushBlock(node, lastFlushableBlock), onResponse);
        }
    }

    awaiter->Complete(FromMethod(
        &TRemoteWriter::OnWindowShifted, 
        TPtr(this),
        lastFlushableBlock));
}

TRemoteWriter::TProxy::TInvFlushBlock::TPtr
TRemoteWriter::FlushBlock(int node, int blockIndex)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Flushing blocks (ChunkId: %s, BlockIndex: %d, Address: %s)",
        ~ChunkId.ToString(), 
        blockIndex,
        ~Nodes[node]->Address);

    auto req = Nodes[node]->Proxy.FlushBlock();
    req->set_chunk_id(ChunkId.ToProto());
    req->set_block_index(blockIndex);
    return req->Invoke();
}

void TRemoteWriter::OnBlockFlushed(int node, int blockIndex)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Blocks flushed (ChunkId: %s, BlockIndex: %d, Address: %s)",
        ~ChunkId.ToString(), 
        blockIndex,
        ~Nodes[node]->Address);

    SchedulePing(node);
}

void TRemoteWriter::OnWindowShifted(int lastFlushedBlock)
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

        LOG_DEBUG("Window shifted (ChunkId: %s, BlockIndex: %d, Size: %d)",
            ~ChunkId.ToString(), 
            group->GetEndBlockIndex(),
            group->GetSize());

        WindowSlots.Release(group->GetSize());
        Window.pop_front();
    }

    if (State.IsActive() && IsCloseRequested) {
        CloseSession();
    }
}

void TRemoteWriter::DoClose(const TChunkAttributes& attributes)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YASSERT(!IsCloseRequested);

    if (!State.IsActive()) {
        State.FinishOperation();
        return;
    }

    LOG_DEBUG("Writer close requested (ChunkId: %s)",
        ~ChunkId.ToString());

    IsCloseRequested = true;
    Attributes.CopyFrom(attributes);

    if (Window.empty() && IsInitComplete) {
        CloseSession();
    }
}

void TRemoteWriter::Shutdown()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Some groups may still be pending.
    // Drop the references to ensure proper resource disposal.
    Window.clear();
    CancelAllPings();
}

void TRemoteWriter::DoCancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    State.Cancel(error);
    Shutdown();

    LOG_DEBUG("Writer canceled (ChunkId: %s)\n%s",
        ~ChunkId.ToString(),
        ~error.ToString());
}

void TRemoteWriter::AddGroup(TGroupPtr group)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YASSERT(!IsCloseRequested);

    if (!State.IsActive())
        return;

    LOG_DEBUG("Group added (ChunkId: %s, Blocks: %d-%d)", 
        ~ChunkId.ToString(), 
        group->GetStartBlockIndex(),
        group->GetEndBlockIndex());

    Window.push_back(group);

    if (IsInitComplete) {
        group->Process();
    }
}

void TRemoteWriter::OnNodeDied(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Nodes[node]->IsAlive)
        return;

    Nodes[node]->IsAlive = false;
    --AliveNodeCount;

    LOG_INFO("Holder died (ChunkId: %s, Address: %s, AliveCount: %d)", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address,
        AliveNodeCount);

    if (State.IsActive() && AliveNodeCount == 0) {
        LOG_WARNING("No alive holders left, chunk writing failed (ChunkId: %s)",
            ~ChunkId.ToString());
        DoCancel(TError("No alive holders left"));
    }
}

template<class TResponse>
void TRemoteWriter::CheckResponse(
    typename TResponse::TPtr rsp,
    int node,
    IAction::TPtr onSuccess, 
    TMetric* metric)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (rsp->IsOK()) {
        metric->AddDelta(rsp->GetStartTime());
        onSuccess->Do();
        return;
    } 

    // TODO: retry?
    LOG_ERROR("Error reported by holder (ChunkId: %s, Address: %s)\n%s", 
        ~ChunkId.ToString(),
        ~Nodes[node]->Address, 
        ~rsp->GetError().ToString());

    OnNodeDied(node);
}

void TRemoteWriter::StartSession()
{
    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    for (int node = 0; node < Nodes.ysize(); ++node) {
        auto onSuccess = FromMethod(
            &TRemoteWriter::OnChunkStarted, 
            TPtr(this), 
            node);
        auto onResponse = FromMethod(
            &TRemoteWriter::CheckResponse<TProxy::TRspStartChunk>,
            TPtr(this),
            node,
            onSuccess,
            &StartChunkTiming);
        awaiter->Await(StartChunk(node), onResponse);
    }
    awaiter->Complete(FromMethod(&TRemoteWriter::OnSessionStarted, TPtr(this)));
}

TRemoteWriter::TProxy::TInvStartChunk::TPtr TRemoteWriter::StartChunk(int node)
{
    LOG_DEBUG("Starting chunk (ChunkId: %s, Address: %s)", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);

    auto req = Nodes[node]->Proxy.StartChunk();
    req->set_chunk_id(ChunkId.ToProto());
    return req->Invoke();
}

void TRemoteWriter::OnChunkStarted(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Chunk started (ChunkId: %s, Address: %s)", 
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);

    SchedulePing(node);
}

void TRemoteWriter::OnSessionStarted()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Check if the session is not canceled yet.
    if (!State.IsActive()) {
        return;
    }

    LOG_DEBUG("Writer is ready (ChunkId: %s)", 
        ~ChunkId.ToString());

    IsInitComplete = true;
    FOREACH(auto& group, Window) {
        group->Process();
    }

    // Possible for an empty chunk.
    if (Window.empty() && IsCloseRequested) {
        CloseSession();
    }
}

void TRemoteWriter::CloseSession()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    YASSERT(IsCloseRequested);

    LOG_DEBUG("Closing writer (ChunkId: %s)",
        ~ChunkId.ToString());

    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    for (int node = 0; node < Nodes.ysize(); ++node) {
        if (Nodes[node]->IsAlive) {
            auto onSuccess = FromMethod(
                &TRemoteWriter::OnChunkFinished, 
                TPtr(this), 
                node);
            auto onResponse = FromMethod(
                &TRemoteWriter::CheckResponse<TProxy::TRspFinishChunk>,
                TPtr(this), 
                node, 
                onSuccess, 
                &FinishChunkTiming);
            awaiter->Await(FinishChunk(node), onResponse);
        }
    }
    awaiter->Complete(FromMethod(&TRemoteWriter::OnSessionFinished, TPtr(this)));
}

TRemoteWriter::TProxy::TInvFinishChunk::TPtr
TRemoteWriter::FinishChunk(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Finishing chunk (ChunkId: %s, Address: %s)",
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);

    auto req = Nodes[node]->Proxy.FinishChunk();
    req->set_chunk_id(ChunkId.ToProto());
    req->mutable_attributes()->CopyFrom(Attributes);
    return req->Invoke();
}

void TRemoteWriter::OnChunkFinished(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Chunk is finished (ChunkId: %s, Address: %s)",
        ~ChunkId.ToString(), 
        ~Nodes[node]->Address);
}

void TRemoteWriter::OnSessionFinished()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Check that we're not holding any references to groups.
    YASSERT(~CurrentGroup == NULL);
    YASSERT(Window.empty());

    if (State.IsActive()) {
        State.Close();
    }

    CancelAllPings();

    LOG_DEBUG("Writer closed (ChunkId: %s)",
        ~ChunkId.ToString());

    State.FinishOperation();
}

void TRemoteWriter::PingSession(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Pinging session (ChunkId: %s, Address: %s)",
        ~ChunkId.ToString(),
        ~Nodes[node]->Address);

    auto req = Nodes[node]->Proxy.PingSession();
    req->set_chunk_id(ChunkId.ToProto());
    req->Invoke();

    SchedulePing(node);
}

void TRemoteWriter::SchedulePing(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!State.IsActive()) {
        return;
    }

    auto cookie = Nodes[node]->Cookie;
    if (cookie != TDelayedInvoker::NullCookie) {
        TDelayedInvoker::Cancel(cookie);
    }

    Nodes[node]->Cookie = TDelayedInvoker::Submit(
        ~FromMethod(
            &TRemoteWriter::PingSession,
            TPtr(this),
            node)
        ->Via(WriterThread->GetInvoker()),
        Config->SessionPingInterval);
}

void TRemoteWriter::CancelPing(int node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto& cookie = Nodes[node]->Cookie;
    if (cookie != TDelayedInvoker::NullCookie) {
        TDelayedInvoker::CancelAndClear(cookie);
    }
}

void TRemoteWriter::CancelAllPings()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    for (int node = 0; node < Nodes.ysize(); ++node) {
        CancelPing(node);
    }
}

TAsyncError::TPtr TRemoteWriter::AsyncWriteBlock(const TSharedRef& data)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    State.StartOperation();
    WindowSlots.AsyncAcquire(data.Size())->Subscribe(FromMethod(
        &TRemoteWriter::AddBlock,
        TPtr(this),
        data));

    return State.GetOperationError();
}

void TRemoteWriter::AddBlock(TVoid, const TSharedRef& data)
{
    if (State.IsActive()) {
        LOG_DEBUG("Block added (ChunkId: %s, BlockIndex: %d)",
            ~ChunkId.ToString(),
            BlockCount);

        CurrentGroup->AddBlock(data);
        ++BlockCount;

        if (CurrentGroup->GetSize() >= Config->GroupSize) {
            WriterThread->GetInvoker()->Invoke(FromMethod(
                &TRemoteWriter::AddGroup,
                TPtr(this),
                CurrentGroup));
            // Construct a new (empty) group.
            CurrentGroup = New<TGroup>(Nodes.ysize(), BlockCount, this);
        }
    }

    State.FinishOperation();
}

TAsyncError::TPtr TRemoteWriter::AsyncClose(const TChunkAttributes& attributes)
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    State.StartOperation();

    LOG_DEBUG("Requesting close (ChunkId: %s)", ~ChunkId.ToString());

    if (CurrentGroup->GetSize() > 0) {
        WriterThread->GetInvoker()->Invoke(FromMethod(
            &TRemoteWriter::AddGroup,
            TPtr(this), 
            CurrentGroup));
    }

    // Drop the cyclic reference.
    CurrentGroup.Reset();

    // Set IsCloseRequested via queue to ensure proper serialization
    // (i.e. the flag will be set when all appended blocks are processed).
    WriterThread->GetInvoker()->Invoke(FromMethod(
        &TRemoteWriter::DoClose, 
        TPtr(this),
        attributes));

    return State.GetOperationError();
}

void TRemoteWriter::Cancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Just a quick check.
    if (!State.IsActive())
        return;

    LOG_DEBUG("Requesting cancel (ChunkId: %s)", ~ChunkId.ToString());

    // Drop the cyclic reference.
    CurrentGroup.Reset();

    WriterThread->GetInvoker()->Invoke(FromMethod(
        &TRemoteWriter::DoCancel,
        TPtr(this),
        error));
}

Stroka TRemoteWriter::GetDebugInfo()
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

TChunkId TRemoteWriter::GetChunkId() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    return ChunkId;
}

TReqConfirmChunks::TChunkInfo TRemoteWriter::GetConfirmationInfo()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(State.IsClosed());

    TReqConfirmChunks::TChunkInfo info;
    info.set_chunkid(ChunkId.ToProto());
    *info.mutable_attributes() = Attributes;
    FOREACH (auto node, Nodes) {
        if (node->IsAlive) {
            info.add_holderaddresses(node->Address);
        }
    }

    return info;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
