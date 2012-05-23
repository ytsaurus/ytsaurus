#include "stdafx.h"
#include "remote_writer.h"
#include "config.h"
#include "holder_channel_cache.h"

#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/chunk_holder/chunk_holder_service.pb.h>

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/metric.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/actions/parallel_awaiter.h>

#include <util/generic/yexception.h>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NChunkHolder::NProto;
using namespace NChunkServer;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkWriterLogger;

///////////////////////////////////////////////////////////////////////////////

class TRemoteWriter::TImpl
    : public IAsyncWriter
{
public:
    TImpl(
        const TRemoteWriterConfigPtr& config,
        const TChunkId& chunkId,
        const std::vector<Stroka>& addresses);

    ~TImpl();

    void Open();

    virtual TAsyncError AsyncWriteBlock(const TSharedRef& block);
    virtual TAsyncError AsyncClose(const NChunkHolder::NProto::TChunkMeta& chunkMeta);

    const NChunkHolder::NProto::TChunkInfo& GetChunkInfo() const;
    const std::vector<Stroka> GetNodeAddresses() const;
    const TChunkId& GetChunkId() const;

    Stroka GetDebugInfo();

private:
    friend class TRemoteWriter::TGroup;

    TRemoteWriterConfigPtr Config;
    TChunkId ChunkId;
    std::vector<Stroka> Addresses;

    TAsyncStreamState State;

    bool IsOpen;
    bool IsInitComplete;
    bool IsClosing;

    //! This flag is raised whenever #Close is invoked.
    //! All access to this flag happens from #WriterThread.
    bool IsCloseRequested;
    NChunkHolder::NProto::TChunkMeta ChunkMeta;

    TWindow Window;
    TAsyncSemaphore WindowSlots;

    std::vector<TNodePtr> Nodes;

    //! Number of nodes that are still alive.
    int AliveNodeCount;

    //! A new group of blocks that is currently being filled in by the client.
    //! All access to this field happens from client thread.
    TGroupPtr CurrentGroup;

    //! Number of blocks that are already added via #AddBlock.
    int BlockCount;

    //! Returned from node in Finish.
    NChunkHolder::NProto::TChunkInfo ChunkInfo;

    TMetric StartChunkTiming;
    TMetric PutBlocksTiming;
    TMetric SendBlocksTiming;
    TMetric FlushBlockTiming;
    TMetric FinishChunkTiming;

    NLog::TTaggedLogger Logger;

    void DoClose();

    void AddGroup(TGroupPtr group);

    void RegisterReadyEvent(TFuture<void> windowReady);

    void OnNodeFailed(TNodePtr node);

    void ShiftWindow();

    TProxy::TInvFlushBlock FlushBlock(TNodePtr node, int blockIndex);

    void OnBlockFlushed(TNodePtr node, int blockIndex, TProxy::TRspFlushBlockPtr rsp);

    void OnWindowShifted(int blockIndex);

    TProxy::TInvStartChunk StartChunk(TNodePtr node);

    void OnChunkStarted(TNodePtr node, TProxy::TRspStartChunkPtr rsp);

    void OnSessionStarted();

    void CloseSession();

    TProxy::TInvFinishChunk FinishChunk(TNodePtr node);

    void OnChunkFinished(TNodePtr node, TProxy::TRspFinishChunkPtr rsp);

    void OnSessionFinished();

    void SendPing(TNodePtr node);
    void StartPing(TNodePtr node);
    void CancelPing(TNodePtr node);
    void CancelAllPings();

    template <class TResponse>
    void CheckResponse(
        TNodePtr node,
        TCallback<void(TIntrusivePtr<TResponse>)> onSuccess,
        TMetric* metric,
        TIntrusivePtr<TResponse> rsp);

    void AddBlock(const TSharedRef& block);
    void DoWriteBlock(const TSharedRef& block);

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);
};

///////////////////////////////////////////////////////////////////////////////

struct TRemoteWriter::TNode 
    : public TRefCounted
{
    int Index;
    bool IsAlive;
    const Stroka Address;
    TProxy Proxy;
    TPeriodicInvoker::TPtr PingInvoker;

    TNode(int index, const Stroka& address)
        : Index(index)
        , IsAlive(true)
        , Address(address)
        , Proxy(NodeChannelCache->GetChannel(address))
    { }
};

///////////////////////////////////////////////////////////////////////////////

class TRemoteWriter::TGroup 
    : public TRefCounted
{
public:
    TGroup(
        int nodeCount, 
        int startBlockIndex, 
        TRemoteWriter::TImpl* writer);

    void AddBlock(const TSharedRef& block);
    void Process();
    bool IsWritten() const;
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
     * \note Thread affinity: WriterThread.
     */
    bool IsFlushing() const;

    /*!
     * \note Thread affinity: WriterThread.
     */
    void SetFlushing();

private:
    bool IsFlushing_;
    std::vector<bool> IsSent;

    std::vector<TSharedRef> Blocks;
    int StartBlockIndex;

    i64 Size;

    TWeakPtr<TRemoteWriter::TImpl> Writer;

    NLog::TTaggedLogger& Logger;

    /*!
     * \note Thread affinity: WriterThread.
     */
    void PutGroup();

    /*!
     * \note Thread affinity: WriterThread.
     */
    TProxy::TInvPutBlocks PutBlocks(TNodePtr node);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void OnPutBlocks(TNodePtr node, TProxy::TRspPutBlocksPtr rsp);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void SendGroup(TNodePtr srcNode);

    /*!
     * \note Thread affinity: WriterThread.
     */
    TProxy::TInvSendBlocks SendBlocks(TNodePtr srcNode, TNodePtr dstNode);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void CheckSendResponse(
        TNodePtr srcNode, 
        TNodePtr dstNode,
        TRemoteWriter::TProxy::TRspSendBlocksPtr rsp);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void OnSentBlocks(TNodePtr srcNode, TNodePtr dstNod, TProxy::TRspSendBlocksPtr rsp);
};

///////////////////////////////////////////////////////////////////////////////

TRemoteWriter::TGroup::TGroup(int nodeCount,
    int startBlockIndex,
    TImpl* writer)
    : IsFlushing_(false)
    , IsSent(nodeCount, false)
    , StartBlockIndex(startBlockIndex)
    , Size(0)
    , Writer(writer)
    , Logger(writer->Logger)
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
    return StartBlockIndex + Blocks.size() - 1;
}

i64 TRemoteWriter::TGroup::GetSize() const
{
    return Size;
}

bool TRemoteWriter::TGroup::IsWritten() const
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    for (int nodeIndex = 0; nodeIndex < IsSent.size(); ++nodeIndex) {
        if (writer->Nodes[nodeIndex]->IsAlive && !IsSent[nodeIndex]) {
            return false;
        }
    }
    return true;
}

void TRemoteWriter::TGroup::PutGroup()
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    int nodeIndex = 0;
    while (!writer->Nodes[nodeIndex]->IsAlive) {
        ++nodeIndex;
        YASSERT(nodeIndex < writer->Nodes.size());
    }

    auto node = writer->Nodes[nodeIndex];
    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    auto onSuccess = BIND(
        &TGroup::OnPutBlocks, 
        MakeWeak(this), 
        node);
    auto onResponse = BIND(
        &TRemoteWriter::TImpl::CheckResponse<TProxy::TRspPutBlocks>,
        Writer,
        node, 
        onSuccess,
        &writer->PutBlocksTiming);
    awaiter->Await(PutBlocks(node), onResponse);
    awaiter->Complete(BIND(
        &TRemoteWriter::TGroup::Process, 
        MakeWeak(this)));
}

TRemoteWriter::TProxy::TInvPutBlocks
TRemoteWriter::TGroup::PutBlocks(TNodePtr node)
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    auto req = node->Proxy.PutBlocks();
    *req->mutable_chunk_id() = writer->ChunkId.ToProto();
    req->set_start_block_index(StartBlockIndex);
    req->Attachments().insert(req->Attachments().begin(), Blocks.begin(), Blocks.end());

    LOG_DEBUG("Putting blocks %d-%d to %s",
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~node->Address);

    return req->Invoke();
}

void TRemoteWriter::TGroup::OnPutBlocks(TNodePtr node, TProxy::TRspPutBlocksPtr rsp)
{
    auto writer = Writer.Lock();
    if (!writer)
        return;

    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    IsSent[node->Index] = true;

    LOG_DEBUG("Blocks %d-%d are put to %s",
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~node->Address);
}

void TRemoteWriter::TGroup::SendGroup(TNodePtr srcNode)
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    for (int dstNodIndex = 0; dstNodIndex < IsSent.size(); ++dstNodIndex) {
        auto dstNod = writer->Nodes[dstNodIndex];
        if (dstNod->IsAlive && !IsSent[dstNodIndex]) {
            auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
            auto onResponse = BIND(
                &TGroup::CheckSendResponse,
                MakeWeak(this),
                srcNode,
                dstNod);
            awaiter->Await(SendBlocks(srcNode, dstNod), onResponse);
            awaiter->Complete(BIND(&TGroup::Process, MakeWeak(this)));
            break;
        }
    }
}

TRemoteWriter::TProxy::TInvSendBlocks
TRemoteWriter::TGroup::SendBlocks(
    TNodePtr srcNode, 
    TNodePtr dstNod)
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    LOG_DEBUG("Sending blocks %d-%d from %s to %s",
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~srcNode->Address,
        ~dstNod->Address);

    auto req = srcNode->Proxy.SendBlocks();
    *req->mutable_chunk_id() = writer->ChunkId.ToProto();
    req->set_start_block_index(StartBlockIndex);
    req->set_block_count(Blocks.size());
    req->set_address(dstNod->Address);
    return req->Invoke();
}

void TRemoteWriter::TGroup::CheckSendResponse(
    TNodePtr srcNode,
    TNodePtr dstNod,
    TRemoteWriter::TProxy::TRspSendBlocksPtr rsp)
{
    auto writer = Writer.Lock();
    if (!writer)
        return;

    if (rsp->GetErrorCode() == EErrorCode::PutBlocksFailed) {
        writer->OnNodeFailed(dstNod);
        return;
    }

    auto onSuccess = BIND(
        &TGroup::OnSentBlocks, 
        Unretained(this), // No need for a smart pointer here -- we're invoking action directly.
        srcNode, 
        dstNod);

    writer->CheckResponse<TRemoteWriter::TProxy::TRspSendBlocks>(
        srcNode, 
        onSuccess,
        &writer->SendBlocksTiming,
        rsp);
}

void TRemoteWriter::TGroup::OnSentBlocks(
    TNodePtr srcNode, 
    TNodePtr dstNod,
    TProxy::TRspSendBlocksPtr rsp)
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    LOG_DEBUG("Blocks %d-%d are sent from %s to %s",
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~srcNode->Address,
        ~dstNod->Address);

    IsSent[dstNod->Index] = true;
}

bool TRemoteWriter::TGroup::IsFlushing() const
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    return IsFlushing_;
}

void TRemoteWriter::TGroup::SetFlushing()
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    IsFlushing_ = true;
}

void TRemoteWriter::TGroup::Process()
{
    auto writer = Writer.Lock();
    if (!writer)
        return;

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    if (!writer->State.IsActive()) {
        return;
    }

    YASSERT(writer->IsInitComplete);

    LOG_DEBUG("Processing blocks %d-%d",
        StartBlockIndex, 
        GetEndBlockIndex());

    TNodePtr nodeWithBlocks;
    bool emptyHolderFound = false;
    for (int nodeIndex = 0; nodeIndex < IsSent.size(); ++nodeIndex) {
        auto node = writer->Nodes[nodeIndex];
        if (node->IsAlive) {
            if (IsSent[nodeIndex]) {
                nodeWithBlocks = node;
            } else {
                emptyHolderFound = true;
            }
        }
    }

    if (!emptyHolderFound) {
        writer->ShiftWindow();
    } else if (!nodeWithBlocks) {
        PutGroup();
    } else {
        SendGroup(nodeWithBlocks);
    }
}

///////////////////////////////////////////////////////////////////////////////

TRemoteWriter::TImpl::TImpl(
    const TRemoteWriterConfigPtr& config, 
    const TChunkId& chunkId,
    const std::vector<Stroka>& addresses)
    : Config(config)
    , ChunkId(chunkId) 
    , Addresses(addresses)
    , IsOpen(false)
    , IsInitComplete(false)
    , IsClosing(false)
    , IsCloseRequested(false)
    , WindowSlots(config->WindowSize)
    , AliveNodeCount(addresses.size())
    , CurrentGroup(New<TGroup>(AliveNodeCount, 0, this))
    , BlockCount(0)
    , StartChunkTiming(0, 1000, 20)
    , PutBlocksTiming(0, 1000, 20)
    , SendBlocksTiming(0, 1000, 20)
    , FlushBlockTiming(0, 1000, 20)
    , FinishChunkTiming(0, 1000, 20)
    , Logger(ChunkWriterLogger)
{
    YASSERT(AliveNodeCount > 0);

    Logger.AddTag(Sprintf("ChunkId: %s", ~ChunkId.ToString()));

    for (int index = 0; index < static_cast<int>(addresses.size()); ++index) {
        auto address = addresses[index];
        auto node = New<TNode>(index, address);
        node->Proxy.SetDefaultTimeout(Config->HolderRpcTimeout);
        node->PingInvoker = New<TPeriodicInvoker>(
            WriterThread->GetInvoker(),
            BIND(&TRemoteWriter::TImpl::SendPing, MakeWeak(this), node),
            Config->SessionPingInterval);
        Nodes.push_back(node);
    }
}

TRemoteWriter::TImpl::~TImpl()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Just a quick check.
    if (!State.IsActive())
        return;

    LOG_DEBUG("Writer canceled");
    State.Cancel(TError(TError::Fail, "Writer canceled"));
}

void TRemoteWriter::TImpl::Open()
{
    LOG_DEBUG("Opening writer (Addresses: [%s])", ~JoinToString(Addresses));

    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    FOREACH (auto node, Nodes) {
        auto onSuccess = BIND(
            &TRemoteWriter::TImpl::OnChunkStarted,
            MakeWeak(this), 
            node);
        auto onResponse = BIND(
            &TImpl::CheckResponse<TProxy::TRspStartChunk>,
            MakeWeak(this),
            node,
            onSuccess,
            &StartChunkTiming);
        awaiter->Await(StartChunk(node), onResponse);
    }
    awaiter->Complete(BIND(&TImpl::OnSessionStarted, MakeWeak(this)));

    IsOpen = true;
}

void TRemoteWriter::TImpl::ShiftWindow()
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
    FOREACH (auto node, Nodes) {
        if (node->IsAlive) {
            auto onSuccess = BIND(
                &TImpl::OnBlockFlushed,
                MakeWeak(this), 
                node,
                lastFlushableBlock);
            auto onResponse = BIND(
                &TImpl::CheckResponse<TProxy::TRspFlushBlock>,
                MakeWeak(this), 
                node, 
                onSuccess,
                &FlushBlockTiming);
            awaiter->Await(FlushBlock(node, lastFlushableBlock), onResponse);
        }
    }

    awaiter->Complete(BIND(
        &TImpl::OnWindowShifted,
        MakeWeak(this),
        lastFlushableBlock));
}

TRemoteWriter::TProxy::TInvFlushBlock
TRemoteWriter::TImpl::FlushBlock(TNodePtr node, int blockIndex)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Flushing block %d at %s",
        blockIndex,
        ~node->Address);

    auto req = node->Proxy.FlushBlock();
    *req->mutable_chunk_id() = ChunkId.ToProto();
    req->set_block_index(blockIndex);
    return req->Invoke();
}

void TRemoteWriter::TImpl::OnBlockFlushed(TNodePtr node, int blockIndex, TProxy::TRspFlushBlockPtr rsp)
{
    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Block %d is flushed at %s",
        blockIndex,
        ~node->Address);
}

void TRemoteWriter::TImpl::OnWindowShifted(int lastFlushedBlock)
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

        LOG_DEBUG("Window %d-%d shifted (Size: %" PRId64 ")",
            group->GetStartBlockIndex(),
            group->GetEndBlockIndex(),
            group->GetSize());

        WindowSlots.Release(group->GetSize());
        Window.pop_front();
    }

    if (State.IsActive() && IsCloseRequested) {
        CloseSession();
    }
}

void TRemoteWriter::TImpl::AddGroup(TGroupPtr group)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YASSERT(!IsCloseRequested);

    if (!State.IsActive())
        return;

    LOG_DEBUG("Added block group (Group: %p, BlockIndexes: %d-%d)",
        ~group,
        group->GetStartBlockIndex(),
        group->GetEndBlockIndex());

    Window.push_back(group);

    if (IsInitComplete) {
        group->Process();
    }
}

void TRemoteWriter::TImpl::OnNodeFailed(TNodePtr node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!node->IsAlive)
        return;

    node->IsAlive = false;
    --AliveNodeCount;

    LOG_INFO("Node %s failed, %d nodes remaining",
        ~node->Address,
        AliveNodeCount);

    if (State.IsActive() && AliveNodeCount == 0) {
        TError error(
            TError::Fail,
            Sprintf("All target nodes [%s] have failed", ~JoinToString(Addresses)));
        LOG_WARNING("Chunk writer failed\n%s", ~error.ToString());
        State.Fail(error);
    }
}

template <class TResponse>
void TRemoteWriter::TImpl::CheckResponse(
    TNodePtr node,
    TCallback<void(TIntrusivePtr<TResponse>)> onSuccess, 
    TMetric* metric,
    TIntrusivePtr<TResponse> rsp)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (rsp->IsOK()) {
        metric->AddDelta(rsp->GetStartTime());
        onSuccess.Run(rsp);
    } else {
        // TODO: retry?
        LOG_ERROR("Error reported by node %s\n%s",
            ~node->Address, 
            ~rsp->GetError().ToString());
        OnNodeFailed(node);
    }
}

TRemoteWriter::TProxy::TInvStartChunk
TRemoteWriter::TImpl::StartChunk(TNodePtr node)
{
    LOG_DEBUG("Starting chunk session at %s", ~node->Address);

    auto req = node->Proxy.StartChunk();
    *req->mutable_chunk_id() = ChunkId.ToProto();
    return req->Invoke();
}

void TRemoteWriter::TImpl::OnChunkStarted(TNodePtr node, TProxy::TRspStartChunkPtr rsp)
{
    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Chunk session started at %s", ~node->Address);

    StartPing(node);
}

void TRemoteWriter::TImpl::OnSessionStarted()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Check if the session is not canceled yet.
    if (!State.IsActive()) {
        return;
    }

    LOG_DEBUG("Writer is ready");

    IsInitComplete = true;
    FOREACH (auto& group, Window) {
        group->Process();
    }

    // Possible for an empty chunk.
    if (Window.empty() && IsCloseRequested) {
        CloseSession();
    }
}

void TRemoteWriter::TImpl::CloseSession()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    YASSERT(IsCloseRequested);

    LOG_DEBUG("Closing writer");

    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    FOREACH (auto node, Nodes) {
        if (node->IsAlive) {
            auto onSuccess = BIND(
                &TImpl::OnChunkFinished,
                MakeWeak(this), 
                node);
            auto onResponse = BIND(
                &TImpl::CheckResponse<TProxy::TRspFinishChunk>,
                MakeWeak(this), 
                node, 
                onSuccess, 
                &FinishChunkTiming);
            awaiter->Await(FinishChunk(node), onResponse);
        }
    }
    awaiter->Complete(BIND(&TImpl::OnSessionFinished, MakeWeak(this)));
}

void TRemoteWriter::TImpl::OnChunkFinished(TNodePtr node, TProxy::TRspFinishChunkPtr rsp)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto& chunkInfo = rsp->chunk_info();
    LOG_DEBUG("Chunk session is finished at %s (Size: %" PRId64 ")",
        ~node->Address,
        chunkInfo.size());

    // If ChunkInfo is set.
    if (ChunkInfo.has_size()) {
        if (ChunkInfo.meta_checksum() != chunkInfo.meta_checksum() ||
            ChunkInfo.size() != chunkInfo.size()) 
        {
            LOG_FATAL("Mismatched chunk info reported by %s (KnownInfo: {%s}, NewInfo: {%s})",
                ~node->Address,
                ~ChunkInfo.DebugString(),
                ~chunkInfo.DebugString());
        }
    } else {
        ChunkInfo = chunkInfo;
    }
}

TRemoteWriter::TProxy::TInvFinishChunk
TRemoteWriter::TImpl::FinishChunk(TNodePtr node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Finishing chunk session at %s", ~node->Address);

    auto req = node->Proxy.FinishChunk();
    *req->mutable_chunk_id() = ChunkId.ToProto();
    *req->mutable_chunk_meta() = ChunkMeta;
    return req->Invoke();
}

void TRemoteWriter::TImpl::OnSessionFinished()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    YASSERT(Window.empty());

    if (State.IsActive()) {
        State.Close();
    }

    CancelAllPings();

    LOG_DEBUG("Writer closed");

    State.FinishOperation();
}

void TRemoteWriter::TImpl::SendPing(TNodePtr node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Sending ping to %s", ~node->Address);

    auto req = node->Proxy.PingSession();
    *req->mutable_chunk_id() = ChunkId.ToProto();
    req->Invoke();

    node->PingInvoker->ScheduleNext();
}

void TRemoteWriter::TImpl::StartPing( TNodePtr node )
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    node->PingInvoker->Start();
}

void TRemoteWriter::TImpl::CancelPing(TNodePtr node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    node->PingInvoker->Stop();
}

void TRemoteWriter::TImpl::CancelAllPings()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    FOREACH (auto node, Nodes) {
        CancelPing(node);
    }
}

TAsyncError TRemoteWriter::TImpl::AsyncWriteBlock(const TSharedRef& block)
{
    YASSERT(IsOpen);
    YASSERT(!IsClosing);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    State.StartOperation();

    WindowSlots.AsyncAcquire(block.Size()).Subscribe(BIND(
        &TImpl::DoWriteBlock,
        MakeWeak(this),
        block));

    return State.GetOperationError();
}

void TRemoteWriter::TImpl::DoWriteBlock(const TSharedRef& block)
{
    if (State.IsActive()) {
        AddBlock(block);
    }

    State.FinishOperation();
}

void TRemoteWriter::TImpl::AddBlock(const TSharedRef& block)
{
    YASSERT(!IsClosing);

    CurrentGroup->AddBlock(block);
    ++BlockCount;

    LOG_DEBUG("Added block %d (Group: %p, Size: %" PRISZT ")",
        BlockCount,
        ~CurrentGroup,
        block.Size());
    if (CurrentGroup->GetSize() >= Config->GroupSize) {
        WriterThread->GetInvoker()->Invoke(BIND(
            &TImpl::AddGroup,
            MakeWeak(this),
            CurrentGroup));
        // Construct a new (empty) group.
        CurrentGroup = New<TGroup>(Nodes.size(), BlockCount, this);
    }
}

void TRemoteWriter::TImpl::DoClose()
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YASSERT(!IsCloseRequested);

    LOG_DEBUG("Writer close requested");

    if (!State.IsActive()) {
        State.FinishOperation();
        return;
    }

    if (CurrentGroup->GetSize() > 0) {
        AddGroup(CurrentGroup);
    }

    IsCloseRequested = true;

    if (Window.empty() && IsInitComplete) {
        CloseSession();
    }
}

TAsyncError TRemoteWriter::TImpl::AsyncClose(const NChunkHolder::NProto::TChunkMeta& chunkMeta)
{
    YASSERT(IsOpen);
    YASSERT(!IsClosing);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    IsClosing = true;
    ChunkMeta = chunkMeta;

    LOG_DEBUG("Requesting writer to close");
    State.StartOperation();

    WriterThread->GetInvoker()->Invoke(BIND(&TImpl::DoClose, MakeWeak(this)));

    return State.GetOperationError();
}

Stroka TRemoteWriter::TImpl::GetDebugInfo()
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

const NChunkHolder::NProto::TChunkInfo& TRemoteWriter::TImpl::GetChunkInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    return ChunkInfo;
}

const std::vector<Stroka> TRemoteWriter::TImpl::GetNodeAddresses() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    std::vector<Stroka> addresses;
    FOREACH (auto node, Nodes) {
        if (node->IsAlive) {
            addresses.push_back(node->Address);
        }
    }
    return addresses;
}

const TChunkId& TRemoteWriter::TImpl::GetChunkId() const
{
    return ChunkId;
}

///////////////////////////////////////////////////////////////////////////////

TRemoteWriter::TRemoteWriter(
    const TRemoteWriterConfigPtr& config,
    const TChunkId& chunkId,
    const std::vector<Stroka>& addresses)
    : Impl(New<TImpl>(config, chunkId, addresses))
{ }

TRemoteWriter::~TRemoteWriter()
{ }

void TRemoteWriter::Open()
{
    Impl->Open();
}

TAsyncError TRemoteWriter::AsyncWriteBlock(const TSharedRef& block)
{
    Impl->AsyncWriteBlock(block);
}

TAsyncError TRemoteWriter::AsyncClose(const NChunkHolder::NProto::TChunkMeta& chunkMeta)
{
    Impl->AsyncClose(chunkMeta);
}

const NChunkHolder::NProto::TChunkInfo& TRemoteWriter::GetChunkInfo() const
{
    Impl->GetChunkInfo();
}

const std::vector<Stroka> TRemoteWriter::GetNodeAddresses() const
{
    Impl->GetNodeAddresses();
}

const TChunkId& TRemoteWriter::GetChunkId() const
{
    Impl->GetChunkId();
}

Stroka TRemoteWriter::GetDebugInfo()
{
    Impl->GetDebugInfo();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
