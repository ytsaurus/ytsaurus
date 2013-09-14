#include "stdafx.h"
#include "replication_writer.h"
#include "config.h"
#include "dispatcher.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "chunk_ypath_proxy.h"
#include "data_node_service_proxy.h"

#include <core/misc/metric.h>
#include <core/misc/string.h>
#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/async_stream_state.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/action_queue.h>
#include <core/concurrency/async_semaphore.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/chunk_client/data_node_service.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <util/generic/deque.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

using namespace NRpc;
using namespace NNodeTrackerClient;
using namespace NConcurrency;

typedef TDataNodeServiceProxy TProxy;

///////////////////////////////////////////////////////////////////////////////

static auto& Logger = ChunkWriterLogger;

///////////////////////////////////////////////////////////////////////////////

class TReplicationWriter;

struct TNode
    : public TRefCounted
{
    int Index;
    TError Error;
    TNodeDescriptor Descriptor;
    TProxy LightProxy;
    TProxy HeavyProxy;
    TPeriodicExecutorPtr PingExecutor;

    TNode(int index, const TNodeDescriptor& descriptor)
        : Index(index)
        , Descriptor(descriptor)
        , LightProxy(LightNodeChannelCache->GetChannel(descriptor.Address))
        , HeavyProxy(HeavyNodeChannelCache->GetChannel(descriptor.Address))
    { }

    bool IsAlive() const
    {
        return Error.IsOK();
    }

    void MarkFailed(const TError& error)
    {
        Error = error;
    }
};

typedef TIntrusivePtr<TNode> TNodePtr;
typedef TWeakPtr<TNode> TNodeWeakPtr;

///////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TRefCounted
{
public:
    TGroup(
        int nodeCount,
        int startBlockIndex,
        TReplicationWriter* writer);

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
    std::vector<bool> IsSentTo;

    std::vector<TSharedRef> Blocks;
    int StartBlockIndex;

    i64 Size;

    TWeakPtr<TReplicationWriter> Writer;

    NLog::TTaggedLogger Logger;

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
        TProxy::TRspSendBlocksPtr rsp);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void OnSentBlocks(TNodePtr srcNode, TNodePtr dstNode, TProxy::TRspSendBlocksPtr rsp);
};

typedef TIntrusivePtr<TGroup> TGroupPtr;
typedef ydeque<TGroupPtr> TWindow;

///////////////////////////////////////////////////////////////////////////////

class TReplicationWriter
    : public IAsyncWriter
{
public:
    TReplicationWriter(
        TReplicationWriterConfigPtr config,
        const TChunkId& chunkId,
        const std::vector<TNodeDescriptor>& targets,
        EWriteSessionType sessionType,
        IThroughputThrottlerPtr throttler);

    ~TReplicationWriter();

    virtual void Open() override;

    virtual bool WriteBlock(const TSharedRef& block) override;
    virtual TAsyncError GetReadyEvent() override;

    virtual TAsyncError AsyncClose(const NChunkClient::NProto::TChunkMeta& chunkMeta) override;

    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override;
    virtual const std::vector<int> GetWrittenIndexes() const override;

    Stroka GetDebugInfo();

private:
    friend class TGroup;

    TReplicationWriterConfigPtr Config;
    TChunkId ChunkId;
    std::vector<TNodeDescriptor> Targets;
    EWriteSessionType SessionType;
    IThroughputThrottlerPtr Throttler;

    TAsyncStreamState State;

    bool IsOpen;
    bool IsInitComplete;
    bool IsClosing;

    //! This flag is raised whenever #Close is invoked.
    //! All access to this flag happens from #WriterThread.
    bool IsCloseRequested;
    NChunkClient::NProto::TChunkMeta ChunkMeta;

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
    NChunkClient::NProto::TChunkInfo ChunkInfo;

    TMetric StartChunkTiming;
    TMetric PutBlocksTiming;
    TMetric SendBlocksTiming;
    TMetric FlushBlockTiming;
    TMetric FinishChunkTiming;

    NLog::TTaggedLogger Logger;

    void DoClose();

    void AddGroup(TGroupPtr group);

    void RegisterReadyEvent(TFuture<void> windowReady);

    void OnNodeFailed(TNodePtr node, const TError& error);

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

    void SendPing(TNodeWeakPtr node);
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

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);
};

typedef TIntrusivePtr<TReplicationWriter> TReplicationWriterPtr;

///////////////////////////////////////////////////////////////////////////////

TGroup::TGroup(
    int nodeCount,
    int startBlockIndex,
    TReplicationWriter* writer)
    : IsFlushing_(false)
    , IsSentTo(nodeCount, false)
    , StartBlockIndex(startBlockIndex)
    , Size(0)
    , Writer(writer)
    , Logger(writer->Logger)
{ }

void TGroup::AddBlock(const TSharedRef& block)
{
    Blocks.push_back(block);
    Size += block.Size();
}

int TGroup::GetStartBlockIndex() const
{
    return StartBlockIndex;
}

int TGroup::GetEndBlockIndex() const
{
    return StartBlockIndex + Blocks.size() - 1;
}

i64 TGroup::GetSize() const
{
    return Size;
}

bool TGroup::IsWritten() const
{
    auto writer = Writer.Lock();
    YCHECK(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    for (int nodeIndex = 0; nodeIndex < IsSentTo.size(); ++nodeIndex) {
        if (writer->Nodes[nodeIndex]->IsAlive() && !IsSentTo[nodeIndex]) {
            return false;
        }
    }
    return true;
}

void TGroup::PutGroup()
{
    auto writer = Writer.Lock();
    YCHECK(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    int nodeIndex = 0;
    while (!writer->Nodes[nodeIndex]->IsAlive()) {
        ++nodeIndex;
        YCHECK(nodeIndex < writer->Nodes.size());
    }

    auto node = writer->Nodes[nodeIndex];
    auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker());
    auto onSuccess = BIND(
        &TGroup::OnPutBlocks,
        MakeWeak(this),
        node);
    auto onResponse = BIND(
        &TReplicationWriter::CheckResponse<TProxy::TRspPutBlocks>,
        Writer,
        node,
        onSuccess,
        &writer->PutBlocksTiming);
    awaiter->Await(PutBlocks(node), onResponse);
    awaiter->Complete(BIND(
        &TGroup::Process,
        MakeWeak(this)));
}

TProxy::TInvPutBlocks TGroup::PutBlocks(TNodePtr node)
{
    auto writer = Writer.Lock();
    YCHECK(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    auto req = node->HeavyProxy.PutBlocks();
    ToProto(req->mutable_chunk_id(), writer->ChunkId);
    req->set_start_block_index(StartBlockIndex);
    req->Attachments().insert(req->Attachments().begin(), Blocks.begin(), Blocks.end());
    req->set_enable_caching(writer->Config->EnableNodeCaching);

    LOG_DEBUG("Ready to put blocks (Blocks: %d-%d, Address: %s, Size: %" PRId64 ")",
        StartBlockIndex,
        GetEndBlockIndex(),
        ~node->Descriptor.Address,
        Size);

    auto this_ = MakeStrong(this);
    return writer->Throttler->Throttle(Size).Apply(BIND([this, this_, req, node] () -> TProxy::TInvPutBlocks {
        LOG_DEBUG("Putting blocks (Blocks: %d-%d, Address: %s)",
            StartBlockIndex,
            GetEndBlockIndex(),
            ~node->Descriptor.Address);

        return req->Invoke();
    }));
}

void TGroup::OnPutBlocks(TNodePtr node, TProxy::TRspPutBlocksPtr rsp)
{
    auto writer = Writer.Lock();
    if (!writer)
        return;

    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    IsSentTo[node->Index] = true;

    LOG_DEBUG("Blocks are put (Blocks: %d-%d, Address: %s)",
        StartBlockIndex,
        GetEndBlockIndex(),
        ~node->Descriptor.Address);
}

void TGroup::SendGroup(TNodePtr srcNode)
{
    auto writer = Writer.Lock();
    YCHECK(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    for (int dstNodeIndex = 0; dstNodeIndex < IsSentTo.size(); ++dstNodeIndex) {
        auto dstNode = writer->Nodes[dstNodeIndex];
        if (dstNode->IsAlive() && !IsSentTo[dstNodeIndex]) {
            auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker());
            auto onResponse = BIND(
                &TGroup::CheckSendResponse,
                MakeWeak(this),
                srcNode,
                dstNode);
            awaiter->Await(SendBlocks(srcNode, dstNode), onResponse);
            awaiter->Complete(BIND(&TGroup::Process, MakeWeak(this)));
            break;
        }
    }
}

TProxy::TInvSendBlocks TGroup::SendBlocks(
    TNodePtr srcNode,
    TNodePtr dstNode)
{
    auto writer = Writer.Lock();
    YCHECK(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    LOG_DEBUG("Sending blocks (Blocks: %d-%d, SrcAddress: %s, DstAddress: %s)",
        StartBlockIndex,
        GetEndBlockIndex(),
        ~srcNode->Descriptor.Address,
        ~dstNode->Descriptor.Address);

    auto req = srcNode->LightProxy.SendBlocks();

    // Set double timeout for SendBlocks since executing it implies another (src->dst) RPC call.
    req->SetTimeout(writer->Config->NodeRpcTimeout + writer->Config->NodeRpcTimeout);
    ToProto(req->mutable_chunk_id(), writer->ChunkId);
    req->set_start_block_index(StartBlockIndex);
    req->set_block_count(Blocks.size());
    ToProto(req->mutable_target(), dstNode->Descriptor);
    return req->Invoke();
}

void TGroup::CheckSendResponse(
    TNodePtr srcNode,
    TNodePtr dstNode,
    TProxy::TRspSendBlocksPtr rsp)
{
    auto writer = Writer.Lock();
    if (!writer)
        return;

    const auto& error = rsp->GetError();
    if (error.GetCode() == EErrorCode::PipelineFailed) {
        writer->OnNodeFailed(dstNode, error);
        return;
    }

    auto onSuccess = BIND(
        &TGroup::OnSentBlocks,
        Unretained(this), // No need for a smart pointer here -- we're invoking action directly.
        srcNode,
        dstNode);

    writer->CheckResponse<TProxy::TRspSendBlocks>(
        srcNode,
        onSuccess,
        &writer->SendBlocksTiming,
        rsp);
}

void TGroup::OnSentBlocks(
    TNodePtr srcNode,
    TNodePtr dstNode,
    TProxy::TRspSendBlocksPtr rsp)
{
    auto writer = Writer.Lock();
    YCHECK(writer);

    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    LOG_DEBUG("Blocks are sent (Blocks: %d-%d, SrcAddress: %s, DstAddress: %s)",
        StartBlockIndex,
        GetEndBlockIndex(),
        ~srcNode->Descriptor.Address,
        ~dstNode->Descriptor.Address);

    IsSentTo[dstNode->Index] = true;
}

bool TGroup::IsFlushing() const
{
    auto writer = Writer.Lock();
    YCHECK(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    return IsFlushing_;
}

void TGroup::SetFlushing()
{
    auto writer = Writer.Lock();
    YCHECK(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    IsFlushing_ = true;
}

void TGroup::Process()
{
    auto writer = Writer.Lock();
    if (!writer)
        return;

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    if (!writer->State.IsActive()) {
        return;
    }

    YCHECK(writer->IsInitComplete);

    LOG_DEBUG("Processing blocks (Blocks: %d-%d)",
        StartBlockIndex,
        GetEndBlockIndex());

    TNodePtr nodeWithBlocks;
    bool emptyHolderFound = false;
    for (int nodeIndex = 0; nodeIndex < IsSentTo.size(); ++nodeIndex) {
        auto node = writer->Nodes[nodeIndex];
        if (node->IsAlive()) {
            if (IsSentTo[nodeIndex]) {
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

TReplicationWriter::TReplicationWriter(
    TReplicationWriterConfigPtr config,
    const TChunkId& chunkId,
    const std::vector<TNodeDescriptor>& targets,
    EWriteSessionType sessionType,
    IThroughputThrottlerPtr throttler)
    : Config(config)
    , ChunkId(chunkId)
    , Targets(targets)
    , SessionType(sessionType)
    , Throttler(throttler)
    , IsOpen(false)
    , IsInitComplete(false)
    , IsClosing(false)
    , IsCloseRequested(false)
    , WindowSlots(config->SendWindowSize)
    , AliveNodeCount(targets.size())
    , BlockCount(0)
    , StartChunkTiming(0, 1000, 20)
    , PutBlocksTiming(0, 1000, 20)
    , SendBlocksTiming(0, 1000, 20)
    , FlushBlockTiming(0, 1000, 20)
    , FinishChunkTiming(0, 1000, 20)
    , Logger(ChunkWriterLogger)
{
    YCHECK(!targets.empty());

    Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(ChunkId)));

    CurrentGroup = New<TGroup>(AliveNodeCount, 0, this);

    for (int index = 0; index < static_cast<int>(targets.size()); ++index) {
        auto replica = targets[index];
        auto node = New<TNode>(index, Targets[index]);
        node->LightProxy.SetDefaultTimeout(Config->NodeRpcTimeout);
        node->HeavyProxy.SetDefaultTimeout(Config->NodeRpcTimeout);
        node->PingExecutor = New<TPeriodicExecutor>(
            TDispatcher::Get()->GetWriterInvoker(),
            BIND(&TReplicationWriter::SendPing, MakeWeak(this), MakeWeak(node)),
            Config->NodePingInterval);
        Nodes.push_back(node);
    }
}

TReplicationWriter::~TReplicationWriter()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Just a quick check.
    if (!State.IsActive())
        return;

    LOG_INFO("Writer canceled");
    State.Cancel(TError("Writer canceled"));
    CancelAllPings();
}

void TReplicationWriter::Open()
{
    LOG_INFO("Opening writer (Addresses: [%s], EnableCaching: %s, SessionType: %s)",
        ~JoinToString(Targets),
        ~FormatBool(Config->EnableNodeCaching),
        ~SessionType.ToString());

    auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker());
    FOREACH (auto node, Nodes) {
        auto onSuccess = BIND(
            &TReplicationWriter::OnChunkStarted,
            MakeWeak(this),
            node);
        auto onResponse = BIND(
            &TReplicationWriter::CheckResponse<TProxy::TRspStartChunk>,
            MakeWeak(this),
            node,
            onSuccess,
            &StartChunkTiming);
        awaiter->Await(StartChunk(node), onResponse);
    }
    awaiter->Complete(BIND(&TReplicationWriter::OnSessionStarted, MakeWeak(this)));

    IsOpen = true;
}

void TReplicationWriter::ShiftWindow()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!State.IsActive()) {
        YCHECK(Window.empty());
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

    auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker());
    FOREACH (auto node, Nodes) {
        if (node->IsAlive()) {
            auto onSuccess = BIND(
                &TReplicationWriter::OnBlockFlushed,
                MakeWeak(this),
                node,
                lastFlushableBlock);
            auto onResponse = BIND(
                &TReplicationWriter::CheckResponse<TProxy::TRspFlushBlock>,
                MakeWeak(this),
                node,
                onSuccess,
                &FlushBlockTiming);
            awaiter->Await(FlushBlock(node, lastFlushableBlock), onResponse);
        }
    }

    awaiter->Complete(BIND(
        &TReplicationWriter::OnWindowShifted,
        MakeWeak(this),
        lastFlushableBlock));
}

TProxy::TInvFlushBlock TReplicationWriter::FlushBlock(TNodePtr node, int blockIndex)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Flushing block (Block: %d, Address: %s)",
        blockIndex,
        ~node->Descriptor.Address);

    auto req = node->LightProxy.FlushBlock();
    ToProto(req->mutable_chunk_id(), ChunkId);
    req->set_block_index(blockIndex);
    return req->Invoke();
}

void TReplicationWriter::OnBlockFlushed(TNodePtr node, int blockIndex, TProxy::TRspFlushBlockPtr rsp)
{
    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Block flushed (Block: %d, Address: %s)",
        blockIndex,
        ~node->Descriptor.Address);
}

void TReplicationWriter::OnWindowShifted(int lastFlushedBlock)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (Window.empty()) {
        // This happens when FlushBlocks responses are reordered
        // (i.e. a larger BlockIndex is flushed before a smaller one)
        // We should prevent repeated calls to CloseSession.
        return;
    }

    while (!Window.empty()) {
        auto group = Window.front();
        if (group->GetEndBlockIndex() > lastFlushedBlock)
            return;

        LOG_DEBUG("Window shifted (Blocks: %d-%d, Size: %" PRId64 ")",
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

void TReplicationWriter::AddGroup(TGroupPtr group)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YCHECK(!IsCloseRequested);

    if (!State.IsActive())
        return;

    LOG_DEBUG("Block group added (Group: %p, Blocks: %d-%d)",
        ~group,
        group->GetStartBlockIndex(),
        group->GetEndBlockIndex());

    Window.push_back(group);

    if (IsInitComplete) {
        group->Process();
    }
}

void TReplicationWriter::OnNodeFailed(TNodePtr node, const TError& error)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!node->IsAlive())
        return;

    auto wrappedError = TError("Node %s failed",
        ~node->Descriptor.Address)
        << error;
    LOG_ERROR(wrappedError);

    node->MarkFailed(wrappedError);
    --AliveNodeCount;

    if (State.IsActive() && AliveNodeCount < Config->MinUploadReplicationFactor) {
        TError cumulativeError(
            NChunkClient::EErrorCode::AllTargetNodesFailed,
            "Not enough target nodes to finish upload");
        FOREACH (const auto node, Nodes) {
            YCHECK(!node->IsAlive());
            cumulativeError.InnerErrors().push_back(node->Error);
        }
        LOG_WARNING(cumulativeError, "Chunk writer failed");
        CancelAllPings();
        State.Fail(cumulativeError);
    }
}

template <class TResponse>
void TReplicationWriter::CheckResponse(
    TNodePtr node,
    TCallback<void(TIntrusivePtr<TResponse>)> onSuccess,
    TMetric* metric,
    TIntrusivePtr<TResponse> rsp)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!rsp->IsOK()) {
        OnNodeFailed(node, rsp->GetError());
        return;
    }

    metric->AddDelta(rsp->GetStartTime());
    onSuccess.Run(rsp);
}

TProxy::TInvStartChunk TReplicationWriter::StartChunk(TNodePtr node)
{
    LOG_DEBUG("Starting chunk (Address: %s)", ~node->Descriptor.Address);

    auto req = node->LightProxy.StartChunk();
    ToProto(req->mutable_chunk_id(), ChunkId);
    req->set_session_type(SessionType);
    req->set_sync_on_close(Config->SyncOnClose);
    return req->Invoke();
}

void TReplicationWriter::OnChunkStarted(TNodePtr node, TProxy::TRspStartChunkPtr rsp)
{
    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Chunk started (Address: %s)", ~node->Descriptor.Address);

    StartPing(node);
}

void TReplicationWriter::OnSessionStarted()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Check if the session is not canceled yet.
    if (!State.IsActive()) {
        return;
    }

    LOG_INFO("Writer is ready");

    IsInitComplete = true;
    FOREACH (auto& group, Window) {
        group->Process();
    }

    // Possible for an empty chunk.
    if (Window.empty() && IsCloseRequested) {
        CloseSession();
    }
}

void TReplicationWriter::CloseSession()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    YCHECK(IsCloseRequested);

    LOG_INFO("Closing writer");

    auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker());
    FOREACH (auto node, Nodes) {
        if (node->IsAlive()) {
            auto onSuccess = BIND(
                &TReplicationWriter::OnChunkFinished,
                MakeWeak(this),
                node);
            auto onResponse = BIND(
                &TReplicationWriter::CheckResponse<TProxy::TRspFinishChunk>,
                MakeWeak(this),
                node,
                onSuccess,
                &FinishChunkTiming);
            awaiter->Await(FinishChunk(node), onResponse);
        }
    }
    awaiter->Complete(BIND(&TReplicationWriter::OnSessionFinished, MakeWeak(this)));
}

void TReplicationWriter::OnChunkFinished(TNodePtr node, TProxy::TRspFinishChunkPtr rsp)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto& chunkInfo = rsp->chunk_info();
    LOG_DEBUG("Chunk finished (Address: %s, DiskSpace: %" PRId64 ")",
        ~node->Descriptor.Address,
        chunkInfo.disk_space());

    // If ChunkInfo is set.
    if (ChunkInfo.has_disk_space()) {
        if (ChunkInfo.meta_checksum() != chunkInfo.meta_checksum() ||
            ChunkInfo.disk_space() != chunkInfo.disk_space())
        {
            LOG_FATAL("Mismatched chunk info reported by node (Address: %s, ExpectedInfo: {%s}, ReceivedInfo: {%s})",
                ~node->Descriptor.Address,
                ~ChunkInfo.DebugString(),
                ~chunkInfo.DebugString());
        }
    } else {
        ChunkInfo = chunkInfo;
    }
}

TProxy::TInvFinishChunk TReplicationWriter::FinishChunk(TNodePtr node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Finishing chunk (Address: %s)",
        ~node->Descriptor.Address);

    auto req = node->LightProxy.FinishChunk();
    ToProto(req->mutable_chunk_id(), ChunkId);
    *req->mutable_chunk_meta() = ChunkMeta;
    req->set_block_count(BlockCount);
    return req->Invoke();
}

void TReplicationWriter::OnSessionFinished()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    YCHECK(Window.empty());

    if (State.IsActive()) {
        State.Close();
    }

    CancelAllPings();

    LOG_INFO("Writer closed");

    State.FinishOperation();
}

void TReplicationWriter::SendPing(TNodeWeakPtr node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto node_ = node.Lock();
    if (!node_) {
        return;
    }

    LOG_DEBUG("Sending ping (Address: %s)",
        ~node_->Descriptor.Address);

    auto req = node_->LightProxy.PingSession();
    ToProto(req->mutable_chunk_id(), ChunkId);
    req->Invoke();
}

void TReplicationWriter::StartPing(TNodePtr node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    node->PingExecutor->Start();
}

void TReplicationWriter::CancelPing(TNodePtr node)
{
    node->PingExecutor->Stop();
}

void TReplicationWriter::CancelAllPings()
{
    // No thread affinity - called from dtor.
    FOREACH (auto node, Nodes) {
        CancelPing(node);
    }
}

bool TReplicationWriter::WriteBlock(const TSharedRef& block)
{
    YCHECK(IsOpen);
    YCHECK(!IsClosing);
    YCHECK(!State.IsClosed());

    WindowSlots.Acquire(block.Size());
    TDispatcher::Get()->GetWriterInvoker()->Invoke(BIND(
        &TReplicationWriter::AddBlock,
        MakeWeak(this),
        block));

    return WindowSlots.IsReady();
}

TAsyncError TReplicationWriter::GetReadyEvent()
{
    YCHECK(IsOpen);
    YCHECK(!IsClosing);
    YCHECK(!State.HasRunningOperation());
    YCHECK(!State.IsClosed());

    if (!WindowSlots.IsReady()) {
        State.StartOperation();

        auto this_ = MakeStrong(this);
        WindowSlots.GetReadyEvent().Subscribe(BIND([this, this_] () {
            State.FinishOperation(TError());
        }));
    }

    return State.GetOperationError();
}

void TReplicationWriter::AddBlock(const TSharedRef& block)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YCHECK(!IsCloseRequested);

    if (!State.IsActive())
        return;

    CurrentGroup->AddBlock(block);

    LOG_DEBUG("Block added (Block: %d, Group: %p, Size: %" PRISZT ")",
        BlockCount,
        ~CurrentGroup,
        block.Size());

    ++BlockCount;

    if (CurrentGroup->GetSize() >= Config->GroupSize) {
        AddGroup(CurrentGroup);
        // Construct a new (empty) group.
        CurrentGroup = New<TGroup>(Nodes.size(), BlockCount, this);
    }
}

void TReplicationWriter::DoClose()
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YCHECK(!IsCloseRequested);

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

TAsyncError TReplicationWriter::AsyncClose(const NChunkClient::NProto::TChunkMeta& chunkMeta)
{
    YCHECK(IsOpen);
    YCHECK(!IsClosing);
    YCHECK(!State.HasRunningOperation());
    YCHECK(!State.IsClosed());

    IsClosing = true;
    ChunkMeta = chunkMeta;

    LOG_DEBUG("Requesting writer to close");
    State.StartOperation();

    TDispatcher::Get()->GetWriterInvoker()->Invoke(
        BIND(&TReplicationWriter::DoClose, MakeWeak(this)));

    return State.GetOperationError();
}

Stroka TReplicationWriter::GetDebugInfo()
{
    return Sprintf(
        "ChunkId: %s; "
        "StartChunk: (%s); "
        "FinishChunk timing: (%s); "
        "PutBlocks timing: (%s); "
        "SendBlocks timing: (%s); "
        "FlushBlocks timing: (%s); ",
        ~ToString(ChunkId),
        ~StartChunkTiming.GetDebugInfo(),
        ~FinishChunkTiming.GetDebugInfo(),
        ~PutBlocksTiming.GetDebugInfo(),
        ~SendBlocksTiming.GetDebugInfo(),
        ~FlushBlockTiming.GetDebugInfo());
}

const NChunkClient::NProto::TChunkInfo& TReplicationWriter::GetChunkInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ChunkInfo;
}

const std::vector<int> TReplicationWriter::GetWrittenIndexes() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<int> result;
    FOREACH (auto node, Nodes) {
        if (node->IsAlive()) {
            result.push_back(node->Index);
        }
    }
    return result;
}

///////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr CreateReplicationWriter(
    TReplicationWriterConfigPtr config,
    const TChunkId& chunkId,
    const std::vector<TNodeDescriptor>& targets,
    EWriteSessionType sessionType,
    IThroughputThrottlerPtr throttler)
{
    return New<TReplicationWriter>(
        config,
        chunkId,
        targets,
        sessionType,
        throttler);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT


