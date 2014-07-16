#include "stdafx.h"
#include "replication_writer.h"
#include "writer.h"
#include "config.h"
#include "chunk_meta_extensions.h"
#include "data_node_service_proxy.h"
#include "dispatcher.h"
#include "private.h"

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/concurrency/async_semaphore.h>
#include <core/concurrency/scheduler.h>
#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/thread_affinity.h>

#include <core/misc/async_stream_state.h>
#include <core/misc/nullable.h>

#include <core/logging/log.h>

#include <deque>
#include <atomic>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;

///////////////////////////////////////////////////////////////////////////////

class TReplicationWriter;
typedef TIntrusivePtr<TReplicationWriter> TReplicationWriterPtr;

struct TNode
    : public TRefCounted
{
    int Index;
    TError Error;
    TNodeDescriptor Descriptor;
    TDataNodeServiceProxy LightProxy;
    TDataNodeServiceProxy HeavyProxy;
    TPeriodicExecutorPtr PingExecutor;
    std::atomic_flag Canceled;

    TNode(int index, const TNodeDescriptor& descriptor)
        : Index(index)
        , Descriptor(descriptor)
        , LightProxy(LightNodeChannelFactory->CreateChannel(descriptor.GetDefaultAddress()))
        , HeavyProxy(HeavyNodeChannelFactory->CreateChannel(descriptor.GetDefaultAddress()))
    {
        Canceled.clear();
    }

    bool IsAlive() const
    {
        return Error.IsOK();
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
        TReplicationWriter* writer,
        int startBlockIndex);

    void AddBlock(const TSharedRef& block);
    void ScheduleProcess();
    void SetFlushing();

    bool IsWritten() const;
    bool IsFlushing() const;

    i64 GetSize() const;
    int GetStartBlockIndex() const;
    int GetEndBlockIndex() const;

private:
    bool Flushing_ = false;
    std::vector<bool> SentTo_;

    std::vector<TSharedRef> Blocks_;
    int FirstBlockIndex_;

    i64 Size_ = 0;

    TWeakPtr<TReplicationWriter> Writer_;

    NLog::TLogger Logger;

    void PutGroup(TReplicationWriterPtr writer);
    void SendGroup(TReplicationWriterPtr writer, TNodePtr srcNode);
    void Process();

};

typedef TIntrusivePtr<TGroup> TGroupPtr;
typedef std::deque<TGroupPtr> TWindow;

///////////////////////////////////////////////////////////////////////////////

class TReplicationWriter
    : public IWriter
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
    virtual bool WriteBlocks(const std::vector<TSharedRef>& blocks) override;
    virtual TAsyncError GetReadyEvent() override;

    virtual TAsyncError Close(const TChunkMeta& chunkMeta) override;

    virtual const TChunkInfo& GetChunkInfo() const override;
    virtual TReplicaIndexes GetWrittenReplicaIndexes() const override;

private:
    friend class TGroup;

    TReplicationWriterConfigPtr Config_;
    TChunkId ChunkId_;
    std::vector<TNodeDescriptor> Targets_;
    EWriteSessionType SessionType_;
    IThroughputThrottlerPtr Throttler_;

    TAsyncStreamState State_;

    bool IsOpen_;
    bool IsInitComplete_;
    bool IsClosing_;

    //! This flag is raised whenever #Close is invoked.
    //! All access to this flag happens from #WriterThread.
    bool IsCloseRequested_;
    TChunkMeta ChunkMeta_;

    TWindow Window_;
    TAsyncSemaphore WindowSlots_;

    std::vector<TNodePtr> Nodes_;

    //! Number of nodes that are still alive.
    int AliveNodeCount_;

    const int MinUploadReplicationFactor_;

    //! A new group of blocks that is currently being filled in by the client.
    //! All access to this field happens from client thread.
    TGroupPtr CurrentGroup_;

    //! Number of blocks that are already added via #AddBlocks.
    int BlockCount_;

    //! Returned from node on Finish.
    TChunkInfo ChunkInfo_;

    NLog::TLogger Logger;

    void DoClose();

    void EnsureCurrentGroup();
    void FlushCurrentGroup();

    void OnNodeFailed(TNodePtr node, const TError& error);

    void ShiftWindow();

    void OnWindowShifted(int blockIndex);

    void FlushBlocks(TNodePtr node, int blockIndex);

    void StartChunk(TNodePtr node);

    void OnSessionStarted();

    void CloseSession();

    void FinishChunk(TNodePtr node);

    void OnSessionFinished();

    void SendPing(TNodeWeakPtr node);

    void CancelWriter(bool abort);
    void CancelNode(TNodePtr node, bool abort);

    void AddBlocks(const std::vector<TSharedRef>& blocks);

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);
};

///////////////////////////////////////////////////////////////////////////////

TGroup::TGroup(
    TReplicationWriter* writer,
    int startBlockIndex)
    : SentTo_(writer->Nodes_.size(), false)
    , FirstBlockIndex_(startBlockIndex)
    , Writer_(writer)
    , Logger(writer->Logger)
{ }

void TGroup::AddBlock(const TSharedRef& block)
{
    Blocks_.push_back(block);
    Size_ += block.Size();
}

int TGroup::GetStartBlockIndex() const
{
    return FirstBlockIndex_;
}

int TGroup::GetEndBlockIndex() const
{
    return FirstBlockIndex_ + Blocks_.size() - 1;
}

i64 TGroup::GetSize() const
{
    return Size_;
}

bool TGroup::IsWritten() const
{
    auto writer = Writer_.Lock();
    YCHECK(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    for (int nodeIndex = 0; nodeIndex < SentTo_.size(); ++nodeIndex) {
        if (writer->Nodes_[nodeIndex]->IsAlive() && !SentTo_[nodeIndex]) {
            return false;
        }
    }
    return true;
}

void TGroup::PutGroup(TReplicationWriterPtr writer)
{
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    int nodeIndex = 0;
    while (!writer->Nodes_[nodeIndex]->IsAlive()) {
        ++nodeIndex;
        YCHECK(nodeIndex < writer->Nodes_.size());
    }

    auto node = writer->Nodes_[nodeIndex];

    auto req = node->HeavyProxy.PutBlocks();
    ToProto(req->mutable_chunk_id(), writer->ChunkId_);
    req->set_first_block_index(FirstBlockIndex_);
    req->Attachments().insert(req->Attachments().begin(), Blocks_.begin(), Blocks_.end());
    req->set_enable_caching(writer->Config_->EnableNodeCaching);

    LOG_DEBUG("Ready to put blocks (Blocks: %v-%v, Address: %v, Size: %v)",
        GetStartBlockIndex(),
        GetEndBlockIndex(),
        node->Descriptor.GetDefaultAddress(),
        Size_);

    WaitFor(writer->Throttler_->Throttle(Size_));

    LOG_DEBUG("Putting blocks (Blocks: %v-%v, Address: %v)",
        FirstBlockIndex_,
        GetEndBlockIndex(),
        node->Descriptor.GetDefaultAddress());

    auto rsp = WaitFor(req->Invoke());

    if (rsp->IsOK()) {
        SentTo_[node->Index] = true;

        LOG_DEBUG("Blocks are put (Blocks: %v-%v, Address: %v)",
            GetStartBlockIndex(),
            GetEndBlockIndex(),
            node->Descriptor.GetDefaultAddress());
    } else {
        writer->OnNodeFailed(node, rsp->GetError());
    }

    ScheduleProcess();
}

void TGroup::SendGroup(TReplicationWriterPtr writer, TNodePtr srcNode)
{
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    for (int dstNodeIndex = 0; dstNodeIndex < SentTo_.size(); ++dstNodeIndex) {
        auto dstNode = writer->Nodes_[dstNodeIndex];
        if (dstNode->IsAlive() && !SentTo_[dstNodeIndex]) {
            LOG_DEBUG("Sending blocks (Blocks: %v-%v, SrcAddress: %v, DstAddress: %v)",
                GetStartBlockIndex(),
                GetEndBlockIndex(),
                srcNode->Descriptor.GetDefaultAddress(),
                dstNode->Descriptor.GetDefaultAddress());

            auto req = srcNode->LightProxy.SendBlocks();

            // Set double timeout for SendBlocks since executing it implies another (src->dst) RPC call.
            req->SetTimeout(writer->Config_->NodeRpcTimeout + writer->Config_->NodeRpcTimeout);
            ToProto(req->mutable_chunk_id(), writer->ChunkId_);
            req->set_first_block_index(FirstBlockIndex_);
            req->set_block_count(Blocks_.size());
            ToProto(req->mutable_target(), dstNode->Descriptor);

            auto rsp = WaitFor(req->Invoke());

            if (rsp->IsOK()) {
                LOG_DEBUG("Blocks are sent (Blocks: %v-%v, SrcAddress: %v, DstAddress: %v)",
                    FirstBlockIndex_,
                    GetEndBlockIndex(),
                    srcNode->Descriptor.GetDefaultAddress(),
                    dstNode->Descriptor.GetDefaultAddress());

                SentTo_[dstNode->Index] = true;
            } else {
                auto error = rsp->GetError();
                if (error.GetCode() == EErrorCode::PipelineFailed) {
                    writer->OnNodeFailed(dstNode, error);
                } else {
                    writer->OnNodeFailed(srcNode, error);
                }
            }

            break;
        }
    }

    ScheduleProcess();
}

bool TGroup::IsFlushing() const
{
    auto writer = Writer_.Lock();
    YCHECK(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    return Flushing_;
}

void TGroup::SetFlushing()
{
    auto writer = Writer_.Lock();
    YCHECK(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    Flushing_ = true;
}

void TGroup::ScheduleProcess()
{
    TDispatcher::Get()->GetWriterInvoker()->Invoke(
        BIND(&TGroup::Process, MakeWeak(this)));
}

void TGroup::Process()
{
    auto writer = Writer_.Lock();
    if (!writer || !writer->State_.IsActive())
        return;

    VERIFY_THREAD_AFFINITY(writer->WriterThread);
    YCHECK(writer->IsInitComplete_);

    LOG_DEBUG("Processing blocks (Blocks: %v-%v)",
        FirstBlockIndex_,
        GetEndBlockIndex());

    TNodePtr nodeWithBlocks;
    bool emptyNodeFound = false;
    for (int nodeIndex = 0; nodeIndex < SentTo_.size(); ++nodeIndex) {
        auto node = writer->Nodes_[nodeIndex];
        if (node->IsAlive()) {
            if (SentTo_[nodeIndex]) {
                nodeWithBlocks = node;
            } else {
                emptyNodeFound = true;
            }
        }
    }

    if (!emptyNodeFound) {
        writer->ShiftWindow();
    } else if (!nodeWithBlocks) {
        PutGroup(writer);
    } else {
        SendGroup(writer, nodeWithBlocks);
    }
}

///////////////////////////////////////////////////////////////////////////////

TReplicationWriter::TReplicationWriter(
    TReplicationWriterConfigPtr config,
    const TChunkId& chunkId,
    const std::vector<TNodeDescriptor>& targets,
    EWriteSessionType sessionType,
    IThroughputThrottlerPtr throttler)
    : Config_(config)
    , ChunkId_(chunkId)
    , Targets_(targets)
    , SessionType_(sessionType)
    , Throttler_(throttler)
    , IsOpen_(false)
    , IsInitComplete_(false)
    , IsClosing_(false)
    , IsCloseRequested_(false)
    , WindowSlots_(config->SendWindowSize)
    , AliveNodeCount_(targets.size())
    , MinUploadReplicationFactor_(std::min(Config_->MinUploadReplicationFactor, AliveNodeCount_))
    , BlockCount_(0)
    , Logger(ChunkClientLogger)
{
    YCHECK(!targets.empty());

    Logger.AddTag("ChunkId: %v", ChunkId_);

    for (int index = 0; index < static_cast<int>(targets.size()); ++index) {
        auto node = New<TNode>(index, Targets_[index]);
        node->LightProxy.SetDefaultTimeout(Config_->NodeRpcTimeout);
        node->HeavyProxy.SetDefaultTimeout(Config_->NodeRpcTimeout);
        node->PingExecutor = New<TPeriodicExecutor>(
            TDispatcher::Get()->GetWriterInvoker(),
            BIND(&TReplicationWriter::SendPing, MakeWeak(this), MakeWeak(node)),
            Config_->NodePingPeriod);
        Nodes_.push_back(node);
    }
}

TReplicationWriter::~TReplicationWriter()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Just a quick check.
    if (!State_.IsActive())
        return;

    LOG_INFO("Writer canceled");
    State_.Cancel(TError("Writer canceled"));

    CancelWriter(true);
}

void TReplicationWriter::Open()
{
    LOG_INFO("Opening writer (Addresses: [%v], EnableCaching: %v, SessionType: %v)",
        JoinToString(Targets_),
        Config_->EnableNodeCaching,
        SessionType_);

    auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker());
    for (auto node : Nodes_) {
        awaiter->Await(
            BIND(&TReplicationWriter::StartChunk, MakeWeak(this), node)
                .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
                .Run());
    }

    awaiter->Complete(
        BIND(&TReplicationWriter::OnSessionStarted, MakeWeak(this)));

    IsOpen_ = true;
}

void TReplicationWriter::ShiftWindow()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!State_.IsActive()) {
        YCHECK(Window_.empty());
        return;
    }

    int lastFlushableBlock = -1;
    for (auto it = Window_.begin(); it != Window_.end(); ++it) {
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
    for (auto node : Nodes_) {
        awaiter->Await(
            BIND(&TReplicationWriter::FlushBlocks, MakeWeak(this), node,lastFlushableBlock)
                .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
                .Run());
    }
    awaiter->Complete(
        BIND(&TReplicationWriter::OnWindowShifted, MakeWeak(this), lastFlushableBlock));
}

void TReplicationWriter::FlushBlocks(TNodePtr node, int blockIndex)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!node->IsAlive())
        return;

    LOG_DEBUG("Flushing block (Block: %v, Address: %v)",
        blockIndex,
        node->Descriptor.GetDefaultAddress());

    auto req = node->LightProxy.FlushBlocks();
    ToProto(req->mutable_chunk_id(), ChunkId_);
    req->set_block_index(blockIndex);

    auto rsp = WaitFor(req->Invoke());

    if (rsp->IsOK()) {
        LOG_DEBUG("Block flushed (Block: %v, Address: %v)",
            blockIndex,
            node->Descriptor.GetDefaultAddress());
    } else {
        OnNodeFailed(node, rsp->GetError());
    }
}

void TReplicationWriter::OnWindowShifted(int lastFlushedBlock)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (Window_.empty()) {
        // This happens when FlushBlocks responses are reordered
        // (i.e. a larger BlockIndex is flushed before a smaller one)
        // We should prevent repeated calls to CloseSession.
        return;
    }

    while (!Window_.empty()) {
        auto group = Window_.front();
        if (group->GetEndBlockIndex() > lastFlushedBlock)
            return;

        LOG_DEBUG("Window shifted (Blocks: %v-%v, Size: %" PRId64 ")",
            group->GetStartBlockIndex(),
            group->GetEndBlockIndex(),
            group->GetSize());

        WindowSlots_.Release(group->GetSize());
        Window_.pop_front();
    }

    if (State_.IsActive() && IsCloseRequested_) {
        CloseSession();
    }
}

void TReplicationWriter::EnsureCurrentGroup()
{
    if (!CurrentGroup_) {
        CurrentGroup_ = New<TGroup>(this, BlockCount_);
    }
}

void TReplicationWriter::FlushCurrentGroup()
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YCHECK(!IsCloseRequested_);

    if (!State_.IsActive())
        return;

    LOG_DEBUG("Block group added (Blocks: %v-%v, Group: %p)",
        CurrentGroup_->GetStartBlockIndex(),
        CurrentGroup_->GetEndBlockIndex(),
        CurrentGroup_.Get());

    Window_.push_back(CurrentGroup_);

    if (IsInitComplete_) {
        CurrentGroup_->ScheduleProcess();
    }

    CurrentGroup_.Reset();
}

void TReplicationWriter::OnNodeFailed(TNodePtr node, const TError& error)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!node->IsAlive())
        return;

    auto wrappedError = TError("Node %v failed",
        node->Descriptor.GetDefaultAddress())
        << error;
    LOG_ERROR(wrappedError);

    node->Error = wrappedError;
    --AliveNodeCount_;

    if (State_.IsActive() && AliveNodeCount_ < MinUploadReplicationFactor_) {
        auto cumulativeError = TError(
            NChunkClient::EErrorCode::AllTargetNodesFailed,
            "Not enough target nodes to finish upload");
        for (auto node : Nodes_) {
            if (!node->IsAlive()) {
                cumulativeError.InnerErrors().push_back(node->Error);
            }
        }
        LOG_WARNING(cumulativeError, "Chunk writer failed");
        CancelWriter(true);
        State_.Fail(cumulativeError);
    }
}

void TReplicationWriter::StartChunk(TNodePtr node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Starting chunk (Address: %v)",
        node->Descriptor.GetDefaultAddress());

    auto req = node->LightProxy.StartChunk();
    ToProto(req->mutable_chunk_id(), ChunkId_);
    req->set_session_type(SessionType_);
    req->set_sync_on_close(Config_->SyncOnClose);

    auto rsp = WaitFor(req->Invoke());
    if (!rsp->IsOK()) {
        OnNodeFailed(node, rsp->GetError());
        return;
    }

    LOG_DEBUG("Chunk started (Address: %v)",
        node->Descriptor.GetDefaultAddress());

    node->PingExecutor->Start();
}

void TReplicationWriter::OnSessionStarted()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Check if the session is not canceled yet.
    if (!State_.IsActive()) {
        return;
    }

    LOG_INFO("Writer is ready");

    IsInitComplete_ = true;
    for (auto& group : Window_) {
        group->ScheduleProcess();
    }

    // Possible for an empty chunk.
    if (Window_.empty() && IsCloseRequested_) {
        CloseSession();
    }
}

void TReplicationWriter::CloseSession()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    YCHECK(IsCloseRequested_);

    LOG_INFO("Closing writer");

    auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker());
    for (auto node : Nodes_) {
        awaiter->Await(
            BIND(&TReplicationWriter::FinishChunk, MakeWeak(this), node)
                .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
                .Run());
    }
    awaiter->Complete(
        BIND(&TReplicationWriter::OnSessionFinished, MakeWeak(this)));
}

void TReplicationWriter::FinishChunk(TNodePtr node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!node->IsAlive())
        return;

    LOG_DEBUG("Finishing chunk (Address: %v)",
        node->Descriptor.GetDefaultAddress());

    auto req = node->LightProxy.FinishChunk();
    ToProto(req->mutable_chunk_id(), ChunkId_);
    *req->mutable_chunk_meta() = ChunkMeta_;
    req->set_block_count(BlockCount_);

    auto rsp = WaitFor(req->Invoke());

    if (!rsp->IsOK()) {
        OnNodeFailed(node, rsp->GetError());
        return;
    }

    auto& chunkInfo = rsp->chunk_info();
    LOG_DEBUG("Chunk finished (Address: %v, DiskSpace: %v)",
        node->Descriptor.GetDefaultAddress(),
        chunkInfo.disk_space());

    ChunkInfo_ = chunkInfo;
}

void TReplicationWriter::OnSessionFinished()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    YCHECK(Window_.empty());

    if (State_.IsActive()) {
        State_.Close();
    }

    CancelWriter(false);

    LOG_INFO("Writer closed");

    State_.FinishOperation();
}

void TReplicationWriter::SendPing(TNodeWeakPtr node)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto node_ = node.Lock();
    if (!node_) {
        return;
    }

    LOG_DEBUG("Sending ping (Address: %v)",
        node_->Descriptor.GetDefaultAddress());

    auto req = node_->LightProxy.PingSession();
    ToProto(req->mutable_chunk_id(), ChunkId_);
    req->Invoke();
}

void TReplicationWriter::CancelWriter(bool abort)
{
    // No thread affinity; may be called from dtor.

    for (auto node : Nodes_) {
        CancelNode(node, abort);
    }
}

void TReplicationWriter::CancelNode(TNodePtr node, bool abort)
{
    if (node->Canceled.test_and_set())
        return;

    node->PingExecutor->Stop();

    if (abort) {
        auto req = node->LightProxy.CancelChunk();
        ToProto(req->mutable_chunk_id(), ChunkId_);
        req->Invoke();
    }
}

bool TReplicationWriter::WriteBlock(const TSharedRef& block)
{
    return WriteBlocks(std::vector<TSharedRef>(1, block));
}

bool TReplicationWriter::WriteBlocks(const std::vector<TSharedRef>& blocks)
{
    YCHECK(IsOpen_);
    YCHECK(!IsClosing_);
    YCHECK(!State_.IsClosed());

    WindowSlots_.Acquire(GetTotalSize(blocks));
    TDispatcher::Get()->GetWriterInvoker()->Invoke(
        BIND(&TReplicationWriter::AddBlocks, MakeWeak(this), blocks));

    return WindowSlots_.IsReady();
}

TAsyncError TReplicationWriter::GetReadyEvent()
{
    YCHECK(IsOpen_);
    YCHECK(!IsClosing_);
    YCHECK(!State_.HasRunningOperation());
    YCHECK(!State_.IsClosed());

    if (!WindowSlots_.IsReady()) {
        State_.StartOperation();

        // No need to capture #this by strong reference, because
        // WindowSlots are always released when Writer is alive,
        // and callcack is called synchronously.
        WindowSlots_.GetReadyEvent().Subscribe(BIND([ = ] () {
            State_.FinishOperation(TError());
        }));
    }

    return State_.GetOperationError();
}

void TReplicationWriter::AddBlocks(const std::vector<TSharedRef>& blocks)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YCHECK(!IsCloseRequested_);

    if (!State_.IsActive())
        return;

    int firstBlockIndex = BlockCount_;

    for (const auto& block : blocks) {
        EnsureCurrentGroup();

        CurrentGroup_->AddBlock(block);
        ++BlockCount_;

        if (CurrentGroup_->GetSize() >= Config_->GroupSize) {
            FlushCurrentGroup();
        }
    }

    int lastBlockIndex = BlockCount_ - 1;

    LOG_DEBUG("Blocks added (Blocks: %v-%v, Size: %v)",
        firstBlockIndex,
        lastBlockIndex,
        GetTotalSize(blocks));
}

void TReplicationWriter::DoClose()
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YCHECK(!IsCloseRequested_);

    LOG_DEBUG("Writer close requested");

    if (!State_.IsActive()) {
        State_.FinishOperation();
        return;
    }

    if (CurrentGroup_ && CurrentGroup_->GetSize() > 0) {
        FlushCurrentGroup();
    }

    IsCloseRequested_ = true;

    if (Window_.empty() && IsInitComplete_) {
        CloseSession();
    }
}

TAsyncError TReplicationWriter::Close(const TChunkMeta& chunkMeta)
{
    YCHECK(IsOpen_);
    YCHECK(!IsClosing_);
    YCHECK(!State_.HasRunningOperation());
    YCHECK(!State_.IsClosed());

    IsClosing_ = true;
    ChunkMeta_ = chunkMeta;

    LOG_DEBUG("Requesting writer to close");
    
    State_.StartOperation();

    TDispatcher::Get()->GetWriterInvoker()->Invoke(
        BIND(&TReplicationWriter::DoClose, MakeWeak(this)));

    return State_.GetOperationError();
}

const TChunkInfo& TReplicationWriter::GetChunkInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ChunkInfo_;
}

IWriter::TReplicaIndexes TReplicationWriter::GetWrittenReplicaIndexes() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReplicaIndexes result;
    for (auto node : Nodes_) {
        if (node->IsAlive()) {
            result.push_back(node->Index);
        }
    }
    return result;
}

///////////////////////////////////////////////////////////////////////////////

IWriterPtr CreateReplicationWriter(
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

