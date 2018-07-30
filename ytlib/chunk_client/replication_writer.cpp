#include "replication_writer.h"
#include "private.h"
#include "traffic_meter.h"
#include "block_cache.h"
#include "chunk_meta_extensions.h"
#include "chunk_service_proxy.h"
#include "chunk_writer.h"
#include "config.h"
#include "data_node_service_proxy.h"
#include "dispatcher.h"
#include "helpers.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/client/api/config.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/ytlib/chunk_client/session_id.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/rpc/retrying_channel.h>

#include <yt/core/net/local_address.h>

#include <atomic>
#include <deque>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NApi;
using namespace NNet;
using namespace NObjectClient;

// Don't use NChunkClient::NProto as a whole: avoid ambiguity with NProto::TSessionId.
using NProto::TChunkMeta;
using NProto::TChunkInfo;
using NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReplicationWriter)
DECLARE_REFCOUNTED_STRUCT(TNode)
DECLARE_REFCOUNTED_CLASS(TGroup)

DEFINE_ENUM(EReplicationWriterState,
    (Created)
    (Open)
    (Closing)
    (Closed)
);

////////////////////////////////////////////////////////////////////////////////

struct TNode
    : public TRefCounted
{
    const int Index;
    const TNodeDescriptor Descriptor;
    const TChunkReplica ChunkReplica;
    const IChannelPtr Channel;

    TError Error;
    TPeriodicExecutorPtr PingExecutor;
    std::atomic_flag Canceled = ATOMIC_FLAG_INIT;

    bool IsClosing = false;
    bool IsFinished = false;

    NLogging::TLogger Logger;

    TNode(
        int index,
        const TNodeDescriptor& descriptor,
        TChunkReplica chunkReplica,
        IChannelPtr channel,
        NLogging::TLogger logger)
        : Index(index)
        , Descriptor(descriptor)
        , ChunkReplica(chunkReplica)
        , Channel(std::move(channel))
        , Logger(logger)
    { }

    bool IsAlive() const
    {
        return Error.IsOK();
    }

    void SendPing(const TDuration& rpcTimeout, const TSessionId& sessionId)
    {
        LOG_DEBUG("Sending ping (Address: %v)",
            Descriptor.GetDefaultAddress());

        TDataNodeServiceProxy proxy(Channel);
        auto req = proxy.PingSession();
        req->SetTimeout(rpcTimeout);
        ToProto(req->mutable_session_id(), sessionId);
        req->Invoke();
    }
};

TString ToString(const TNodePtr& node)
{
    return node->Descriptor.GetDefaultAddress();
}

DEFINE_REFCOUNTED_TYPE(TNode)

////////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TRefCounted
{
public:
    TGroup(
        TReplicationWriter* writer,
        int startBlockIndex);

    void AddBlock(const TBlock& block);

    void ScheduleProcess();

    void SetFlushing();

    bool IsWritten() const;

    bool IsFlushing() const;

    i64 GetSize() const;

    int GetStartBlockIndex() const;

    int GetEndBlockIndex() const;

private:
    const TWeakPtr<TReplicationWriter> Writer_;
    const NLogging::TLogger Logger;

    bool Flushing_ = false;
    std::vector<bool> SentTo_;

    std::vector<TBlock> Blocks_;
    int FirstBlockIndex_;

    i64 Size_ = 0;

    void PutGroup(TReplicationWriterPtr writer);

    void SendGroup(TReplicationWriterPtr writer, TNodePtr srcNode);

    void Process();
};

DEFINE_REFCOUNTED_TYPE(TGroup)

////////////////////////////////////////////////////////////////////////////////

class TReplicationWriter
    : public IChunkWriter
{
public:
    TReplicationWriter(
        TReplicationWriterConfigPtr config,
        TRemoteWriterOptionsPtr options,
        const TSessionId& sessionId,
        const TChunkReplicaList& initialTargets,
        TNodeDirectoryPtr nodeDirectory,
        NNative::IClientPtr client,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache,
        TTrafficMeterPtr trafficMeter)
        : Config_(config)
        , Options_(options)
        , SessionId_(sessionId)
        , InitialTargets_(initialTargets)
        , Client_(client)
        , NodeDirectory_(nodeDirectory)
        , Throttler_(throttler)
        , BlockCache_(blockCache)
        , TrafficMeter_(trafficMeter)
        , Logger(NLogging::TLogger(ChunkClientLogger)
            .AddTag("ChunkId: %v", SessionId_))
        , Networks_(client->GetNativeConnection()->GetNetworks())
        , WindowSlots_(New<TAsyncSemaphore>(config->SendWindowSize))
        , UploadReplicationFactor_(Config_->UploadReplicationFactor)
        , MinUploadReplicationFactor_(std::min(Config_->UploadReplicationFactor, Config_->MinUploadReplicationFactor))
        , AllocateWriteTargetsTimestamp_(TInstant::Zero())
    {
        ClosePromise_.TrySetFrom(StateError_.ToFuture());
    }

    ~TReplicationWriter()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Just a quick check.
        if (State_.load() == EReplicationWriterState::Closed) {
            return;
        }

        LOG_INFO("Writer canceled");
        StateError_.TrySet(TError("Writer canceled"));

        CancelWriter();
    }

    virtual TFuture<void> Open() override
    {
        return BIND(&TReplicationWriter::DoOpen, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    virtual bool WriteBlock(const TBlock& block) override
    {
        return WriteBlocks({block});
    }

    virtual bool WriteBlocks(const std::vector<TBlock>& blocks) override
    {
        YCHECK(State_.load() == EReplicationWriterState::Open);

        if (StateError_.IsSet()) {
            return false;
        }

        WindowSlots_->Acquire(GetByteSize(blocks));
        TDispatcher::Get()->GetWriterInvoker()->Invoke(
            BIND(&TReplicationWriter::AddBlocks, MakeWeak(this), blocks));

        return WindowSlots_->IsReady();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        YCHECK(State_.load() == EReplicationWriterState::Open);

        auto promise = NewPromise<void>();
        promise.TrySetFrom(StateError_.ToFuture());
        promise.TrySetFrom(WindowSlots_->GetReadyEvent());

        return promise.ToFuture();
    }

    virtual TFuture<void> Close(const TChunkMeta& chunkMeta) override
    {
        YCHECK(State_.load() == EReplicationWriterState::Open);

        State_.store(EReplicationWriterState::Closing);
        ChunkMeta_ = chunkMeta;

        LOG_DEBUG("Requesting writer to close");

        TDispatcher::Get()->GetWriterInvoker()->Invoke(
            BIND(&TReplicationWriter::DoClose, MakeWeak(this)));

        return ClosePromise_.ToFuture();
    }

    virtual const TChunkInfo& GetChunkInfo() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ChunkInfo_;
    }

    virtual const TDataStatistics& GetDataStatistics() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Y_UNREACHABLE();
    }

    virtual TChunkReplicaList GetWrittenChunkReplicas() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TChunkReplicaList chunkReplicas;
        for (auto node : Nodes_) {
            if (node->IsAlive()) {
                chunkReplicas.push_back(node->ChunkReplica);
            }
        }
        return chunkReplicas;
    }

    virtual TChunkId GetChunkId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return SessionId_.ChunkId;
    }

    virtual NErasure::ECodec GetErasureCodecId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return NErasure::ECodec::None;
    }

private:
    friend class TGroup;

    const TReplicationWriterConfigPtr Config_;
    const TRemoteWriterOptionsPtr Options_;
    const TSessionId SessionId_;
    const TChunkReplicaList InitialTargets_;
    const NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const IThroughputThrottlerPtr Throttler_;
    const IBlockCachePtr BlockCache_;
    const TTrafficMeterPtr TrafficMeter_;

    const NLogging::TLogger Logger;
    const TNetworkPreferenceList Networks_;

    TAsyncSemaphorePtr WindowSlots_;

    const int UploadReplicationFactor_;
    const int MinUploadReplicationFactor_;

    TPromise<void> StateError_ = NewPromise<void>();
    TPromise<void> ClosePromise_ = NewPromise<void>();
    std::atomic<EReplicationWriterState> State_ = { EReplicationWriterState::Created };

    //! This flag is raised whenever #Close is invoked.
    //! All access to this flag happens from #WriterThread.
    bool IsCloseRequested_ = false;
    TChunkMeta ChunkMeta_;

    std::deque<TGroupPtr> Window_;
    std::vector<TNodePtr> Nodes_;

    //! Number of nodes that are still alive.
    int AliveNodeCount_ = 0;

    //! A new group of blocks that is currently being filled in by the client.
    //! All access to this field happens from client thread.
    TGroupPtr CurrentGroup_;

    //! Number of blocks that are already added via #AddBlocks.
    int BlockCount_ = 0;

    //! Returned from node on Finish.
    TChunkInfo ChunkInfo_;

    //! Last time write targets were allocated from the master.
    TInstant AllocateWriteTargetsTimestamp_;
    int AllocateWriteTargetsRetryIndex_ = 0;

    std::vector<TString> BannedNodes_;

    void DoOpen()
    {
        try {
            StartSessions(InitialTargets_);

            while (Nodes_.size() < UploadReplicationFactor_) {
                StartSessions(AllocateTargets());
            }

            LOG_INFO("Writer opened (Addresses: %v, PopulateCache: %v, Workload: %v, Networks: %v)",
                Nodes_,
                Config_->PopulateCache,
                Config_->WorkloadDescriptor,
                Networks_);

            State_.store(EReplicationWriterState::Open);
        } catch (const std::exception& ex) {
            CancelWriter();
            THROW_ERROR_EXCEPTION("Not enough target nodes to write blob chunk %v",
                SessionId_)
                    << TErrorAttribute("upload_replication_factor", UploadReplicationFactor_)
                    << ex;
        }
    }

    void DoClose()
    {
        VERIFY_THREAD_AFFINITY(WriterThread);
        YCHECK(!IsCloseRequested_);

        LOG_DEBUG("Writer close requested");

        if (StateError_.IsSet()) {
            return;
        }

        if (CurrentGroup_ && CurrentGroup_->GetSize() > 0) {
            FlushCurrentGroup();
        }

        IsCloseRequested_ = true;

        if (Window_.empty()) {
            CloseSessions();
        }
    }

    TChunkReplicaList AllocateTargets()
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        if (!Options_->AllowAllocatingNewTargetNodes) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::MasterCommunicationFailed,
                "Allocating new target nodes is disabled");
        }

        ++AllocateWriteTargetsRetryIndex_;
        if (AllocateWriteTargetsRetryIndex_ > Config_->AllocateWriteTargetsRetryCount) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::MasterCommunicationFailed,
                "Failed to allocate write targets, retry count limit exceeded")
                    << TErrorAttribute("retry_count", Config_->AllocateWriteTargetsRetryCount);
        }

        auto delayTime = TInstant::Now() - AllocateWriteTargetsTimestamp_;
        if (delayTime < Config_->AllocateWriteTargetsBackoffTime) {
            WaitFor(TDelayedExecutor::MakeDelayed(Config_->AllocateWriteTargetsBackoffTime - delayTime))
                .ThrowOnError();
        }
        AllocateWriteTargetsTimestamp_ = TInstant::Now();

        int activeTargets = Nodes_.size();
        std::vector<TString> forbiddenAddresses;
        for (const auto& node : Nodes_) {
            forbiddenAddresses.push_back(node->Descriptor.GetDefaultAddress());
        }

        forbiddenAddresses.insert(forbiddenAddresses.begin(), BannedNodes_.begin(), BannedNodes_.end());

        return AllocateWriteTargets(
            Client_,
            SessionId_,
            UploadReplicationFactor_ - activeTargets,
            std::max(MinUploadReplicationFactor_ - activeTargets, 1),
            UploadReplicationFactor_,
            Config_->PreferLocalHost,
            forbiddenAddresses,
            NodeDirectory_,
            Logger);
    }

    void StartSessions(const TChunkReplicaList& targets)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        std::vector<TFuture<void>> asyncResults;
        for (auto target : targets) {
            asyncResults.push_back(
                BIND(&TReplicationWriter::StartChunk, MakeWeak(this), target)
                    .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
                    .Run());
        }

        WaitFor(Combine(asyncResults))
            .ThrowOnError();
    }

    void EnsureCurrentGroup()
    {
        if (!CurrentGroup_) {
            CurrentGroup_ = New<TGroup>(this, BlockCount_);
        }
    }

    void FlushCurrentGroup()
    {
        VERIFY_THREAD_AFFINITY(WriterThread);
        YCHECK(!IsCloseRequested_);

        if (StateError_.IsSet()) {
            return;
        }

        LOG_DEBUG("Block group added (Blocks: %v-%v)",
            CurrentGroup_->GetStartBlockIndex(),
            CurrentGroup_->GetEndBlockIndex());

        Window_.push_back(CurrentGroup_);
        CurrentGroup_->ScheduleProcess();
        CurrentGroup_.Reset();
    }


    void OnNodeFailed(TNodePtr node, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        if (!node->IsAlive()) {
            return;
        }

        auto wrappedError = TError("Node %v failed",
            node->Descriptor.GetDefaultAddress())
            << error;
        LOG_ERROR(wrappedError);

        if (Config_->BanFailedNodes) {
            BannedNodes_.push_back(node->Descriptor.GetDefaultAddress());
        }

        node->PingExecutor->Stop();
        node->Error = wrappedError;
        --AliveNodeCount_;

        if (!StateError_.IsSet() && AliveNodeCount_ < MinUploadReplicationFactor_) {
            auto cumulativeError = TError(
                NChunkClient::EErrorCode::AllTargetNodesFailed,
                "Not enough target nodes to finish upload");
            for (auto node : Nodes_) {
                if (!node->IsAlive()) {
                    cumulativeError.InnerErrors().push_back(node->Error);
                }
            }
            LOG_WARNING(cumulativeError, "Chunk writer failed");
            CancelWriter();
            StateError_.TrySet(cumulativeError);
        } else {
            CheckFinished();
        }
    }

    void ShiftWindow()
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        if (StateError_.IsSet()) {
            YCHECK(Window_.empty());
            return;
        }

        int lastFlushableBlock = -1;
        for (auto it = Window_.begin(); it != Window_.end(); ++it) {
            const auto& group = *it;
            if (!group->IsFlushing()) {
                if (group->IsWritten()) {
                    lastFlushableBlock = group->GetEndBlockIndex();
                    group->SetFlushing();
                } else {
                    break;
                }
            }
        }

        if (lastFlushableBlock < 0) {
            return;
        }

        std::vector<TFuture<void>> asyncResults;

        for (auto node : Nodes_) {
            asyncResults.push_back(
                BIND(&TReplicationWriter::FlushBlocks, MakeWeak(this), node, lastFlushableBlock)
                    .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
                    .Run());
        }
        Combine(asyncResults).Subscribe(
            BIND(
                &TReplicationWriter::OnWindowShifted,
                MakeWeak(this),
                lastFlushableBlock)
                .Via(TDispatcher::Get()->GetWriterInvoker()));
    }

    void OnWindowShifted(int blockIndex, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        if (!error.IsOK()) {
            LOG_WARNING(error, "Chunk writer failed");
            CancelWriter();
            StateError_.TrySet(error);
            return;
        }

        if (Window_.empty()) {
            // This happens when FlushBlocks responses are reordered
            // (i.e. a larger BlockIndex is flushed before a smaller one)
            // We should prevent repeated calls to CloseSessions.
            return;
        }

        while (!Window_.empty()) {
            auto group = Window_.front();
            if (group->GetEndBlockIndex() > blockIndex) {
                return;
            }

            LOG_DEBUG("Window shifted (Blocks: %v-%v, Size: %v)",
                group->GetStartBlockIndex(),
                group->GetEndBlockIndex(),
                group->GetSize());

            WindowSlots_->Release(group->GetSize());
            Window_.pop_front();
        }

        if (!StateError_.IsSet() && IsCloseRequested_) {
            CloseSessions();
        }
    }

    void FlushBlocks(TNodePtr node, int blockIndex)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        if (!node->IsAlive()) {
            return;
        }

        LOG_DEBUG("Flushing block (Block: %v, Address: %v)",
            blockIndex,
            node->Descriptor.GetDefaultAddress());

        TDataNodeServiceProxy proxy(node->Channel);
        auto req = proxy.FlushBlocks();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_block_index(blockIndex);

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            LOG_DEBUG("Block flushed (Block: %v, Address: %v)",
                blockIndex,
                node->Descriptor.GetDefaultAddress());

            if (IsCloseRequested_ && blockIndex + 1 == BlockCount_) {
                // We flushed the last block in chunk.

                BIND(&TReplicationWriter::FinishChunk, MakeWeak(this), node)
                    .Via(TDispatcher::Get()->GetWriterInvoker())
                    .Run();
            }

        } else {
            OnNodeFailed(node, rspOrError);
        }
    }

    void StartChunk(TChunkReplica target)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        auto nodeDescriptor = NodeDirectory_->GetDescriptor(target);
        auto address = nodeDescriptor.GetAddressOrThrow(Networks_);
        LOG_DEBUG("Starting write session (Address: %v)", address);

        auto channel = CreateRetryingChannel(
            Config_->NodeChannel,
            Client_->GetChannelFactory()->CreateChannel(address),
            BIND([] (const TError& error) {
                return error.FindMatching(NChunkClient::EErrorCode::WriteThrottlingActive).HasValue();
            }));

        TDataNodeServiceProxy proxy(channel);
        auto req = proxy.StartChunk();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        ToProto(req->mutable_workload_descriptor(), Config_->WorkloadDescriptor);
        req->set_sync_on_close(Config_->SyncOnClose);
        req->set_enable_direct_io(Config_->EnableDirectIO);
        ToProto(req->mutable_placement_id(), Options_->PlacementId);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Failed to start write session on node %v", address);
            return;
        }

        LOG_DEBUG("Write session started (Address: %v)", address);

        auto node = New<TNode>(
            Nodes_.size(),
            nodeDescriptor,
            target,
            channel,
            Logger);

        node->PingExecutor = New<TPeriodicExecutor>(
            TDispatcher::Get()->GetWriterInvoker(),
            BIND(&TNode::SendPing, MakeWeak(node), Config_->NodeRpcTimeout, SessionId_),
            Config_->NodePingPeriod);
        node->PingExecutor->Start();

        Nodes_.push_back(node);
        ++AliveNodeCount_;
    }

    void CloseSessions()
    {
        VERIFY_THREAD_AFFINITY(WriterThread);
        YCHECK(IsCloseRequested_);

        LOG_INFO("Closing writer");

        for (auto node : Nodes_) {
            BIND(&TReplicationWriter::FinishChunk, MakeWeak(this), node)
                .Via(TDispatcher::Get()->GetWriterInvoker())
                .Run();
        }
    }

    void FinishChunk(TNodePtr node)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        if (!node->IsAlive() || node->IsClosing) {
            return;
        }

        node->IsClosing = true;
        LOG_DEBUG("Finishing chunk (Address: %v)",
            node->Descriptor.GetDefaultAddress());

        TDataNodeServiceProxy proxy(node->Channel);
        auto req = proxy.FinishChunk();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        *req->mutable_chunk_meta() = ChunkMeta_;
        req->set_block_count(BlockCount_);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            OnNodeFailed(node, rspOrError);
            return;
        }

        const auto& rsp = rspOrError.Value();
        const auto& chunkInfo = rsp->chunk_info();
        LOG_DEBUG("Chunk finished (Address: %v, DiskSpace: %v)",
            node->Descriptor.GetDefaultAddress(),
            chunkInfo.disk_space());

        node->IsFinished = true;
        node->PingExecutor->Stop();
        ChunkInfo_ = chunkInfo;

        CheckFinished();
    }

    void CheckFinished()
    {
        int finishedNodeCount = 0;
        bool hasUnfinishedNode = false;
        for (const auto& node : Nodes_) {
            if (node->IsAlive()) {
                if (node->IsFinished) {
                    ++finishedNodeCount;
                } else {
                    hasUnfinishedNode = true;
                }
            }
        }

        if (!StateError_.IsSet() &&
            (!hasUnfinishedNode || (Config_->EnableEarlyFinish && finishedNodeCount >= MinUploadReplicationFactor_)))
        {
            State_ = EReplicationWriterState::Closed;
            ClosePromise_.TrySet();
            CancelWriter();
            LOG_INFO("Writer closed");
        }
    }

    void CancelWriter()
    {
        // No thread affinity; may be called from dtor.

        for (auto node : Nodes_) {
            CancelNode(node);
        }
    }

    void CancelNode(TNodePtr node)
    {
        if (node->Canceled.test_and_set()) {
            return;
        }

        node->PingExecutor->Stop();

        if (!node->IsFinished) {
            TDataNodeServiceProxy proxy(node->Channel);
            auto req = proxy.CancelChunk();
            ToProto(req->mutable_session_id(), SessionId_);
            req->Invoke();
        }
    }

    void AddBlocks(const std::vector<TBlock>& blocks)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);
        YCHECK(!IsCloseRequested_);

        if (StateError_.IsSet()) {
            return;
        }

        int firstBlockIndex = BlockCount_;
        int currentBlockIndex = firstBlockIndex;

        for (const auto& block : blocks) {
            EnsureCurrentGroup();

            auto blockId = TBlockId(SessionId_.ChunkId, currentBlockIndex);
            BlockCache_->Put(blockId, EBlockType::CompressedData, block, Null);

            CurrentGroup_->AddBlock(block);

            ++BlockCount_;
            ++currentBlockIndex;

            if (CurrentGroup_->GetSize() >= Config_->GroupSize) {
                FlushCurrentGroup();
            }
        }

        int lastBlockIndex = BlockCount_ - 1;

        LOG_DEBUG("Blocks added (Blocks: %v-%v, Size: %v)",
            firstBlockIndex,
            lastBlockIndex,
            GetByteSize(blocks));
    }

    // Update traffic info: we've uploaded some data.
    void AccountTraffic(
        i64 transferredByteCount,
        const TNodeDescriptor& dstDescriptor)
    {
        if (TrafficMeter_) {
            auto dataCenter = dstDescriptor.GetDataCenter();
            TrafficMeter_->IncrementOutboundByteCount(dataCenter, transferredByteCount);
        }
    }

    // Update traffic info: we initiated data transfer between two non-local
    // nodes and it has just completed.
    void AccountTraffic(
        i64 transferredByteCount,
        const TNodeDescriptor& srcDescriptor,
        const TNodeDescriptor& dstDescriptor)
    {
        if (TrafficMeter_) {
            auto srcDataCenter = srcDescriptor.GetDataCenter();
            auto dstDataCenter = dstDescriptor.GetDataCenter();
            TrafficMeter_->IncrementByteCount(srcDataCenter, dstDataCenter, transferredByteCount);
        }
    }

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);
};

DEFINE_REFCOUNTED_TYPE(TReplicationWriter)

////////////////////////////////////////////////////////////////////////////////

TGroup::TGroup(
    TReplicationWriter* writer,
    int startBlockIndex)
    : Writer_(writer)
    , Logger(writer->Logger)
    , SentTo_(writer->Nodes_.size(), false)
    , FirstBlockIndex_(startBlockIndex)
{ }

void TGroup::AddBlock(const TBlock& block)
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

    TNullable<int> selectedIndex;
    for (int index = 0; index < writer->Nodes_.size(); ++index) {
        const auto& node = writer->Nodes_[index];

        if (!node->IsAlive()) {
            continue;
        }

        if (!selectedIndex) {
            selectedIndex = index;
        }

        if (node->Descriptor.GetDefaultAddress() == GetLocalHostName()) {
            // If we are on the same host - this is always the best candidate.
            selectedIndex = index;
            break;
        }
    }

    YCHECK(selectedIndex);
    auto node = writer->Nodes_[*selectedIndex];

    TDataNodeServiceProxy proxy(node->Channel);
    auto req = proxy.PutBlocks();
    req->SetMultiplexingBand(EMultiplexingBand::Heavy);
    req->SetTimeout(writer->Config_->NodeRpcTimeout);
    ToProto(req->mutable_session_id(), writer->SessionId_);
    req->set_first_block_index(FirstBlockIndex_);
    req->set_populate_cache(writer->Config_->PopulateCache);

    SetRpcAttachedBlocks(req, Blocks_);

    LOG_DEBUG("Ready to put blocks (Blocks: %v-%v, Address: %v, Size: %v)",
        GetStartBlockIndex(),
        GetEndBlockIndex(),
        node->Descriptor.GetDefaultAddress(),
        Size_);

    if (node->Descriptor.GetDefaultAddress() != GetLocalHostName()) {
        auto throttleResult = WaitFor(writer->Throttler_->Throttle(Size_));
        if (!throttleResult.IsOK() && !writer->StateError_.IsSet()) {
            auto error = TError(
                NChunkClient::EErrorCode::BandwidthThrottlingFailed,
                "Failed to throttle bandwidth in writer")
                << throttleResult;
            LOG_WARNING(error, "Chunk writer failed");
            writer->CancelWriter();
            writer->StateError_.TrySet(error);
            return;
        }
    }

    LOG_DEBUG("Putting blocks (Blocks: %v-%v, Address: %v)",
        FirstBlockIndex_,
        GetEndBlockIndex(),
        node->Descriptor.GetDefaultAddress());

    auto rspOrError = WaitFor(req->Invoke());
    if (rspOrError.IsOK()) {
        SentTo_[node->Index] = true;

        writer->AccountTraffic(Size_, node->Descriptor);

        LOG_DEBUG("Blocks are put (Blocks: %v-%v, Address: %v)",
            GetStartBlockIndex(),
            GetEndBlockIndex(),
            node->Descriptor.GetDefaultAddress());
    } else {
        writer->OnNodeFailed(node, rspOrError);
    }

    ScheduleProcess();
}

void TGroup::SendGroup(TReplicationWriterPtr writer, TNodePtr srcNode)
{
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    TNodePtr dstNode;
    for (int index = 0; index < SentTo_.size(); ++index) {
        auto node = writer->Nodes_[index];
        if (node->IsAlive() && !SentTo_[index]) {
            dstNode = node;
        }
    }

    if (dstNode) {
        LOG_DEBUG("Sending blocks (Blocks: %v-%v, SrcAddress: %v, DstAddress: %v)",
            GetStartBlockIndex(),
            GetEndBlockIndex(),
            srcNode->Descriptor.GetDefaultAddress(),
            dstNode->Descriptor.GetDefaultAddress());

        TDataNodeServiceProxy proxy(srcNode->Channel);
        auto req = proxy.SendBlocks();
        // Set double timeout for SendBlocks since executing it implies another (src->dst) RPC call.
        req->SetTimeout(writer->Config_->NodeRpcTimeout * 2);
        ToProto(req->mutable_session_id(), writer->SessionId_);
        req->set_first_block_index(FirstBlockIndex_);
        req->set_block_count(Blocks_.size());
        ToProto(req->mutable_target_descriptor(), dstNode->Descriptor);

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            SentTo_[dstNode->Index] = true;

            writer->AccountTraffic(Size_, srcNode->Descriptor, dstNode->Descriptor);

            LOG_DEBUG("Blocks are sent (Blocks: %v-%v, SrcAddress: %v, DstAddress: %v)",
                FirstBlockIndex_,
                GetEndBlockIndex(),
                srcNode->Descriptor.GetDefaultAddress(),
                dstNode->Descriptor.GetDefaultAddress());
        } else {
            auto failedNode = (rspOrError.GetCode() == EErrorCode::SendBlocksFailed) ? dstNode : srcNode;
            writer->OnNodeFailed(failedNode, rspOrError);
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
    if (!writer || writer->StateError_.IsSet()) {
        return;
    }

    VERIFY_THREAD_AFFINITY(writer->WriterThread);
    YCHECK(writer->State_.load() == EReplicationWriterState::Open ||
        writer->State_.load() == EReplicationWriterState::Closing);

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

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateReplicationWriter(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    const TSessionId& sessionId,
    const TChunkReplicaList& targets,
    TNodeDirectoryPtr nodeDirectory,
    NNative::IClientPtr client,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler)
{
    return New<TReplicationWriter>(
        config,
        options,
        sessionId,
        targets,
        nodeDirectory,
        client,
        throttler,
        blockCache,
        trafficMeter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

