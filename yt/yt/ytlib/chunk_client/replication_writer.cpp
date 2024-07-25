#include "replication_writer.h"

#include "block_cache.h"
#include "block_reorderer.h"
#include "chunk_meta_extensions.h"
#include "chunk_service_proxy.h"
#include "chunk_writer.h"
#include "config.h"
#include "data_node_service_proxy.h"
#include "deferred_chunk_meta.h"
#include "dispatcher.h"
#include "helpers.h"
#include "private.h"
#include "traffic_meter.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/journal_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <atomic>
#include <deque>

namespace NYT::NChunkClient {

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
using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReplicationWriter)
DECLARE_REFCOUNTED_CLASS(TNode)
DECLARE_REFCOUNTED_CLASS(TGroup)

DEFINE_ENUM(EReplicationWriterState,
    (Created)
    (Open)
    (Closing)
    (Closed)
);

////////////////////////////////////////////////////////////////////////////////

class TNode
    : public TRefCounted
{
public:
    TNode(
        TNodeDescriptor descriptor,
        TChunkReplicaWithMedium chunkReplica)
        : Descriptor_(std::move(descriptor))
        , ChunkReplica_(chunkReplica)
    { }

    void SetFinished()
    {
        Finished_ = true;
    }

    bool IsFinished() const
    {
        return Finished_;
    }

    bool IsClosing() const
    {
        return Closing_;
    }

    void SetClosing()
    {
        Closing_ = true;
    }

    int GetIndex() const
    {
        return Index_;
    }

    const IChannelPtr& GetChannel() const
    {
        return Channel_;
    }

    const TChunkLocationUuid& GetTargetLocationUuid() const
    {
        return TargetLocationUuid_;
    }

    const TNodeDescriptor& GetDescriptor() const
    {
        return Descriptor_;
    }

    const TChunkReplicaWithMedium& GetChunkReplica() const
    {
        return ChunkReplica_;
    }

    void InitializeSession(
        int index,
        IChannelPtr channel,
        TChunkLocationUuid targetLocationUuid)
    {
        YT_VERIFY(channel);
        YT_VERIFY(!Channel_);

        Index_ = index;
        Channel_ = channel;
        TargetLocationUuid_ = targetLocationUuid;
    }

    void StartPing(
        TCallback<void()> pingCallback,
        TDuration period)
    {
        YT_VERIFY(!PingExecutor_);

        PingExecutor_ = New<TPeriodicExecutor>(
            TDispatcher::Get()->GetWriterInvoker(),
            pingCallback,
            period);
        PingExecutor_->Start();
    }

    TFuture<void> StopPing()
    {
        return PingExecutor_->Stop();
    }

    void SetError(const TError& error)
    {
        Error_.Store(error);
    }

    TError GetError()
    {
        return Error_.Load();
    }

    bool IsAlive() const
    {
        return Error_.Load().IsOK();
    }

    TString GetDefaultAddress() const
    {
        return Descriptor_.GetDefaultAddress();
    }

private:
    const TNodeDescriptor Descriptor_;
    const TChunkReplicaWithMedium ChunkReplica_;

    int Index_;
    IChannelPtr Channel_;
    TChunkLocationUuid TargetLocationUuid_;

    TAtomicObject<TError> Error_;
    TPeriodicExecutorPtr PingExecutor_;

    bool Closing_ = false;
    bool Finished_ = false;
};

void FormatValue(TStringBuilderBase* builder, const TNodePtr& node, TStringBuf spec)
{
    FormatValue(builder, node->GetDefaultAddress(), spec);
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

    void ReorderBlocks(TBlockReorderer* blockReorderer);

    void SetCumulativeBlockSize(i64 cumulativeBlockSize);

    void ScheduleProcess();

    void SetFlushing();

    bool IsWritten() const;

    bool IsFlushing() const;

    i64 GetSize() const;

    i64 GetCumulativeBlockSize() const;

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
    i64 CumulativeBlockSize_ = 0;

    void PutGroup(const TReplicationWriterPtr& writer);
    void SendGroup(const TReplicationWriterPtr& writer, const std::vector<TNodePtr>& srcNodes);
    bool ShouldThrottle(const TString& address, const TReplicationWriterPtr& writer) const;

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
        TSessionId sessionId,
        TChunkReplicaWithMediumList initialTargets,
        NNative::IClientPtr client,
        TString localHostName,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache,
        TTrafficMeterPtr trafficMeter)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , SessionId_(sessionId)
        , InitialTargets_(std::move(initialTargets))
        , Client_(std::move(client))
        , LocalHostName_(std::move(localHostName))
        , Throttler_(std::move(throttler))
        , BlockCache_(std::move(blockCache))
        , TrafficMeter_(std::move(trafficMeter))
        , Logger(ChunkClientLogger().WithTag("ChunkId: %v", SessionId_))
        , Networks_(Client_->GetNativeConnection()->GetNetworks())
        , WindowSlots_(New<TAsyncSemaphore>(Config_->SendWindowSize))
        , UploadReplicationFactor_(Config_->UploadReplicationFactor)
        , MinUploadReplicationFactor_(std::min(Config_->UploadReplicationFactor, Config_->MinUploadReplicationFactor))
        , DirectUploadNodeCount_(Config_->GetDirectUploadNodeCount())
        , UncancelableStateError_(StateError_.ToFuture().ToUncancelable())
        , BlockReorderer_(Config_)
    {
        ClosePromise_.TrySetFrom(UncancelableStateError_);
    }

    ~TReplicationWriter()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Just a quick check.
        if (State_.load() == EReplicationWriterState::Closed) {
            return;
        }

        StateError_.TrySet(TError("Writer destroyed"));

        YT_UNUSED_FUTURE(CancelWriter());
    }

    TFuture<void> Open() override
    {
        return BIND(&TReplicationWriter::DoOpen, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    bool WriteBlock(const TWorkloadDescriptor& workloadDescriptor, const TBlock& block) override
    {
        return WriteBlocks(workloadDescriptor, {block});
    }

    bool WriteBlocks(
        const TWorkloadDescriptor& /*workloadDescriptor*/,
        const std::vector<TBlock>& blocks) override
    {
        YT_VERIFY(State_.load() == EReplicationWriterState::Open);

        if (StateError_.IsSet()) {
            return false;
        }

        WindowSlots_->Acquire(GetByteSize(blocks));
        TDispatcher::Get()->GetWriterInvoker()->Invoke(
            BIND(&TReplicationWriter::AddBlocks, MakeWeak(this), blocks));

        return WindowSlots_->IsReady();
    }

    TFuture<void> GetReadyEvent() override
    {
        YT_VERIFY(State_.load() == EReplicationWriterState::Open);

        auto promise = NewPromise<void>();
        promise.TrySetFrom(UncancelableStateError_);
        promise.TrySetFrom(WindowSlots_->GetReadyEvent());

        return promise.ToFuture();
    }

    TFuture<void> Close(
        const TWorkloadDescriptor& workloadDescriptor,
        const TDeferredChunkMetaPtr& chunkMeta) override
    {
        YT_VERIFY(State_.load() == EReplicationWriterState::Open);
        YT_VERIFY(chunkMeta || IsJournalChunkId(DecodeChunkId(SessionId_.ChunkId).Id));

        State_.store(EReplicationWriterState::Closing);

        if (chunkMeta) {
            ChunkMeta_ = chunkMeta;
        } else {
            // This is a journal chunk; let's synthesize some meta.
            ChunkMeta_ = New<TDeferredChunkMeta>();
            ChunkMeta_->set_type(ToProto<int>(EChunkType::Journal));
            ChunkMeta_->set_format(ToProto<int>(EChunkFormat::JournalDefault));
            ChunkMeta_->mutable_extensions();
        }

        YT_LOG_DEBUG("Requesting writer to close");

        TDispatcher::Get()->GetWriterInvoker()->Invoke(
            BIND(&TReplicationWriter::DoClose, MakeWeak(this), workloadDescriptor));

        return ClosePromise_.ToFuture();
    }

    const TChunkInfo& GetChunkInfo() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ChunkInfo_;
    }

    const TDataStatistics& GetDataStatistics() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_ABORT();
    }

    TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TWrittenChunkReplicasInfo result;
        for (const auto& node : Nodes_) {
            if (node->IsAlive() && node->IsFinished()) {
                result.Replicas.emplace_back(node->GetChunkReplica(), node->GetTargetLocationUuid());
            }
        }
        return result;
    }

    TChunkId GetChunkId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return SessionId_.ChunkId;
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return NErasure::ECodec::None;
    }

    bool IsCloseDemanded() const override
    {
        return CloseDemanded_;
    }

    TFuture<void> Cancel() override
    {
        State_.store(EReplicationWriterState::Closed);
        return CancelWriter(/*wait*/ true);
    }

private:
    friend class TGroup;

    const TReplicationWriterConfigPtr Config_;
    const TRemoteWriterOptionsPtr Options_;
    const TSessionId SessionId_;
    const TChunkReplicaWithMediumList InitialTargets_;
    const NNative::IClientPtr Client_;
    const TString LocalHostName_;
    const IThroughputThrottlerPtr Throttler_;
    const IBlockCachePtr BlockCache_;
    const TTrafficMeterPtr TrafficMeter_;

    const NLogging::TLogger Logger;
    const TNetworkPreferenceList Networks_;

    TAsyncSemaphorePtr WindowSlots_;

    const int UploadReplicationFactor_;
    const int MinUploadReplicationFactor_;
    const int DirectUploadNodeCount_;

    const TPromise<void> StateError_ = NewPromise<void>();
    const TPromise<void> ClosePromise_ = NewPromise<void>();
    //! We use uncancelable future to make sure that we control all the places, where StateError_ is set.
    TFuture<void> UncancelableStateError_;
    std::atomic<EReplicationWriterState> State_ = EReplicationWriterState::Created;

    //! This flag is raised whenever #Close is invoked.
    //! All access to this flag happens from #WriterThread.
    bool CloseRequested_ = false;
    TDeferredChunkMetaPtr ChunkMeta_;

    std::deque<TGroupPtr> Window_;
    std::vector<TNodePtr> Nodes_;

    //! Number of nodes that are still alive.
    int AliveNodeCount_ = 0;

    //! A new group of blocks that is currently being filled in by the client.
    //! All access to this field happens from client thread.
    TGroupPtr CurrentGroup_;

    //! Number of blocks that are already added via #AddBlocks.
    int BlockCount_ = 0;

    i64 CumulativeBlockSize_ = 0;

    //! Returned from node on Finish.
    TChunkInfo ChunkInfo_;

    //! Last time write targets were allocated from the master.
    TInstant AllocateWriteTargetsTimestamp_;
    int AllocateWriteTargetsRetryIndex_ = 0;

    std::vector<TString> BannedNodeAddresses_;

    bool CloseDemanded_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CandidateNodesLock_);
    //! Stores the set of nodes where write sessions could have been started.
    //! Used to avoid leaving dangling sessions behind on writer cancelation.
    THashSet<IChannelPtr> CandidateNodes_;

    TBlockReorderer BlockReorderer_;

    void RegisterCandidateNode(const IChannelPtr& channel)
    {
        auto guard = Guard(CandidateNodesLock_);
        CandidateNodes_.insert(channel);
    }

    void UnregisterCandidateNode(const IChannelPtr& channel)
    {
        auto guard = Guard(CandidateNodesLock_);
        CandidateNodes_.erase(channel);
    }

    std::vector<IChannelPtr> ExtractCandidateNodes()
    {
        auto guard = Guard(CandidateNodesLock_);
        std::vector<IChannelPtr> result(CandidateNodes_.begin(), CandidateNodes_.end());
        CandidateNodes_.clear();
        return result;
    }

    void DoOpen()
    {
        try {
            bool disableSendBlocks = InitialTargets_.size() <= 1 && (UploadReplicationFactor_ == 1 || !Options_->AllowAllocatingNewTargetNodes);
            StartSessions(InitialTargets_, disableSendBlocks);

            while (std::ssize(Nodes_) < UploadReplicationFactor_) {
                StartSessions(AllocateTargets(), disableSendBlocks);
            }

            YT_LOG_INFO("Writer opened (Addresses: %v, PopulateCache: %v, Workload: %v, Networks: %v)",
                Nodes_,
                Config_->PopulateCache,
                Config_->WorkloadDescriptor,
                Networks_);

            State_.store(EReplicationWriterState::Open);
        } catch (const std::exception& ex) {
            YT_UNUSED_FUTURE(CancelWriter());
            THROW_ERROR_EXCEPTION("Not enough target nodes to write blob chunk %v",
                SessionId_)
                << TErrorAttribute("upload_replication_factor", UploadReplicationFactor_)
                << ex;
        }

        if (Config_->TestingDelay) {
            TDelayedExecutor::WaitForDuration(*Config_->TestingDelay);
        }
    }

    void DoClose(const TWorkloadDescriptor& /*workloadDescriptor*/)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);
        YT_VERIFY(!CloseRequested_);

        YT_LOG_DEBUG("Writer close requested");

        if (StateError_.IsSet()) {
            return;
        }

        if (CurrentGroup_ && CurrentGroup_->GetSize() > 0) {
            FlushCurrentGroup();
        }

        CloseRequested_ = true;

        if (Window_.empty()) {
            CloseSessions();
        }
    }

    TChunkReplicaWithMediumList AllocateTargets()
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        if (!Options_->AllowAllocatingNewTargetNodes) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::MasterCommunicationFailed,
                "Allocating new target nodes is disabled");
        }

        ++AllocateWriteTargetsRetryIndex_;
        if (AllocateWriteTargetsRetryIndex_ > Config_->AllocateWriteTargetsRetryCount) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::MasterCommunicationFailed,
                "Failed to allocate write targets, retry count limit exceeded")
                << TErrorAttribute("retry_count", Config_->AllocateWriteTargetsRetryCount);
        }

        auto delayTime = TInstant::Now() - AllocateWriteTargetsTimestamp_;
        if (delayTime < Config_->AllocateWriteTargetsBackoffTime) {
            TDelayedExecutor::WaitForDuration(Config_->AllocateWriteTargetsBackoffTime - delayTime);
        }
        AllocateWriteTargetsTimestamp_ = TInstant::Now();

        int activeTargets = static_cast<int>(Nodes_.size());

        std::vector<TString> forbiddenAddresses;
        forbiddenAddresses.reserve(Nodes_.size() + BannedNodeAddresses_.size());
        // TODO(gritukan): Do not pass allocated nodes as forbidden when masters will be new.
        for (const auto& node : Nodes_) {
            forbiddenAddresses.push_back(node->GetDefaultAddress());
        }
        forbiddenAddresses.insert(forbiddenAddresses.begin(), BannedNodeAddresses_.begin(), BannedNodeAddresses_.end());

        std::vector<TString> allocatedAddresses;
        allocatedAddresses.reserve(Nodes_.size());
        for (const auto& node : Nodes_) {
            allocatedAddresses.push_back(node->GetDefaultAddress());
        }

        auto preferredHostName = Config_->PreferLocalHost
            ? std::make_optional(LocalHostName_)
            : std::nullopt;

        return AllocateWriteTargets(
            Client_,
            TSessionId(DecodeChunkId(SessionId_.ChunkId).Id, SessionId_.MediumIndex),
            /*desiredTargetCount*/ UploadReplicationFactor_ - activeTargets,
            /*minTargetCount*/ std::max(MinUploadReplicationFactor_ - activeTargets, 1),
            /*replicationFactorOverride*/ UploadReplicationFactor_,
            preferredHostName,
            forbiddenAddresses,
            allocatedAddresses,
            Logger);
    }

    void StartSessions(const TChunkReplicaWithMediumList& targets, bool disableSendBlocks)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        std::vector<TFuture<void>> asyncResults;
        for (auto target : targets) {
            asyncResults.push_back(
                BIND(&TReplicationWriter::StartChunk, MakeWeak(this), target, disableSendBlocks)
                    .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
                    .Run());
        }

        WaitFor(AllSucceeded(asyncResults))
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
        YT_VERIFY(!CloseRequested_);

        if (StateError_.IsSet()) {
            return;
        }

        YT_LOG_DEBUG("Block group added (Blocks: %v-%v)",
            CurrentGroup_->GetStartBlockIndex(),
            CurrentGroup_->GetEndBlockIndex());

        Window_.push_back(CurrentGroup_);
        CurrentGroup_->ReorderBlocks(&BlockReorderer_);
        CurrentGroup_->ScheduleProcess();
        CurrentGroup_.Reset();
    }


    void OnNodeFailed(const TNodePtr& node, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        // Finished flag may have been set in case of reordering of the responses.
        if (!node->IsAlive() || node->IsFinished()) {
            return;
        }

        auto wrappedError = TError("Node %v failed",
            node->GetDefaultAddress())
            << error;
        YT_LOG_ERROR(wrappedError);

        if (Config_->BanFailedNodes) {
            BannedNodeAddresses_.push_back(node->GetDefaultAddress());
        }

        YT_UNUSED_FUTURE(node->StopPing());
        node->SetError(wrappedError);
        --AliveNodeCount_;

        if (!StateError_.IsSet() && AliveNodeCount_ < MinUploadReplicationFactor_) {
            auto cumulativeError = TError(
                NChunkClient::EErrorCode::AllTargetNodesFailed,
                "Not enough target nodes to finish upload");
            for (const auto& node : Nodes_) {
                if (!node->IsAlive()) {
                    cumulativeError.MutableInnerErrors()->push_back(node->GetError());
                }
            }
            YT_LOG_WARNING(cumulativeError, "Chunk writer failed");
            YT_UNUSED_FUTURE(CancelWriter());
            StateError_.TrySet(cumulativeError);
        } else {
            CheckFinished();
        }
    }

    void ShiftWindow()
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        if (StateError_.IsSet()) {
            YT_VERIFY(Window_.empty());
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

        for (const auto& node : Nodes_) {
            asyncResults.push_back(
                BIND(&TReplicationWriter::FlushBlocks, MakeWeak(this), node, lastFlushableBlock)
                    .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
                    .Run());
        }
        AllSucceeded(asyncResults).Subscribe(
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
            YT_LOG_WARNING(error, "Chunk writer failed");
            YT_UNUSED_FUTURE(CancelWriter());
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

            YT_LOG_DEBUG("Window shifted (Blocks: %v-%v, Size: %v)",
                group->GetStartBlockIndex(),
                group->GetEndBlockIndex(),
                group->GetSize());

            WindowSlots_->Release(group->GetSize());
            Window_.pop_front();
        }

        if (!StateError_.IsSet() && CloseRequested_) {
            CloseSessions();
        }
    }

    void FlushBlocks(const TNodePtr& node, int blockIndex)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        if (!node->IsAlive()) {
            return;
        }

        YT_LOG_DEBUG("Flushing block (Block: %v, Address: %v)",
            blockIndex,
            node->GetDefaultAddress());

        TDataNodeServiceProxy proxy(node->GetChannel());
        auto req = proxy.FlushBlocks();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_block_index(blockIndex);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            // See YT-17154.
            if (rspOrError.GetCode() != NChunkClient::EErrorCode::NoSuchSession || !node->IsClosing()) {
                OnNodeFailed(node, rspOrError);
            }
            return;
        }

        YT_LOG_DEBUG("Block flushed (Block: %v, Address: %v)",
            blockIndex,
            node->GetDefaultAddress());

        const auto& rsp = rspOrError.Value();
        if (rsp->close_demanded()) {
            YT_LOG_DEBUG("Close demanded by node (NodeAddress: %v)", node->GetDefaultAddress());
            DemandClose();
        }

        if (CloseRequested_ && blockIndex + 1 == BlockCount_) {
            // We flushed the last block in chunk.

            BIND(&TReplicationWriter::FinishChunk, MakeWeak(this), node)
                .Via(TDispatcher::Get()->GetWriterInvoker())
                .Run();
        }
    }

    void StartChunk(TChunkReplicaWithMedium target, bool disableSendBlocks)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);
        YT_VERIFY(IsErasureChunkPartId(SessionId_.ChunkId) || target.GetReplicaIndex() == GenericChunkReplicaIndex);

        const auto& nodeDirectory = Client_->GetNativeConnection()->GetNodeDirectory();
        const auto& nodeDescriptor = nodeDirectory->GetDescriptor(target);
        const auto& address = nodeDescriptor.GetAddressOrThrow(Networks_);
        YT_LOG_DEBUG("Starting write session (Address: %v)", address);

        auto node = New<TNode>(
            nodeDescriptor,
            target);

        auto channel = CreateRetryingChannel(
            Config_->NodeChannel,
            Client_->GetChannelFactory()->CreateChannel(address),
            BIND([sourceNode = MakeWeak(node), weakThis = MakeWeak(this)] (const TError& error) {
                auto lockedSourceNode = sourceNode.Lock();
                auto lockedWriter = weakThis.Lock();

                if (!lockedSourceNode || !lockedWriter || !lockedSourceNode->IsAlive()) {
                    return false;
                }

                auto innerError = error.FindMatching(NChunkClient::EErrorCode::WriteThrottlingActive);

                if (!innerError.has_value()) {
                    return false;
                }

                auto address = innerError->Attributes().Find<TString>("address");
                auto needRetry = address->Empty() || std::count_if(
                    lockedWriter->Nodes_.begin(),
                    lockedWriter->Nodes_.end(),
                    [&] (const auto& node) -> bool {
                        return node->GetDefaultAddress() == address && !node->IsAlive();
                    }) == 0;

                return needRetry;
            }));

        RegisterCandidateNode(channel);

        TDataNodeServiceProxy proxy(channel);
        auto req = proxy.StartChunk();
        req->SetTimeout(Config_->NodeRpcTimeout);
        SetRequestWorkloadDescriptor(req, Config_->WorkloadDescriptor);
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_sync_on_close(Config_->SyncOnClose);
        req->set_enable_direct_io(Config_->EnableDirectIO);
        req->set_disable_send_blocks(disableSendBlocks);
        ToProto(req->mutable_placement_id(), Options_->PlacementId);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            UnregisterCandidateNode(channel);
            if (Config_->BanFailedNodes) {
                BannedNodeAddresses_.push_back(address);
            }
            YT_LOG_WARNING(rspOrError, "Failed to start write session (Address: %v)",
                address);
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto targetLocationUuid = rsp->has_location_uuid()
            ? FromProto<TChunkLocationUuid>(rsp->location_uuid())
            : InvalidChunkLocationUuid;

        YT_LOG_DEBUG("Write session started (Address: %v)", address);

        node->InitializeSession(
            Nodes_.size(),
            channel,
            targetLocationUuid);
        node->StartPing(
            BIND(&TReplicationWriter::SendPing, MakeWeak(this), MakeWeak(node)),
            Config_->NodePingPeriod);

        Nodes_.push_back(node);
        ++AliveNodeCount_;
    }

    void SendPing(const TWeakPtr<TNode>& weakNode)
    {
        auto node = weakNode.Lock();
        if (!node) {
            return;
        }

        YT_LOG_DEBUG("Sending ping (Address: %v)",
            node->GetDefaultAddress());

        TDataNodeServiceProxy proxy(node->GetChannel());
        auto req = proxy.PingSession();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG("Ping failed (Address: %v)",
                node->GetDefaultAddress());

            if (rspOrError.FindMatching(NYT::NChunkClient::EErrorCode::NoSuchSession) && !node->IsClosing()) {
                OnNodeFailed(node, rspOrError);
            }

            return;
        }

        const auto& rsp = rspOrError.Value();
        if (rsp->close_demanded()) {
            DemandClose();
        }
    }

    void CloseSessions()
    {
        VERIFY_THREAD_AFFINITY(WriterThread);
        YT_VERIFY(CloseRequested_);

        YT_LOG_INFO("Closing writer");

        for (const auto& node : Nodes_) {
            BIND(&TReplicationWriter::FinishChunk, MakeWeak(this), node)
                .Via(TDispatcher::Get()->GetWriterInvoker())
                .Run();
        }
    }

    void FinishChunk(const TNodePtr& node)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);

        if (!node->IsAlive() || node->IsClosing()) {
            return;
        }

        node->SetClosing();
        YT_LOG_DEBUG("Finishing chunk (Address: %v)",
            node->GetDefaultAddress());

        TDataNodeServiceProxy proxy(node->GetChannel());
        auto req = proxy.FinishChunk();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);

        // NB: if we are under erasure writer, he already have called #Finalize() on chunkMeta.
        // In particular, there might be parallel part writers, so in this case we should
        // not modify chunk meta in any way to avoid races.
        // But if we are immediately under confirming writer, we should finalize meta in order
        // to be able to apply block index permutation.
        if (!ChunkMeta_->IsFinalized()) {
            YT_VERIFY(!ChunkMeta_->BlockIndexMapping());
            ChunkMeta_->BlockIndexMapping() = BlockReorderer_.BlockIndexMapping();
            ChunkMeta_->Finalize();
        }

        *req->mutable_chunk_meta() = *ChunkMeta_;

        auto memoryUsageGuard = TMemoryUsageTrackerGuard::Acquire(
            Options_->MemoryUsageTracker,
            req->mutable_chunk_meta()->ByteSize());

        req->set_block_count(BlockCount_);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            OnNodeFailed(node, rspOrError);
            return;
        }

        const auto& rsp = rspOrError.Value();
        const auto& chunkInfo = rsp->chunk_info();
        YT_LOG_DEBUG("Chunk finished (Address: %v, DiskSpace: %v)",
            node->GetDefaultAddress(),
            chunkInfo.disk_space());

        node->SetFinished();
        YT_UNUSED_FUTURE(node->StopPing());
        UnregisterCandidateNode(node->GetChannel());

        ChunkInfo_ = chunkInfo;

        CheckFinished();
    }

    void CheckFinished()
    {
        int finishedNodeCount = 0;
        bool hasUnfinishedNode = false;
        for (const auto& node : Nodes_) {
            if (node->IsAlive()) {
                if (node->IsFinished()) {
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
            YT_UNUSED_FUTURE(CancelWriter());
            YT_LOG_DEBUG("Writer closed");
        }
    }

    TFuture<void> CancelWriter(bool wait = false)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TFuture<void>> cancelFutures;
        auto nodes = ExtractCandidateNodes();
        cancelFutures.reserve(nodes.size());
        for (const auto& channel : nodes) {
            TDataNodeServiceProxy proxy(channel);
            auto req = proxy.CancelChunk();
            req->set_wait_for_cancelation(wait);
            ToProto(req->mutable_session_id(), SessionId_);
            cancelFutures.push_back(req->Invoke().AsVoid());
        }

        return AllSucceeded(std::move(cancelFutures));
    }

    void AddBlocks(const std::vector<TBlock>& blocks)
    {
        VERIFY_THREAD_AFFINITY(WriterThread);
        YT_VERIFY(!CloseRequested_);

        if (StateError_.IsSet()) {
            return;
        }

        if (blocks.empty()) {
            return;
        }

        int firstBlockIndex = BlockCount_;
        int currentBlockIndex = firstBlockIndex;

        for (const auto& block : blocks) {
            EnsureCurrentGroup();

            auto blockId = TBlockId(SessionId_.ChunkId, currentBlockIndex);
            BlockCache_->PutBlock(blockId, EBlockType::CompressedData, block);

            CurrentGroup_->AddBlock(block);
            ++BlockCount_;
            ++currentBlockIndex;
            CumulativeBlockSize_ += block.Size();

            if (CurrentGroup_->GetSize() >= Config_->GroupSize) {
                CurrentGroup_->SetCumulativeBlockSize(CumulativeBlockSize_);
                FlushCurrentGroup();
            }
        }

        int lastBlockIndex = BlockCount_ - 1;

        YT_LOG_DEBUG("Blocks added (Blocks: %v-%v, Size: %v)",
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

    void DemandClose()
    {
        CloseDemanded_ = true;
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

i64 TGroup::GetCumulativeBlockSize() const
{
    return Size_;
}

bool TGroup::IsWritten() const
{
    auto writer = Writer_.Lock();
    YT_VERIFY(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    for (int nodeIndex = 0; nodeIndex < std::ssize(SentTo_); ++nodeIndex) {
        if (writer->Nodes_[nodeIndex]->IsAlive() && !SentTo_[nodeIndex]) {
            return false;
        }
    }
    return true;
}

bool TGroup::ShouldThrottle(const TString& address, const TReplicationWriterPtr& writer) const
{
    return !IsAddressLocal(address) || writer->Config_->EnableLocalThrottling;
}

void TGroup::PutGroup(const TReplicationWriterPtr& writer)
{
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    std::vector<TNodePtr> selectedNodes;
    for (int index = 0; index < std::ssize(writer->Nodes_); ++index) {
        const auto& node = writer->Nodes_[index];

        if (!node->IsAlive()) {
            continue;
        }

        if (std::ssize(selectedNodes) < writer->DirectUploadNodeCount_) {
            selectedNodes.push_back(node);
        } else if (IsAddressLocal(node->GetDefaultAddress())) {
            // If we are on the same host - this is always the best candidate.
            selectedNodes[0] = node;
        }
    }

    YT_VERIFY(!selectedNodes.empty());

    std::vector<TFuture<TDataNodeServiceProxy::TRspPutBlocksPtr>> putBlocksFutures;
    for (const auto& node : selectedNodes) {
        TDataNodeServiceProxy proxy(node->GetChannel());
        auto req = proxy.PutBlocks();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        req->SetTimeout(writer->Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), writer->SessionId_);
        req->set_first_block_index(FirstBlockIndex_);
        req->set_populate_cache(writer->Config_->PopulateCache);
        req->set_cumulative_block_size(CumulativeBlockSize_);

        SetRpcAttachedBlocks(req, Blocks_);

        YT_LOG_DEBUG("Ready to put blocks (Blocks: %v-%v, Address: %v, Size: %v, CumulativeBlockSize: %v)",
            GetStartBlockIndex(),
            GetEndBlockIndex(),
            node->GetDefaultAddress(),
            Size_,
            CumulativeBlockSize_);

        TFuture<void> throttleFuture;
        if (ShouldThrottle(node->GetDefaultAddress(), writer)) {
            throttleFuture = writer->Throttler_->Throttle(Size_).Apply(BIND([] (const TError& error) {
                if (!error.IsOK()) {
                    return TError(
                        NChunkClient::EErrorCode::ReaderThrottlingFailed,
                        "Failed to throttle bandwidth in writer")
                        << error;
                } else {
                    return TError{};
                }
            }));
        } else {
            throttleFuture = VoidFuture;
        }

        putBlocksFutures.push_back(throttleFuture.Apply(BIND([req] {
            return req->Invoke();
        })));
    }

    for (int i = 0; i < std::ssize(selectedNodes); i++) {
        const auto& node = selectedNodes[i];
        auto rspOrError = WaitFor(putBlocksFutures[i]);

        if (rspOrError.IsOK()) {
            if (rspOrError.Value()->close_demanded()) {
                YT_LOG_DEBUG("Close demanded by node (NodeAddress: %v)", node->GetDefaultAddress());
                writer->DemandClose();
            }
            SentTo_[node->GetIndex()] = true;

            writer->AccountTraffic(Size_, node->GetDescriptor());

            YT_LOG_DEBUG("Blocks are put (Blocks: %v-%v, Address: %v)",
                GetStartBlockIndex(),
                GetEndBlockIndex(),
                node->GetDefaultAddress());
        } else {
            if (rspOrError.FindMatching(NChunkClient::EErrorCode::ReaderThrottlingFailed) && !writer->StateError_.IsSet()) {
                YT_LOG_WARNING(rspOrError, "Chunk writer failed");
                YT_UNUSED_FUTURE(writer->CancelWriter());
                writer->StateError_.TrySet(rspOrError);
            } else {
                writer->OnNodeFailed(node, rspOrError);
            }
        }
    }

    ScheduleProcess();
}

void TGroup::SendGroup(const TReplicationWriterPtr& writer, const std::vector<TNodePtr>& srcNodes)
{
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    std::vector<TNodePtr> dstNodes;
    for (int index = 0; index < std::ssize(SentTo_); ++index) {
        const auto& node = writer->Nodes_[index];
        if (node->IsAlive() && !SentTo_[index]) {
            dstNodes.push_back(node);
        }
    }

    std::vector<TFuture<TDataNodeServiceProxy::TRspSendBlocksPtr>> sendBlocksFutures;
    for (int i = 0; i < std::ssize(dstNodes); i++) {
        const auto& dstNode = dstNodes[i];
        const auto& srcNode = srcNodes[i % srcNodes.size()];

        YT_LOG_DEBUG("Sending blocks (Blocks: %v-%v, SrcAddress: %v, DstAddress: %v, Size: %v, CumulativeBlockSize: %v)",
            GetStartBlockIndex(),
            GetEndBlockIndex(),
            srcNode->GetDefaultAddress(),
            dstNode->GetDefaultAddress(),
            Size_,
            CumulativeBlockSize_);

        TDataNodeServiceProxy proxy(srcNode->GetChannel());
        auto req = proxy.SendBlocks();
        // Set double timeout for SendBlocks since executing it implies another (src->dst) RPC call.
        req->SetTimeout(writer->Config_->NodeRpcTimeout * 2);
        ToProto(req->mutable_session_id(), writer->SessionId_);
        req->set_first_block_index(FirstBlockIndex_);
        req->set_block_count(Blocks_.size());
        req->set_cumulative_block_size(CumulativeBlockSize_);
        ToProto(req->mutable_target_descriptor(), dstNode->GetDescriptor());

        sendBlocksFutures.push_back(req->Invoke());
    }

    for (int i = 0; i < std::ssize(sendBlocksFutures); i++) {
        const auto& dstNode = dstNodes[i];
        const auto& srcNode = srcNodes[i % srcNodes.size()];

        auto rspOrError = WaitFor(sendBlocksFutures[i]);
        if (rspOrError.IsOK()) {
            if (rspOrError.Value()->close_demanded()) {
                YT_LOG_DEBUG("Close demanded by node (NodeAddress: %v)", dstNode->GetDefaultAddress());
                writer->DemandClose();
            }
            SentTo_[dstNode->GetIndex()] = true;

            writer->AccountTraffic(Size_, srcNode->GetDescriptor(), dstNode->GetDescriptor());

            YT_LOG_DEBUG("Blocks are sent (Blocks: %v-%v, SrcAddress: %v, DstAddress: %v)",
                FirstBlockIndex_,
                GetEndBlockIndex(),
                srcNode->GetDefaultAddress(),
                dstNode->GetDefaultAddress());
        } else {
            auto failedNode = (rspOrError.GetCode() == NChunkClient::EErrorCode::SendBlocksFailed) ? dstNode : srcNode;
            writer->OnNodeFailed(failedNode, rspOrError);
        }
    }

    ScheduleProcess();
}

bool TGroup::IsFlushing() const
{
    auto writer = Writer_.Lock();
    YT_VERIFY(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    return Flushing_;
}

void TGroup::SetFlushing()
{
    auto writer = Writer_.Lock();
    YT_VERIFY(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    Flushing_ = true;
}

void TGroup::ReorderBlocks(TBlockReorderer* blockReorderer)
{
    blockReorderer->ReorderBlocks(Blocks_);
}

void TGroup::SetCumulativeBlockSize(i64 cumulativeBlockSize)
{
    CumulativeBlockSize_ = cumulativeBlockSize;
}

void TGroup::ScheduleProcess()
{
    TDispatcher::Get()->GetWriterInvoker()->Invoke(
        BIND(&TGroup::Process, MakeWeak(this)));
}

void TGroup::Process()
{
    auto writer = Writer_.Lock();
    if (!writer) {
        return;
    }

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    if (writer->StateError_.IsSet()) {
        return;
    }

    auto state = writer->State_.load();
    if (state != EReplicationWriterState::Open &&
        state != EReplicationWriterState::Closing)
    {
        return;
    }

    YT_LOG_DEBUG("Processing blocks (Blocks: %v-%v)",
        FirstBlockIndex_,
        GetEndBlockIndex());

    std::vector<TNodePtr> nodesWithBlocks;
    bool emptyNodeFound = false;
    for (int nodeIndex = 0; nodeIndex < std::ssize(SentTo_); ++nodeIndex) {
        const auto& node = writer->Nodes_[nodeIndex];
        if (node->IsAlive()) {
            if (SentTo_[nodeIndex]) {
                nodesWithBlocks.push_back(node);
            } else {
                emptyNodeFound = true;
            }
        }
    }

    if (!emptyNodeFound) {
        writer->ShiftWindow();
    } else if (nodesWithBlocks.empty()) {
        PutGroup(writer);
    } else {
        SendGroup(writer, nodesWithBlocks);
    }
}

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateReplicationWriter(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    TSessionId sessionId,
    TChunkReplicaWithMediumList targets,
    NNative::IClientPtr client,
    TString localHostName,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler)
{
    return New<TReplicationWriter>(
        std::move(config),
        std::move(options),
        sessionId,
        std::move(targets),
        std::move(client),
        std::move(localHostName),
        std::move(throttler),
        std::move(blockCache),
        std::move(trafficMeter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

