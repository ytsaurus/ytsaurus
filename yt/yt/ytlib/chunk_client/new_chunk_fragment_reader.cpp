#include "chunk_fragment_reader.h"

#include "private.h"
#include "chunk_reader_options.h"
#include "config.h"
#include "data_node_service_proxy.h"
#include "dispatcher.h"
#include "helpers.h"
#include "chunk_replica_cache.h"
#include "chunk_fragment_read_controller.h"

#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/node_status_directory.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/sync_expiring_cache.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/helpers.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NChunkClient {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NApi;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr double SuspiciousNodePenalty = 1e20;
constexpr double BannedNodePenalty = 2e20;

using TPeerInfoCache = TSyncExpiringCache<TNodeId, TErrorOr<TPeerInfoPtr>>;
using TPeerInfoCachePtr = TIntrusivePtr<TPeerInfoCache>;

struct TChunkProbingResult
{
    TChunkReplicaInfoList Replicas;
};

struct TPeerProbingInfo
{
    TNodeId NodeId;
    TErrorOr<TPeerInfoPtr> PeerInfoOrError;
    std::vector<int> ChunkIndexes;
    std::vector<TChunkIdWithIndex> ChunkIdsWithIndices;
    std::optional<TInstant> SuspicionMarkTime;
};

struct TChunkInfo final
{
    TChunkId ChunkId;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    TInstant LastAccessTime;
    TChunkReplicaInfoList Replicas;
};

using TChunkInfoPtr = TIntrusivePtr<TChunkInfo>;

using TProbeChunkSetResult = TDataNodeServiceProxy::TRspProbeChunkSetPtr;
using TErrorOrProbeChunkSetResult = TDataNodeServiceProxy::TErrorOrRspProbeChunkSetPtr;

struct TChunkFragmentReadState final
{
    int RetryIndex = -1;
    std::vector<IChunkFragmentReader::TChunkFragmentRequest> Requests;
    IChunkFragmentReader::TReadFragmentsResponse Response;
    THashSet<TNodeId> BannedNodeIds;
};

using TChunkFragmentReadStatePtr = TIntrusivePtr<TChunkFragmentReadState>;

}  // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNewChunkFragmentReader)

class TNewChunkFragmentReader
    : public IChunkFragmentReader
{
public:
    TNewChunkFragmentReader(
        TChunkFragmentReaderConfigPtr config,
        NApi::NNative::IClientPtr client,
        INodeStatusDirectoryPtr nodeStatusDirectory,
        const NProfiling::TProfiler& profiler)
        : Config_(std::move(config))
        , Client_(std::move(client))
        , NodeDirectory_(Client_->GetNativeConnection()->GetNodeDirectory())
        , NodeStatusDirectory_(std::move(nodeStatusDirectory))
        , Networks_(Client_->GetNativeConnection()->GetNetworks())
        , Logger(ChunkClientLogger.WithTag("ChunkFragmentReaderId: %v", TGuid::Create()))
        , ReaderInvoker_(TDispatcher::Get()->GetReaderInvoker())
        , PeerInfoCache_(New<TPeerInfoCache>(
            BIND([this_ = MakeWeak(this)] (TNodeId nodeId) -> TErrorOr<TPeerInfoPtr> {
                auto reader = this_.Lock();
                if (!reader) {
                    return TError(NYT::EErrorCode::Canceled, "Reader was destroyed");
                }
                return reader->GetPeerInfo(nodeId);
            }),
            Config_->PeerInfoExpirationTimeout,
            ReaderInvoker_))
        , SuccessfulProbingRequestCounter_(profiler.Counter("/successful_probing_request_count"))
        , FailedProbingRequestCounter_(profiler.Counter("/failed_probing_request_count"))
    {
        SchedulePeriodicProbing();
    }

    TFuture<TReadFragmentsResponse> ReadFragments(
        TClientChunkReadOptions options,
        std::vector<TChunkFragmentRequest> requests) override;

private:
    class TProbingSessionBase;
    class TPeriodicProbingSession;
    class TPopulatingProbingSession;
    class TSimpleReadFragmentsSession;
    class TRetryingReadFragmentsSession;

    const TChunkFragmentReaderConfigPtr Config_;
    const NApi::NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const INodeStatusDirectoryPtr NodeStatusDirectory_;
    const TNetworkPreferenceList Networks_;

    const NLogging::TLogger Logger;
    const IInvokerPtr ReaderInvoker_;
    const TPeerInfoCachePtr PeerInfoCache_;

    NProfiling::TCounter SuccessfulProbingRequestCounter_;
    NProfiling::TCounter FailedProbingRequestCounter_;

    // TODO(babenko): maybe implement sharding
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ChunkIdToChunkInfoLock_);
    THashMap<TChunkId, TChunkInfoPtr> ChunkIdToChunkInfo_;


    TErrorOr<TPeerInfoPtr> GetPeerInfo(TNodeId nodeId) noexcept
    {
        const auto* descriptor = NodeDirectory_->FindDescriptor(nodeId);
        if (!descriptor) {
            return TError(
                NNodeTrackerClient::EErrorCode::NoSuchNode,
                "Unresolved node id %v in node directory",
                nodeId);
        }

        auto address = descriptor->FindAddress(Networks_);
        if (!address) {
            return TError(
                NNodeTrackerClient::EErrorCode::NoSuchNetwork,
                "Cannot find any of %v addresses for seed %v",
                Networks_,
                descriptor->GetDefaultAddress());
        }

        TAddressWithNetwork addressWithNetwork;
        try {
            addressWithNetwork = descriptor->GetAddressWithNetworkOrThrow(Networks_);
        } catch (const std::exception& ex) {
            return TError(ex);
        }

        const auto& channelFactory = Client_->GetChannelFactory();
        return New<TPeerInfo>(TPeerInfo{
            .NodeId = nodeId,
            .Address = addressWithNetwork,
            .Channel = channelFactory->CreateChannel(addressWithNetwork)
        });
    }

    // TODO(babenko): deal with erasure
    // TODO(babenko): drop something in replica cache?
    void DropChunkReplica(TChunkId chunkId, const TPeerInfoPtr& peerInfo)
    {
        auto mapGuard = ReaderGuard(ChunkIdToChunkInfoLock_);

        auto it = ChunkIdToChunkInfo_.find(chunkId);
        if (it == ChunkIdToChunkInfo_.end()) {
            return;
        }

        const auto& chunkInfo = it->second;
        auto entryGuard = Guard(chunkInfo->Lock);
        EraseIf(chunkInfo->Replicas, [&] (const TChunkReplicaInfo& replicaInfo) {
            return replicaInfo.PeerInfo->NodeId == peerInfo->NodeId;
        });
    }

    void RunPeriodicProbing();

    void SchedulePeriodicProbing()
    {
        TDelayedExecutor::Submit(
            BIND([weakReader = MakeWeak(this)] {
                if (auto reader = weakReader.Lock()) {
                    reader->RunPeriodicProbing();
                }
            })
            .Via(ReaderInvoker_),
            Config_->PeriodicUpdateDelay);
    }
};

DEFINE_REFCOUNTED_TYPE(TNewChunkFragmentReader)

////////////////////////////////////////////////////////////////////////////////

class TNewChunkFragmentReader::TProbingSessionBase
    : public TRefCounted
{
public:
    TProbingSessionBase(
        TNewChunkFragmentReaderPtr reader,
        TClientChunkReadOptions options,
        NLogging::TLogger logger)
        : Reader_(std::move(reader))
        , Options_(std::move(options))
        , Config_(Reader_->Config_)
        , SessionInvoker_(Reader_->ReaderInvoker_)
        , Logger(std::move(logger))
    { }

protected:
    const TNewChunkFragmentReaderPtr Reader_;
    const TClientChunkReadOptions Options_;
    const TChunkFragmentReaderConfigPtr Config_;
    const IInvokerPtr SessionInvoker_;

    const NLogging::TLogger Logger;

    NProfiling::TWallTimer Timer_;


    void DoRun(std::vector<TChunkId> chunkIds)
    {
        ChunkIds_ = std::move(chunkIds);

        YT_LOG_DEBUG("Chunk fragment reader probing session started (ChunkCount: %v)",
            ChunkIds_.size());

        auto futures = GetAllyReplicas();

        AllSet(std::move(futures)).Subscribe(BIND(
            &TProbingSessionBase::OnGotAllyReplicas,
            MakeStrong(this))
            .Via(SessionInvoker_));
    }

    virtual void OnNonexistentChunk(TChunkId chunkId) = 0;
    virtual void OnLostChunk(TChunkId chunkId) = 0;
    virtual void OnPeerProbingFailed(
        const TPeerInfoPtr& peerInfo,
        const TError& error) = 0;
    virtual void OnPeerInfoFailed(TNodeId nodeId, const TError& error) = 0;
    virtual void OnFinished(
        int probingRequestCount,
        THashMap<TChunkId, TChunkProbingResult>&& chunkIdToReplicas) = 0;

private:
    std::vector<TChunkId> ChunkIds_;
    std::vector<TAllyReplicasInfo> ReplicaInfos_;
    std::vector<TFuture<TAllyReplicasInfo>> ReplicaInfoFutures_;
    THashMap<TChunkId, TFuture<TAllyReplicasInfo>> ChunkIdToReplicaInfoFuture_;


    std::vector<TFuture<TAllyReplicasInfo>> GetAllyReplicas()
    {
        ReplicaInfoFutures_.reserve(ChunkIds_.size());
        ChunkIdToReplicaInfoFuture_.reserve(ChunkIds_.size());

        const auto& chunkReplicaCache = Reader_->Client_->GetNativeConnection()->GetChunkReplicaCache();
        ReplicaInfoFutures_ = chunkReplicaCache->GetReplicas(ChunkIds_);
        for (int index = 0; index < std::ssize(ReplicaInfoFutures_); ++index) {
            EmplaceOrCrash(ChunkIdToReplicaInfoFuture_, ChunkIds_[index], ReplicaInfoFutures_[index]);
        }

        return ReplicaInfoFutures_;
    }

    void OnGotAllyReplicas(
        TErrorOr<std::vector<TErrorOr<TAllyReplicasInfo>>> resultOrError)
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        YT_VERIFY(resultOrError.IsOK());
        auto allyReplicasInfosOrErrors = std::move(resultOrError.Value());

        YT_VERIFY(allyReplicasInfosOrErrors.size() == ChunkIds_.size());
        ReplicaInfos_.reserve(ChunkIds_.size());

        for (int chunkIndex = 0; chunkIndex < std::ssize(ChunkIds_); ++chunkIndex) {
            auto chunkId = ChunkIds_[chunkIndex];
            auto& allyReplicasInfosOrError = allyReplicasInfosOrErrors[chunkIndex];
            if (!allyReplicasInfosOrError.IsOK()) {
                if (allyReplicasInfosOrError.GetCode() == NChunkClient::EErrorCode::NoSuchChunk) {
                    OnNonexistentChunk(chunkId);
                }
                ReplicaInfos_.emplace_back();
            } else {
                ReplicaInfos_.push_back(std::move(allyReplicasInfosOrError.Value()));
            }

            if (!ReplicaInfos_.back()) {
                TryDiscardChunkReplicas(chunkId);
            }
        }

        auto probingInfos = MakeNodeProbingInfos(ReplicaInfos_);

        std::vector<TFuture<TProbeChunkSetResult>> probingFutures;
        probingFutures.reserve(probingInfos.size());
        for (int nodeIndex = 0; nodeIndex < std::ssize(probingInfos); ++nodeIndex) {
            const auto& probingInfo = probingInfos[nodeIndex];
            if (!probingInfo.PeerInfoOrError.IsOK()) {
                YT_LOG_ERROR(probingInfo.PeerInfoOrError, "Failed to obtain peer info (NodeId: %v)",
                    probingInfo.NodeId);

                OnPeerInfoFailed(probingInfo.NodeId, probingInfo.PeerInfoOrError);
                Reader_->PeerInfoCache_->Invalidate(probingInfo.NodeId);
                probingFutures.push_back(MakeFuture<TProbeChunkSetResult>(nullptr));
                continue;
            }
            probingFutures.push_back(ProbePeer(probingInfo));
        }

        AllSet(std::move(probingFutures)).Subscribe(BIND(
            &TProbingSessionBase::OnPeersProbed,
            MakeStrong(this),
            Passed(std::move(probingInfos)))
            .Via(SessionInvoker_));
    }

    void OnPeersProbed(
        std::vector<TPeerProbingInfo> probingInfos,
        const TErrorOr<std::vector<TErrorOrProbeChunkSetResult>>& resultOrError)
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        YT_VERIFY(resultOrError.IsOK());
        const auto& probingRspOrErrors = resultOrError.Value();

        YT_VERIFY(probingInfos.size() == probingRspOrErrors.size());

        THashMap<TChunkId, TChunkProbingResult> chunkIdToProbingResult;
        for (auto chunkId : ChunkIds_) {
            EmplaceOrCrash(chunkIdToProbingResult, chunkId, TChunkProbingResult());
        }

        int successfulProbingRequestCount = 0;
        int failedProbingRequestCount = 0;
        for (int nodeIndex = 0; nodeIndex < std::ssize(probingInfos); ++nodeIndex) {
            const auto& probingInfo = probingInfos[nodeIndex];
            const auto& peerInfoOrError = probingInfo.PeerInfoOrError;
            if (!peerInfoOrError.IsOK()) {
                continue;
            }
            const auto& peerInfo = peerInfoOrError.Value();

            const auto& probingRspOrError = probingRspOrErrors[nodeIndex];
            auto suspicionMarkTime = probingInfo.SuspicionMarkTime;

            if (!probingRspOrError.IsOK()) {
                YT_VERIFY(probingRspOrError.GetCode() != EErrorCode::NoSuchChunk);

                YT_LOG_ERROR(probingRspOrError, "Failed to probe peer (NodeId: %v, Address: %v)",
                    probingInfo.NodeId,
                    peerInfo->Address);

                MaybeMarkNodeSuspicious(probingRspOrError, probingInfo);

                if (suspicionMarkTime && Config_->SuspiciousNodeGracePeriod) {
                    auto now = NProfiling::GetInstant();
                    if (*suspicionMarkTime + *Config_->SuspiciousNodeGracePeriod < now) {
                        YT_LOG_DEBUG("Discarding seeds due to node being suspicious "
                            "(NodeId: %v, Address: %v, SuspicionTime: %v)",
                            probingInfo.NodeId,
                            peerInfo->Address,
                            suspicionMarkTime);
                        for (const auto& chunkIdWithIndex : probingInfo.ChunkIdsWithIndices) {
                            TryDiscardChunkReplicas(chunkIdWithIndex.Id);
                        }
                    }
                }

                ++failedProbingRequestCount;

                // TODO(akozhikhov): Don't invalidate upon specific errors like RequestQueueSizeLimitExceeded.
                Reader_->PeerInfoCache_->Invalidate(probingInfo.NodeId);

                OnPeerProbingFailed(peerInfo, probingRspOrError);

                continue;
            }

            if (suspicionMarkTime) {
                YT_LOG_DEBUG("Node is not suspicious anymore (NodeId: %v, Address: %v)",
                    probingInfo.NodeId,
                    peerInfo->Address);
                Reader_->NodeStatusDirectory_->UpdateSuspicionMarkTime(
                    probingInfo.NodeId,
                    peerInfo->Address.Address,
                    /*suspicious*/ false,
                    suspicionMarkTime);
            }

            ++successfulProbingRequestCount;

            const auto& probingRsp = probingRspOrError.Value();
            YT_VERIFY(std::ssize(probingInfo.ChunkIndexes) == probingRsp->subresponses_size());

            if (probingRsp->net_throttling()) {
                YT_LOG_DEBUG("Peer is net-throttling (Address: %v, NetQueueSize: %v)",
                    peerInfo->Address,
                    probingRsp->net_queue_size());
            }

            auto mapGuard = ReaderGuard(Reader_->ChunkIdToChunkInfoLock_);
            for (int resultIndex = 0; resultIndex < std::ssize(probingInfo.ChunkIndexes); ++resultIndex) {
                auto chunkIndex = probingInfo.ChunkIndexes[resultIndex];
                const auto& subresponse = probingRsp->subresponses(resultIndex);

                const auto& chunkIdWithIndex = probingInfo.ChunkIdsWithIndices[resultIndex];
                YT_VERIFY(chunkIdWithIndex.Id == ChunkIds_[chunkIndex]);

                TryUpdateChunkReplicas(chunkIdWithIndex.Id, subresponse);

                if (!subresponse.has_complete_chunk()) {
                    YT_LOG_WARNING("Chunk is missing from node (ChunkId: %v, Address: %v)",
                        chunkIdWithIndex,
                        peerInfo->Address);
                    continue;
                }

                if (subresponse.disk_throttling()) {
                    YT_LOG_DEBUG("Peer is disk-throttling (Address: %v, ChunkId: %v, DiskQueueSize: %v)",
                        peerInfo->Address,
                        chunkIdWithIndex,
                        subresponse.disk_queue_size());
                }

                auto& probingResult = chunkIdToProbingResult[chunkIdWithIndex.Id];
                probingResult.Replicas.push_back(TChunkReplicaInfo{
                    .ReplicaIndex = chunkIdWithIndex.ReplicaIndex,
                    .Penalty = ComputeProbingPenalty(probingRsp->net_queue_size(), subresponse.disk_queue_size()),
                    .PeerInfo = peerInfo
                });
            }
        }

        Reader_->SuccessfulProbingRequestCounter_.Increment(successfulProbingRequestCount);
        Reader_->FailedProbingRequestCounter_.Increment(failedProbingRequestCount);

        std::vector<TChunkId> lostChunkIds;
        for (auto it = chunkIdToProbingResult.begin(); it != chunkIdToProbingResult.end();) {
            auto& [chunkId, probingResult] = *it;
            if (probingResult.Replicas.empty()) {
                TryDiscardChunkReplicas(chunkId);
                OnLostChunk(chunkId);

                lostChunkIds.push_back(chunkId);
                chunkIdToProbingResult.erase(it++);
            } else {
                ++it;
            }
        }

        YT_LOG_DEBUG_IF(!lostChunkIds.empty(), "Some chunks are lost (ChunkIds: %v)",
            lostChunkIds);

        YT_LOG_DEBUG("Chunk fragment reader probing session completed (WallTime: %v)",
            Timer_.GetElapsedTime());

        OnFinished(
            std::ssize(probingInfos),
            std::move(chunkIdToProbingResult));
    }

    template <typename TResponse>
    void TryUpdateChunkReplicas(
        TChunkId chunkId,
        const TResponse& response)
    {
        VERIFY_READER_SPINLOCK_AFFINITY(Reader_->ChunkIdToChunkInfoLock_);

        auto it = ChunkIdToReplicaInfoFuture_.find(chunkId);
        if (it == ChunkIdToReplicaInfoFuture_.end()) {
            return;
        }

        const auto& protoAllyReplicas = response.ally_replicas();
        if (protoAllyReplicas.replicas_size() == 0) {
            return;
        }

        const auto& chunkReplicaCache = Reader_->Client_->GetNativeConnection()->GetChunkReplicaCache();
        chunkReplicaCache->UpdateReplicas(chunkId, FromProto<TAllyReplicasInfo>(protoAllyReplicas));
    }

    void TryDiscardChunkReplicas(TChunkId chunkId) const
    {
        auto it = ChunkIdToReplicaInfoFuture_.find(chunkId);
        if (it == ChunkIdToReplicaInfoFuture_.end()) {
            return;
        }

        const auto& chunkReplicaCache = Reader_->Client_->GetNativeConnection()->GetChunkReplicaCache();
        chunkReplicaCache->DiscardReplicas(chunkId, it->second);
    }

    std::vector<TPeerProbingInfo> MakeNodeProbingInfos(const std::vector<TAllyReplicasInfo>& allyReplicasInfos)
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        std::vector<TNodeId> nodeIds;
        std::vector<TPeerProbingInfo> probingInfos;
        THashMap<TNodeId, int> nodeIdToNodeIndex;

        for (int chunkIndex = 0; chunkIndex < std::ssize(allyReplicasInfos); ++chunkIndex) {
            auto chunkId = ChunkIds_[chunkIndex];

            const auto& allyReplicas = allyReplicasInfos[chunkIndex];
            if (!allyReplicas) {
                // NB: This branch is possible within periodic updates.
                continue;
            }

            for (auto chunkReplica : allyReplicas.Replicas) {
                auto nodeId = chunkReplica.GetNodeId();
                auto chunkIdWithIndex = TChunkIdWithIndex(chunkId, chunkReplica.GetReplicaIndex());
                auto [it, emplaced] = nodeIdToNodeIndex.try_emplace(nodeId);
                if (emplaced) {
                    it->second = probingInfos.size();
                    probingInfos.push_back({
                        .NodeId = nodeId
                    });
                    nodeIds.push_back(nodeId);
                }
                probingInfos[it->second].ChunkIndexes.push_back(chunkIndex);
                probingInfos[it->second].ChunkIdsWithIndices.push_back(chunkIdWithIndex);
            }
        }

        auto peerInfoOrErrors = Reader_->PeerInfoCache_->Get(nodeIds);
        for (int i = 0; i < std::ssize(peerInfoOrErrors); ++i) {
            YT_VERIFY(!peerInfoOrErrors[i].IsOK() || peerInfoOrErrors[i].Value()->Channel);
            probingInfos[i].PeerInfoOrError = std::move(peerInfoOrErrors[i]);
        }

        auto nodeIdToSuspicionMarkTime = Reader_->NodeStatusDirectory_->RetrieveSuspiciousNodeIdsWithMarkTime(nodeIds);
        for (auto nodeIndex = 0; nodeIndex < std::ssize(probingInfos); ++nodeIndex) {
            auto& probingInfo = probingInfos[nodeIndex];
            auto it = nodeIdToSuspicionMarkTime.find(probingInfo.NodeId);
            if (it != nodeIdToSuspicionMarkTime.end()) {
                probingInfo.SuspicionMarkTime = it->second;
            }
        }

        return probingInfos;
    }

    TFuture<TProbeChunkSetResult> ProbePeer(const TPeerProbingInfo& probingInfo) const
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        YT_VERIFY(probingInfo.PeerInfoOrError.IsOK());
        const auto& peerInfo = probingInfo.PeerInfoOrError.Value();
        YT_VERIFY(peerInfo->Channel);

        TDataNodeServiceProxy proxy(peerInfo->Channel);
        proxy.SetDefaultTimeout(Config_->ProbeChunkSetRpcTimeout);

        auto req = proxy.ProbeChunkSet();
        req->SetResponseHeavy(true);
        SetRequestWorkloadDescriptor(req, Options_.WorkloadDescriptor);
        for (const auto& chunkIdWithIndex : probingInfo.ChunkIdsWithIndices) {
            ToProto(req->add_chunk_ids(), EncodeChunkId(chunkIdWithIndex));
        }
        req->SetAcknowledgementTimeout(std::nullopt);

        return req->Invoke();
    }

    double ComputeProbingPenalty(i64 netQueueSize, i64 diskQueueSize) const
    {
        return
            Config_->NetQueueSizeFactor * netQueueSize +
            Config_->DiskQueueSizeFactor * diskQueueSize;
    }

    void MaybeMarkNodeSuspicious(const TError& error, const TPeerProbingInfo& probingInfo)
    {
        const auto& peerInfo = probingInfo.PeerInfoOrError.Value();
        if (!probingInfo.SuspicionMarkTime &&
            Reader_->NodeStatusDirectory_->ShouldMarkNodeSuspicious(error))
        {
            YT_LOG_WARNING(error, "Node is marked as suspicious (NodeId: %v, Address: %v)",
                probingInfo.NodeId,
                peerInfo->Address);
            Reader_->NodeStatusDirectory_->UpdateSuspicionMarkTime(
                probingInfo.NodeId,
                peerInfo->Address.Address,
                /*suspicious*/ true,
                std::nullopt);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNewChunkFragmentReader::TPeriodicProbingSession
    : public TProbingSessionBase
{
public:
    explicit TPeriodicProbingSession(TNewChunkFragmentReaderPtr reader)
        : TProbingSessionBase(
            reader,
            MakeOptions(),
            reader->Logger.WithTag("PeriodicProbingSessionId: %v", TGuid::Create()))
    { }

    void Run()
    {
        ScanChunks();
        DoRun(std::move(LiveChunkIds_));
    }

private:
    std::vector<TChunkId> LiveChunkIds_;
    std::vector<TChunkId> ExpiredChunkIds_;
    std::vector<TChunkId> NonexistentChunkIds_;
    std::vector<TChunkId> LostChunkIds_;


    static TClientChunkReadOptions MakeOptions()
    {
        return {
            // TODO(akozhikhov): Employ some system category.
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserBatch),
            .ReadSessionId = TReadSessionId::Create()
        };
    }

    void ScanChunks()
    {
        auto now = NProfiling::GetInstant();
        auto mapGuard = ReaderGuard(Reader_->ChunkIdToChunkInfoLock_);
        for (const auto& [chunkId, chunkInfo] : Reader_->ChunkIdToChunkInfo_) {
            auto entryGuard = Guard(chunkInfo->Lock);
            if (IsChunkInfoExpired(chunkInfo, now)) {
                ExpiredChunkIds_.push_back(chunkId);
            } else {
                LiveChunkIds_.push_back(chunkId);
            }
        }
    }

    bool IsChunkInfoExpired(const TChunkInfoPtr& chunkInfo, TInstant now) const
    {
        VERIFY_SPINLOCK_AFFINITY(chunkInfo->Lock);
        return chunkInfo->LastAccessTime + Config_->ChunkInfoCacheExpirationTimeout < now;
    }

    void OnNonexistentChunk(TChunkId chunkId) override
    {
        NonexistentChunkIds_.push_back(chunkId);
    }

    void OnLostChunk(TChunkId chunkId) override
    {
        LostChunkIds_.push_back(chunkId);
    }

    void OnPeerInfoFailed(TNodeId /*nodeId*/, const TError& /*error*/) override
    { }

    void OnPeerProbingFailed(const TPeerInfoPtr& /*peerInfo*/, const TError& /*error*/) override
    { }

    virtual void OnFinished(
        int /*probingRequestCount*/,
        THashMap<TChunkId, TChunkProbingResult>&& chunkIdToProbingResult) override
    {
        {
            auto mapGuard = ReaderGuard(Reader_->ChunkIdToChunkInfoLock_);

            for (auto& [chunkId, probingResult] : chunkIdToProbingResult) {
                auto it = Reader_->ChunkIdToChunkInfo_.find(chunkId);
                if (it == Reader_->ChunkIdToChunkInfo_.end()) {
                    continue;
                }

                const auto& chunkInfo = it->second;
                auto entryGuard = Guard(chunkInfo->Lock);
                chunkInfo->Replicas = std::move(probingResult.Replicas);
            }
        }

        EraseBadChunksIfAny();

        Reader_->SchedulePeriodicProbing();
    }

    void EraseBadChunksIfAny()
    {
        if (ExpiredChunkIds_.empty() ||
            NonexistentChunkIds_.empty() ||
            LostChunkIds_.empty())
        {
            return;
        }

        auto now = NProfiling::GetInstant();

        {
            auto mapGuard = WriterGuard(Reader_->ChunkIdToChunkInfoLock_);

            for (auto chunkId : ExpiredChunkIds_) {
                auto it = Reader_->ChunkIdToChunkInfo_.find(chunkId);
                if (it != Reader_->ChunkIdToChunkInfo_.end()) {
                    if (IsChunkInfoExpired(it->second, now)) {
                        Reader_->ChunkIdToChunkInfo_.erase(it);
                    }
                }
            }

            for (auto chunkId : NonexistentChunkIds_) {
                Reader_->ChunkIdToChunkInfo_.erase(chunkId);
            }

            for (auto chunkId : LostChunkIds_) {
                Reader_->ChunkIdToChunkInfo_.erase(chunkId);
            }
        }

        YT_LOG_DEBUG("Erased chunk infos within periodic probing session "
            "(ExpiredChunkCount: %v, NonexistentChunkCount: %v, LostChunkCount: %v)",
            ExpiredChunkIds_.size(),
            NonexistentChunkIds_.size(),
            LostChunkIds_.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNewChunkFragmentReader::TPopulatingProbingSession
    : public TProbingSessionBase
{
public:
    TPopulatingProbingSession(
        TNewChunkFragmentReaderPtr reader,
        TClientChunkReadOptions options,
        NLogging::TLogger logger)
        : TProbingSessionBase(
            std::move(reader),
            std::move(options),
            std::move(logger))
    { }

    TFuture<void> Run(std::vector<TChunkId> chunkIds)
    {
        DoRun(std::move(chunkIds));
        return Promise_;
    }

    TChunkInfoPtr FindChunkInfo(TChunkId chunkId)
    {
        auto it = ChunkIdToInfo_.find(chunkId);
        return it == ChunkIdToInfo_.end() ? nullptr : it->second;
    }

    int GetProbingRequestCount()
    {
        return ProbingRequestCount_;
    }

    const std::vector<std::pair<TPeerInfoPtr, TError>>& GetFailures()
    {
        return Failures_;
    }

private:
    const TPromise<void> Promise_ = NewPromise<void>();

    THashMap<TChunkId, TChunkInfoPtr> ChunkIdToInfo_;
    int ProbingRequestCount_ = 0;
    std::vector<std::pair<TPeerInfoPtr, TError>> Failures_;


    void OnFatalError(TError error)
    {
        Promise_.TrySet(std::move(error));
    }

    void OnNonexistentChunk(TChunkId chunkId) override
    {
        OnFatalError(TError(
            NChunkClient::EErrorCode::NoSuchChunk,
            "No such chunk %v",
            chunkId));
    }

    void OnLostChunk(TChunkId /*chunkId*/) override
    { }

    void OnPeerInfoFailed(TNodeId /*nodeId*/, const TError& error) override
    {
        if (error.FindMatching(NNodeTrackerClient::EErrorCode::NoSuchNetwork)) {
            OnFatalError(error);
        }
    }

    void OnPeerProbingFailed(const TPeerInfoPtr& peerInfo, const TError& error) override
    {
        Failures_.emplace_back(peerInfo, error);
    }

    void OnFinished(
        int probingRequestCount,
        THashMap<TChunkId, TChunkProbingResult>&& chunkIdToProbingResult) override
    {
        ProbingRequestCount_ = probingRequestCount;

        auto now = TInstant::Now();

        int addedChunkCount = 0;
        {
            auto mapGuard = WriterGuard(Reader_->ChunkIdToChunkInfoLock_);

            for (auto& [chunkId, probingResult] : chunkIdToProbingResult) {
                YT_VERIFY(!probingResult.Replicas.empty());

                auto it = Reader_->ChunkIdToChunkInfo_.find(chunkId);
                if (it == Reader_->ChunkIdToChunkInfo_.end()) {
                    auto chunkInfo = New<TChunkInfo>();
                    chunkInfo->ChunkId = chunkId;
                    chunkInfo->LastAccessTime = now;
                    it = EmplaceOrCrash(Reader_->ChunkIdToChunkInfo_, chunkId, std::move(chunkInfo));
                    ++addedChunkCount;
                }

                const auto& chunkInfo = it->second;
                EmplaceOrCrash(ChunkIdToInfo_, chunkId, chunkInfo);

                auto entryGuard = Guard(chunkInfo->Lock);
                chunkInfo->Replicas = std::move(probingResult.Replicas);
            }
        }

        YT_LOG_DEBUG_IF(addedChunkCount > 0, "Added chunk infos within populating probing session (ChunkCount: %v)",
            addedChunkCount);

        Promise_.TrySet();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNewChunkFragmentReader::TSimpleReadFragmentsSession
    : public TRefCounted
{
public:
    TSimpleReadFragmentsSession(
        TNewChunkFragmentReaderPtr reader,
        TClientChunkReadOptions options,
        TChunkFragmentReadStatePtr state,
        NLogging::TLogger logger)
        : Reader_(std::move(reader))
        , Options_(std::move(options))
        , State_(std::move(state))
        , Logger(std::move(logger))
        , SessionInvoker_(CreateSerializedInvoker(Reader_->ReaderInvoker_))
    { }

    TFuture<void> Run()
    {
        DoRun();
        return Promise_;
    }

    std::vector<TError>& Errors()
    {
        return Errors_;
    }

private:
    const TNewChunkFragmentReaderPtr Reader_;
    const TClientChunkReadOptions Options_;
    const TChunkFragmentReadStatePtr State_;
    const NLogging::TLogger Logger;

    const IInvokerPtr SessionInvoker_;
    const TPromise<void> Promise_ = NewPromise<void>();

    NProfiling::TWallTimer Timer_;

    int PendingFragmentCount_ = 0;
    int PendingChunkCount_ = 0;

    int TotalBackendReadRequestCount_ = 0;
    int TotalBackendResponseCount_ = 0;

    int BackendReadRequestCount_ = 0;
    int BackendHedgingReadRequestCount_ = 0;

    std::vector<TError> Errors_;

    struct TChunkState
    {
        TChunkReplicaInfoList Replicas;
        std::unique_ptr<IChunkFragmentReadController> Controller;
    };

    THashMap<TChunkId, TChunkState> ChunkIdToChunkState_;

    THashMap<TNodeId, TInstant> NodeIdToSuspicionMarkTime_;

    struct TPerPeerPlanItem
    {
        TChunkState* ChunkState = nullptr;
        const TChunkFragmentReadControllerPlan* Plan = nullptr;
        int PeerIndex = -1;
        int RequestedFragmentCount = 0;
    };

    struct TPerPeerPlan final
    {
        TPeerInfoPtr PeerInfo;
        std::vector<TPerPeerPlanItem> Items;
    };

    using TPerPeerPlanPtr = TIntrusivePtr<TPerPeerPlan>;


    void OnFatalError(TError error)
    {
        Promise_.TrySet(std::move(error));
    }

    void OnError(TError error)
    {
        YT_LOG_DEBUG(error, "Error recorded while reading chunk fragments");
        Errors_.push_back(std::move(error));
    }

    void BanPeer(const TPeerInfoPtr& peerInfo)
    {
        if (State_->BannedNodeIds.insert(peerInfo->NodeId).second) {
            YT_LOG_DEBUG("Peer banned (NodeId: %v, Address: %v)",
                peerInfo->NodeId,
                peerInfo->Address);
        }
    }

    void OnCompleted()
    {
        State_->Response.BackendReadRequestCount += BackendReadRequestCount_;
        State_->Response.BackendHedgingReadRequestCount += BackendHedgingReadRequestCount_;

        if (Promise_.TrySet()) {
            YT_LOG_DEBUG("Chunk fragment read session completed (RetryIndex: %v/%v, WallTime: %v)",
                State_->RetryIndex + 1,
                Reader_->Config_->RetryCountLimit,
                Timer_.GetElapsedTime(),
                BackendReadRequestCount_,
                BackendHedgingReadRequestCount_);
        }
    }

    void OnFailed()
    {
        Promise_.TrySet(TError("Failed to read chunk fragments")
            << std::move(Errors_));
    }

    void DoRun()
    {
        // Group requested fragments by chunks, construct controllers.
        i64 dataWeight = 0;
        for (int index = 0; index < std::ssize(State_->Requests); ++index) {
            if (State_->Response.Fragments[index]) {
                continue;
            }

            const auto& request = State_->Requests[index];

            ++PendingFragmentCount_;
            dataWeight += request.Length;

            auto it = ChunkIdToChunkState_.find(request.ChunkId);
            if (it == ChunkIdToChunkState_.end()) {
                it = ChunkIdToChunkState_.emplace(request.ChunkId, TChunkState()).first;
                it->second.Controller = CreateChunkFragmentReadController(
                    request.ChunkId,
                    request.ErasureCodec,
                    &State_->Response.Fragments);
            }

            auto& controller = *it->second.Controller;
            controller.RegisterRequest({
                .FragmentIndex = index,
                .Length = request.Length,
                .BlockIndex = request.BlockIndex,
                .BlockOffset = request.BlockOffset
            });
        }

        // Fill some basic response statistics on the very first retry.
        if (State_->RetryIndex == 0) {
            State_->Response.ChunkCount = std::ssize(ChunkIdToChunkState_);
            State_->Response.DataWeight = dataWeight;
        }

        // Fetch chunk infos from cache.
        std::vector<TChunkId> uncachedChunkIds;
        auto now = Timer_.GetStartTime();
        {
            auto mapGuard = ReaderGuard(Reader_->ChunkIdToChunkInfoLock_);

            for (auto& [chunkId, chunkState] : ChunkIdToChunkState_) {
                auto it = Reader_->ChunkIdToChunkInfo_.find(chunkId);
                if (it == Reader_->ChunkIdToChunkInfo_.end()) {
                    uncachedChunkIds.push_back(chunkId);
                } else {
                    const auto& chunkInfo = it->second;
                    auto entryGuard = Guard(chunkInfo->Lock);
                    chunkState.Replicas = chunkInfo->Replicas;
                    if (chunkInfo->LastAccessTime < now) {
                        chunkInfo->LastAccessTime = now;
                    }
                }
            }
        }

        if (uncachedChunkIds.empty()) {
            OnChunkInfosReady();
        } else {
            PopulateChunkInfos(std::move(uncachedChunkIds));
        }
     }

     void PopulateChunkInfos(std::vector<TChunkId> chunkIds)
     {
        YT_LOG_DEBUG("Some chunk infos are missing; will populate the cache (ChunkIds: %v)",
            chunkIds);

        auto subsession = New<TPopulatingProbingSession>(Reader_, Options_, Logger);
        subsession->Run(std::move(chunkIds)).Subscribe(
            BIND(&TSimpleReadFragmentsSession::OnChunkInfosPopulated, MakeStrong(this), subsession)
                .Via(SessionInvoker_));
     }

     void OnChunkInfosPopulated(const TIntrusivePtr<TPopulatingProbingSession>& subsession, const TError& error)
     {
        if (!error.IsOK()) {
            OnFatalError(error);
            return;
        }

        State_->Response.BackendProbingRequestCount += subsession->GetProbingRequestCount();

        for (const auto& [peerInfo, error] : subsession->GetFailures()) {
            OnError(error);
            BanPeer(peerInfo);
        }

        // Take chunk replicas from just finished populating session.
        for (auto& [chunkId, chunkState] : ChunkIdToChunkState_) {
            if (!chunkState.Replicas.empty()) {
                continue;
            }
            if (auto chunkInfo = subsession->FindChunkInfo(chunkId)) {
                auto entryGuard = Guard(chunkInfo->Lock);
                chunkState.Replicas = chunkInfo->Replicas;
            }
        }

         OnChunkInfosReady();
     }

    void OnChunkInfosReady()
    {
        // Construct the set of potential nodes we're about to contact during this session.
        std::vector<TNodeId> nodeIds;
        for (const auto& [chunkId, chunkState] : ChunkIdToChunkState_) {
            for (const auto& replica : chunkState.Replicas) {
                nodeIds.push_back(replica.PeerInfo->NodeId);
            }
        }

        SortUnique(nodeIds);

        NodeIdToSuspicionMarkTime_ = Reader_->NodeStatusDirectory_->RetrieveSuspiciousNodeIdsWithMarkTime(nodeIds);

        // Provide same penalty for each node across all replicas.
        // Adjust replica penalties based on suspiciousness and bans.
        // Sort replicas and feed them to controllers.
        THashMap<TNodeId, double> nodeIdToPenalty;
        for (auto& [chunkId, chunkState] : ChunkIdToChunkState_) {
            if (chunkState.Replicas.empty()) {
                continue;
            }

            for (auto& replica : chunkState.Replicas) {
                auto nodeId = replica.PeerInfo->NodeId;
                if (auto it = nodeIdToPenalty.find(nodeId); it != nodeIdToPenalty.end()) {
                    replica.Penalty = it->second;
                } else {
                    EmplaceOrCrash(nodeIdToPenalty, nodeId, replica.Penalty);
                }
                if (NodeIdToSuspicionMarkTime_.contains(nodeId)) {
                    replica.Penalty += SuspiciousNodePenalty;
                } else if (State_->BannedNodeIds.contains(nodeId)) {
                    replica.Penalty += BannedNodePenalty;
                }
            }

            chunkState.Controller->SetReplicas(chunkState.Replicas);
            ++PendingChunkCount_;
        }

        int unavailableChunkCount = PendingChunkCount_ - std::ssize(ChunkIdToChunkState_);

        YT_LOG_DEBUG("Chunk fragment read session started "
            "(RetryIndex: %v/%v, PendingFragmentCount: %v, TotalChunkCount: %v, UnavailableChunkCount: %v, TotalNodeCount: %v, SuspiciousNodeCount: %v)",
            State_->RetryIndex + 1,
            Reader_->Config_->RetryCountLimit,
            PendingFragmentCount_,
            ChunkIdToChunkState_.size(),
            unavailableChunkCount,
            nodeIds.size(),
            NodeIdToSuspicionMarkTime_.size());

        NextRound(/*isHedged*/ false);

        if (auto hedgingDelay = Reader_->Config_->FragmentReadHedgingDelay) {
            if (*hedgingDelay == TDuration::Zero()) {
                OnHedgingDeadlineReached();
            } else {
                TDelayedExecutor::Submit(
                    BIND(&TSimpleReadFragmentsSession::OnHedgingDeadlineReached, MakeStrong(this)),
                    *hedgingDelay,
                    SessionInvoker_);
            }
        }
    }

    void OnHedgingDeadlineReached()
    {
        // Shortcut.
        if (Promise_.IsSet()) {
            return;
        }

        YT_LOG_DEBUG("Hedging deadline reached (Delay: %v)",
            Reader_->Config_->FragmentReadHedgingDelay);

        NextRound(/*isHedged*/ true);
    }

    void NextRound(bool isHedged)
    {
        auto peerInfoToPlan = MakePlans();
        RequestFragments(std::move(peerInfoToPlan), isHedged);
    }

    THashMap<TPeerInfoPtr, TPerPeerPlanPtr> MakePlans()
    {
        // Request controllers to make their plans and group plans by nodes.
        THashMap<TPeerInfoPtr, TPerPeerPlanPtr> peerInfoToPlan;
        for (auto& [chunkId, chunkState] : ChunkIdToChunkState_) {
            if (chunkState.Replicas.empty()) {
                continue;
            }

            if (chunkState.Controller->IsDone()) {
                continue;
            }

            if (auto plan = chunkState.Controller->TryMakePlan()) {
                for (auto peerIndex : plan->PeerIndices) {
                    const auto& replicaInfo = chunkState.Controller->GetReplica(peerIndex);
                    auto& perPeerPlan = peerInfoToPlan[replicaInfo.PeerInfo];
                    if (!perPeerPlan) {
                        perPeerPlan = New<TPerPeerPlan>();
                        perPeerPlan->PeerInfo = replicaInfo.PeerInfo;
                    }
                    perPeerPlan->Items.push_back({
                        .ChunkState = &chunkState,
                        .Plan = plan,
                        .PeerIndex = peerIndex
                    });
                }
            }
        }
        return peerInfoToPlan;
    }

    void RequestFragments(THashMap<TPeerInfoPtr, TPerPeerPlanPtr> peerInfoToPlan, bool isHedged)
    {
        int requestCount = std::ssize(peerInfoToPlan);
        TotalBackendReadRequestCount_ += requestCount;
        (isHedged ? BackendHedgingReadRequestCount_ : BackendReadRequestCount_) += requestCount;

        for (const auto& [peerInfo, plan] : peerInfoToPlan) {
            TDataNodeServiceProxy proxy(peerInfo->Channel);
            proxy.SetDefaultTimeout(Reader_->Config_->GetChunkFragmentSetRpcTimeout);

            auto req = proxy.GetChunkFragmentSet();
            req->SetResponseHeavy(true);
            req->SetMultiplexingBand(Options_.MultiplexingBand);
            req->SetMultiplexingParallelism(Options_.MultiplexingParallelism);
            SetRequestWorkloadDescriptor(req, Options_.WorkloadDescriptor);
            ToProto(req->mutable_read_session_id(), Options_.ReadSessionId);
            req->set_use_direct_io(Reader_->Config_->UseDirectIO);

            for (auto& item : plan->Items) {
                auto* subrequest = req->add_subrequests();
                item.ChunkState->Controller->PrepareRpcSubrequest(
                    item.Plan,
                    item.PeerIndex,
                    subrequest);
                item.RequestedFragmentCount = subrequest->fragments_size();
            }

            YT_LOG_DEBUG("Requesting chunk fragments (Address: %v, ChunkIds: %v)",
                peerInfo->Address,
                MakeFormattableView(plan->Items, [] (auto* builder, const auto& item) {
                    builder->AppendFormat("%v", item.ChunkState->Controller->GetChunkId());
                }));

            req->Invoke().Subscribe(
                BIND(
                    &TSimpleReadFragmentsSession::OnGotChunkFragments,
                    MakeStrong(this),
                    plan)
                .Via(SessionInvoker_));
        }
    }

    void MaybeMarkNodeSuspicious(const TError& error, const TPeerInfoPtr& peerInfo)
    {
        if (!NodeIdToSuspicionMarkTime_.contains(peerInfo->NodeId) &&
            Reader_->NodeStatusDirectory_->ShouldMarkNodeSuspicious(error))
        {
            YT_LOG_DEBUG("Node is marked as suspicious (NodeId: %v, Address: %v)",
                peerInfo->NodeId,
                peerInfo->Address);
            Reader_->NodeStatusDirectory_->UpdateSuspicionMarkTime(
                peerInfo->NodeId,
                peerInfo->Address.Address,
                /*suspicious*/ true,
                std::nullopt);
        }
    }

    template <typename TResponse>
    void TryUpdateChunkReplicas(
        TChunkId chunkId,
        const TResponse& response)
    {
        VERIFY_READER_SPINLOCK_AFFINITY(Reader_->ChunkIdToChunkInfoLock_);

        auto it = Reader_->ChunkIdToChunkInfo_.find(chunkId);
        if (it == Reader_->ChunkIdToChunkInfo_.end()) {
            return;
        }

        const auto& protoAllyReplicas = response.ally_replicas();
        if (protoAllyReplicas.replicas_size() == 0) {
            return;
        }

        const auto& chunkReplicaCache = Reader_->Client_->GetNativeConnection()->GetChunkReplicaCache();
        chunkReplicaCache->UpdateReplicas(chunkId, FromProto<TAllyReplicasInfo>(protoAllyReplicas));
    }

    void OnGotChunkFragments(
        const TPerPeerPlanPtr& plan,
        const TDataNodeServiceProxy::TErrorOrRspGetChunkFragmentSetPtr& rspOrError)
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        if (Promise_.IsSet()) {
            // Shortcut.
            return;
        }

        ++TotalBackendResponseCount_;
        bool lastResponse = (TotalBackendResponseCount_ == TotalBackendReadRequestCount_);

        HandleFragments(plan, rspOrError);

        if (lastResponse) {
            OnFailed();
        }
    }

    void HandleFragments(
        const TPerPeerPlanPtr& plan,
        const TDataNodeServiceProxy::TErrorOrRspGetChunkFragmentSetPtr& rspOrError)
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        const auto& peerInfo = plan->PeerInfo;

        if (!rspOrError.IsOK()) {
            MaybeMarkNodeSuspicious(rspOrError, peerInfo);
            BanPeer(peerInfo);
            OnError(rspOrError);
            return;
        }

        YT_LOG_DEBUG("Chunk fragments received (Address: %v)",
            peerInfo->Address);

        const auto& rsp = rspOrError.Value();

        if (rsp->has_chunk_reader_statistics()) {
            UpdateFromProto(&Options_.ChunkReaderStatistics, rsp->chunk_reader_statistics());
        }

        Options_.ChunkReaderStatistics->DataBytesTransmitted += rsp->GetTotalSize();

        {
            // Feed responses to controllers.
            int subresponseIndex = 0;
            int currentFragmentIndex = 0;
            for (const auto& item : plan->Items) {
                auto fragments = MakeMutableRange(
                    rsp->Attachments().data() + currentFragmentIndex,
                    rsp->Attachments().data() + currentFragmentIndex + item.RequestedFragmentCount);
                currentFragmentIndex += item.RequestedFragmentCount;

                const auto& subresponse = rsp->subresponses(subresponseIndex++);

                auto& controller = *item.ChunkState->Controller;
                if (controller.IsDone()) {
                    continue;
                }

                auto chunkId = controller.GetChunkId();

                if (!subresponse.has_complete_chunk()) {
                    Reader_->DropChunkReplica(chunkId, peerInfo);
                    auto error = TError("Peer %v does not contain chunk %v",
                        peerInfo->Address,
                        chunkId);
                    // TODO(babenko): maybe ban it "strongly"?
                    BanPeer(peerInfo);
                    OnError(std::move(error));
                    continue;
                }

                controller.HandleRpcSubresponse(
                    item.Plan,
                    item.PeerIndex,
                    subresponse,
                    fragments);

                if (controller.IsDone()) {
                    if (--PendingChunkCount_ == 0) {
                        OnCompleted();
                        break;
                    }
                }
            }
        }

        {
            // Update replicas.
            auto mapGuard = ReaderGuard(Reader_->ChunkIdToChunkInfoLock_);
            int subresponseIndex = 0;
            for (const auto& item : plan->Items) {
                const auto& subresponse = rsp->subresponses(subresponseIndex++);
                const auto& controller = *item.ChunkState->Controller;
                TryUpdateChunkReplicas(controller.GetChunkId(), subresponse);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNewChunkFragmentReader::TRetryingReadFragmentsSession
    : public TRefCounted
{
public:
    TRetryingReadFragmentsSession(
        TNewChunkFragmentReaderPtr reader,
        TClientChunkReadOptions options,
        std::vector<TNewChunkFragmentReader::TChunkFragmentRequest> requests)
        : Reader_(std::move(reader))
        , Options_(std::move(options))
        , SessionInvoker_(CreateSerializedInvoker(Reader_->ReaderInvoker_))
        , Logger(Reader_->Logger.WithTag("ChunkFragmentReadSessionId: %v, ReadSessionId: %v",
            TGuid::Create(),
            Options_.ReadSessionId))
    {
        State_->Requests = std::move(requests);
        State_->Response.Fragments.resize(State_->Requests.size());
    }

    ~TRetryingReadFragmentsSession()
    {
        if (!Promise_.IsSet()) {
            Promise_.TrySet(TError(NYT::EErrorCode::Canceled, "Chunk fragment read session destroyed")
                << TErrorAttribute("elapsed_time", Timer_.GetElapsedTime()));
        }
    }

    TFuture<TReadFragmentsResponse> Run()
    {
        try {
            DoRun();
        } catch (const std::exception& ex) {
            OnFatalError(ex);
        }
        return Promise_;
    }

private:
    const TNewChunkFragmentReaderPtr Reader_;
    const TClientChunkReadOptions Options_;

    const IInvokerPtr SessionInvoker_;
    const TChunkFragmentReadStatePtr State_ = New<TChunkFragmentReadState>();
    const NLogging::TLogger Logger;
    const TPromise<TReadFragmentsResponse> Promise_ = NewPromise<TReadFragmentsResponse>();

    NProfiling::TWallTimer Timer_;

    std::vector<TError> Errors_;


    void OnFatalError(TError error)
    {
        YT_LOG_WARNING(error, "Chunk fragment read failed");
        Promise_.TrySet(std::move(error) << std::move(Errors_));
    }

    void OnSuccess()
    {
        Promise_.TrySet(std::move(State_->Response));
    }

    void DoRun()
    {
        if (State_->Requests.empty()) {
            OnSuccess();
            return;
        }

        if (++State_->RetryIndex >= Reader_->Config_->RetryCountLimit) {
            OnFatalError(TError("Chunk fragment reader has exceed retry count limit")
                << TErrorAttribute("retry_count_limit", Reader_->Config_->RetryCountLimit));
            return;
        }

        if (Timer_.GetElapsedTime() >= Reader_->Config_->ReadTimeLimit) {
            OnFatalError(TError("Chunk fragment reader has exceeded read time limit")
                << TErrorAttribute("read_time_limit", Reader_->Config_->ReadTimeLimit));
            return;
        }

        auto subsession = New<TSimpleReadFragmentsSession>(
            Reader_,
            Options_,
            State_,
            Logger);

        subsession->Run().Subscribe(
            // NB: Intentionally no Via.
            BIND(&TRetryingReadFragmentsSession::OnSubsessionFinished, MakeStrong(this), subsession));
    }

    void OnSubsessionFinished(const TIntrusivePtr<TSimpleReadFragmentsSession>& subsession, const TError& error)
    {
        if (!error.IsOK()) {
            OnFatalError(error);
            return;
        }

        int totalFragmentCount = std::ssize(State_->Requests);
        int readFragmentCount = 0;
        for (const auto& fragment : State_->Response.Fragments) {
            if (fragment) {
                ++readFragmentCount;
            }
        }

        if (readFragmentCount == totalFragmentCount) {
            OnSuccess();
            return;
        }

        for (auto& error : subsession->Errors()) {
            Errors_.push_back(std::move(error));
        }

        YT_LOG_DEBUG("Not all fragments were read; will backoff and retry (TotalFragmentCount: %v, ReadFragmentCount: %v, RetryBackoffTime: %v)",
            totalFragmentCount,
            readFragmentCount,
            Reader_->Config_->RetryBackoffTime);

        TDelayedExecutor::Submit(
            BIND(&TRetryingReadFragmentsSession::DoRun, MakeStrong(this))
                .Via(SessionInvoker_),
            Reader_->Config_->RetryBackoffTime);
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<IChunkFragmentReader::TReadFragmentsResponse> TNewChunkFragmentReader::ReadFragments(
    TClientChunkReadOptions options,
    std::vector<TChunkFragmentRequest> requests)
{
    auto session = New<TRetryingReadFragmentsSession>(
        this,
        std::move(options),
        std::move(requests));
    return session->Run();
}

void TNewChunkFragmentReader::RunPeriodicProbing()
{
    auto session = New<TPeriodicProbingSession>(this);
    session->Run();
}

////////////////////////////////////////////////////////////////////////////////

IChunkFragmentReaderPtr CreateNewChunkFragmentReader(
    TChunkFragmentReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    INodeStatusDirectoryPtr nodeStatusDirectory,
    const NProfiling::TProfiler& profiler)
{
    return New<TNewChunkFragmentReader>(
        std::move(config),
        std::move(client),
        std::move(nodeStatusDirectory),
        profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
