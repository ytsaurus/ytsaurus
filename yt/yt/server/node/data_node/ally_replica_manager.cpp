#include "ally_replica_manager.h"
#include "private.h"
#include "bootstrap.h"
#include "chunk.h"
#include "chunk_store.h"
#include "config.h"
#include "master_connector.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/chunk_client/proto/heartbeat.pb.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

const static auto& Logger = DataNodeLogger;
constexpr auto ProfilingPeriod = TDuration::Seconds(1);
constexpr auto QueueFlushPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NChunkClient::NProto::TChunkReplicaAnnouncement* protoAnnouncement,
    const TChunkReplicaAnnouncement& announcement)
{
    ToProto(protoAnnouncement->mutable_chunk_id(), announcement.ChunkId);
    ToProto(protoAnnouncement->mutable_replicas(), announcement.Replicas);
    protoAnnouncement->set_revision(ToProto<i64>(announcement.Revision));
}

void FromProto(
    TChunkReplicaAnnouncement* announcement,
    const NChunkClient::NProto::TChunkReplicaAnnouncement& protoAnnouncement)
{
    FromProto(&announcement->ChunkId, protoAnnouncement.chunk_id());
    FromProto(&announcement->Replicas, protoAnnouncement.replicas());
    announcement->Revision = FromProto<i64>(protoAnnouncement.revision());
}

////////////////////////////////////////////////////////////////////////////////

class TAllyReplicaManager
    : public IAllyReplicaManager
{
public:
    explicit TAllyReplicaManager(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Throttler_(Bootstrap_->GetAnnounceChunkReplicaRpsOutThrottler())
        , ProfilingExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetStorageLightInvoker(),
            BIND(&TAllyReplicaManager::OnProfiling, MakeWeak(this)),
            ProfilingPeriod))
        , QueueFlushExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetStorageLightInvoker(),
            BIND(&TAllyReplicaManager::OnQueueFlush, MakeWeak(this)),
            QueueFlushPeriod))
    { }

    void Start() override
    {
        auto profiler = DataNodeProfiler
            .WithPrefix("/ally_replica_manager");
        AnnouncementsSent_ = profiler.Counter("/announcements_sent");
        AnnouncementsReceived_ = profiler.Counter("/announcements_received");
        OutdatedAnnouncementsReceived_ = profiler.Counter("/outdated_announcements_received");
        DelayedAnnouncementsPromoted_ = profiler.Counter("/delayed_announcements_promoted");
        LazyAnnouncementsPromoted_ = profiler.Counter("/lazy_announcements_promoted");
        PendingAnnouncementCount_ = profiler.Gauge("/pending_announcement_count");
        AllyAwareChunkCount_ = profiler.Gauge("/ally_aware_chunk_count");

        ProfilingExecutor_->Start();
        QueueFlushExecutor_->Start();

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        chunkStore->SubscribeChunkRemoved(
            BIND(&TAllyReplicaManager::OnChunkRemoved, MakeWeak(this))
                .Via(Bootstrap_->GetStorageLightInvoker()));
    }

    void ScheduleAnnouncements(
        TRange<const NChunkClient::NProto::TChunkReplicaAnnouncementRequest*> requests,
        NHydra::TRevision revision,
        bool onFullHeartbeat) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto selfNodeId = Bootstrap_->GetNodeId();
        auto now = TInstant::Now();

        for (const auto& request : requests) {
            auto chunkId = FromProto<TChunkId>(request->chunk_id());
            auto replicas = FromProto<TChunkReplicaWithMediumList>(request->replicas());
            auto delay = FromProto<TDuration>(request->delay());

            YT_VERIFY(IsBlobChunkId(chunkId));

            if (request->confirmation_needed()) {
                UnconfirmedAnnouncements_[CellTagFromId(chunkId)].emplace_back(chunkId, revision);
            }

            {
                auto& bucket = AllyReplicasInfos_.GetBucketForKey(chunkId);
                auto bucketGuard = Guard(bucket.GetMutex());

                auto [it, inserted] = bucket.GetMap().emplace(chunkId, TAllyReplicasInfo{});
                auto knownRevision = inserted
                    ? NullRevision
                    : it->second.Revision;

                if (revision > knownRevision) {
                    it->second.Replicas = replicas;
                    it->second.Revision = revision;
                }

                bucketGuard.Release();

                // NB: Keep logging out of the critical section.
                if (revision > knownRevision) {
                    YT_LOG_DEBUG_UNLESS(onFullHeartbeat,
                        "Received announcement request for chunk (ChunkId: %v, "
                        "Revision: %x, ReplicaCount: %v, Delay: %v, Lazy: %v, "
                        "ConfirmationNeeded: %v)",
                        chunkId,
                        revision,
                        replicas.size(),
                        delay,
                        request->lazy(),
                        request->confirmation_needed());
                } else {
                    YT_LOG_DEBUG_UNLESS(onFullHeartbeat,
                        "Received outdated announcement request for chunk (ChunkId: %v, "
                        "Revision: %x, KnownRevision: %x, ReplicaCount: %v, Delay: %v, "
                        "Lazy: %v, ConfirmationNeeded: %v)",
                        chunkId,
                        revision,
                        knownRevision,
                        replicas.size(),
                        delay,
                        request->lazy(),
                        request->confirmation_needed());
                    continue;
                }
            }

            TChunkReplicaAnnouncement announcement{chunkId, replicas, revision};

            for (auto replica : replicas) {
                auto nodeId = replica.GetNodeId();
                if (nodeId == selfNodeId) {
                    continue;
                }

                auto* state = GetOrCreateNodeState(nodeId);
                auto guard = Guard(state->SpinLock);

                if (request->lazy()) {
                    state->LazyAnnouncements.push_back(announcement);
                } else if (request->has_delay()) {
                    state->DelayedAnnouncements.emplace(now + delay, announcement);
                } else {
                    state->ImmediateAnnouncements.push_back(announcement);
                }
            }
        }
    }

    void OnAnnouncementsReceived(
        TRange<const NChunkClient::NProto::TChunkReplicaAnnouncement*> announcements,
        TNodeId sourceNodeId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        auto selfNodeId = Bootstrap_->GetNodeId();

        const auto& nodeDirectory = Bootstrap_->GetNodeDirectory();
        auto* sourceNodeDescriptor = nodeDirectory->FindDescriptor(sourceNodeId);
        auto sourceNodeAddress = sourceNodeDescriptor
            ? sourceNodeDescriptor->GetDefaultAddress()
            : Format("<id:%v>", sourceNodeId);

        auto isChunkStored = [&] (auto chunkId, const auto& replicas) {
            if (TypeFromId(chunkId) == EObjectType::Chunk) {
                return static_cast<bool>(chunkStore->FindChunk(chunkId));
            }

            for (auto replica : replicas) {
                if (replica.GetNodeId() == selfNodeId) {
                    auto encodedChunkId = EncodeChunkId(
                        TChunkIdWithIndex(
                            chunkId,
                            replica.GetReplicaIndex()));
                    if (chunkStore->FindChunk(encodedChunkId)) {
                        return true;
                    }
                }
            }

            return false;
        };

        int announcementsReceived = 0;
        int outdatedAnnouncementsReceived = 0;

        for (const auto& protoAnnouncement : announcements) {
            auto announcement = FromProto<TChunkReplicaAnnouncement>(*protoAnnouncement);
            // Do not remember replicas for chunks not stored at the node.
            if (!isChunkStored(announcement.ChunkId, announcement.Replicas)) {
                YT_LOG_DEBUG("Received ally replica announcement for unknown chunk, ignored "
                    "(ChunkId: %v, Revision: %x, SourceNode: %v)",
                    announcement.ChunkId,
                    announcement.Revision,
                    sourceNodeAddress);
                continue;
            }

            auto& bucket = AllyReplicasInfos_.GetBucketForKey(announcement.ChunkId);
            auto bucketGuard = Guard(bucket.GetMutex());

            auto [it, inserted] = bucket.GetMap().emplace(announcement.ChunkId, TAllyReplicasInfo{});
            auto knownRevision = inserted
                ? NullRevision
                : it->second.Revision;

            if (announcement.Revision > knownRevision) {
                it->second.Replicas = announcement.Replicas;
                it->second.Revision = announcement.Revision;
                ++announcementsReceived;
            } else {
                ++outdatedAnnouncementsReceived;
            }

            bucketGuard.Release();

            // NB: Keep logging out of the critical section.
            if (announcement.Revision > knownRevision) {
                YT_LOG_DEBUG("Received ally replica announcement for chunk "
                    "(ChunkId: %v, Revision: %x, ReplicaCount: %v, SourceNode: %v)",
                    announcement.ChunkId,
                    announcement.Revision,
                    announcement.Replicas.size(),
                    sourceNodeAddress);
            } else {
                YT_LOG_DEBUG("Received outdated ally replica announcement for chunk "
                    "(ChunkId: %v, Revision: %x, KnownRevision: %x, ReplicaCount: %v, "
                    "SourceNode: %v)",
                    announcement.ChunkId,
                    announcement.Revision,
                    knownRevision,
                    announcement.Replicas.size(),
                    sourceNodeAddress);
            }
        }

        AnnouncementsReceived_.Increment(announcementsReceived);
        OutdatedAnnouncementsReceived_.Increment(outdatedAnnouncementsReceived);
    }

    std::vector<std::pair<TChunkId, NHydra::TRevision>>
        TakeUnconfirmedAnnouncementRequests(TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = UnconfirmedAnnouncements_.find(cellTag);
        if (it == UnconfirmedAnnouncements_.end()) {
            return {};
        }

        return std::move(it->second);
    }

    void SetEnableLazyAnnouncements(bool enable) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        EnableLazyAnnouncements_ = enable;
    }

    TAllyReplicasInfo GetAllyReplicas(TChunkId chunkId) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        chunkId = DecodeChunkId(chunkId).Id;
        if (!IsBlobChunkId(chunkId)) {
            return {};
        }

        if (auto allyReplicas = FindAllyReplicas(chunkId)) {
            return *allyReplicas;
        }

        return {};
    }

    IYPathServicePtr GetOrchidService() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND(
            &TAllyReplicaManager::BuildOrchid,
            MakeStrong(this)))
            ->Via(Bootstrap_->GetControlInvoker());
    }

    void BuildChunkOrchidYson(NYTree::TFluentMap fluent, TChunkId chunkId) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        chunkId = DecodeChunkId(chunkId).Id;
        if (!IsBlobChunkId(chunkId)) {
            return;
        }

        const auto& nodeDirectory = Bootstrap_->GetNodeDirectory();
        auto allyReplicas = FindAllyReplicas(chunkId).value_or(TAllyReplicasInfo{});

        fluent
            .Item("ally_replicas").DoListFor(allyReplicas.Replicas, [&] (TFluentList fluent, TChunkReplicaWithMedium replica) {
                auto* descriptor = nodeDirectory->FindDescriptor(replica.GetNodeId());
                auto address = descriptor ? descriptor->GetDefaultAddress() : "<unknown>";
                if (IsErasureChunkId(chunkId)) {
                    fluent
                        .Item()
                        .BeginAttributes()
                            .Item("index").Value(replica.GetReplicaIndex())
                        .EndAttributes()
                        .Value(address);
                } else {
                    fluent
                        .Item()
                        .Value(address);
                }
            })
            .Item("ally_replica_update_revision").Value(allyReplicas.Revision);
    }

private:
    IBootstrap* const Bootstrap_;

    const IThroughputThrottlerPtr Throttler_;
    const TPeriodicExecutorPtr ProfilingExecutor_;
    const TPeriodicExecutorPtr QueueFlushExecutor_;

    TConcurrentHashMap<TChunkId, TAllyReplicasInfo, 32, NThreading::TSpinLock> AllyReplicasInfos_;

    struct TNodeState
    {
        explicit TNodeState(TNodeId nodeId)
            : NodeId(nodeId)
        { }

        TNodeId NodeId;

        // When node is scanned:
        //  - lazy announcements move to immediate if lazy announcements are enabled;
        //  - delayed announcements move to immediate as soon as the time has come;
        //  - all immediate announcements are sent at once.
        // Thus, only immediate announcements can be in-flight.
        std::vector<TChunkReplicaAnnouncement> ImmediateAnnouncements;
        std::vector<TChunkReplicaAnnouncement> LazyAnnouncements;
        std::multimap<TInstant, TChunkReplicaAnnouncement> DelayedAnnouncements;

        // Stores the number of immediate announcements sent by the previous request
        // if the response has not yet been received.
        int InFlightAnnouncementCount = 0;

        // Last request time, successful or not.
        TInstant LastRequestTime;

        // Next item in the linked list of all nodes.
        TAtomicObject<TNodeState*> NextInList;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock);
    };

    // Accessed only from control thread.
    THashMap<TNodeId, std::unique_ptr<TNodeState>> NodeStates_;

    // Modified only from control thread.
    TAtomicObject<TNodeState*> NodeListHead_ = nullptr;

    // Accessed only from control thread.
    TNodeState* NodeListTail_ = nullptr;

    // Accessed only from storage thread.
    TNodeState* NodeListFlushPosition_ = nullptr;

    // Announcements with required confirmation since last heartbeat.
    THashMap<TCellTag, std::vector<std::pair<TChunkId, TRevision>>> UnconfirmedAnnouncements_;

    TAtomicObject<TAllyReplicaManagerDynamicConfigPtr> Config_ =
        New<TAllyReplicaManagerDynamicConfig>();

    std::atomic<bool> EnableLazyAnnouncements_ = false;

    NProfiling::TCounter AnnouncementsSent_;
    NProfiling::TCounter AnnouncementsReceived_;
    NProfiling::TCounter OutdatedAnnouncementsReceived_;
    NProfiling::TCounter DelayedAnnouncementsPromoted_;
    NProfiling::TCounter LazyAnnouncementsPromoted_;
    NProfiling::TGauge PendingAnnouncementCount_;
    NProfiling::TGauge AllyAwareChunkCount_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    std::optional<TAllyReplicasInfo> FindAllyReplicas(TChunkId chunkId) const
    {
        TAllyReplicasInfo result;
        return AllyReplicasInfos_.Get(chunkId, result)
            ? result
            : std::make_optional<TAllyReplicasInfo>();
    }

    TNodeState* GetOrCreateNodeState(TNodeId nodeId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (auto it = NodeStates_.find(nodeId); it != NodeStates_.end()) {
            return it->second.get();
        }

        auto holder = std::make_unique<TNodeState>(nodeId);
        auto* state = holder.get();

        if (auto* head = NodeListHead_.Load()) {
            state->NextInList.Store(head);
            NodeListTail_->NextInList.Store(state);
        } else {
            state->NextInList.Store(state);
            NodeListTail_ = state;
        }

        NodeListHead_.Store(state);
        NodeStates_[nodeId] = std::move(holder);
        return state;
    }

    template <class TFunctor>
    TNodeState* DoForAllNodesGuarded(TNodeState* start, TFunctor&& callback)
    {
        if (!start) {
            return nullptr;
        }

        auto* current = start;
        do {
            auto guard = Guard(current->SpinLock);

            if (!callback(current)) {
                break;
            }

            current = current->NextInList.Load();
        } while (current != start);

        return current;
    }

    bool IsOutdated(const TChunkReplicaAnnouncement& announcement) const
    {
        auto allyReplicas = FindAllyReplicas(announcement.ChunkId);
        return allyReplicas && allyReplicas->Revision > announcement.Revision;
    }

    void PromoteLazyAndDelayedAnnouncements(TNodeId nodeId, TNodeState* nodeState)
    {
        VERIFY_SPINLOCK_AFFINITY(nodeState->SpinLock);

        auto now = TInstant::Now();

        int lazyPromotedCount = 0;
        int delayedPromotedCount = 0;

        if (EnableLazyAnnouncements_) {
            if (!nodeState->LazyAnnouncements.empty()) {
                for (auto& announcement : nodeState->LazyAnnouncements) {
                    if (!IsOutdated(announcement)) {
                        ++lazyPromotedCount;
                        nodeState->ImmediateAnnouncements.push_back(std::move(announcement));
                    }
                }
                nodeState->LazyAnnouncements.clear();
                nodeState->LazyAnnouncements.shrink_to_fit();
            }
        }

        auto it = nodeState->DelayedAnnouncements.begin();
        while (it != nodeState->DelayedAnnouncements.end() && it->first <= now) {
            if (!IsOutdated(it->second)) {
                ++delayedPromotedCount;
                nodeState->ImmediateAnnouncements.push_back(std::move(it->second));
            }
            ++it;
        }
        nodeState->DelayedAnnouncements.erase(nodeState->DelayedAnnouncements.begin(), it);

        DelayedAnnouncementsPromoted_.Increment(delayedPromotedCount);
        LazyAnnouncementsPromoted_.Increment(lazyPromotedCount);

        if (lazyPromotedCount + delayedPromotedCount > 0) {
            YT_LOG_DEBUG("Promoted ally replica announcements (NodeId: %v, LazyCount: %v, DelayedCount: %v)",
                nodeId,
                lazyPromotedCount,
                delayedPromotedCount);
        }
    }

    void FilterOutdatedAnnouncements(TNodeState* nodeState)
    {
        VERIFY_SPINLOCK_AFFINITY(nodeState->SpinLock);

        auto& announcements = nodeState->ImmediateAnnouncements;
        EraseIf(
            announcements,
            [&] (const auto& announcement) {
                return IsOutdated(announcement);
            });
    }

    void OnProfiling()
    {
        // Nodes.
        int pendingAnnouncementCount = 0;

        DoForAllNodesGuarded(
            NodeListHead_.Load(),
            [&] (TNodeState* state) {
                pendingAnnouncementCount +=
                    ssize(state->ImmediateAnnouncements) +
                    ssize(state->LazyAnnouncements) +
                    ssize(state->DelayedAnnouncements);
                return true;
            });

        PendingAnnouncementCount_.Update(pendingAnnouncementCount);

        // Chunks.
        int allyAwareChunkCount = 0;
        for (const auto& bucket : AllyReplicasInfos_.Buckets) {
            auto guard = Guard(bucket.GetMutex());
            allyAwareChunkCount += bucket.GetMap().size();
        }

        AllyAwareChunkCount_.Update(allyAwareChunkCount);
    }

    void OnQueueFlush()
    {
        const auto& masterConnector = Bootstrap_->GetMasterConnector();
        if (!masterConnector->IsOnline()) {
            return;
        }

        auto now = TInstant::Now();

        std::vector<TNodeState*> processedNodes;
        std::vector<TFuture<TDataNodeServiceProxy::TRspAnnounceChunkReplicasPtr>> asyncRsps;

        auto selfNodeId = Bootstrap_->GetNodeId();

        const auto& nodeDirectory = Bootstrap_->GetNodeDirectory();
        const auto& channelFactory = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetChannelFactory();

        int announcementsSent = 0;

        auto config = Config_.Load();

        if (!NodeListFlushPosition_) {
            NodeListFlushPosition_ = NodeListHead_.Load();
        }

        NodeListFlushPosition_ = DoForAllNodesGuarded(
            NodeListFlushPosition_,
            [&] (TNodeState* nodeState) {
                YT_VERIFY(nodeState->InFlightAnnouncementCount == 0);

                if (nodeState->LastRequestTime + config->AnnouncementBackoffTime > now) {
                    return true;
                }

                auto nodeId = nodeState->NodeId;

                PromoteLazyAndDelayedAnnouncements(nodeId, nodeState);
                FilterOutdatedAnnouncements(nodeState);

                if (nodeState->ImmediateAnnouncements.empty()) {
                    return true;
                }

                auto* nodeDescriptor = nodeDirectory->FindDescriptor(nodeId);
                if (!nodeDescriptor) {
                    YT_LOG_WARNING("Found node id not present in node directory, will not "
                        "announce replicas (NodeId: %v)",
                        nodeId);
                    return true;
                }

                if (!Throttler_->TryAcquire(1)) {
                    return false;
                }

                nodeState->LastRequestTime = now;

                NRpc::IChannelPtr channel;

                try {
                    const auto& address = nodeDescriptor->GetAddressOrThrow(Bootstrap_->GetLocalNetworks());
                    channel = channelFactory->CreateChannel(address);
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Failed to create channel to node (Address: %v)",
                        nodeDescriptor->GetDefaultAddress());
                    return true;
                }

                i64 parcelSize = std::min(
                    ssize(nodeState->ImmediateAnnouncements),
                    config->MaxChunksPerAnnouncementRequest);
                auto parcel = MakeRange(nodeState->ImmediateAnnouncements)
                    .Slice(0, parcelSize);

                TDataNodeServiceProxy proxy(channel);

                auto req = proxy.AnnounceChunkReplicas();
                req->set_source_node_id(selfNodeId);
                ToProto(req->mutable_announcements(), parcel);
                req->SetTimeout(config->AnnouncementRequestTimeout);

                nodeState->InFlightAnnouncementCount = parcel.Size();
                asyncRsps.push_back(req->Invoke());
                processedNodes.push_back(nodeState);
                announcementsSent += parcel.Size();

                YT_LOG_DEBUG("Sent chunk replica announcement request (Address: %v, AnnouncementCount: %v)",
                    nodeDescriptor->GetDefaultAddress(),
                    parcel.Size());

                return true;
            });

        AnnouncementsSent_.Increment(announcementsSent);

        // Process responses.
        auto rsps = WaitFor(AllSet(asyncRsps))
            .Value();

        for (int index = 0; index < ssize(processedNodes); ++index) {
            auto* state = processedNodes[index];
            auto nodeId = state->NodeId;
            const auto& rspOrError = rsps[index];
            const auto& nodeDescriptor = nodeDirectory->GetDescriptor(nodeId);

            auto guard = Guard(state->SpinLock);

            if (rspOrError.IsOK()) {
                YT_LOG_DEBUG("Successfully reported replica announcements to node "
                    "(Address: %v, AnnouncementCount: %v)",
                    nodeDescriptor.GetDefaultAddress(),
                    state->InFlightAnnouncementCount);

                state->ImmediateAnnouncements.erase(
                    state->ImmediateAnnouncements.begin(),
                    state->ImmediateAnnouncements.begin() + state->InFlightAnnouncementCount);

                if (state->ImmediateAnnouncements.empty()) {
                    state->ImmediateAnnouncements.shrink_to_fit();
                }
            } else {
                YT_LOG_DEBUG(rspOrError, "Failed to report replica announcements to node (Address: %v)",
                    nodeDescriptor.GetDefaultAddress());
            }

            state->InFlightAnnouncementCount = 0;
        }
    }

    void OnChunkRemoved(const IChunkPtr& chunk)
    {
        // TODO(ifsmirnov): think about delayed removal so node could report ally replicas
        // even if it doesn't store the chunk anymore.

        auto chunkId = DecodeChunkId(chunk->GetId()).Id;
        if (!IsBlobChunkId(chunkId)) {
            return;
        }

        auto& bucket = AllyReplicasInfos_.GetBucketForKey(chunkId);
        auto guard = Guard(bucket.GetMutex());
        bucket.GetMap().erase(chunkId);
    }

    void BuildOrchid(IYsonConsumer* consumer) const
    {
        // TODO(ifsmirnov): display node state map.
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("enable_lazy_annnouncements").Value(EnableLazyAnnouncements_)
            .EndMap();
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        Config_.Store(newNodeConfig->DataNode->AllyReplicaManager);
    }
};

////////////////////////////////////////////////////////////////////////////////

IAllyReplicaManagerPtr CreateAllyReplicaManager(IBootstrap* bootstrap)
{
    return New<TAllyReplicaManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
