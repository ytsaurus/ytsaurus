#include "node_disposal_manager.h"

#include "node.h"
#include "config.h"
#include "public.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_replica_fetcher.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/ytlib/data_node_tracker_client/location_directory.h>

#include <yt/yt/ytlib/sequoia_client/records/location_replicas.record.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/serialize.h>

#include <util/generic/scope.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NProto;
using namespace NProfiling;
using namespace NObjectClient;
using namespace NDataNodeTrackerClient::NProto;
using namespace NChunkClient::NProto;
using namespace NDataNodeTrackerClient;
using namespace NSequoiaClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = NodeTrackerServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TNodeDisposalManager
    : public INodeDisposalManager
    , public TMasterAutomatonPart
    , public virtual TRefCounted
{
public:
    explicit TNodeDisposalManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::NodeTracker)
    {
        RegisterLoader(
            "NodeDisposalManager",
            BIND_NO_PROPAGATE(&TNodeDisposalManager::Load, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "NodeDisposalManager",
            BIND_NO_PROPAGATE(&TNodeDisposalManager::Save, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TNodeDisposalManager::HydraStartNodeDisposal, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TNodeDisposalManager::HydraDisposeLocation, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TNodeDisposalManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void DisposeNodeWithSemaphore(TNode* node) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(IsLeader());

        TReqStartNodeDisposal request;
        request.set_node_id(ToProto(node->GetId()));

        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TNodeDisposalManager::HydraStartNodeDisposal,
            this);

        DisposeNodeSemaphore_->AsyncAcquire().AsUnique().Subscribe(
            BIND([mutation = std::move(mutation)] (TErrorOr<TAsyncSemaphoreGuard>&& guardOrError) {
                // Even if acquiring semaphore failed, we still have to commit mutation.
                YT_LOG_ALERT_UNLESS(guardOrError.IsOK(), guardOrError, "Failed to acquire node disposal semaphore");

                Y_UNUSED(WaitFor(mutation->CommitAndLog(Logger())));
            })
            .Via(EpochAutomatonInvoker_));
    }

    void DisposeNodeCompletely(TNode* node) override
    {
        YT_VERIFY(HasMutationContext());

        if (node->Flavors().contains(ENodeFlavor::Data)) {
            YT_LOG_ALERT("Data node is being disposed completely (NodeId: %v)", node->GetId());
        }

        YT_LOG_INFO("Disposing node completely (NodeId: %v)", node->GetId());

        YT_PROFILE_TIMING("/node_tracker/node_dispose_time") {
            // Node was being disposed location by location, but smth needs it to be disposed right now.
            if (node->GetLocalState() == ENodeState::BeingDisposed) {
                for (const auto& location : node->ChunkLocations()) {
                    if (ChunkLocationsBeingDisposed_.contains(location->GetIndex())) {
                        YT_LOG_WARNING(
                            "Chunk location was being disposed but it was forcibly disposed (LocationIndex: %v, NodeId: %v)",
                            location->GetIndex(),
                            node->GetId());
                        EraseFromDisposalQueue(location->GetIndex());
                    }
                }
                const auto& nodeTracker = Bootstrap_->GetNodeTracker();
                nodeTracker->SetNodeLocalState(node, ENodeState::Unregistered);
            }

            DoStartNodeDisposal(node);

            DisposeAllLocations(node);

            DoFinishNodeDisposal(node);
        }
    }

    void OnProfiling(TSensorBuffer* buffer) const override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        buffer->AddGauge("/chunk_locations_being_disposed", ChunkLocationsBeingDisposed_.size());
        buffer->AddGauge("/chunk_locations_awaiting_disposal", ChunkLocationsAwaitingDisposal_.size());
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    // This is still shared between all nodes.
    const TAsyncSemaphorePtr DisposeNodeSemaphore_ = New<TAsyncSemaphore>(0);

    TPeriodicExecutorPtr LocationDisposalExecutor_;
    THashSet<TChunkLocationIndex> ChunkLocationsBeingDisposed_;
    std::deque<TChunkLocationIndex> ChunkLocationsAwaitingDisposal_;

    // COMPAT(grphil)
    THashSet<TNodeId> DataNodesBeingDisposed_;
    std::deque<TNodeId> DataNodesAwaitingForBeingDisposed_;

    void OnLeaderActive() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        const auto& config = GetDynamicConfig();
        LocationDisposalExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::NodeTracker),
            BIND(&TNodeDisposalManager::LocationDisposalTick, MakeWeak(this)),
            config->NodeDisposalTickPeriod);
        LocationDisposalExecutor_->Start();
    }

    void OnStopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();
        if (LocationDisposalExecutor_) {
            YT_UNUSED_FUTURE(LocationDisposalExecutor_->Stop());
            LocationDisposalExecutor_.Reset();
        }
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        // COMPAT(grphil)
        if (context.GetVersion() < EMasterReign::ChunkLocationDisposal) {
            Load(context, DataNodesBeingDisposed_);
            Load(context, DataNodesAwaitingForBeingDisposed_);
        } else {
            Load(context, ChunkLocationsBeingDisposed_);
            Load(context, ChunkLocationsAwaitingDisposal_);
        }
    }

    void Save(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, ChunkLocationsBeingDisposed_);
        Save(context, ChunkLocationsAwaitingDisposal_);
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        // COMPAT(grphil)
        for (auto nodeId : DataNodesBeingDisposed_) {
            DataNodesAwaitingForBeingDisposed_.push_back(nodeId);
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto nodeId : DataNodesAwaitingForBeingDisposed_) {
            auto* node = nodeTracker->FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                continue;
            }

            for (auto localLocationIndex = node->GetNextDisposedLocationIndex(); localLocationIndex < std::ssize(node->ChunkLocations()); ++localLocationIndex) {
                ChunkLocationsAwaitingDisposal_.push_back(node->ChunkLocations()[localLocationIndex]->GetIndex());
            }

            for (auto localLocationIndex = 0; localLocationIndex < node->GetNextDisposedLocationIndex(); ++localLocationIndex) {
                node->ChunkLocations()[localLocationIndex]->SetState(NChunkServer::EChunkLocationState::Disposed);
            }
        }
        DataNodesAwaitingForBeingDisposed_.clear();
        DataNodesBeingDisposed_.clear();
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        ChunkLocationsBeingDisposed_.clear();
        ChunkLocationsAwaitingDisposal_.clear();
        DataNodesBeingDisposed_.clear();
        DataNodesAwaitingForBeingDisposed_.clear();
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& config = GetDynamicConfig();
        if (LocationDisposalExecutor_) {
            LocationDisposalExecutor_->SetPeriod(config->NodeDisposalTickPeriod);
        }

        DisposeNodeSemaphore_->SetTotal(config->MaxConcurrentNodeUnregistrations);
        TopUpLocationsBeingDisposed();
    }

    const TDynamicNodeTrackerConfigPtr& GetDynamicConfig()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        return Bootstrap_->GetConfigManager()->GetConfig()->NodeTracker;
    }

    void LocationDisposalTick()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(IsLeader());

        for (auto locationIndex : ChunkLocationsBeingDisposed_) {
            StartLocationDisposal(locationIndex);
        }
    }

    void StartLocationDisposal(TChunkLocationIndex locationIndex)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(IsLeader());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();
        const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();

        auto* location = dataNodeTracker->FindChunkLocationByIndex(locationIndex);
        if (!IsObjectAlive(location) || location->GetBeingDisposed()) {
            return;
        }

        location->SetBeingDisposed(true);

        auto node = location->GetNode();

        chunkReplicaFetcher->GetSequoiaLocationReplicas(node->GetId(), locationIndex)
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<NRecords::TLocationReplicas>>& replicasOrError) {
                if (!replicasOrError.IsOK()) {
                    location->SetBeingDisposed(false);
                    YT_LOG_ERROR(replicasOrError, "Error getting Sequoia location replicas");
                    return;
                }

                auto nodeId = node->GetId();
                YT_LOG_INFO("Disposing location (NodeId: %v, Address: %v LocationUuid: %v, LocationIndex: %v)",
                    nodeId,
                    node->GetDefaultAddress(),
                    location->GetUuid(),
                    locationIndex);

                TReqDisposeLocation request;
                request.set_node_id(ToProto(nodeId));
                request.set_global_location_index(ToProto(locationIndex));
                auto mutation = CreateMutation(
                    Bootstrap_->GetHydraFacade()->GetHydraManager(),
                    request,
                    &TNodeDisposalManager::HydraDisposeLocation,
                    this);

                const auto& sequoiaReplicas = replicasOrError.Value();
                if (sequoiaReplicas.empty()) {
                    YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger()));
                    return;
                }

                auto prepareSequoiaRequest = BIND([locationIndex, nodeId, sequoiaReplicas = std::move(replicasOrError.Value())] {
                    auto sequoiaRequest = std::make_unique<TReqModifyReplicas>();
                    sequoiaRequest->set_node_id(ToProto(nodeId));
                    sequoiaRequest->set_caused_by_node_disposal(true);
                    for (const auto& replica : sequoiaReplicas) {
                        TChunkRemoveInfo chunkRemoveInfo;

                        TChunkIdWithIndex idWithIndex;
                        idWithIndex.Id = replica.Key.ChunkId;
                        idWithIndex.ReplicaIndex = replica.Key.ReplicaIndex;

                        ToProto(chunkRemoveInfo.mutable_chunk_id(), EncodeChunkId(idWithIndex));
                        chunkRemoveInfo.set_location_index(ToProto(locationIndex));

                        *sequoiaRequest->add_removed_chunks() = chunkRemoveInfo;
                    }

                    return sequoiaRequest;
                });

                auto sequoiaRequest = WaitFor(std::move(prepareSequoiaRequest)
                    .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
                    .Run()
                    .AsUnique())
                    .ValueOrThrow();

                chunkManager
                    ->ModifySequoiaReplicas(ESequoiaTransactionType::ChunkLocationDisposal, std::move(sequoiaRequest))
                    .Subscribe(BIND([=, mutation = std::move(mutation), this, this_ = MakeStrong(this)] (const TErrorOr<TRspModifyReplicas>& rspOrError) {
                        if (!rspOrError.IsOK()) {
                            const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();

                            auto* location = dataNodeTracker->FindChunkLocationByIndex(locationIndex);
                            if (!IsObjectAlive(location)) {
                                return;
                            }

                            location->SetBeingDisposed(false);
                            return;
                        }

                        YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger()));
                    }).Via(Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::NodeTracker)));
        }).Via(Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::NodeTracker)));
    }

    void DoStartNodeDisposal(TNode* node)
    {
        YT_VERIFY(HasMutationContext());

        YT_VERIFY(node->GetLocalState() == ENodeState::Unregistered);

        YT_LOG_INFO("Starting node disposal (NodeId: %v, Address: %v)",
            node->GetId(),
            node->GetDefaultAddress());

        node->ReportedHeartbeats().clear();
    }

    void DisposeAllLocations(TNode* node)
    {
        YT_VERIFY(HasMutationContext());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (auto location : node->ChunkLocations()) {
            chunkManager->DisposeLocation(location);
        }
    }

    void DoFinishNodeDisposal(TNode* node)
    {
        YT_VERIFY(HasMutationContext());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->DisposeNode(node);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->SetNodeLocalState(node, ENodeState::Offline);

        YT_LOG_INFO("Node offline (NodeId: %v, Address: %v)",
            node->GetId(),
            node->GetDefaultAddress());
    }

    void TopUpLocationsBeingDisposed()
    {
        while (std::ssize(ChunkLocationsBeingDisposed_) < GetDynamicConfig()->MaxLocationsBeingDisposed &&
            !ChunkLocationsAwaitingDisposal_.empty())
        {
            InsertOrCrash(ChunkLocationsBeingDisposed_, ChunkLocationsAwaitingDisposal_.front());
            ChunkLocationsAwaitingDisposal_.pop_front();
        }
    }

    void EraseFromDisposalQueue(TChunkLocationIndex locationIndex)
    {
        EraseOrCrash(ChunkLocationsBeingDisposed_, locationIndex);
        TopUpLocationsBeingDisposed();
    }

    void HydraStartNodeDisposal(TReqStartNodeDisposal* request)
    {
        auto nodeId = FromProto<NNodeTrackerClient::TNodeId>(request->node_id());

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->FindNode(nodeId);
        if (!IsObjectAlive(node)) {
            return;
        }

        if (node->GetLocalState() != ENodeState::Unregistered) {
            return;
        }

        const auto& config = GetDynamicConfig();
        if (node->Flavors().contains(ENodeFlavor::Data)) {
            DoStartNodeDisposal(node);
            nodeTracker->SetNodeLocalState(node, ENodeState::BeingDisposed);

            for (const auto& location : node->ChunkLocations()) {
                if (std::ssize(ChunkLocationsBeingDisposed_) < config->MaxLocationsBeingDisposed) {
                    InsertOrCrash(ChunkLocationsBeingDisposed_, location->GetIndex());
                } else {
                    ChunkLocationsAwaitingDisposal_.push_back(location->GetIndex());
                }
            }

            MaybeFinishNodeDisposal(node);
        } else {
            DisposeNodeCompletely(node);
        }
    }

    void HydraDisposeLocation(TReqDisposeLocation* request)
    {
        YT_PROFILE_TIMING("/node_tracker/node_dispose_time") {
            auto nodeId = FromProto<NNodeTrackerClient::TNodeId>(request->node_id());

            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            auto* node = nodeTracker->FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                return;
            }

            auto locationIndex = FromProto<TChunkLocationIndex>(request->global_location_index());
            const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
            auto* location = dataNodeTracker->FindChunkLocationByIndex(locationIndex);
            YT_VERIFY(IsObjectAlive(location));

            auto finallyGuard = Finally([&] {
                if (ChunkLocationsBeingDisposed_.contains(locationIndex)) {
                    EraseFromDisposalQueue(locationIndex);
                }
            });

            if (IsLeader()) {
                YT_VERIFY(location->GetBeingDisposed());
            }

            YT_LOG_INFO("Disposing location (NodeId: %v, Address: %v, Location: %v)",
                nodeId,
                node->GetDefaultAddress(),
                location->GetUuid());

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            chunkManager->DisposeLocation(location);

            location->SetBeingDisposed(false);
            location->SetState(NChunkServer::EChunkLocationState::Disposed);

            MaybeFinishNodeDisposal(node);
        }
    }

    void MaybeFinishNodeDisposal(TNode* node)
    {
        YT_VERIFY(HasMutationContext());

        if (node->GetLocalState() == ENodeState::BeingDisposed && std::ranges::all_of(
            node->ChunkLocations(),
            [] (const auto& location) { return location->GetState() == NChunkServer::EChunkLocationState::Disposed; }))
        {
            DoFinishNodeDisposal(node);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeDisposalManagerPtr CreateNodeDisposalManager(TBootstrap* bootstrap)
{
    return New<TNodeDisposalManager>(bootstrap);
}

} // namespace NYT::NNodeTrackerServer
