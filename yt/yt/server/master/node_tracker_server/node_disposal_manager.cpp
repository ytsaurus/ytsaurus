#include "node_disposal_manager.h"

#include "node.h"
#include "config.h"
#include "public.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>

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

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NodeTrackerServerLogger;

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
            BIND(&TNodeDisposalManager::Load, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "NodeDisposalManager",
            BIND(&TNodeDisposalManager::Save, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TNodeDisposalManager::HydraStartNodeDisposal, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TNodeDisposalManager::HydraFinishNodeDisposal, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TNodeDisposalManager::HydraDisposeLocation, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TNodeDisposalManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void DisposeNodeWithSemaphore(TNode* node) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(IsLeader());

        TReqStartNodeDisposal request;
        request.set_node_id(ToProto<ui32>(node->GetId()));

        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TNodeDisposalManager::HydraStartNodeDisposal,
            this);

        DisposeNodeSemaphore_->AsyncAcquire().SubscribeUnique(
            BIND([mutation = std::move(mutation)] (TErrorOr<TAsyncSemaphoreGuard>&& guardOrError) {
                // Even if acquiring semaphore failed, we still have to commit mutation.
                YT_LOG_ALERT_UNLESS(guardOrError.IsOK(), guardOrError, "Failed to acquire node disposal semaphore");

                Y_UNUSED(WaitFor(mutation->CommitAndLog(Logger)));
            })
            .Via(EpochAutomatonInvoker_));
    }

    void DisposeNodeCompletely(TNode* node) override
    {
        YT_VERIFY(HasMutationContext());

        YT_PROFILE_TIMING("/node_tracker/node_dispose_time") {
            // Node was being disposed location by location, but smth needs it to be disposed right now.
            if (node->GetLocalState() == ENodeState::BeingDisposed) {
                EraseFromDisposalQueue(node->GetId());
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
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        buffer->AddGauge("/nodes_being_disposed", NodesBeingDisposed_.size());
        buffer->AddGauge("/nodes_awaiting_for_being_disposed", NodesAwaitingForBeingDisposed_.size());
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    const TAsyncSemaphorePtr DisposeNodeSemaphore_ = New<TAsyncSemaphore>(0);

    TPeriodicExecutorPtr NodeDisposalExecutor_;
    THashSet<TNodeId> NodesBeingDisposed_;
    std::deque<TNodeId> NodesAwaitingForBeingDisposed_;

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        const auto& config = GetDynamicConfig();
        NodeDisposalExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::NodeTracker),
            BIND(&TNodeDisposalManager::NodeDisposalTick, MakeWeak(this)),
            config->NodeDisposalTickPeriod);
        NodeDisposalExecutor_->Start();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();
        if (NodeDisposalExecutor_) {
            YT_UNUSED_FUTURE(NodeDisposalExecutor_->Stop());
            NodeDisposalExecutor_.Reset();
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto nodeId : NodesBeingDisposed_) {
            auto* node = nodeTracker->FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                continue;
            }

            node->SetDisposalTickScheduled(false);
        }
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        Load(context, NodesBeingDisposed_);
        if (context.GetVersion() >= EMasterReign::DisposalNodesLimit) {
            Load(context, NodesAwaitingForBeingDisposed_);
        }
    }

    void Save(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, NodesBeingDisposed_);
        Save(context, NodesAwaitingForBeingDisposed_);
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        NodesBeingDisposed_.clear();
        NodesAwaitingForBeingDisposed_.clear();
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& config = GetDynamicConfig();
        if (NodeDisposalExecutor_) {
            NodeDisposalExecutor_->SetPeriod(config->NodeDisposalTickPeriod);
        }

        DisposeNodeSemaphore_->SetTotal(config->MaxConcurrentNodeUnregistrations);
        TopUpNodesBeingDisposed();
    }

    const TDynamicNodeTrackerConfigPtr& GetDynamicConfig()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return Bootstrap_->GetConfigManager()->GetConfig()->NodeTracker;
    }

    void FinishNodeDisposal(TNodeId nodeId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(IsLeader());

        TReqFinishNodeDisposal request;
        request.set_node_id(ToProto<ui32>(nodeId));

        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TNodeDisposalManager::HydraFinishNodeDisposal,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
    }

    void NodeDisposalTick()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(IsLeader());

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& config = GetDynamicConfig();

        for (auto nodeId : NodesBeingDisposed_) {
            auto* node = nodeTracker->FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                // TODO(aleksandra-zh): ensure there are no replicas left.
                FinishNodeDisposal(nodeId);
                return;
            }

            YT_VERIFY(node->GetLocalState() == ENodeState::BeingDisposed);
            auto locationIndex = node->GetNextDisposedLocationIndex();
            if (locationIndex < std::ssize(node->RealChunkLocations())) {
                StartLocationDisposal(node, locationIndex);
            } else {
                YT_VERIFY(locationIndex == std::ssize(node->RealChunkLocations()));
                if (config->Testing->DisableDisposalFinishing) {
                    continue;
                }

                FinishNodeDisposal(nodeId);
            }
        }
    }

    void StartLocationDisposal(TNode* node, int locationIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(IsLeader());

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto* location = node->RealChunkLocations()[locationIndex];
        if (location->GetBeingDisposed()) {
            return;
        }
        location->SetBeingDisposed(true);

        auto sequoiaReplicasFuture = chunkManager->GetSequoiaLocationReplicas(node->GetId(), location->GetUuid());
        auto errorOrSequoiaReplicas = WaitFor(sequoiaReplicasFuture);
        if (!errorOrSequoiaReplicas.IsOK()) {
            location->SetBeingDisposed(false);
            YT_LOG_ERROR(errorOrSequoiaReplicas, "Error getting Sequoia location replicas");
            return;
        }
        const auto& sequoiaReplicas = errorOrSequoiaReplicas.Value();

        YT_LOG_INFO("Disposing location (NodeId: %v, Address: %v, LocationIndex: %v, LocationUuid: %v)",
            node->GetId(),
            node->GetDefaultAddress(),
            locationIndex,
            location->GetUuid());

        TReqModifyReplicas sequoiaRequest;
        TChunkLocationDirectory locationDirectory;
        sequoiaRequest.set_node_id(ToProto<ui32>(node->GetId()));
        for (const auto& replica : sequoiaReplicas) {
            TChunkRemoveInfo chunkRemoveInfo;
            ToProto(chunkRemoveInfo.mutable_chunk_id(), replica.Key.ChunkId);
            chunkRemoveInfo.set_location_index(locationDirectory.GetOrCreateIndex(location->GetUuid()));

            *sequoiaRequest.add_removed_chunks() = chunkRemoveInfo;
        }

        ToProto(sequoiaRequest.mutable_location_directory(), locationDirectory);

        TReqDisposeLocation request;
        request.set_node_id(ToProto<ui32>(node->GetId()));
        request.set_location_index(locationIndex);
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TNodeDisposalManager::HydraDisposeLocation,
            this);

        if (sequoiaRequest.removed_chunks_size() == 0) {
            YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
            return;
        }

        auto future = chunkManager->ModifySequoiaReplicas(sequoiaRequest);
        YT_UNUSED_FUTURE(future.Apply(BIND([=, mutation = std::move(mutation), nodeId = node->GetId(), this, this_ = MakeStrong(this)] (const TErrorOr<TRspModifyReplicas>& rspOrError) {
            if (!rspOrError.IsOK()) {
                const auto& nodeTracker = Bootstrap_->GetNodeTracker();
                auto* node = nodeTracker->FindNode(nodeId);
                if (!IsObjectAlive(node)) {
                    return;
                }

                auto* location = node->RealChunkLocations()[locationIndex];
                location->SetBeingDisposed(false);
                return;
            }

            YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
        }).AsyncVia(Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::NodeTracker))));
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
        for (auto* location : node->ChunkLocations()) {
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

    void TopUpNodesBeingDisposed()
    {
        while (std::ssize(NodesBeingDisposed_) < GetDynamicConfig()->MaxNodesBeingDisposed &&
            !NodesAwaitingForBeingDisposed_.empty())
        {
            InsertOrCrash(NodesBeingDisposed_, NodesAwaitingForBeingDisposed_.front());
            NodesAwaitingForBeingDisposed_.pop_front();
        }
    }

    void EraseFromDisposalQueue(TNodeId nodeId)
    {
        EraseOrCrash(NodesBeingDisposed_, nodeId);
        TopUpNodesBeingDisposed();
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

        DoStartNodeDisposal(node);
        nodeTracker->SetNodeLocalState(node, ENodeState::BeingDisposed);
        node->SetNextDisposedLocationIndex(0);

        if (std::ssize(NodesBeingDisposed_) < GetDynamicConfig()->MaxNodesBeingDisposed) {
            InsertOrCrash(NodesBeingDisposed_, node->GetId());
        } else {
            NodesAwaitingForBeingDisposed_.push_back(node->GetId());
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

            node->SetDisposalTickScheduled(false);

            if (!NodesBeingDisposed_.contains(nodeId)) {
                YT_VERIFY(node->GetLocalState() != ENodeState::BeingDisposed);
                return;
            }

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto locationIndex = node->GetNextDisposedLocationIndex();
            YT_VERIFY(locationIndex == request->location_index());
            YT_VERIFY(locationIndex < std::ssize(node->RealChunkLocations()));

            auto* location = node->RealChunkLocations()[locationIndex];
            if (IsLeader()) {
                YT_VERIFY(location->GetBeingDisposed());
            }

            YT_LOG_INFO("Disposing location (NodeId: %v, Address: %v, Location: %v)",
                nodeId,
                node->GetDefaultAddress(),
                location->GetUuid());
            chunkManager->DisposeLocation(location);

            location->SetBeingDisposed(false);

            node->SetNextDisposedLocationIndex(locationIndex + 1);
        }
    }

    void HydraFinishNodeDisposal(TReqFinishNodeDisposal* request)
    {
        auto nodeId = FromProto<NNodeTrackerClient::TNodeId>(request->node_id());
        auto finalyGuard = Finally([&]() {
            if (NodesBeingDisposed_.contains(nodeId)) {
                EraseFromDisposalQueue(nodeId);
            }
        });

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->FindNode(nodeId);
        if (!IsObjectAlive(node)) {
            return;
        }

        if (node->GetLocalState() != ENodeState::BeingDisposed) {
            return;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        if (node->UseImaginaryChunkLocations()) {
            for (auto* location : node->ChunkLocations()) {
                chunkManager->DisposeLocation(location);
            }
        }

        DoFinishNodeDisposal(node);
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeDisposalManagerPtr CreateNodeDisposalManager(TBootstrap* bootstrap)
{
    return New<TNodeDisposalManager>(bootstrap);
}

} // namespace NYT::NNodeTrackerServer
