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

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NProto;
using namespace NProfiling;

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

        RegisterMethod(BIND(&TNodeDisposalManager::HydraDisposeNode, Unretained(this)));
        RegisterMethod(BIND(&TNodeDisposalManager::HydraNodeDisposalTick, Unretained(this)));
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

        TReqDisposeNode request;
        request.set_node_id(ToProto<ui32>(node->GetId()));

        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TNodeDisposalManager::HydraDisposeNode,
            this);

        auto handler = BIND([mutation = std::move(mutation)] (TAsyncSemaphoreGuard) {
            Y_UNUSED(WaitFor(mutation->CommitAndLog(Logger)));
        });

        DisposeNodeSemaphore_->AsyncAcquire(handler, EpochAutomatonInvoker_);
    }

    void DisposeNodeCompletely(TNode* node) override
    {
        YT_VERIFY(HasMutationContext());

        YT_PROFILE_TIMING("/node_tracker/node_dispose_time") {
            // Node was being disposed location by location, but smth needs it to be disposed right now.
            if (node->GetLocalState() == ENodeState::BeingDisposed) {
                EraseOrCrash(NodesBeingDisposed_, node->GetId());
                node->SetLocalState(ENodeState::Unregistered);
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
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    const TAsyncSemaphorePtr DisposeNodeSemaphore_ = New<TAsyncSemaphore>(0);

    TPeriodicExecutorPtr NodeDisposalExecutor_;
    THashSet<TNodeId> NodesBeingDisposed_;

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
            NodeDisposalExecutor_->Stop();
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
    }

    void Save(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, NodesBeingDisposed_);
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        NodesBeingDisposed_.clear();
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& config = GetDynamicConfig();
        if (NodeDisposalExecutor_) {
            NodeDisposalExecutor_->SetPeriod(config->NodeDisposalTickPeriod);
        }

        DisposeNodeSemaphore_->SetTotal(config->MaxConcurrentNodeUnregistrations);
    }

    const TDynamicNodeTrackerConfigPtr& GetDynamicConfig()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return Bootstrap_->GetConfigManager()->GetConfig()->NodeTracker;
    }

    void StartNodeDisposal(TNode* node)
    {
        YT_VERIFY(HasMutationContext());

        DoStartNodeDisposal(node);
        node->SetLocalState(ENodeState::BeingDisposed);
        node->SetNextDisposedLocationIndex(0);

        InsertOrCrash(NodesBeingDisposed_, node->GetId());
    }

    void NodeDisposalTick()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(IsLeader());

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto nodeId : NodesBeingDisposed_) {
            auto* node = nodeTracker->FindNode(nodeId);

            if (!IsObjectAlive(node)) {
                continue;
            }

            if (node->GetDisposalTickScheduled()) {
                continue;
            }

            node->SetDisposalTickScheduled(true);

            YT_LOG_DEBUG("Scheduling node disposal tick (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());

            TReqNodeDisposalTick request;
            request.set_node_id(ToProto<ui32>(nodeId));

            auto mutation = CreateMutation(
                Bootstrap_->GetHydraFacade()->GetHydraManager(),
                request,
                &TNodeDisposalManager::HydraNodeDisposalTick,
                this);

            auto handler = BIND([mutation = std::move(mutation)] (TAsyncSemaphoreGuard) {
                Y_UNUSED(WaitFor(mutation->CommitAndLog(Logger)));
            });

            DisposeNodeSemaphore_->AsyncAcquire(handler, EpochAutomatonInvoker_);
        }
    }

    void HydraDisposeNode(TReqDisposeNode* request)
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

        if (GetDynamicConfig()->EnablePerLocationNodeDisposal && !node->UseImaginaryChunkLocations()) {
            StartNodeDisposal(node);
        } else {
            DisposeNodeCompletely(node);
        }
    }

    void HydraNodeDisposalTick(TReqNodeDisposalTick* request)
    {
        YT_PROFILE_TIMING("/node_tracker/node_dispose_time") {
            auto nodeId = FromProto<NNodeTrackerClient::TNodeId>(request->node_id());

            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            auto* node = nodeTracker->FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                return;
            }

            node->SetDisposalTickScheduled(false);

            YT_LOG_INFO("Starting node disposal tick (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());

            if (!NodesBeingDisposed_.contains(nodeId)) {
                YT_VERIFY(node->GetLocalState() != ENodeState::BeingDisposed);
                return;
            }

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto locationIndex = node->GetNextDisposedLocationIndex();

            // locationIndex can be equal if there are no real locations or because of DisableDisposalFinishing.
            if (locationIndex < std::ssize(node->RealChunkLocations())) {
                YT_VERIFY(!node->UseImaginaryChunkLocations());
                auto* location = node->RealChunkLocations()[locationIndex];
                YT_LOG_INFO("Disposing location (NodeId: %v, Address: %v, Location: %v)",
                    node->GetId(),
                    node->GetDefaultAddress(),
                    location->GetUuid());
                chunkManager->DisposeLocation(location);

                ++locationIndex;
                node->SetNextDisposedLocationIndex(locationIndex);
            }

            const auto& config = GetDynamicConfig();
            if (locationIndex >= std::ssize(node->RealChunkLocations()) && !config->Testing->DisableDisposalFinishing) {
                // If UseImaginaryChunkLocations is monotonic (and it should be), we should never get here.
                if (node->UseImaginaryChunkLocations()) {
                    YT_LOG_ALERT("Node with imaginary locations went through per-location disposal (NodeId: %v, Address: %v)",
                        node->GetId(),
                        node->GetDefaultAddress());
                    DisposeAllLocations(node);
                }
                EraseOrCrash(NodesBeingDisposed_, node->GetId());
                DoFinishNodeDisposal(node);
            }
        }
    }

    void DisposeAllLocations(TNode* node)
    {
        YT_VERIFY(HasMutationContext());

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        for (auto* location : node->ChunkLocations()) {
            chunkManager->DisposeLocation(location);
        }
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

    void DoFinishNodeDisposal(TNode* node)
    {
        YT_VERIFY(HasMutationContext());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->DisposeNode(node);

        node->SetLocalState(ENodeState::Offline);

        YT_LOG_INFO("Node offline (NodeId: %v, Address: %v)",
            node->GetId(),
            node->GetDefaultAddress());
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeDisposalManagerPtr CreateNodeDisposalManager(TBootstrap* bootstrap)
{
    return New<TNodeDisposalManager>(bootstrap);
}

} // namespace NYT::NNodeTrackerServer
