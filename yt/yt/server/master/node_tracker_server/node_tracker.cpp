#include "node_tracker.h"
#include "node_discovery_manager.h"
#include "private.h"
#include "config.h"
#include "node.h"
#include "host.h"
#include "rack.h"
#include "data_center.h"
#include "node_tracker_log.h"
#include "node_type_handler.h"
#include "host_type_handler.h"
#include "rack_type_handler.h"
#include "node_disposal_manager.h"
#include "data_center_type_handler.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cell_server/cellar_node_tracker.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/data_node_tracker.h>
#include <yt/yt/server/master/chunk_server/job.h>
#include <yt/yt/server/master/chunk_server/domestic_medium.h>
#include <yt/yt/server/master/chunk_server/chunk_location.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/maintenance_tracker_server/proto/maintenance_tracker.pb.h>

#include <yt/yt/server/master/node_tracker_server/proto/node_tracker.pb.h>

#include <yt/yt/server/master/object_server/attribute_set.h>
#include <yt/yt/server/master/object_server/object_manager.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/tablet_server/tablet_node_tracker.h>
#include <yt/yt/server/master/tablet_server/tablet_manager.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/lib/node_tracker_server/name_helpers.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/cellar_node_tracker_service.pb.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/tablet_node_tracker_client/proto/tablet_node_tracker_service.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/cellar_client/tablet_cell_service_proxy.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/id_generator.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/transparent_pair_compare.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NHiveServer;
using namespace NHydra;
using namespace NMaintenanceTrackerServer;
using namespace NNet;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NNodeTrackerServer::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NProfiling;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NodeTrackerServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker
    : public INodeTracker
    , public TMasterAutomatonPart
{
public:
    explicit TNodeTracker(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::NodeTracker)
        , NodeDisposalManager_(CreateNodeDisposalManager(bootstrap))
    {
        RegisterMethod(BIND(&TNodeTracker::HydraRegisterNode, Unretained(this)));
        RegisterMethod(BIND(&TNodeTracker::HydraUnregisterNode, Unretained(this)));
        RegisterMethod(BIND(&TNodeTracker::HydraClusterNodeHeartbeat, Unretained(this)));
        // COMPAT(aleksandra-zh)
        RegisterMethod(BIND(&TNodeTracker::HydraSetCellNodeDescriptors, Unretained(this)));
        RegisterMethod(BIND(&TNodeTracker::HydraUpdateNodeResources, Unretained(this)));
        RegisterMethod(BIND(&TNodeTracker::HydraUpdateNodesForRole, Unretained(this)));
        RegisterMethod(BIND(&TNodeTracker::HydraAddMaintenance, Unretained(this)));
        RegisterMethod(BIND(&TNodeTracker::HydraRemoveMaintenance, Unretained(this)));
        RegisterMethod(BIND(&TNodeTracker::HydraSendNodeStates, Unretained(this)));
        RegisterMethod(BIND(&TNodeTracker::HydraSetNodeStatistics, Unretained(this)));
        // COMPAT(aleksandra-zh)
        RegisterMethod(BIND(&TNodeTracker::HydraSetNodeStates, Unretained(this)));
        RegisterMethod(BIND(&TNodeTracker::HydraSetNodeState, Unretained(this)));
        RegisterMethod(BIND(&TNodeTracker::HydraResetNodePendingRestartMaintenance, Unretained(this)));

        RegisterLoader(
            "NodeTracker.Keys",
            BIND(&TNodeTracker::LoadKeys, Unretained(this)));
        RegisterLoader(
            "NodeTracker.Values",
            BIND(&TNodeTracker::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "NodeTracker.Keys",
            BIND(&TNodeTracker::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "NodeTracker.Values",
            BIND(&TNodeTracker::SaveValues, Unretained(this)));

        BufferedProducer_ = New<TBufferedProducer>();
        NodeTrackerProfiler
            .WithDefaultDisabled()
            .WithSparse()
            .WithTag("cell_tag", ToString(Bootstrap_->GetMulticellManager()->GetCellTag()))
            .AddProducer("", BufferedProducer_);

        if (Bootstrap_->IsPrimaryMaster()) {
            MasterCacheManager_ = New<TNodeDiscoveryManager>(Bootstrap_, ENodeRole::MasterCache);
            TimestampProviderManager_ = New<TNodeDiscoveryManager>(Bootstrap_, ENodeRole::TimestampProvider);
        }

        ResetNodePendingRestartMaintenanceExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            BIND(&TNodeTracker::ResetNodesPendingRestartMaintenanceOnTimeout, MakeWeak(this)));
        ResetNodePendingRestartMaintenanceExecutor_->Start();
    }

    void SubscribeToAggregatedNodeStateChanged(TNode* node)
    {
        node->SubscribeAggregatedStateChanged(BIND_NO_PROPAGATE(&TNodeTracker::OnAggregatedNodeStateChanged, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TNodeTracker::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND_NO_PROPAGATE(&TNodeTracker::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND_NO_PROPAGATE(&TNodeTracker::OnTransactionFinished, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateNodeTypeHandler(Bootstrap_, &NodeMap_));
        objectManager->RegisterHandler(CreateHostTypeHandler(Bootstrap_, &HostMap_));
        objectManager->RegisterHandler(CreateRackTypeHandler(Bootstrap_, &RackMap_));
        objectManager->RegisterHandler(CreateDataCenterTypeHandler(Bootstrap_, &DataCenterMap_));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeValidateSecondaryMasterRegistration(
                BIND_NO_PROPAGATE(&TNodeTracker::OnValidateSecondaryMasterRegistration, MakeWeak(this)));
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND_NO_PROPAGATE(&TNodeTracker::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND_NO_PROPAGATE(&TNodeTracker::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            BIND(&TNodeTracker::OnProfiling, MakeWeak(this)),
            TDynamicNodeTrackerConfig::DefaultProfilingPeriod);
        ProfilingExecutor_->Start();

        NodeDisposalManager_->Initialize();
    }

    void ProcessRegisterNode(const TString& address, TCtxRegisterNodePtr context) override
    {
        if (PendingRegisterNodeAddresses_.contains(address)) {
            context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Node is already being registered"));
            return;
        }

        auto groups = GetGroupsForNode(address);
        for (auto* group : groups) {
            if (group->PendingRegisterNodeMutationCount + group->LocalRegisteredNodeCount >= group->Config->MaxConcurrentNodeRegistrations) {
                context->Reply(TError(
                    NRpc::EErrorCode::Unavailable,
                    "Node registration throttling is active in group %Qv",
                    group->Id));
                return;
            }
        }

        InsertOrCrash(PendingRegisterNodeAddresses_, address);
        for (auto* group : groups) {
            ++group->PendingRegisterNodeMutationCount;
        }

        YT_LOG_DEBUG("Node register mutation scheduled (Address: %v, NodeGroups: %v)",
            address,
            MakeFormattableView(groups, [] (auto* builder, const auto* group) {
                builder->AppendFormat("%v", group->Id);
            }));

        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TNodeTracker::HydraRegisterNode,
            this);
        mutation->SetCurrentTraceContext();
        mutation->CommitAndReply(context)
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& /*error*/) {
                // NB: May be missing if OnLeadingStopped was called prior to mutation failure.
                PendingRegisterNodeAddresses_.erase(address);

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                if (multicellManager->IsPrimaryMaster() && IsLeader()) {
                    auto groups = GetGroupsForNode(address);
                    for (auto* group : groups) {
                        --group->PendingRegisterNodeMutationCount;
                    }
                }
            }).Via(EpochAutomatonInvoker_));
    }

    void ProcessHeartbeat(TCtxHeartbeatPtr context) override
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TNodeTracker::HydraClusterNodeHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), HeartbeatSemaphore_);
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Node, TNode);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Host, THost);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Rack, TRack);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(DataCenter, TDataCenter);

    DEFINE_SIGNAL_OVERRIDE(void(TNode* node), NodeRegistered);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node), NodeOnline);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node), NodeUnregistered);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node), NodeZombified);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node), NodeBanChanged);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node), NodeDecommissionChanged);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node), NodeDisableWriteSessionsChanged);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node), NodeDisableTabletCellsChanged);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node), NodePendingRestartChanged);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node), NodeTagsChanged);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node, TRack*), NodeRackChanged);
    DEFINE_SIGNAL_OVERRIDE(void(TNode* node, TDataCenter*), NodeDataCenterChanged);
    DEFINE_SIGNAL_OVERRIDE(void(TDataCenter*), DataCenterCreated);
    DEFINE_SIGNAL_OVERRIDE(void(TDataCenter*), DataCenterRenamed);
    DEFINE_SIGNAL_OVERRIDE(void(TDataCenter*), DataCenterDestroyed);
    DEFINE_SIGNAL_OVERRIDE(void(TRack*), RackCreated);
    DEFINE_SIGNAL_OVERRIDE(void(TRack*), RackRenamed);
    DEFINE_SIGNAL_OVERRIDE(void(TRack*, TDataCenter*), RackDataCenterChanged);
    DEFINE_SIGNAL_OVERRIDE(void(TRack*), RackDestroyed);
    DEFINE_SIGNAL_OVERRIDE(void(THost*), HostCreated);
    DEFINE_SIGNAL_OVERRIDE(void(THost*, TRack*), HostRackChanged);
    DEFINE_SIGNAL_OVERRIDE(void(THost*), HostDestroyed);

    void ZombifyNode(TNode* node) override
    {
        // NB: This is typically redundant since it's not possible to remove a node unless
        // it is offline. Secondary masters, however, may receive a removal request from primaries
        // and must obey it regardless of the node's state.
        EnsureNodeDisposed(node);

        RemoveFromAddressMaps(node);

        RecomputePendingRegisterNodeMutationCounters();

        RemoveFromNodeLists(node);

        RemoveFromFlavorSets(node);

        // Detach node from host.
        node->SetHost(nullptr);

        NodeZombified_.Fire(node);
    }

    TObjectId ObjectIdFromNodeId(TNodeId nodeId) override
    {
        return NNodeTrackerClient::ObjectIdFromNodeId(
            nodeId,
            Bootstrap_->GetMulticellManager()->GetPrimaryCellTag());
    }

    TNode* FindNode(TNodeId id) override
    {
        return FindNode(ObjectIdFromNodeId(id));
    }

    TNode* GetNode(TNodeId id) override
    {
        return GetNode(ObjectIdFromNodeId(id));
    }

    TNode* GetNodeOrThrow(TNodeId id) override
    {
        auto* node = FindNode(id);
        if (!node) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::NoSuchNode,
                "Invalid or expired node id %v",
                id);
        }
        return node;
    }

    TNode* FindNodeByAddress(const TString& address) override
    {
        auto it = AddressToNodeMap_.find(address);
        return it == AddressToNodeMap_.end() ? nullptr : it->second;
    }

    TNode* GetNodeByAddress(const TString& address) override
    {
        auto* node = FindNodeByAddress(address);
        YT_VERIFY(node);
        return node;
    }

    TNode* GetNodeByAddressOrThrow(const TString& address) override
    {
        auto* node = FindNodeByAddress(address);
        if (!node) {
            THROW_ERROR_EXCEPTION("No such cluster node %Qv", address);
        }
        return node;
    }

    TNode* FindNodeByHostName(const TString& hostName) override
    {
        auto it = HostNameToNodeMap_.find(hostName);
        return it == HostNameToNodeMap_.end() ? nullptr : it->second;
    }

    THost* GetHostByNameOrThrow(const TString& name) override
    {
        auto* host = FindHostByName(name);
        if (!host) {
            THROW_ERROR_EXCEPTION("No such host %Qv", name);
        }
        return host;
    }

    THost* FindHostByName(const TString& name) override
    {
        auto it = NameToHostMap_.find(name);
        return it == NameToHostMap_.end() ? nullptr : it->second;
    }

    THost* GetHostByName(const TString& name) override
    {
        auto* host = FindHostByName(name);
        YT_VERIFY(host);
        return host;
    }

    void SetHostRack(THost* host, TRack* rack) override
    {
        if (host->GetRack() != rack) {
            auto* oldRack = host->GetRack();
            host->SetRack(rack);
            HostRackChanged_.Fire(host, oldRack);

            const auto& nodes = host->Nodes();
            for (auto* node : nodes) {
                UpdateNodeCounters(node, -1);
                node->RebuildTags();
                NodeTagsChanged_.Fire(node);
                NodeRackChanged_.Fire(node, oldRack);
                UpdateNodeCounters(node, +1);
            }

            YT_LOG_INFO(
                "Host rack changed (Host: %v, Rack: %v -> %v)",
                host->GetName(),
                oldRack ? std::optional(oldRack->GetName()) : std::nullopt,
                rack ? std::optional(rack->GetName()) : std::nullopt);
        }
    }

    std::vector<THost*> GetRackHosts(const TRack* rack) override
    {
        std::vector<THost*> hosts;
        for (auto [hostId, host] : HostMap_) {
            if (!IsObjectAlive(host)) {
                continue;
            }
            if (host->GetRack() == rack) {
                hosts.push_back(host);
            }
        }

        return hosts;
    }

    std::vector<TNode*> GetRackNodes(const TRack* rack) override
    {
        std::vector<TNode*> nodes;
        for (const auto* host : GetRackHosts(rack)) {
            for (auto* node : host->Nodes()) {
                if (!IsObjectAlive(node)) {
                    continue;
                }
                nodes.push_back(node);
            }
        }

        return nodes;
    }

    std::vector<TRack*> GetDataCenterRacks(const TDataCenter* dc) override
    {
        std::vector<TRack*> result;
        for (auto [rackId, rack] : RackMap_) {
            if (!IsObjectAlive(rack)) {
                continue;
            }
            if (rack->GetDataCenter() == dc) {
                result.push_back(rack);
            }
        }
        return result;
    }

    const THashSet<TNode*>& GetNodesWithFlavor(ENodeFlavor flavor) const override
    {
        return NodesWithFlavor_[flavor];
    }

    void UpdateLastSeenTime(TNode* node) override
    {
        const auto* mutationContext = GetCurrentMutationContext();
        node->SetLastSeenTime(mutationContext->GetTimestamp());
    }

    void OnNodeMaintenanceUpdated(TNode* node, EMaintenanceType type) override
    {
        YT_VERIFY(HasHydraContext());

        switch (type) {
        case EMaintenanceType::Ban:
            OnNodeBanUpdated(node);
            break;
        case EMaintenanceType::Decommission:
            OnNodeDecommissionUpdated(node);
            break;
        case EMaintenanceType::DisableTabletCells:
            OnDisableTabletCellsUpdated(node);
            break;
        case EMaintenanceType::DisableWriteSessions:
            OnDisableWriteSessionsUpdated(node);
            break;
        case EMaintenanceType::DisableSchedulerJobs:
            break;
        case EMaintenanceType::PendingRestart:
            OnNodePendingRestartUpdated(node);
            break;
        default:
            YT_LOG_ALERT("Invalid maintenance type (Type: %v)", type);
            THROW_ERROR_EXCEPTION("Invalid maintenance type")
                << TErrorAttribute("type", type);
        }
    }

    void SetNodeHost(TNode* node, THost* host) override
    {
        if (node->GetHost() != host) {
            auto* oldHost = node->GetHost();
            UpdateNodeCounters(node, -1);
            node->SetHost(host);
            YT_LOG_INFO("Node host changed (NodeId: %v, Address: %v, Host: %v -> %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                oldHost ? std::optional(oldHost->GetName()) : std::nullopt,
                host ? std::optional(host->GetName()) : std::nullopt);
            NodeTagsChanged_.Fire(node);
            UpdateNodeCounters(node, +1);
        }
    }

    void SetNodeUserTags(TNode* node, const std::vector<TString>& tags) override
    {
        UpdateNodeCounters(node, -1);
        node->SetUserTags(tags);
        NodeTagsChanged_.Fire(node);
        UpdateNodeCounters(node, +1);
    }

    std::unique_ptr<TMutation> CreateUpdateNodeResourcesMutation(const NProto::TReqUpdateNodeResources& request) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TNodeTracker::HydraUpdateNodeResources,
            this);
    }

    THost* CreateHost(const TString& name, TObjectId hintId) override
    {
        ValidateHostName(name);

        if (FindHostByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Host %Qv already exists",
                name);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Host, hintId);

        auto hostHolder = TPoolAllocator::New<THost>(id);
        hostHolder->SetName(name);

        auto* host = HostMap_.Insert(id, std::move(hostHolder));
        YT_VERIFY(NameToHostMap_.emplace(name, host).second);

        // Make the fake reference.
        YT_VERIFY(host->RefObject() == 1);

        HostCreated_.Fire(host);

        YT_LOG_DEBUG(
            "Host created (HostId: %v, HostName: %v)",
            host->GetId(),
            host->GetName());

        return host;
    }

    void ZombifyHost(THost* host) override
    {
        YT_VERIFY(host->Nodes().empty());

        // Remove host from maps.
        YT_VERIFY(NameToHostMap_.erase(host->GetName()) > 0);

        HostDestroyed_.Fire(host);
    }

    TRack* CreateRack(const TString& name, TObjectId hintId) override
    {
        ValidateRackName(name);

        if (FindRackByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Rack %Qv already exists",
                name);
        }

        if (RackCount_ >= MaxRackCount) {
            THROW_ERROR_EXCEPTION("Rack count limit %v is reached",
                MaxRackCount);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Rack, hintId);

        auto rackHolder = TPoolAllocator::New<TRack>(id);
        rackHolder->SetName(name);
        rackHolder->SetIndex(AllocateRackIndex());

        auto* rack = RackMap_.Insert(id, std::move(rackHolder));
        YT_VERIFY(NameToRackMap_.emplace(name, rack).second);

        // Make the fake reference.
        YT_VERIFY(rack->RefObject() == 1);

        RackCreated_.Fire(rack);

        return rack;
    }

    void ZombifyRack(TRack* rack) override
    {
        // Unbind hosts from this rack.
        for (auto* host : GetRackHosts(rack)) {
            SetHostRack(host, /*rack*/ nullptr);
        }

        // Remove rack from maps.
        YT_VERIFY(NameToRackMap_.erase(rack->GetName()) == 1);
        FreeRackIndex(rack->GetIndex());

        RackDestroyed_.Fire(rack);
    }

    void RenameRack(TRack* rack, const TString& newName) override
    {
        if (rack->GetName() == newName)
            return;

        if (FindRackByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Rack %Qv already exists",
                newName);
        }

        // Update name.
        YT_VERIFY(NameToRackMap_.erase(rack->GetName()) == 1);
        YT_VERIFY(NameToRackMap_.emplace(newName, rack).second);
        rack->SetName(newName);

        // Rebuild node tags since they depend on rack name.
        for (auto* node : GetRackNodes(rack)) {
            UpdateNodeCounters(node, -1);
            node->RebuildTags();
            UpdateNodeCounters(node, +1);
        }

        RackRenamed_.Fire(rack);
    }

    TRack* FindRackByName(const TString& name) override
    {
        auto it = NameToRackMap_.find(name);
        return it == NameToRackMap_.end() ? nullptr : it->second;
    }

    TRack* GetRackByNameOrThrow(const TString& name) override
    {
        auto* rack = FindRackByName(name);
        if (!rack) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::NoSuchRack,
                "No such rack %Qv",
                name);
        }
        return rack;
    }

    void SetRackDataCenter(TRack* rack, TDataCenter* dataCenter) override
    {
        if (rack->GetDataCenter() != dataCenter) {
            auto* oldDataCenter = rack->GetDataCenter();
            rack->SetDataCenter(dataCenter);

            // Node's tags take into account not only its rack, but also its
            // rack's DC.
            auto nodes = GetRackNodes(rack);
            for (auto* node : nodes) {
                UpdateNodeCounters(node, -1);
                node->RebuildTags();
                UpdateNodeCounters(node, +1);
            }

            YT_LOG_INFO("Rack data center changed (Rack: %v, DataCenter: %v)",
                std::optional(rack->GetName()),
                dataCenter ? std::optional(dataCenter->GetName()) : std::nullopt);

            RackDataCenterChanged_.Fire(rack, oldDataCenter);

            for (auto* node : nodes) {
                NodeDataCenterChanged_.Fire(node, oldDataCenter);
            }
        }
    }


    TDataCenter* CreateDataCenter(const TString& name, TObjectId hintId) override
    {
        ValidateDataCenterName(name);

        if (FindDataCenterByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Data center %Qv already exists",
                name);
        }

        if (DataCenterMap_.GetSize() >= MaxDataCenterCount) {
            THROW_ERROR_EXCEPTION("Data center count limit %v is reached",
                MaxDataCenterCount);
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::DataCenter, hintId);

        auto dcHolder = TPoolAllocator::New<TDataCenter>(id);
        dcHolder->SetName(name);

        auto* dc = DataCenterMap_.Insert(id, std::move(dcHolder));
        YT_VERIFY(NameToDataCenterMap_.emplace(name, dc).second);

        // Make the fake reference.
        YT_VERIFY(dc->RefObject() == 1);

        DataCenterCreated_.Fire(dc);

        return dc;
    }

    void ZombifyDataCenter(TDataCenter* dc) override
    {
        // Unbind racks from this DC.
        for (auto* rack : GetDataCenterRacks(dc)) {
            SetRackDataCenter(rack, nullptr);
        }

        // Remove DC from maps.
        YT_VERIFY(NameToDataCenterMap_.erase(dc->GetName()) == 1);

        DataCenterDestroyed_.Fire(dc);
    }

    void RenameDataCenter(TDataCenter* dc, const TString& newName) override
    {
        if (dc->GetName() == newName)
            return;

        if (FindDataCenterByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Data center %Qv already exists",
                newName);
        }

        // Update name.
        YT_VERIFY(NameToDataCenterMap_.erase(dc->GetName()) == 1);
        YT_VERIFY(NameToDataCenterMap_.emplace(newName, dc).second);
        dc->SetName(newName);

        // Rebuild node tags since they depend on DC name.
        for (auto* rack : GetDataCenterRacks(dc)) {
            for (auto* node : GetRackNodes(rack)) {
                UpdateNodeCounters(node, -1);
                node->RebuildTags();
                UpdateNodeCounters(node, +1);
            }
        }

        DataCenterRenamed_.Fire(dc);
    }

    TDataCenter* FindDataCenterByName(const TString& name) override
    {
        auto it = NameToDataCenterMap_.find(name);
        return it == NameToDataCenterMap_.end() ? nullptr : it->second;
    }

    TDataCenter* GetDataCenterByNameOrThrow(const TString& name) override
    {
        auto* dc = FindDataCenterByName(name);
        if (!dc) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::NoSuchDataCenter,
                "No such data center %Qv",
                name);
        }
        return dc;
    }


    TAggregatedNodeStatistics GetAggregatedNodeStatistics() override
    {
        MaybeRebuildAggregatedNodeStatistics();

        auto guard = ReaderGuard(NodeStatisticsLock_);
        return AggregatedNodeStatistics_;
    }

    TAggregatedNodeStatistics GetFlavoredNodeStatistics(ENodeFlavor flavor) override
    {
        MaybeRebuildAggregatedNodeStatistics();

        auto guard = ReaderGuard(NodeStatisticsLock_);
        return FlavoredNodeStatistics_[flavor];
    }

    int GetOnlineNodeCount() override
    {
        return AggregatedOnlineNodeCount_;
    }

    const std::vector<TNode*>& GetNodesForRole(ENodeRole nodeRole) override
    {
        return NodeListPerRole_[nodeRole].Nodes();
    }

    const std::vector<TString>& GetNodeAddressesForRole(ENodeRole nodeRole) override
    {
        return NodeListPerRole_[nodeRole].Addresses();
    }

    void OnNodeHeartbeat(TNode* node, ENodeHeartbeatType heartbeatType) override
    {
        if (node->ReportedHeartbeats().emplace(heartbeatType).second) {
            YT_LOG_INFO("Node reported heartbeat for the first time "
                "(NodeId: %v, Address: %v, HeartbeatType: %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                heartbeatType);

            CheckNodeOnline(node);
        }
    }

    void RequestCellarHeartbeat(TNodeId nodeId) override
    {
        auto* node = FindNode(nodeId);
        if (!node) {
            return;
        }

        const auto& descriptor = node->GetDescriptor();
        YT_LOG_DEBUG("Requesting out of order heartbeat from node (NodeId: %v, DefaultNodeAddress: %v)",
            nodeId,
            descriptor.GetDefaultAddress());

        auto nodeChannel = Bootstrap_->GetNodeChannelFactory()->CreateChannel(descriptor);

        NCellarClient::TTabletCellServiceProxy proxy(nodeChannel);
        auto req = proxy.RequestHeartbeat();
        req->SetTimeout(GetDynamicConfig()->ForceNodeHeartbeatRequestTimeout);
        Y_UNUSED(req->Invoke());
    }

    void SetNodeLocalState(TNode* node, ENodeState state) override
    {
        node->SetLocalState(state);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsSecondaryMaster()) {
            SendNodeState(node);
        }
    }

private:
    const INodeDisposalManagerPtr NodeDisposalManager_;

    TPeriodicExecutorPtr ProfilingExecutor_;

    TBufferedProducerPtr BufferedProducer_;

    TIdGenerator NodeIdGenerator_;
    NHydra::TEntityMap<TNode> NodeMap_;
    NHydra::TEntityMap<THost> HostMap_;
    NHydra::TEntityMap<TRack> RackMap_;
    NHydra::TEntityMap<TDataCenter> DataCenterMap_;

    int AggregatedOnlineNodeCount_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, NodeStatisticsLock_);
    TCpuInstant NodeStatisticsUpdateDeadline_ = 0;
    TAggregatedNodeStatistics AggregatedNodeStatistics_;
    TEnumIndexedArray<ENodeFlavor, TAggregatedNodeStatistics> FlavoredNodeStatistics_;

    // Cf. YT-7009.
    // Maintain a dedicated counter of alive racks since RackMap_ may contain zombies.
    // This is exactly the number of 1-bits in UsedRackIndexes_.
    int RackCount_ = 0;
    TRackSet UsedRackIndexes_;

    THashMap<TString, TNode*> AddressToNodeMap_;
    THashMultiMap<TString, TNode*> HostNameToNodeMap_;
    THashMap<TTransaction*, TNode*> TransactionToNodeMap_;
    THashMap<TString, THost*> NameToHostMap_;
    THashMap<TString, TRack*> NameToRackMap_;
    THashMap<TString, TDataCenter*> NameToDataCenterMap_;

    TPeriodicExecutorPtr FullNodeStatesGossipExecutor_;
    TPeriodicExecutorPtr NodeStatisticsGossipExecutor_;
    TPeriodicExecutorPtr ResetNodePendingRestartMaintenanceExecutor_;

    using TMaintenanceNodeIdSet = std::set<std::pair<TInstant, TNodeId>, TTransparentPairCompare<TInstant, TNodeId>>;
    TMaintenanceNodeIdSet PendingRestartMaintenanceNodeIds_;
    std::map<TNodeId, TMaintenanceNodeIdSet::iterator> PendingRestartMaintenanceNodeIdToSetIt_;

    const TAsyncSemaphorePtr HeartbeatSemaphore_ = New<TAsyncSemaphore>(0);

    TEnumIndexedArray<ENodeRole, TNodeListForRole> NodeListPerRole_;

    TEnumIndexedArray<ENodeFlavor, THashSet<TNode*>> NodesWithFlavor_;

    struct TNodeGroup
    {
        TString Id;
        TNodeGroupConfigPtr Config;
        int LocalRegisteredNodeCount = 0;
        int PendingRegisterNodeMutationCount = 0;
    };

    std::vector<TNodeGroup> NodeGroups_;
    TNodeGroup* DefaultNodeGroup_ = nullptr;
    THashSet<TString> PendingRegisterNodeAddresses_;
    TNodeDiscoveryManagerPtr MasterCacheManager_;
    TNodeDiscoveryManagerPtr TimestampProviderManager_;

    using TNodeGroupList = TCompactVector<TNodeGroup*, 4>;

    void OnAggregatedNodeStateChanged(TNode* node)
    {
        LogNodeState(Bootstrap_, node);
    }

    TNodeId GenerateNodeId()
    {
        TNodeId id;
        while (true) {
            id = TNodeId(NodeIdGenerator_.Next());
            // Beware of sentinels!
            if (id == InvalidNodeId) {
                // Just wait for the next attempt.
            } else if (id > MaxNodeId) {
                NodeIdGenerator_.Reset();
            } else {
                break;
            }
        }
        return id;
    }


    static TYPath GetNodePath(const TString& address)
    {
        return GetClusterNodesPath() + "/" + ToYPathLiteral(address);
    }

    static TYPath GetNodePath(TNode* node)
    {
        return GetNodePath(node->GetDefaultAddress());
    }

    void FillResponseNodeTags(
        ::google::protobuf::RepeatedPtrField<TProtoStringType>* rspTags,
        const THashSet<TString>& tags)
    {
        TCompactVector<TString, 16> sortedTags(tags.begin(), tags.end());
        std::sort(sortedTags.begin(), sortedTags.end());
        rspTags->Reserve(sortedTags.size());
        for (auto& tag : sortedTags) {
            rspTags->Add(std::move(tag));
        }
    }

    void HydraRegisterNode(
        const TCtxRegisterNodePtr& context,
        TReqRegisterNode* request,
        TRspRegisterNode* response)
    {
        auto nodeAddresses = FromProto<TNodeAddressMap>(request->node_addresses());
        const auto& addresses = GetAddressesOrThrow(nodeAddresses, EAddressType::InternalRpc);
        const auto& address = GetDefaultAddress(addresses);
        auto leaseTransactionId = FromProto<TTransactionId>(request->lease_transaction_id());
        auto tags = FromProto<std::vector<TString>>(request->tags());
        auto flavors = FromProto<THashSet<ENodeFlavor>>(request->flavors());
        auto execNodeIsNotDataNode = request->exec_node_is_not_data_node();

        TString hostName;
        // COMPAT(gritukan)
        if (request->has_host_name()) {
            hostName = request->host_name();
        } else {
            hostName = address;
        }

        // COMPAT(gritukan)
        if (flavors.empty()) {
            flavors = {
                ENodeFlavor::Data,
                ENodeFlavor::Exec,
                ENodeFlavor::Tablet,
            };
        }

        if (flavors.contains(ENodeFlavor::Data) || flavors.contains(ENodeFlavor::Exec)) {
            const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
            dataNodeTracker->ValidateRegisterNode(address, request);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        // Check lease transaction.
        TTransaction* leaseTransaction = nullptr;
        if (leaseTransactionId) {
            YT_VERIFY(multicellManager->IsPrimaryMaster());

            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            leaseTransaction = transactionManager->GetTransactionOrThrow(leaseTransactionId);

            if (leaseTransaction->GetPersistentState() != ETransactionState::Active) {
                leaseTransaction->ThrowInvalidState();
            }
        }

        TRack* oldNodeRack = nullptr;

        // Kick-out any previous incarnation.
        auto* node = FindNodeByAddress(address);
        auto isNodeNew = !IsObjectAlive(node);
        if (!isNodeNew) {
            node->ValidateNotBanned();

            if (multicellManager->IsPrimaryMaster()) {
                auto localState = node->GetLocalState();
                if (localState == ENodeState::Registered || localState == ENodeState::Online) {
                    YT_LOG_INFO("Kicking node out due to address conflict (NodeId: %v, Address: %v, State: %v)",
                        node->GetId(),
                        address,
                        localState);
                    UnregisterNode(node, true);
                }
                auto aggregatedState = node->GetAggregatedState();
                if (aggregatedState != ENodeState::Offline) {
                    THROW_ERROR_EXCEPTION("Node %Qv is still in %Qlv state; must wait for it to become fully offline",
                        node->GetDefaultAddress(),
                        aggregatedState);
                }

                if (node->GetRegistrationPending()) {
                    THROW_ERROR_EXCEPTION("Node %Qv is already being registered; must wait for it to become fully offline",
                        node->GetDefaultAddress());
                }
            } else {
                EnsureNodeDisposed(node);
            }

            oldNodeRack = node->GetRack();
        }

        auto* host = FindHostByName(hostName);
        if (!IsObjectAlive(host)) {
            YT_VERIFY(multicellManager->IsPrimaryMaster());

            auto req = TMasterYPathProxy::CreateObject();
            req->set_type(static_cast<int>(EObjectType::Host));

            auto attributes = CreateEphemeralAttributes();
            attributes->Set("name", hostName);
            ToProto(req->mutable_object_attributes(), *attributes);

            const auto& rootService = Bootstrap_->GetObjectManager()->GetRootService();
            try {
                SyncExecuteVerb(rootService, req);
            } catch (const std::exception& ex) {
                YT_LOG_ALERT(ex, "Failed to create host for a node");

                const auto& objectManager = Bootstrap_->GetObjectManager();
                objectManager->UnrefObject(node);
                throw;
            }

            host = GetHostByName(hostName);

            if (oldNodeRack) {
                SetHostRack(host, oldNodeRack);
            }
        }

        if (isNodeNew) {
            auto nodeId = request->has_node_id() ? FromProto<TNodeId>(request->node_id()) : GenerateNodeId();
            node = CreateNode(nodeId, nodeAddresses);
        } else {
            // NB: Default address should not change.
            auto oldDefaultAddress = node->GetDefaultAddress();
            node->SetNodeAddresses(nodeAddresses);
            YT_VERIFY(node->GetDefaultAddress() == oldDefaultAddress);
        }

        node->SetHost(host);
        node->SetNodeTags(tags);
        SetNodeFlavors(node, flavors);

        if (request->has_cypress_annotations()) {
            node->SetAnnotations(TYsonString(request->cypress_annotations(), EYsonType::Node));
        }

        if (request->has_build_version()) {
            node->SetVersion(request->build_version());
        }

        node->SetExecNodeIsNotDataNode(execNodeIsNotDataNode);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto tableMountConfigKeys = FromProto<std::vector<TString>>(request->table_mount_config_keys());
        tabletManager->UpdateExtraMountConfigKeys(std::move(tableMountConfigKeys));

        UpdateLastSeenTime(node);
        UpdateRegisterTime(node);

        SetNodeLocalState(node, ENodeState::Registered);
        node->ReportedHeartbeats().clear();

        UpdateNodeCounters(node, +1);

        if (leaseTransaction) {
            node->SetLeaseTransaction(leaseTransaction);
            RegisterLeaseTransaction(node);
        }

        // COMPAT(kvk1920)
        if (GetDynamicConfig()->EnableRealChunkLocations) {
            if (!request->chunk_locations_supported() &&
                !request->suppress_unsupported_chunk_locations_alert())
            {
                YT_LOG_ALERT(
                    "Real chunk locations are enabled but node does not support them "
                    "(NodeId: %v, NodeAddress: %v)",
                    node->GetId(),
                    address);
            }
            node->UseImaginaryChunkLocations() = !request->chunk_locations_supported();
        } else {
            node->UseImaginaryChunkLocations() = true;
        }

        if (node->IsDataNode() || (node->IsExecNode() && !execNodeIsNotDataNode)) {
            const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
            dataNodeTracker->ProcessRegisterNode(node, request, response);
        }

        if (node->ClearMaintenanceFlag(EMaintenanceType::PendingRestart)) {
            OnNodePendingRestartUpdated(node);

            YT_LOG_INFO("Removed pending restart flag (NodeId: %v, Address: %v)",
                node->GetId(),
                address);
        }

        NodeRegistered_.Fire(node);

        YT_LOG_INFO(
            "Node registered "
            "(NodeId: %v, Address: %v, Tags: %v, Flavors: %v, "
            "LeaseTransactionId: %v, UseImaginaryChunkLocations: %v)",
            node->GetId(),
            address,
            tags,
            flavors,
            leaseTransactionId,
            node->UseImaginaryChunkLocations());

        // NB: Exec nodes should not report heartbeats to secondary masters,
        // so node can already be online for this cell.
        CheckNodeOnline(node);

        if (multicellManager->IsPrimaryMaster()) {
            node->SetRegistrationPending(multicellManager->GetCellTag());
            PostRegisterNodeMutation(node, request);
        }

        response->set_node_id(ToProto<ui32>(node->GetId()));

        FillResponseNodeTags(response->mutable_tags(), node->Tags());

        if (context) {
            context->SetResponseInfo("NodeId: %v",
                node->GetId());
        }
    }

    void HydraUnregisterNode(TReqUnregisterNode* request)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());

        auto* node = FindNode(nodeId);
        if (!IsObjectAlive(node)) {
            return;
        }

        auto state = node->GetLocalState();
        if (state != ENodeState::Registered && state != ENodeState::Online) {
            return;
        }

        UnregisterNode(node, true);
    }

    void HydraClusterNodeHeartbeat(
        const TCtxHeartbeatPtr& /*context*/,
        TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto* node = GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        YT_PROFILE_TIMING("/node_tracker/cluster_node_heartbeat_time") {
            YT_LOG_DEBUG("Processing cluster node heartbeat (NodeId: %v, Address: %v, State: %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState());

            UpdateLastSeenTime(node);

            DoProcessHeartbeat(node, request, response);
        }
    }

    bool ValidateGossipCell(TCellTag cellTag)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster());

        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            YT_LOG_ERROR("Received node gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return false;
        }

        return true;
    }

    // COMPAT(aleksandra-zh)
    void HydraSetCellNodeDescriptors(TReqSetCellNodeDescriptors* request)
    {
        auto cellTag = FromProto<TCellTag>(request->cell_tag());
        if (!ValidateGossipCell(cellTag)) {
            return;
        }

        YT_LOG_INFO("Received cell node descriptor gossip message (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
            auto nodeId = FromProto<TNodeId>(entry.node_id());
            auto* node = FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                continue;
            }

            const auto& descriptor = entry.node_descriptor();
            auto statistics = FromProto<TCellNodeStatistics>(descriptor.statistics());
            node->SetStatistics(cellTag, statistics);

            UpdateNodeCounters(node, -1);
            auto state = ENodeState(descriptor.state());
            node->SetState(cellTag, state, /*redundant*/ false);
            UpdateNodeCounters(node, +1);
        }
    }

    void HydraSetNodeStatistics(TReqSetNodeStatistics* request)
    {
        auto cellTag = FromProto<TCellTag>(request->cell_tag());
        if (!ValidateGossipCell(cellTag)) {
            return;
        }

        YT_LOG_INFO("Received node statistics (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
            auto nodeId = FromProto<TNodeId>(entry.node_id());
            auto* node = FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                continue;
            }

            auto statistics = FromProto<TCellNodeStatistics>(entry.statistics());
            node->SetStatistics(cellTag, statistics);
        }
    }

    // COMPAT(aleksandra-zh)
    void HydraSetNodeStates(TReqSetNodeStates* request)
    {
        auto cellTag = FromProto<TCellTag>(request->cell_tag());
        if (!ValidateGossipCell(cellTag)) {
            return;
        }

        YT_LOG_INFO("Received node states (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
            auto nodeId = FromProto<TNodeId>(entry.node_id());
            auto* node = FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                continue;
            }

            auto state = ENodeState(entry.state());

            UpdateNodeCounters(node, -1);
            node->SetState(cellTag, state, /*redundant*/ true);
            UpdateNodeCounters(node, +1);
        }
    }

    void HydraSetNodeState(TReqSetNodeState* request)
    {
        auto cellTag = FromProto<TCellTag>(request->cell_tag());
        if (!ValidateGossipCell(cellTag)) {
            return;
        }

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto state = ENodeState(request->state());

        YT_LOG_DEBUG("Received node state (NodeId: %v, State: %v, CellTag: %v)",
            nodeId,
            state,
            cellTag);

        auto* node = FindNode(nodeId);
        if (!IsObjectAlive(node)) {
            return;
        }

        UpdateNodeCounters(node, -1);
        node->SetState(cellTag, state, /*redundant*/ false);
        UpdateNodeCounters(node, +1);
    }

    void HydraUpdateNodeResources(NProto::TReqUpdateNodeResources* request)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto* node = FindNode(nodeId);
        if (!node) {
            YT_LOG_ERROR(
                "Error updating cluster node resource usage and limits: node not found (NodeId: %v)",
                nodeId);
            return;
        }

        node->SetResourceUsage(request->resource_usage());
        node->SetResourceLimits(request->resource_limits());
    }

    void HydraUpdateNodesForRole(NProto::TReqUpdateNodesForRole* request)
    {
        auto nodeRole = FromProto<ENodeRole>(request->node_role());
        auto& nodeList = NodeListPerRole_[nodeRole].Nodes();
        nodeList.clear();

        for (auto protoNodeId: request->node_ids()) {
            auto nodeId = FromProto<TNodeId>(protoNodeId);
            auto* node = FindNode(nodeId);
            if (IsObjectAlive(node)) {
                nodeList.push_back(node);
            } else {
                YT_LOG_DEBUG("New node for role is dead, ignoring (NodeRole: %v, NodeId: %v)",
                    nodeRole,
                    node->GetId());
            }
        }

        NodeListPerRole_[nodeRole].UpdateAddresses();

        YT_LOG_DEBUG("Updated nodes for role (NodeRole: %v, Nodes: %v)",
            nodeRole,
            MakeFormattableView(nodeList, TNodePtrAddressFormatter()));
    }

    void HydraAddMaintenance(
        const TCtxAddMaintenancePtr& /*context*/,
        TReqAddClusterNodeMaintenance* request,
        NNodeTrackerClient::NProto::TRspAddMaintenance* response)
    {
        auto nodeAddress = request->node_address();
        auto* node = GetNodeByAddressOrThrow(nodeAddress);
        if (!IsObjectAlive(node)) {
            THROW_ERROR_EXCEPTION("No such node")
                << TErrorAttribute("address", request->node_address());
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        if (multicellManager->IsPrimaryMaster()) {
            YT_VERIFY(!request->has_id());

            // NB: Code duplication is OK here because this mutation will be removed in near future.
            // See TMaintenanceTracker::GenerateMaintenanceId().
            const auto& generator = NHydra::GetCurrentMutationContext()->RandomGenerator();
            TMaintenanceId id;
            do {
                id = TMaintenanceId(generator->Generate<ui64>(), generator->Generate<ui64>());
            } while (node->MaintenanceRequests().contains(id) || IsBuiltinMaintenanceId(id));

            ToProto(request->mutable_id(), id);
        } else {
            YT_VERIFY(request->has_id());
        }

        auto id = FromProto<TMaintenanceId>(request->id());
        auto type = CheckedEnumCast<EMaintenanceType>(request->type());
        if (node->AddMaintenance(
            id,
            {request->user_name(), type, request->comment(), GetCurrentHydraContext()->GetTimestamp()}))
        {
            OnNodeMaintenanceUpdated(node, type);
        }

        ToProto(response->mutable_id(), FromProto<TMaintenanceId>(request->id()));

        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(*request);
        }
    }

    void HydraRemoveMaintenance(
        const TCtxRemoveMaintenancePtr& /*context*/,
        TReqRemoveClusterNodeMaintenance* request,
        NNodeTrackerClient::NProto::TRspRemoveMaintenance* /*response*/)
    {
        auto nodeAddress = request->node_address();
        auto* node = GetNodeByAddressOrThrow(nodeAddress);
        if (!IsObjectAlive(node)) {
            THROW_ERROR_EXCEPTION("Node does not exist")
                << TErrorAttribute("node_address", request->node_address());
        }

        auto id = FromProto<TMaintenanceId>(request->id());
        THROW_ERROR_EXCEPTION_IF(!node->MaintenanceRequests().contains(id), "Maintenance %Qv does not exist", id);

        if (auto type = node->RemoveMaintenance(id)) {
            OnNodeMaintenanceUpdated(node, *type);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(*request);
        }
    }

    void HydraResetNodePendingRestartMaintenance(TReqResetNodePendingRestartMaintenance* request)
    {
        for (auto protoNodeId : request->node_ids()) {
            auto nodeId = FromProto<TNodeId>(protoNodeId);
            auto* node = FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                continue;
            }

            if (node->ClearMaintenanceFlag(EMaintenanceType::PendingRestart)) {
                OnNodePendingRestartUpdated(node);
            }
        }
    }

    void DoProcessHeartbeat(
        TNode* node,
        TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster());

        auto& statistics = *request->mutable_statistics();
        if (!GetDynamicConfig()->EnableNodeCpuStatistics) {
            statistics.clear_cpu();
        }
        node->SetClusterNodeStatistics(std::move(statistics));

        node->Alerts() = FromProto<std::vector<TError>>(request->alerts());

        OnNodeHeartbeat(node, ENodeHeartbeatType::Cluster);

        if (auto* rack = node->GetRack()) {
            response->set_rack(rack->GetName());
            if (auto* dc = rack->GetDataCenter()) {
                response->set_data_center(dc->GetName());
            }
        }

        // COMPAT(gritukan)
        if (GetDynamicConfig()->UseResourceStatisticsFromClusterNodeHeartbeat && request->has_resource_usage()) {
            node->SetResourceUsage(request->resource_usage());
            node->SetResourceLimits(request->resource_limits());
        }

        FillResponseNodeTags(response->mutable_tags(), node->Tags());

        *response->mutable_resource_limits_overrides() = node->ResourceLimitsOverrides();
        response->set_decommissioned(node->IsDecommissioned());

        node->SetDisableWriteSessionsSentToNode(node->AreWriteSessionsDisabled());
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveKeys(context);
        RackMap_.SaveKeys(context);
        DataCenterMap_.SaveKeys(context);
        HostMap_.SaveKeys(context);

        // COMPAT(kvk1920): Remove after real chunk locations are enabled everywhere.
        // We need to know if node uses imaginary chunk locations before loading TChunkLocationPtrWithSomething
        // but the order of different LoadValues() is unspecified. So we just load this information
        // during keys loading.
        THashMap<TObjectId, bool> useImaginaryLocationsMap;
        useImaginaryLocationsMap.reserve(NodeMap_.size());
        for (auto [nodeId, node] : NodeMap_) {
            useImaginaryLocationsMap[nodeId] = node->UseImaginaryChunkLocations();
        }
        Save(context, useImaginaryLocationsMap);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        Save(context, NodeIdGenerator_);
        Save(context, NodeListPerRole_);
        NodeMap_.SaveValues(context);
        RackMap_.SaveValues(context);
        DataCenterMap_.SaveValues(context);
        HostMap_.SaveValues(context);
        Save(context, PendingRestartMaintenanceNodeIds_);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        NodeMap_.LoadKeys(context);
        RackMap_.LoadKeys(context);
        DataCenterMap_.LoadKeys(context);
        HostMap_.LoadKeys(context);

        auto useImaginaryLocationsMap = Load<THashMap<TObjectId, bool>>(context);
        for (auto [nodeId, useImaginaryLocations] : useImaginaryLocationsMap) {
            auto* node = NodeMap_.Get(nodeId);
            node->UseImaginaryChunkLocations() = useImaginaryLocations;
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        Load(context, NodeIdGenerator_);
        Load(context, NodeListPerRole_);
        NodeMap_.LoadValues(context);
        RackMap_.LoadValues(context);
        DataCenterMap_.LoadValues(context);
        HostMap_.LoadValues(context);

        if (context.GetVersion() >= EMasterReign::AutoTurnOffPendingRestartMaintenanceFlag) {
            Load(context, PendingRestartMaintenanceNodeIds_);
        }
    }

    void Clear() override
    {
        TMasterAutomatonPart::Clear();

        NodeIdGenerator_.Reset();
        NodeMap_.Clear();
        HostMap_.Clear();
        RackMap_.Clear();
        DataCenterMap_.Clear();

        AddressToNodeMap_.clear();
        HostNameToNodeMap_.clear();
        TransactionToNodeMap_.clear();

        NameToHostMap_.clear();

        NameToRackMap_.clear();
        NameToDataCenterMap_.clear();
        UsedRackIndexes_.reset();
        RackCount_ = 0;

        AggregatedOnlineNodeCount_ = 0;

        NodeGroups_.clear();
        DefaultNodeGroup_ = nullptr;
        for (auto& nodeList : NodeListPerRole_) {
            nodeList.Clear();
        }
        for (auto& nodeSet : NodesWithFlavor_) {
            nodeSet.clear();
        }
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        AddressToNodeMap_.clear();
        HostNameToNodeMap_.clear();
        TransactionToNodeMap_.clear();

        AggregatedOnlineNodeCount_ = 0;

        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }

            node->RebuildTags();
            SubscribeToAggregatedNodeStateChanged(node);
            InitializeNodeStates(node);
            InitializeNodeIOWeights(node);
            InsertToAddressMaps(node);
            InsertToFlavorSets(node);
            UpdateNodeCounters(node, +1);

            if (node->GetLeaseTransaction()) {
                RegisterLeaseTransaction(node);
            }
        }

        for (auto [hostId, host] : HostMap_) {
            if (!IsObjectAlive(host)) {
                continue;
            }

            YT_VERIFY(NameToHostMap_.emplace(host->GetName(), host).second);
        }

        UsedRackIndexes_.reset();
        RackCount_ = 0;
        for (auto [rackId, rack] : RackMap_) {
            if (!IsObjectAlive(rack)) {
                continue;
            }

            YT_VERIFY(NameToRackMap_.emplace(rack->GetName(), rack).second);

            auto rackIndex = rack->GetIndex();
            YT_VERIFY(!UsedRackIndexes_.test(rackIndex));
            UsedRackIndexes_.set(rackIndex);
            ++RackCount_;
        }

        for (auto [dcId, dc] : DataCenterMap_) {
            if (!IsObjectAlive(dc)) {
                continue;
            }

            YT_VERIFY(NameToDataCenterMap_.emplace(dc->GetName(), dc).second);
        }

        for (auto nodeRole : TEnumTraits<ENodeRole>::GetDomainValues()) {
            NodeListPerRole_[nodeRole].UpdateAddresses();
        }

        for (auto it = PendingRestartMaintenanceNodeIds_.begin();
            it != PendingRestartMaintenanceNodeIds_.end();
            ++it)
        {
            PendingRestartMaintenanceNodeIdToSetIt_.emplace(it->second, it);
        }
    }

    void OnRecoveryStarted() override
    {
        TMasterAutomatonPart::OnRecoveryStarted();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto [nodeId, node] : NodeMap_) {
            node->Reset(nodeTracker);
        }

        BufferedProducer_->SetEnabled(false);
    }

    void OnRecoveryComplete() override
    {
        TMasterAutomatonPart::OnRecoveryComplete();

        BufferedProducer_->SetEnabled(true);
    }

    void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        // NB: Node states gossip is one way: secondary-to-primary.
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsSecondaryMaster()) {
            NodeStatisticsGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::NodeTrackerGossip),
                BIND(&TNodeTracker::OnNodeStatisticsGossip, MakeWeak(this)));
            NodeStatisticsGossipExecutor_->Start();

            // COMPAT(aleksandra-zh).
            FullNodeStatesGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::NodeTrackerGossip),
                BIND(&TNodeTracker::OnNodeStatesGossip, MakeWeak(this)));
            FullNodeStatesGossipExecutor_->Start();
        }

        for (auto& group : NodeGroups_) {
            group.PendingRegisterNodeMutationCount = 0;
        }

        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            if (node->GetLocalState() == ENodeState::Unregistered) {
                NodeDisposalManager_->DisposeNodeWithSemaphore(node);
            }
        }
    }

    void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        if (FullNodeStatesGossipExecutor_) {
            YT_UNUSED_FUTURE(FullNodeStatesGossipExecutor_->Stop());
            FullNodeStatesGossipExecutor_.Reset();
        }

        if (NodeStatisticsGossipExecutor_) {
            YT_UNUSED_FUTURE(NodeStatisticsGossipExecutor_->Stop());
            NodeStatisticsGossipExecutor_.Reset();
        }

        PendingRegisterNodeAddresses_.clear();
    }


    THashSet<ENodeHeartbeatType> GetExpectedHeartbeats(TNode* node, bool primaryMaster)
    {
        THashSet<ENodeHeartbeatType> result;
        if (primaryMaster) {
            result.insert(ENodeHeartbeatType::Cluster);
        }

        for (auto flavor : node->Flavors()) {
            switch (flavor) {
                case ENodeFlavor::Data:
                    result.insert(ENodeHeartbeatType::Data);
                    break;

                case ENodeFlavor::Exec:
                    if (!node->GetExecNodeIsNotDataNode()) {
                        result.insert(ENodeHeartbeatType::Data);
                    }
                    if (primaryMaster) {
                        result.insert(ENodeHeartbeatType::Exec);
                    }
                    break;

                case ENodeFlavor::Tablet:
                    result.insert(ENodeHeartbeatType::Tablet);
                    result.insert(ENodeHeartbeatType::Cellar);
                    break;

                case ENodeFlavor::Chaos:
                    result.insert(ENodeHeartbeatType::Cellar);
                    break;

                default:
                    YT_ABORT();
            }
        }
        return result;
    }

    void CheckNodeOnline(TNode* node)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto expectedHeartbeats = GetExpectedHeartbeats(node, multicellManager->IsPrimaryMaster());
        if (node->GetLocalState() == ENodeState::Registered && node->ReportedHeartbeats() == expectedHeartbeats) {
            UpdateNodeCounters(node, -1);
            SetNodeLocalState(node, ENodeState::Online);
            UpdateNodeCounters(node, +1);

            NodeOnline_.Fire(node);

            YT_LOG_INFO("Node online (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }
    }

    void InitializeNodeStates(TNode* node)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        node->InitializeStates(multicellManager->GetCellTag(), multicellManager->GetSecondaryCellTags());
    }

    void InitializeNodeIOWeights(TNode* node)
    {
        node->RecomputeIOWeights(Bootstrap_->GetChunkManager());
    }

    void UpdateNodeCounters(TNode* node, int delta)
    {
        if (node->GetLocalState() == ENodeState::Registered) {
            auto groups = GetGroupsForNode(node);
            for (auto* group : groups) {
                group->LocalRegisteredNodeCount += delta;
            }
        }

        if (node->GetAggregatedState() == ENodeState::Online) {
            AggregatedOnlineNodeCount_ += delta;
        }
    }

    void RegisterLeaseTransaction(TNode* node)
    {
        auto* transaction = node->GetLeaseTransaction();
        YT_VERIFY(transaction);
        YT_VERIFY(transaction->GetPersistentState() == ETransactionState::Active);
        YT_VERIFY(TransactionToNodeMap_.emplace(transaction, node).second);
    }

    TTransaction* UnregisterLeaseTransaction(TNode* node)
    {
        auto* transaction = node->GetLeaseTransaction();
        if (transaction) {
            YT_VERIFY(TransactionToNodeMap_.erase(transaction) == 1);
        }
        node->SetLeaseTransaction(nullptr);
        return transaction;
    }

    void UpdateRegisterTime(TNode* node)
    {
        const auto* mutationContext = GetCurrentMutationContext();
        node->SetRegisterTime(mutationContext->GetTimestamp());
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        auto it = TransactionToNodeMap_.find(transaction);
        if (it == TransactionToNodeMap_.end()) {
            return;
        }

        auto* node = it->second;
        YT_LOG_INFO("Node lease transaction finished (NodeId: %v, Address: %v, TransactionId: %v)",
            node->GetId(),
            node->GetDefaultAddress(),
            transaction->GetId());

        UnregisterNode(node, true);
    }


    TNode* CreateNode(TNodeId nodeId, const TNodeAddressMap& nodeAddresses)
    {
        auto objectId = ObjectIdFromNodeId(nodeId);

        auto nodeHolder = TPoolAllocator::New<TNode>(objectId);
        auto* node = NodeMap_.Insert(objectId, std::move(nodeHolder));

        // Make the fake reference.
        YT_VERIFY(node->RefObject() == 1);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (node->GetNativeCellTag() != multicellManager->GetCellTag()) {
            node->SetForeign();
        }

        SubscribeToAggregatedNodeStateChanged(node);

        InitializeNodeStates(node);

        node->SetNodeAddresses(nodeAddresses);
        InsertToAddressMaps(node);

        return node;
    }

    void UnregisterNode(TNode* node, bool propagate)
    {
        YT_PROFILE_TIMING("/node_tracker/node_unregister_time") {
            auto* transaction = UnregisterLeaseTransaction(node);
            if (IsObjectAlive(transaction)) {
                const auto& transactionManager = Bootstrap_->GetTransactionManager();
                // NB: This will trigger OnTransactionFinished, however we've already evicted the
                // lease so the latter call is no-op.
                NTransactionSupervisor::TTransactionAbortOptions options{
                    .Force = true
                };
                transactionManager->AbortMasterTransaction(transaction, options);
            }

            UpdateNodeCounters(node, -1);
            SetNodeLocalState(node, ENodeState::Unregistered);
            node->ReportedHeartbeats().clear();

            NodeUnregistered_.Fire(node);

            if (propagate) {
                if (IsLeader()) {
                    NodeDisposalManager_->DisposeNodeWithSemaphore(node);
                }

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                if (multicellManager->IsPrimaryMaster()) {
                    PostUnregisterNodeMutation(node);
                }
            }

            YT_LOG_INFO("Node unregistered (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }
    }

    void EnsureNodeDisposed(TNode* node)
    {
        if (node->GetLocalState() != ENodeState::Offline) {
            YT_LOG_ALERT("Node is not offline when it should be (NodeId: %v)");
        }

        // Everything below is COMPAT(aleksandra-zh).
        if (node->GetLocalState() == ENodeState::Registered ||
            node->GetLocalState() == ENodeState::Online)
        {
            UnregisterNode(node, false);
        }

        if (node->GetLocalState() == ENodeState::Unregistered ||
            node->GetLocalState() == ENodeState::BeingDisposed)
        {
            // This does not remove Sequoia replicas.
            NodeDisposalManager_->DisposeNodeCompletely(node);
        }
    }

    void OnNodeStatisticsGossip()
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

        TReqSetNodeStatistics request;
        request.set_cell_tag(ToProto<int>(multicellManager->GetCellTag()));
        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }

            auto* entry = request.add_entries();
            entry->set_node_id(ToProto<ui32>(node->GetId()));
            ToProto(entry->mutable_statistics(), node->ComputeCellStatistics());
        }

        if (request.entries_size() == 0) {
            return;
        }

        YT_LOG_INFO("Sending node statistics gossip message (NodeCount: %v)",
            request.entries_size());

        multicellManager->PostToPrimaryMaster(request, /*reliable*/ false);
    }

    void OnNodeStatesGossip()
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

        TReqSendNodeStates request;
        YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->CommitAndLog(Logger));
    }

    void HydraSendNodeStates(NProto::TReqSendNodeStates* /*mutationRequest*/)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        TReqSetNodeStates gossipRequest;
        gossipRequest.set_cell_tag(ToProto<int>(multicellManager->GetCellTag()));

        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }

            auto state = node->GetLocalState();
            if (state != node->GetLastGossipState()) {
                YT_LOG_ALERT("Node state was not reported on change (CurrentNodeState: %v, LastReportedState: %v)",
                    state,
                    node->GetLastGossipState());
            }

            auto* entry = gossipRequest.add_entries();
            entry->set_node_id(ToProto<ui32>(node->GetId()));
            entry->set_state(ToProto<int>(state));
            node->SetLastGossipState(state);
        }

        if (gossipRequest.entries_size() == 0) {
            return;
        }

        std::sort(
            gossipRequest.mutable_entries()->begin(),
            gossipRequest.mutable_entries()->end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.node_id() < rhs.node_id();
            });

        YT_LOG_INFO("Sending node states gossip message (NodeCount: %v)",
            gossipRequest.entries_size());
        multicellManager->PostToPrimaryMaster(gossipRequest);
    }

    void SendNodeState(TNode* node)
    {
        YT_VERIFY(HasMutationContext());
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        TReqSetNodeState request;
        request.set_cell_tag(ToProto<int>(multicellManager->GetCellTag()));

        auto state = node->GetLocalState();
        if (state == node->GetLastGossipState()) {
            return;
        }

        request.set_node_id(ToProto<ui32>(node->GetId()));
        request.set_state(ToProto<int>(state));
        node->SetLastGossipState(state);

        YT_LOG_INFO("Sending node state (NodeId: %v, State: %v)",
            node->GetId(),
            state);

        multicellManager->PostToPrimaryMaster(request);
    }

    void ResetNodesPendingRestartMaintenanceOnTimeout()
    {
        if (!IsLeader()) {
            return;
        }

        NProto::TReqResetNodePendingRestartMaintenance request;

        auto endIt = PendingRestartMaintenanceNodeIds_.upper_bound(
            TInstant::Now() - GetDynamicConfig()->PendingRestartLeaseTimeout);

        for (auto it = PendingRestartMaintenanceNodeIds_.begin(); it != endIt; ++it) {
            request.add_node_ids(ToProto<ui32>(it->second));
        }

        if (request.node_ids_size() != 0) {
            YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
                ->CommitAndLog(Logger));
        }
    }

    void CommitMutationWithSemaphore(
        std::unique_ptr<TMutation> mutation,
        NRpc::IServiceContextPtr context,
        const TAsyncSemaphorePtr& semaphore)
    {
        auto timeBefore = NProfiling::GetInstant();

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        auto expectedMutationCommitDuration = config->CellMaster->ExpectedMutationCommitDuration;

        semaphore->AsyncAcquire().SubscribeUnique(
            BIND([=, mutation = std::move(mutation), context = std::move(context)] (TErrorOr<TAsyncSemaphoreGuard>&& guardOrError) mutable {
                if (!guardOrError.IsOK()) {
                    context->Reply(TError("Failed to acquire semaphore") << guardOrError);
                    return;
                }

                auto requestTimeout = context->GetTimeout();
                auto timeAfter = NProfiling::GetInstant();
                if (requestTimeout && timeAfter + expectedMutationCommitDuration >= timeBefore + *requestTimeout) {
                    context->Reply(TError(NYT::EErrorCode::Timeout, "Semaphore acquisition took too long"));
                } else {
                    Y_UNUSED(WaitFor(mutation->CommitAndReply(context)));
                }

                // Offload mutation destruction to another thread.
                NRpc::TDispatcher::Get()->GetHeavyInvoker()
                    ->Invoke(BIND([mutation = std::move(mutation)] { }));
            }).Via(EpochAutomatonInvoker_));
    }

    void PostRegisterNodeMutation(TNode* node, const TReqRegisterNode* originalRequest)
    {
        TReqRegisterNode request;
        request.set_node_id(ToProto<ui32>(node->GetId()));
        ToProto(request.mutable_node_addresses(), node->GetNodeAddresses());
        for (const auto& tag : node->NodeTags()) {
            request.add_tags(tag);
        }
        request.set_cypress_annotations(node->GetAnnotations().ToString());
        request.set_build_version(node->GetVersion());

        for (auto flavor : node->Flavors()) {
            request.add_flavors(static_cast<int>(flavor));
        }

        for (const auto* location : node->RealChunkLocations()) {
            ToProto(request.add_chunk_location_uuids(), location->GetUuid());
        }

        request.set_host_name(node->GetHost()->GetName());

        request.mutable_table_mount_config_keys()->CopyFrom(originalRequest->table_mount_config_keys());

        request.set_exec_node_is_not_data_node(originalRequest->exec_node_is_not_data_node());

        request.set_chunk_locations_supported(originalRequest->chunk_locations_supported());

        // COMPAT(kvk1920)
        request.set_location_directory_supported(originalRequest->location_directory_supported());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToSecondaryMasters(request);
    }

    void PostUnregisterNodeMutation(TNode* node)
    {
        TReqUnregisterNode request;
        request.set_node_id(ToProto<ui32>(node->GetId()));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToSecondaryMasters(request);
    }

    int AllocateRackIndex()
    {
        for (int index = 0; index < std::ssize(UsedRackIndexes_); ++index) {
            if (index == NullRackIndex) {
                continue;
            }
            if (!UsedRackIndexes_.test(index)) {
                UsedRackIndexes_.set(index);
                ++RackCount_;
                return index;
            }
        }
        YT_ABORT();
    }

    void FreeRackIndex(int index)
    {
        YT_VERIFY(UsedRackIndexes_.test(index));
        UsedRackIndexes_.reset(index);
        --RackCount_;
    }

    void OnValidateSecondaryMasterRegistration(TCellTag cellTag)
    {
        auto nodes = GetValuesSortedByKey(NodeMap_);
        for (const auto* node : nodes) {
            if (node->GetAggregatedState() != ENodeState::Offline) {
                THROW_ERROR_EXCEPTION("Cannot register a new secondary master %v while node %v is not offline",
                    cellTag,
                    node->GetDefaultAddress());
            }
        }
    }

    void OnReplicateKeysToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto replicateKeys = [&] (const auto& objectMap) {
            for (auto* object : GetValuesSortedByKey(objectMap)) {
                objectManager->ReplicateObjectCreationToSecondaryMaster(object, cellTag);
            }
        };

        replicateKeys(HostMap_);
        replicateKeys(RackMap_);
        replicateKeys(DataCenterMap_);
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        auto replicateValues = [&] (const auto& objectMap) {
            for (auto* object : GetValuesSortedByKey(objectMap)) {
                objectManager->ReplicateObjectAttributesToSecondaryMaster(object, cellTag);
            }
        };

        replicateValues(HostMap_);
        replicateValues(RackMap_);
        replicateValues(DataCenterMap_);

        for (const auto* node : GetValuesSortedByKey(NodeMap_)) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            // NB: TReqRegisterNode+TReqUnregisterNode create an offline node at the secondary master.
            {
                TReqRegisterNode request;
                request.set_node_id(ToProto<ui32>(node->GetId()));
                ToProto(request.mutable_node_addresses(), node->GetNodeAddresses());
                request.set_suppress_unsupported_chunk_locations_alert(true);

                // NB: Hosts must be replicated prior to node replication.
                request.set_host_name(node->GetHost()->GetName());

                multicellManager->PostToMaster(request, cellTag);
            }
            {
                TReqUnregisterNode request;
                request.set_node_id(ToProto<ui32>(node->GetId()));
                multicellManager->PostToMaster(request, cellTag);
            }
            for (const auto& [id, request] : node->MaintenanceRequests()) {
                using NMaintenanceTrackerServer::NProto::TReqReplicateMaintenanceRequestCreation;
                TReqReplicateMaintenanceRequestCreation addMaintenance;
                addMaintenance.set_component(ToProto<i32>(EMaintenanceComponent::ClusterNode));
                addMaintenance.set_comment(request.Comment);
                addMaintenance.set_user(request.User);
                addMaintenance.set_type(ToProto<i32>(request.Type));
                addMaintenance.set_address(node->GetDefaultAddress());
                ToProto(addMaintenance.mutable_id(), id);
                multicellManager->PostToMaster(addMaintenance, cellTag);
            }
        }

        replicateValues(NodeMap_);
    }

    void InsertToAddressMaps(TNode* node)
    {
        YT_VERIFY(AddressToNodeMap_.emplace(node->GetDefaultAddress(), node).second);
        for (const auto& [_, address] : node->GetAddressesOrThrow(EAddressType::InternalRpc)) {
            HostNameToNodeMap_.emplace(TString(GetServiceHostName(address)), node);
        }
    }

    void RemoveFromAddressMaps(TNode* node)
    {
        YT_VERIFY(AddressToNodeMap_.erase(node->GetDefaultAddress()) == 1);
        for (const auto& [_, address] : node->GetAddressesOrThrow(EAddressType::InternalRpc)) {
            auto hostNameRange = HostNameToNodeMap_.equal_range(TString(GetServiceHostName(address)));
            for (auto it = hostNameRange.first; it != hostNameRange.second; ++it) {
                if (it->second == node) {
                    HostNameToNodeMap_.erase(it);
                    break;
                }
            }
        }
    }

    void RemoveFromNodeLists(TNode* node)
    {
        for (auto nodeRole : TEnumTraits<ENodeRole>::GetDomainValues()) {
            auto& nodes = NodeListPerRole_[nodeRole].Nodes();
            auto nodeIt = std::find(nodes.begin(), nodes.end(), node);
            if (nodeIt != nodes.end()) {
                nodes.erase(nodeIt);
                NodeListPerRole_[nodeRole].UpdateAddresses();
            }
        }
    }

    void SetNodeFlavors(TNode* node, const THashSet<ENodeFlavor>& newFlavors)
    {
        YT_VERIFY(HasHydraContext());

        RemoveFromFlavorSets(node);
        node->SetFlavors(newFlavors);
        InsertToFlavorSets(node);
    }

    void RemoveFromFlavorSets(TNode* node)
    {
        YT_VERIFY(HasHydraContext());

        for (auto flavor : node->Flavors()) {
            EraseOrCrash(NodesWithFlavor_[flavor], node);
        }
    }

    void InsertToFlavorSets(TNode* node)
    {
        YT_VERIFY(HasHydraContext());

        for (auto flavor : node->Flavors()) {
            InsertOrCrash(NodesWithFlavor_[flavor], node);
        }
    }

    void OnProfiling()
    {
        if (!IsLeader()) {
            BufferedProducer_->SetEnabled(false);
            return;
        }

        BufferedProducer_->SetEnabled(true);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsPrimaryMaster()) {
            return;
        }

        TSensorBuffer buffer;
        auto statistics = GetAggregatedNodeStatistics();

        auto profileStatistics = [&] (const TAggregatedNodeStatistics& statistics) {
            buffer.AddGauge("/available_space", statistics.TotalSpace.Available);
            buffer.AddGauge("/used_space", statistics.TotalSpace.Used);

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            for (auto [mediumIndex, space] : statistics.SpacePerMedium) {
                const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                if (!IsObjectAlive(medium)) {
                    continue;
                }
                TWithTagGuard tagGuard(&buffer, "medium", medium->GetName());
                buffer.AddGauge("/available_space_per_medium", space.Available);
                buffer.AddGauge("/used_space_per_medium", space.Used);
            }

            buffer.AddGauge("/chunk_replica_count", statistics.ChunkReplicaCount);

            buffer.AddGauge("/online_node_count", statistics.OnlineNodeCount);
            buffer.AddGauge("/offline_node_count", statistics.OfflineNodeCount);
            buffer.AddGauge("/banned_node_count", statistics.BannedNodeCount);
            buffer.AddGauge("/decommissioned_node_count", statistics.DecommissinedNodeCount);
            buffer.AddGauge("/with_alerts_node_count", statistics.WithAlertsNodeCount);
            buffer.AddGauge("/full_node_count", statistics.FullNodeCount);

            for (auto nodeRole : TEnumTraits<ENodeRole>::GetDomainValues()) {
                TWithTagGuard tagGuard(&buffer, "node_role", FormatEnum(nodeRole));
                buffer.AddGauge("/node_count", NodeListPerRole_[nodeRole].Nodes().size());
            }
        };

        {
            TWithTagGuard tagGuard(&buffer, "flavor", "cluster");
            profileStatistics(GetAggregatedNodeStatistics());
        }

        for (auto flavor : TEnumTraits<ENodeFlavor>::GetDomainValues()) {
            if (flavor == ENodeFlavor::Cluster) {
                continue;
            }
            TWithTagGuard tagGuard(&buffer, "flavor", FormatEnum(flavor));
            profileStatistics(GetFlavoredNodeStatistics(flavor));
        }

        NodeDisposalManager_->OnProfiling(&buffer);

        BufferedProducer_->Update(buffer);
    }


    TNodeGroupList GetGroupsForNode(TNode* node)
    {
        TNodeGroupList result;
        for (auto& group : NodeGroups_) {
            if (group.Config->NodeTagFilter.IsSatisfiedBy(node->Tags())) {
                result.push_back(&group);
            }
        }
        return result;
    }

    TNodeGroupList GetGroupsForNode(const TString& address)
    {
        auto* node = FindNodeByAddress(address);
        if (!IsObjectAlive(node)) {
            YT_VERIFY(DefaultNodeGroup_);
            return {DefaultNodeGroup_}; // default is the last one
        }
        return GetGroupsForNode(node);
    }

    void RebuildNodeGroups()
    {
        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            UpdateNodeCounters(node, -1);
        }

        NodeGroups_.clear();

        for (const auto& [id, config] : GetDynamicConfig()->NodeGroups) {
            NodeGroups_.emplace_back();
            auto& group = NodeGroups_.back();
            group.Id = id;
            group.Config = config;
        }

        {
            NodeGroups_.emplace_back();
            DefaultNodeGroup_ = &NodeGroups_.back();
            DefaultNodeGroup_->Id = "default";
            DefaultNodeGroup_->Config = New<TNodeGroupConfig>();
            DefaultNodeGroup_->Config->MaxConcurrentNodeRegistrations = GetDynamicConfig()->MaxConcurrentNodeRegistrations;
        }

        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            UpdateNodeCounters(node, +1);
        }
    }

    void RecomputePendingRegisterNodeMutationCounters()
    {
        for (auto& group : NodeGroups_) {
            group.PendingRegisterNodeMutationCount = 0;
        }

        for (const auto& address : PendingRegisterNodeAddresses_) {
            auto groups = GetGroupsForNode(address);
            for (auto* group : groups) {
                ++group->PendingRegisterNodeMutationCount;
            }
        }
    }

    void ReconfigureGossipPeriods()
    {
        if (FullNodeStatesGossipExecutor_) {
            FullNodeStatesGossipExecutor_->SetPeriod(GetDynamicConfig()->FullNodeStatesGossipPeriod);
        }
        if (NodeStatisticsGossipExecutor_) {
            NodeStatisticsGossipExecutor_->SetPeriod(GetDynamicConfig()->NodeStatisticsGossipPeriod);
        }
    }

    void ReconfigureNodeSemaphores()
    {
        HeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentClusterNodeHeartbeats);
    }

    void MaybeRebuildAggregatedNodeStatistics()
    {
        auto guard = ReaderGuard(NodeStatisticsLock_);

        auto now = GetCpuInstant();
        if (now > NodeStatisticsUpdateDeadline_) {
            guard.Release();
            RebuildAggregatedNodeStatistics();
        }
    }

    void RebuildAggregatedNodeStatistics()
    {
        auto guard = WriterGuard(NodeStatisticsLock_);

        AggregatedNodeStatistics_ = TAggregatedNodeStatistics();
        for (auto flavor : TEnumTraits<ENodeFlavor>::GetDomainValues()) {
            FlavoredNodeStatistics_[flavor] = TAggregatedNodeStatistics();
        }

        auto increment = [] (
            NNodeTrackerClient::TIOStatistics* statistics,
            const NNodeTrackerClient::NProto::TIOStatistics& source)
        {
            statistics->FilesystemReadRate += source.filesystem_read_rate();
            statistics->FilesystemWriteRate += source.filesystem_write_rate();
            statistics->DiskReadRate += source.disk_read_rate();
            statistics->DiskWriteRate += source.disk_write_rate();
            statistics->DiskReadCapacity += source.disk_read_capacity();
            statistics->DiskWriteCapacity += source.disk_write_capacity();
        };

        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }

            // It's forbidden to capture structured binding in lambda, so we copy #node here.
            auto* node_ = node;
            auto updateStatistics = [&] (TAggregatedNodeStatistics* statistics) {
                statistics->BannedNodeCount += node_->IsBanned();
                statistics->DecommissinedNodeCount += node_->IsDecommissioned();
                statistics->WithAlertsNodeCount += !node_->Alerts().empty();

                if (node_->GetAggregatedState() != ENodeState::Online) {
                    ++statistics->OfflineNodeCount;
                    return;
                }
                statistics->OnlineNodeCount++;

                const auto& nodeStatistics = node_->DataNodeStatistics();
                for (const auto& location : nodeStatistics.chunk_locations()) {
                    int mediumIndex = location.medium_index();
                    if (!node_->IsDecommissioned()) {
                        statistics->SpacePerMedium[mediumIndex].Available += location.available_space();
                        statistics->TotalSpace.Available += location.available_space();
                    }
                    statistics->SpacePerMedium[mediumIndex].Used += location.used_space();
                    statistics->TotalSpace.Used += location.used_space();
                    increment(&statistics->TotalIO, location.io_statistics());
                    increment(&statistics->IOPerMedium[mediumIndex], location.io_statistics());
                }
                statistics->ChunkReplicaCount += nodeStatistics.total_stored_chunk_count();
                statistics->FullNodeCount += nodeStatistics.full() ? 1 : 0;
            };
            updateStatistics(&AggregatedNodeStatistics_);

            for (auto flavor : node->Flavors()) {
                updateStatistics(&FlavoredNodeStatistics_[flavor]);
            }
        }

        NodeStatisticsUpdateDeadline_ =
            GetCpuInstant() +
            DurationToCpuDuration(GetDynamicConfig()->TotalNodeStatisticsUpdatePeriod);
    }

    const TDynamicNodeTrackerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->NodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        RebuildNodeGroups();
        RecomputePendingRegisterNodeMutationCounters();
        ReconfigureGossipPeriods();
        ReconfigureNodeSemaphores();
        RebuildAggregatedNodeStatistics();

        ProfilingExecutor_->SetPeriod(GetDynamicConfig()->ProfilingPeriod);

        ResetNodePendingRestartMaintenanceExecutor_->SetPeriod(GetDynamicConfig()->ResetNodePendingRestartMaintenancePeriod);
    }

    void OnNodeBanUpdated(TNode* node)
    {
        if (node->IsBanned()) {
            YT_LOG_INFO("Node is banned (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (multicellManager->IsPrimaryMaster()) {
                auto state = node->GetLocalState();
                if (state == ENodeState::Online || state == ENodeState::Registered) {
                    UnregisterNode(node, true);
                }
            }
        } else {
            YT_LOG_INFO("Node is no longer banned (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }
        NodeBanChanged_.Fire(node);
    }

    void OnNodeDecommissionUpdated(TNode* node)
    {
        if (node->IsDecommissioned()) {
            YT_LOG_INFO("Node is decommissioned (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        } else {
            YT_LOG_INFO("Node is no longer decommissioned (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }
        NodeDecommissionChanged_.Fire(node);
    }

    void OnDisableWriteSessionsUpdated(TNode* node)
    {
        if (node->AreWriteSessionsDisabled()) {
            YT_LOG_INFO("Disabled write sessions on node (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        } else {
            YT_LOG_INFO("Enabled write sessions on node (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }
        NodeDisableWriteSessionsChanged_.Fire(node);
    }

    void OnDisableTabletCellsUpdated(TNode* node)
    {
        if (node->AreTabletCellsDisabled()) {
            YT_LOG_INFO("Disabled tablet cells on node (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        } else {
            YT_LOG_INFO("Enabled tablet cells on node (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }
        NodeDisableTabletCellsChanged_.Fire(node);
    }

    void OnNodePendingRestartUpdated(TNode* node)
    {
        auto setTransactionTimeoutOnPrimary = [&] (TTransaction* transaction, TDuration timeout) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (!multicellManager->IsPrimaryMaster()) {
                return;
            }

            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            transactionManager->SetTransactionTimeout(transaction, timeout);

            if (node->IsPendingRestart() && IsLeader()) {
                const auto& invoker = Bootstrap_->GetHydraFacade()->GetTransactionTrackerInvoker();
                invoker->Invoke(BIND([=] {
                    try {
                        transactionManager->PingTransaction(
                            transaction->GetId(),
                            /*pingAncestors*/ false);
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING(
                            ex,
                            "Failed to ping node lease transaction after "
                            "extending its timeout for a pending restart");
                    }
                }));
            }
        };

        if (auto transaction = node->GetLeaseTransaction()) {
            if (auto timeout = transaction->GetTimeout()) {
                // COPMAT(danilalexeev)
                const auto& config = Bootstrap_->GetConfig()->NodeTracker;
                auto defaultTimeout = node->IsDataNode()
                    ? config->DefaultDataNodeLeaseTransactionTimeout
                    : config->DefaultNodeTransactionTimeout;

                auto newTimeout = node->IsPendingRestart()
                    ? GetDynamicConfig()->PendingRestartLeaseTimeout
                    : node->GetLastSeenLeaseTransactionTimeout().value_or(defaultTimeout);

                node->SetLastSeenLeaseTransactionTimeout(timeout);

                setTransactionTimeoutOnPrimary(transaction, newTimeout);
            }
        }

        auto nodeId = node->GetId();

        if (auto it = PendingRestartMaintenanceNodeIdToSetIt_.find(nodeId);
            it != PendingRestartMaintenanceNodeIdToSetIt_.end())
        {
            PendingRestartMaintenanceNodeIds_.erase(it->second);
            PendingRestartMaintenanceNodeIdToSetIt_.erase(it);
        }

        if (node->IsPendingRestart()) {
            YT_LOG_INFO("Node restart is pending (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());

            auto* mutationContext = GetCurrentMutationContext();
            auto it = PendingRestartMaintenanceNodeIds_.emplace(mutationContext->GetTimestamp(), nodeId).first;
            PendingRestartMaintenanceNodeIdToSetIt_.emplace(nodeId, it);
        } else {
            YT_LOG_INFO("Node restart is no longer pending (NodeId: %v, Address: %v)",
                nodeId,
                node->GetDefaultAddress());
        }
        NodePendingRestartChanged_.Fire(node);
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker, Node, TNode, NodeMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker, Host, THost, HostMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker, Rack, TRack, RackMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker, DataCenter, TDataCenter, DataCenterMap_);

////////////////////////////////////////////////////////////////////////////////

INodeTrackerPtr CreateNodeTracker(TBootstrap* bootstrap)
{
    return New<TNodeTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
