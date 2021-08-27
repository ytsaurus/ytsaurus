#include "node_tracker.h"
#include "node_discovery_manager.h"
#include "private.h"
#include "config.h"
#include "node.h"
#include "rack.h"
#include "data_center.h"
#include "node_type_handler.h"
#include "rack_type_handler.h"
#include "data_center_type_handler.h"

// COMPAT(gritukan)
#include "exec_node_tracker.h"
#include <yt/yt/server/master/chunk_server/data_node_tracker.h>
#include <yt/yt/server/master/cell_server/cellar_node_tracker.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/job.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/cell_master/automaton.h>

#include <yt/yt/server/master/object_server/attribute_set.h>
#include <yt/yt/server/master/object_server/object_manager.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/tablet_server/tablet_node_tracker.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/lib/node_tracker_server/name_helpers.h>

#include <yt/yt/ytlib/node_tracker_client/interop.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/cellar_node_tracker_service.pb.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/ytlib/exec_node_tracker_client/proto/exec_node_tracker_service.pb.h>

#include <yt/yt/ytlib/tablet_node_tracker_client/proto/tablet_node_tracker_service.pb.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>
#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/tablet_cell_client/tablet_cell_service_proxy.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/id_generator.h>
#include <yt/yt/core/misc/small_vector.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/profiling/profile_manager.h>
#include <yt/yt/core/profiling/timing.h>

namespace NYT::NNodeTrackerServer {

using namespace NConcurrency;
using namespace NNet;
using namespace NYTree;
using namespace NYPath;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NHydra;
using namespace NHiveServer;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NNodeTrackerServer::NProto;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NProfiling;
using namespace NYson;
using namespace NChunkClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NodeTrackerServerLogger;

static const auto ProfilingPeriod = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::NodeTracker)
    {
        RegisterMethod(BIND(&TImpl::HydraRegisterNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnregisterNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraDisposeNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraClusterNodeHeartbeat, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraFullHeartbeat, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraIncrementalHeartbeat, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetCellNodeDescriptors, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateNodeResources, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateNodesForRole, Unretained(this)));

        RegisterLoader(
            "NodeTracker.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "NodeTracker.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "NodeTracker.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "NodeTracker.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        BufferedProducer_ = New<TBufferedProducer>();
        NodeTrackerProfiler
            .WithDefaultDisabled()
            .WithTag("cell_tag", ToString(Bootstrap_->GetMulticellManager()->GetCellTag()))
            .AddProducer("", BufferedProducer_);

        if (Bootstrap_->IsPrimaryMaster()) {
            MasterCacheManager_ = New<TNodeDiscoveryManager>(Bootstrap_, ENodeRole::MasterCache);
            TimestampProviderManager_ = New<TNodeDiscoveryManager>(Bootstrap_, ENodeRole::TimestampProvider);
        }
    }

    void Initialize()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateNodeTypeHandler(Bootstrap_, &NodeMap_));
        objectManager->RegisterHandler(CreateRackTypeHandler(Bootstrap_, &RackMap_));
        objectManager->RegisterHandler(CreateDataCenterTypeHandler(Bootstrap_, &DataCenterMap_));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeValidateSecondaryMasterRegistration(
                BIND(&TImpl::OnValidateSecondaryMasterRegistration, MakeWeak(this)));
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND(&TImpl::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND(&TImpl::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }

    void ProcessRegisterNode(const TString& address, TCtxRegisterNodePtr context)
    {
        if (PendingRegisterNodeAddreses_.find(address) != PendingRegisterNodeAddreses_.end()) {
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

        YT_VERIFY(PendingRegisterNodeAddreses_.insert(address).second);
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
            std::move(context),
            &TImpl::HydraRegisterNode,
            this);
        mutation->SetCurrentTraceContext();
        mutation->CommitAndReply(context);
    }

    void ProcessHeartbeat(TCtxHeartbeatPtr context)
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TImpl::HydraClusterNodeHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), HeartbeatSemaphore_);
    }

    void ProcessFullHeartbeat(TCtxFullHeartbeatPtr context)
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TImpl::HydraFullHeartbeat,
            this);
        mutation->SetCurrentTraceContext();
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), FullHeartbeatSemaphore_);
   }

    void ProcessIncrementalHeartbeat(TCtxIncrementalHeartbeatPtr context)
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TImpl::HydraIncrementalHeartbeat,
            this);
        mutation->SetCurrentTraceContext();
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), IncrementalHeartbeatSemaphore_);
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Node, TNode);
    DECLARE_ENTITY_MAP_ACCESSORS(Rack, TRack);
    DECLARE_ENTITY_MAP_ACCESSORS(DataCenter, TDataCenter);

    DEFINE_SIGNAL(void(TNode* node), NodeRegistered);
    DEFINE_SIGNAL(void(TNode* node), NodeOnline);
    DEFINE_SIGNAL(void(TNode* node), NodeUnregistered);
    DEFINE_SIGNAL(void(TNode* node), NodeDisposed);
    DEFINE_SIGNAL(void(TNode* node), NodeBanChanged);
    DEFINE_SIGNAL(void(TNode* node), NodeDecommissionChanged);
    DEFINE_SIGNAL(void(TNode* node), NodeDisableTabletCellsChanged);
    DEFINE_SIGNAL(void(TNode* node), NodeTagsChanged);
    DEFINE_SIGNAL(void(TNode* node, TRack*), NodeRackChanged);
    DEFINE_SIGNAL(void(TNode* node, TDataCenter*), NodeDataCenterChanged);
    DEFINE_SIGNAL(void(TDataCenter*), DataCenterCreated);
    DEFINE_SIGNAL(void(TDataCenter*), DataCenterRenamed);
    DEFINE_SIGNAL(void(TDataCenter*), DataCenterDestroyed);


    void ZombifyNode(TNode* node)
    {
        auto nodeMapProxy = GetNodeMap();
        auto nodeNodeProxy = nodeMapProxy->FindChild(ToString(node->GetDefaultAddress()));
        if (nodeNodeProxy) {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            cypressManager->AbortSubtreeTransactions(nodeNodeProxy);
            nodeMapProxy->RemoveChild(nodeNodeProxy);
        }

        // NB: This is typically redundant since it's not possible to remove a node unless
        // it is offline. Secondary masters, however, may receive a removal request from primaries
        // and must obey it regardless of the node's state.
        EnsureNodeDisposed(node);

        RemoveFromAddressMaps(node);

        RecomputePendingRegisterNodeMutationCounters();

        RemoveFromNodeLists(node);
    }

    TObjectId ObjectIdFromNodeId(TNodeId nodeId)
    {
        return NNodeTrackerClient::ObjectIdFromNodeId(
            nodeId,
            Bootstrap_->GetMulticellManager()->GetPrimaryCellTag());
    }

    TNode* FindNode(TNodeId id)
    {
        return FindNode(ObjectIdFromNodeId(id));
    }

    TNode* GetNode(TNodeId id)
    {
        return GetNode(ObjectIdFromNodeId(id));
    }

    TNode* GetNodeOrThrow(TNodeId id)
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

    TNode* FindNodeByAddress(const TString& address)
    {
        auto it = AddressToNodeMap_.find(address);
        return it == AddressToNodeMap_.end() ? nullptr : it->second;
    }

    TNode* GetNodeByAddress(const TString& address)
    {
        auto* node = FindNodeByAddress(address);
        YT_VERIFY(node);
        return node;
    }

    TNode* GetNodeByAddressOrThrow(const TString& address)
    {
        auto* node = FindNodeByAddress(address);
        if (!node) {
            THROW_ERROR_EXCEPTION("No such cluster node %Qv", address);
        }
        return node;
    }

    TNode* FindNodeByHostName(const TString& hostName)
    {
        auto it = HostNameToNodeMap_.find(hostName);
        return it == HostNameToNodeMap_.end() ? nullptr : it->second;
    }

    std::vector<TNode*> GetRackNodes(const TRack* rack)
    {
        std::vector<TNode*> result;
        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            if (node->GetRack() == rack) {
                result.push_back(node);
            }
        }
        return result;
    }

    std::vector<TRack*> GetDataCenterRacks(const TDataCenter* dc)
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

    void UpdateLastSeenTime(TNode* node)
    {
        const auto* mutationContext = GetCurrentMutationContext();
        node->SetLastSeenTime(mutationContext->GetTimestamp());
    }

    void SetNodeBanned(TNode* node, bool value)
    {
        if (node->GetBanned() != value) {
            node->SetBanned(value);
            if (value) {
                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node banned (NodeId: %v, Address: %v)",
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
                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node is no longer banned (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            }
            NodeBanChanged_.Fire(node);
        }
    }

    void SetNodeDecommissioned(TNode* node, bool value)
    {
        if (node->GetDecommissioned() != value) {
            node->SetDecommissioned(value);
            if (value) {
                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node decommissioned (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            } else {
                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node is no longer decommissioned (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            }
            NodeDecommissionChanged_.Fire(node);
        }
    }

    void SetDisableWriteSessions(TNode* node, bool value)
    {
        if (node->GetDisableWriteSessions() != value) {
            node->SetDisableWriteSessions(value);
            if (value) {
                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Disabled write sessions on node (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            } else {
                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Enabled write sessions on node (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            }
        }
    }

    void SetDisableTabletCells(TNode* node, bool value)
    {
        if (node->GetDisableTabletCells() != value) {
            node->SetDisableTabletCells(value);
            if (value) {
                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Disabled tablet cells on node (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            } else {
                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Enabled tablet cells on node (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            }
            NodeDisableTabletCellsChanged_.Fire(node);
        }
    }

    void SetNodeRack(TNode* node, TRack* rack)
    {
        if (node->GetRack() != rack) {
            auto* oldRack = node->GetRack();
            UpdateNodeCounters(node, -1);
            node->SetRack(rack);
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node rack changed (NodeId: %v, Address: %v, Rack: %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                rack ? std::make_optional(rack->GetName()) : std::nullopt);
            NodeRackChanged_.Fire(node, oldRack);
            NodeTagsChanged_.Fire(node);
            UpdateNodeCounters(node, +1);
        }
    }

    void SetNodeUserTags(TNode* node, const std::vector<TString>& tags)
    {
        UpdateNodeCounters(node, -1);
        node->SetUserTags(tags);
        NodeTagsChanged_.Fire(node);
        UpdateNodeCounters(node, +1);
    }

    std::unique_ptr<TMutation> CreateUpdateNodeResourcesMutation(const NProto::TReqUpdateNodeResources& request)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TImpl::HydraUpdateNodeResources,
            this);
    }

    TRack* CreateRack(const TString& name, TObjectId hintId)
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

        return rack;
    }

    void ZombifyRack(TRack* rack)
    {
        // Unbind nodes from this rack.
        for (auto* node : GetRackNodes(rack)) {
            SetNodeRack(node, nullptr);
        }

        // Remove rack from maps.
        YT_VERIFY(NameToRackMap_.erase(rack->GetName()) == 1);
        FreeRackIndex(rack->GetIndex());
    }

    void RenameRack(TRack* rack, const TString& newName)
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
        auto nodes = GetRackNodes(rack);
        for (auto* node : nodes) {
            UpdateNodeCounters(node, -1);
            node->RebuildTags();
            UpdateNodeCounters(node, +1);
        }
    }

    TRack* FindRackByName(const TString& name)
    {
        auto it = NameToRackMap_.find(name);
        return it == NameToRackMap_.end() ? nullptr : it->second;
    }

    TRack* GetRackByNameOrThrow(const TString& name)
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

    void SetRackDataCenter(TRack* rack, TDataCenter* dataCenter)
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

            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Rack data center changed (Rack: %v, DataCenter: %v)",
                std::make_optional(rack->GetName()),
                dataCenter ? std::make_optional(dataCenter->GetName()) : std::nullopt);

            for (auto* node : nodes) {
                NodeDataCenterChanged_.Fire(node, oldDataCenter);
            }
        }
    }


    TDataCenter* CreateDataCenter(const TString& name, TObjectId hintId)
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

    void ZombifyDataCenter(TDataCenter* dc)
    {
        // Unbind racks from this DC.
        for (auto* rack : GetDataCenterRacks(dc)) {
            SetRackDataCenter(rack, nullptr);
        }

        // Remove DC from maps.
        YT_VERIFY(NameToDataCenterMap_.erase(dc->GetName()) == 1);

        DataCenterDestroyed_.Fire(dc);
    }

    void RenameDataCenter(TDataCenter* dc, const TString& newName)
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
        auto racks = GetDataCenterRacks(dc);
        for (auto* rack : racks) {
            auto nodes = GetRackNodes(rack);
            for (auto* node : nodes) {
                UpdateNodeCounters(node, -1);
                node->RebuildTags();
                UpdateNodeCounters(node, +1);
            }
        }

        DataCenterRenamed_.Fire(dc);
    }

    TDataCenter* FindDataCenterByName(const TString& name)
    {
        auto it = NameToDataCenterMap_.find(name);
        return it == NameToDataCenterMap_.end() ? nullptr : it->second;
    }

    TDataCenter* GetDataCenterByNameOrThrow(const TString& name)
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


    TTotalNodeStatistics GetTotalNodeStatistics()
    {
        auto now = GetCpuInstant();
        if (now < TotalNodeStatisticsUpdateDeadline_) {
            return TotalNodeStatistics_;
        }

        RebuildTotalNodeStatistics();

        return TotalNodeStatistics_;
    }

    int GetOnlineNodeCount()
    {
        return AggregatedOnlineNodeCount_;
    }

    const std::vector<TNode*>& GetNodesForRole(ENodeRole nodeRole)
    {
        return NodeListPerRole_[nodeRole].Nodes();
    }

    const std::vector<TString>& GetNodeAddressesForRole(ENodeRole nodeRole)
    {
        return NodeListPerRole_[nodeRole].Addresses();
    }

    void OnNodeHeartbeat(TNode* node, ENodeHeartbeatType heartbeatType)
    {
        if (node->ReportedHeartbeats().emplace(heartbeatType).second) {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node reported heartbeat for the first time "
                "(NodeId: %v, Address: %v, HeartbeatType: %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                heartbeatType);

            CheckNodeOnline(node);
        }
    }

    THashSet<ENodeHeartbeatType> GetExpectedHeartbeatsForFlavors(
        const THashSet<ENodeFlavor>& flavors,
        bool primaryMaster)
    {
        THashSet<ENodeHeartbeatType> result;
        if (primaryMaster) {
            result.insert(ENodeHeartbeatType::Cluster);
        }

        for (auto flavor : flavors) {
            switch (flavor) {
                case ENodeFlavor::Data:
                    result.insert(ENodeHeartbeatType::Data);
                    break;

                case ENodeFlavor::Exec:
                    result.insert(ENodeHeartbeatType::Data);
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
        auto expectedHeartbeats = GetExpectedHeartbeatsForFlavors(node->Flavors(), multicellManager->IsPrimaryMaster());
        if (node->GetLocalState() == ENodeState::Registered && node->ReportedHeartbeats() == expectedHeartbeats) {
            UpdateNodeCounters(node, -1);
            node->SetLocalState(ENodeState::Online);
            UpdateNodeCounters(node, +1);

            NodeOnline_.Fire(node);

            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node is online (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }
    }

    void RequestNodeHeartbeat(TNodeId nodeId)
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

        NTabletCellClient::TTabletCellServiceProxy proxy(nodeChannel);
        auto req = proxy.RequestHeartbeat();
        req->SetTimeout(GetDynamicConfig()->ForceNodeHeartbeatRequestTimeout);
        Y_UNUSED(req->Invoke());
    }

private:
    TPeriodicExecutorPtr ProfilingExecutor_;

    TBufferedProducerPtr BufferedProducer_;

    TIdGenerator NodeIdGenerator_;
    NHydra::TEntityMap<TNode> NodeMap_;
    NHydra::TEntityMap<TRack> RackMap_;
    NHydra::TEntityMap<TDataCenter> DataCenterMap_;

    int AggregatedOnlineNodeCount_ = 0;

    TCpuInstant TotalNodeStatisticsUpdateDeadline_ = 0;
    TTotalNodeStatistics TotalNodeStatistics_;

    // Cf. YT-7009.
    // Maintain a dedicated counter of alive racks since RackMap_ may contain zombies.
    // This is exactly the number of 1-bits in UsedRackIndexes_.
    int RackCount_ = 0;
    TRackSet UsedRackIndexes_;

    THashMap<TString, TNode*> AddressToNodeMap_;
    THashMultiMap<TString, TNode*> HostNameToNodeMap_;
    THashMap<TTransaction*, TNode*> TransactionToNodeMap_;
    THashMap<TString, TRack*> NameToRackMap_;
    THashMap<TString, TDataCenter*> NameToDataCenterMap_;

    TPeriodicExecutorPtr IncrementalNodeStatesGossipExecutor_;
    TPeriodicExecutorPtr FullNodeStatesGossipExecutor_;

    const TAsyncSemaphorePtr HeartbeatSemaphore_ = New<TAsyncSemaphore>(0);
    const TAsyncSemaphorePtr FullHeartbeatSemaphore_ = New<TAsyncSemaphore>(0);
    const TAsyncSemaphorePtr IncrementalHeartbeatSemaphore_ = New<TAsyncSemaphore>(0);
    const TAsyncSemaphorePtr DisposeNodeSemaphore_ = New<TAsyncSemaphore>(0);

    TEnumIndexedVector<ENodeRole, TNodeListForRole> NodeListPerRole_;

    struct TNodeGroup
    {
        TString Id;
        TNodeGroupConfigPtr Config;
        int LocalRegisteredNodeCount = 0;
        int PendingRegisterNodeMutationCount = 0;
    };

    std::vector<TNodeGroup> NodeGroups_;
    TNodeGroup* DefaultNodeGroup_ = nullptr;
    THashSet<TString> PendingRegisterNodeAddreses_;
    THashMap<TLocationUuid, TNode*> RegisteredLocationUuids_;
    TNodeDiscoveryManagerPtr MasterCacheManager_;
    TNodeDiscoveryManagerPtr TimestampProviderManager_;

    // COMPAT(gritukan)
    bool NeedToRunCompatForNodeFlavors_ = false;

    using TNodeGroupList = SmallVector<TNodeGroup*, 4>;

    TNodeId GenerateNodeId()
    {
        TNodeId id;
        while (true) {
            id = NodeIdGenerator_.Next();
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

    IMapNodePtr GetNodeMap()
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->ResolvePathToNodeProxy(GetClusterNodesPath())->AsMap();
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
        auto locationUuids = FromProto<std::vector<TLocationUuid>>(request->location_uuids());

        ValidateNoDuplicateLocationUuids(address, locationUuids);

        // COMPAT(gritukan)
        if (flavors.empty()) {
            flavors = THashSet<ENodeFlavor>{
                ENodeFlavor::Data,
                ENodeFlavor::Exec,
                ENodeFlavor::Tablet,
            };
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster() && IsLeader()) {
            YT_VERIFY(PendingRegisterNodeAddreses_.erase(address) == 1);
            auto groups = GetGroupsForNode(address);
            for (auto* group : groups) {
                --group->PendingRegisterNodeMutationCount;
            }
        }

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

        // Kick-out any previous incarnation.
        auto* node = FindNodeByAddress(address);
        auto isNodeNew = !IsObjectAlive(node);
        if (!isNodeNew) {
            node->ValidateNotBanned();

            if (multicellManager->IsPrimaryMaster()) {
                auto localState = node->GetLocalState();
                if (localState == ENodeState::Registered || localState == ENodeState::Online) {
                    YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Kicking node out due to address conflict (NodeId: %v, Address: %v, State: %v)",
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
            } else {
                EnsureNodeDisposed(node);
            }
        }

        for (auto locationUuid : locationUuids) {
            if (auto it = RegisteredLocationUuids_.find(locationUuid)) {
                THROW_ERROR_EXCEPTION("Cannot register node %Qv: there is a registered node %Qv with the same location uuid %v",
                    address,
                    it->second->GetDefaultAddress(),
                    locationUuid);
            }
        }

        if (!isNodeNew) {
            UpdateNode(node, nodeAddresses);
        } else {
            auto nodeId = request->has_node_id() ? request->node_id() : GenerateNodeId();
            node = CreateNode(nodeId, nodeAddresses);
        }

        node->SetNodeTags(tags);

        node->Flavors() = flavors;

        if (request->has_cypress_annotations()) {
            node->SetAnnotations(TYsonString(request->cypress_annotations(), EYsonType::Node));
        }

        if (request->has_build_version()) {
            node->SetVersion(request->build_version());
        }

        UpdateLastSeenTime(node);
        UpdateRegisterTime(node);

        if (multicellManager->IsPrimaryMaster()) {
            PostRegisterNodeMutation(node);
        }

        if (isNodeNew && GetDynamicConfig()->BanNewNodes) {
            node->SetBanned(true);
            {
                auto nodePath = GetNodePath(node);
                auto req = TCypressYPathProxy::Set(nodePath + "/@" + BanMessageAttributeName);
                req->set_value(ConvertToYsonString("The node has never been seen before and has been banned provisionally").ToString());
                auto rootService = Bootstrap_->GetObjectManager()->GetRootService();
                SyncExecuteVerb(rootService, req);
            }
            node->SetLocalState(ENodeState::Offline);
            node->ReportedHeartbeats().clear();

            THROW_ERROR_EXCEPTION("Node %Qv (#%v) created and provisionally banned",
                address,
                node->GetId());
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node registered (NodeId: %v, Address: %v, Tags: %v, Flavors: %v, LeaseTransactionId: %v)",
            node->GetId(),
            address,
            tags,
            flavors,
            leaseTransactionId);

        node->SetLocalState(ENodeState::Registered);
        node->ReportedHeartbeats().clear();

        UpdateNodeCounters(node, +1);

        if (leaseTransaction) {
            node->SetLeaseTransaction(leaseTransaction);
            RegisterLeaseTransaction(node);
        }

        for (auto locationUuid : locationUuids) {
            YT_VERIFY(RegisteredLocationUuids_.emplace(locationUuid, node).second);
        }

        node->LocationUuids() = std::move(locationUuids);

        NodeRegistered_.Fire(node);

        response->set_node_id(node->GetId());
        response->set_use_new_heartbeats(GetDynamicConfig()->UseNewHeartbeats);

        // NB: Exec nodes should not report heartbeats to secondary masters,
        // so node can already be online for this cell.
        CheckNodeOnline(node);

        if (context) {
            context->SetResponseInfo("NodeId: %v",
                node->GetId());
        }
    }

    void HydraUnregisterNode(TReqUnregisterNode* request)
    {
        auto nodeId = request->node_id();

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

    void HydraDisposeNode(TReqDisposeNode* request)
    {
        auto nodeId = request->node_id();
        auto* node = FindNode(nodeId);
        if (!IsObjectAlive(node)) {
            return;
        }

        if (node->GetLocalState() != ENodeState::Unregistered) {
            return;
        }

        DisposeNode(node);
    }

    void HydraClusterNodeHeartbeat(
        const TCtxHeartbeatPtr& /*context*/,
        TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        auto nodeId = request->node_id();
        auto& statistics = request->statistics();

        auto* node = GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        YT_PROFILE_TIMING("/node_tracker/cluster_node_heartbeat_time") {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Processing cluster node heartbeat (NodeId: %v, Address: %v, State: %v, %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            UpdateLastSeenTime(node);

            DoProcessHeartbeat(node, request, response);
        }
    }

    void HydraFullHeartbeat(
        const TCtxFullHeartbeatPtr& /*context*/,
        TReqFullHeartbeat* request,
        TRspFullHeartbeat* response)
    {
        auto nodeId = request->node_id();
        auto& statistics = *request->mutable_statistics();

        auto* node = GetNodeOrThrow(nodeId);
        if (node->GetLocalState() != ENodeState::Registered) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process a full heartbeat in %Qlv state",
                node->GetLocalState());
        }

        YT_PROFILE_TIMING("/node_tracker/full_heartbeat_time") {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Processing full heartbeat (NodeId: %v, Address: %v, State: %v, %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            UpdateLastSeenTime(node);

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (multicellManager->IsPrimaryMaster()) {
                TReqHeartbeat req;
                TRspHeartbeat rsp;
                FromFullHeartbeatRequest(&req, *request);
                DoProcessHeartbeat(node, &req, &rsp);
            }

            {
                NDataNodeTrackerClient::NProto::TReqFullHeartbeat req;
                NDataNodeTrackerClient::NProto::TRspFullHeartbeat rsp;
                FromFullHeartbeatRequest(&req, *request);

                const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
                dataNodeTracker->ProcessFullHeartbeat(node, &req, &rsp);
                FillFullHeartbeatResponse(response, rsp);
            }

            if (multicellManager->IsPrimaryMaster()) {
                NExecNodeTrackerClient::NProto::TReqHeartbeat req;
                NExecNodeTrackerClient::NProto::TRspHeartbeat rsp;
                FromFullHeartbeatRequest(&req, *request);

                const auto& execNodeTracker = Bootstrap_->GetExecNodeTracker();
                execNodeTracker->ProcessHeartbeat(node, &req, &rsp);
            }

            {
                NCellarNodeTrackerClient::NProto::TReqHeartbeat req;
                NCellarNodeTrackerClient::NProto::TRspHeartbeat rsp;
                FromFullHeartbeatRequest(&req, *request);

                const auto& cellarNodeTracker = Bootstrap_->GetCellarNodeTracker();
                cellarNodeTracker->ProcessHeartbeat(node, &req, &rsp, /* legacyFullHeartbeat */ true);
            }

            {
                NTabletNodeTrackerClient::NProto::TReqHeartbeat req;
                NTabletNodeTrackerClient::NProto::TRspHeartbeat rsp;

                const auto& tabletNodeTracker = Bootstrap_->GetTabletNodeTracker();
                tabletNodeTracker->ProcessHeartbeat(node, &req, &rsp, /* legacyFullHeartbeat */ true);
            }

            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node online (NodeId: %v, Address: %v)",
                nodeId,
                node->GetDefaultAddress());
        }
    }

    void HydraIncrementalHeartbeat(
        const TCtxIncrementalHeartbeatPtr& /*context*/,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response)
    {
        auto nodeId = request->node_id();
        auto& statistics = *request->mutable_statistics();

        auto* node = GetNodeOrThrow(nodeId);
        if (node->GetLocalState() != ENodeState::Online) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process an incremental heartbeat in %Qlv state",
                node->GetLocalState());
        }

        YT_PROFILE_TIMING("/node_tracker/incremental_heartbeat_time") {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Processing incremental heartbeat (NodeId: %v, Address: %v, State: %v, %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            UpdateLastSeenTime(node);

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (multicellManager->IsPrimaryMaster()) {
                TReqHeartbeat req;
                TRspHeartbeat rsp;
                FromIncrementalHeartbeatRequest(&req, *request);
                DoProcessHeartbeat(node, &req, &rsp);
                FillIncrementalHeartbeatResponse(response, rsp);
            }

            {
                NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat req;
                NDataNodeTrackerClient::NProto::TRspIncrementalHeartbeat rsp;
                FromIncrementalHeartbeatRequest(&req, *request);

                const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
                dataNodeTracker->ProcessIncrementalHeartbeat(node, &req, &rsp);

                FillIncrementalHeartbeatResponse(response, rsp);
            }

            if (multicellManager->IsPrimaryMaster()) {
                NExecNodeTrackerClient::NProto::TReqHeartbeat req;
                NExecNodeTrackerClient::NProto::TRspHeartbeat rsp;
                FromIncrementalHeartbeatRequest(&req, *request);

                const auto& execNodeTracker = Bootstrap_->GetExecNodeTracker();
                execNodeTracker->ProcessHeartbeat(node, &req, &rsp);

                FillIncrementalHeartbeatResponse(response, rsp);
            }

            {
                NCellarNodeTrackerClient::NProto::TReqHeartbeat req;
                NCellarNodeTrackerClient::NProto::TRspHeartbeat rsp;
                FromIncrementalHeartbeatRequest(&req, *request);

                const auto& cellarNodeTracker = Bootstrap_->GetCellarNodeTracker();
                cellarNodeTracker->ProcessHeartbeat(node, &req, &rsp);

                FillIncrementalHeartbeatResponse(response, rsp);
            }

            {
                NTabletNodeTrackerClient::NProto::TReqHeartbeat req;
                NTabletNodeTrackerClient::NProto::TRspHeartbeat rsp;
                FromIncrementalHeartbeatRequest(&req, *request);

                const auto& tabletNodeTracker = Bootstrap_->GetTabletNodeTracker();
                tabletNodeTracker->ProcessHeartbeat(node, &req, &rsp);
            }
        }
    }

    void HydraSetCellNodeDescriptors(TReqSetCellNodeDescriptors* request)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster());

        auto cellTag = request->cell_tag();
        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), "Received cell node descriptor gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Received cell node descriptor gossip message (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
            auto* node = FindNode(entry.node_id());
            if (!IsObjectAlive(node)) {
                continue;
            }

            auto newDescriptor = FromProto<TCellNodeDescriptor>(entry.node_descriptor());
            UpdateNodeCounters(node, -1);
            node->SetCellDescriptor(cellTag, newDescriptor);
            UpdateNodeCounters(node, +1);
        }
    }

    void HydraUpdateNodeResources(NProto::TReqUpdateNodeResources* request)
    {
        auto* node = FindNode(request->node_id());
        if (!node) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(),
                "Error updating cluster node resource usage and limits: node not found (NodeId: %v)",
                request->node_id());
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

        for (auto nodeId: request->node_ids()) {
            auto* node = FindNode(nodeId);
            if (IsObjectAlive(node)) {
                nodeList.push_back(node);
            } else {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "New node for role is dead, ignoring (NodeRole: %v, NodeId: %v)",
                    nodeRole,
                    node->GetId());
            }
        }

        NodeListPerRole_[nodeRole].UpdateAddresses();

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Updated nodes for role (NodeRole: %v, Nodes: %v)",
            nodeRole,
            MakeFormattableView(nodeList, TNodePtrAddressFormatter()));
    }

    void DoProcessHeartbeat(
        TNode* node,
        TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster());

        auto& statistics = *request->mutable_statistics();
        node->SetClusterNodeStatistics(std::move(statistics));

        node->Alerts() = FromProto<std::vector<TError>>(request->alerts());

        OnNodeHeartbeat(node, ENodeHeartbeatType::Cluster);

        if (auto* rack = node->GetRack()) {
            response->set_rack(rack->GetName());
            if (auto* dc = rack->GetDataCenter()) {
                response->set_data_center(dc->GetName());
            }
        }

        auto rspTags = response->mutable_tags();
        SmallVector<TString, 16> sortedTags(node->Tags().begin(), node->Tags().end());
        std::sort(sortedTags.begin(), sortedTags.end());
        for (auto tag : sortedTags) {
            rspTags->Add(std::move(tag));
        }

        *response->mutable_resource_limits_overrides() = node->ResourceLimitsOverrides();
        response->set_decommissioned(node->GetDecommissioned());

        node->SetDisableWriteSessionsSentToNode(node->GetDisableWriteSessions());
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveKeys(context);
        RackMap_.SaveKeys(context);
        DataCenterMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        Save(context, NodeIdGenerator_);
        Save(context, NodeListPerRole_);
        NodeMap_.SaveValues(context);
        RackMap_.SaveValues(context);
        DataCenterMap_.SaveValues(context);
        Save(context, RegisteredLocationUuids_);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        NodeMap_.LoadKeys(context);
        RackMap_.LoadKeys(context);
        DataCenterMap_.LoadKeys(context);

        NeedToRunCompatForNodeFlavors_ = context.GetVersion() < EMasterReign::NodeFlavors;
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        Load(context, NodeIdGenerator_);
        Load(context, NodeListPerRole_);
        NodeMap_.LoadValues(context);
        RackMap_.LoadValues(context);
        DataCenterMap_.LoadValues(context);

        // COMPAT(aleksandra-zh)
        if (context.GetVersion() >= EMasterReign::RegisteredLocationUuids) {
            Load(context, RegisteredLocationUuids_);
        }
    }


    void Clear() override
    {
        TMasterAutomatonPart::Clear();

        NodeIdGenerator_.Reset();
        NodeMap_.Clear();
        RackMap_.Clear();
        DataCenterMap_.Clear();

        AddressToNodeMap_.clear();
        HostNameToNodeMap_.clear();
        TransactionToNodeMap_.clear();

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
            InitializeNodeStates(node);
            InitializeNodeIOWeights(node);
            InsertToAddressMaps(node);
            UpdateNodeCounters(node, +1);

            if (node->GetLeaseTransaction()) {
                RegisterLeaseTransaction(node);
            }
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

        // COMPAT(gritukan)
        if (NeedToRunCompatForNodeFlavors_) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            auto fullFlavor = THashSet<ENodeFlavor>{
                ENodeFlavor::Data,
                ENodeFlavor::Exec,
                ENodeFlavor::Tablet,
            };
            auto fullHeartbeat = GetExpectedHeartbeatsForFlavors(fullFlavor, multicellManager->IsPrimaryMaster());

            for (const auto& [nodeId, node] : NodeMap_) {
                if (!IsObjectAlive(node)) {
                    continue;
                }

                node->Flavors() = fullFlavor;
                if (node->GetLocalState() == ENodeState::Online) {
                    node->ReportedHeartbeats() = fullHeartbeat;
                } else {
                    node->ReportedHeartbeats() = {};
                }
            }
        }
    }

    void OnRecoveryStarted() override
    {
        TMasterAutomatonPart::OnRecoveryStarted();

        for (auto [nodeId, node] : NodeMap_) {
            node->Reset();
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
            IncrementalNodeStatesGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::NodeTrackerGossip),
                BIND(&TImpl::OnNodeStatesGossip, MakeWeak(this), true));
            IncrementalNodeStatesGossipExecutor_->Start();

            FullNodeStatesGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::NodeTrackerGossip),
                BIND(&TImpl::OnNodeStatesGossip, MakeWeak(this), false));
            FullNodeStatesGossipExecutor_->Start();
        }

        PendingRegisterNodeAddreses_.clear();
        for (auto& group : NodeGroups_) {
            group.PendingRegisterNodeMutationCount = 0;
        }

        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            if (node->GetLocalState() == ENodeState::Unregistered) {
                CommitDisposeNodeWithSemaphore(node);
            }
        }
    }

    void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        if (IncrementalNodeStatesGossipExecutor_) {
            IncrementalNodeStatesGossipExecutor_->Stop();
            IncrementalNodeStatesGossipExecutor_.Reset();
        }

        if (FullNodeStatesGossipExecutor_) {
            FullNodeStatesGossipExecutor_->Stop();
            FullNodeStatesGossipExecutor_.Reset();
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
        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node lease transaction finished (NodeId: %v, Address: %v, TransactionId: %v)",
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

        InitializeNodeStates(node);

        node->SetNodeAddresses(nodeAddresses);
        InsertToAddressMaps(node);

        node->ResetDestroyedReplicasIterator();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto rootService = objectManager->GetRootService();
        auto nodePath = GetNodePath(node);

        try {
            // Create Cypress node.
            {
                auto req = TCypressYPathProxy::Create(nodePath);
                req->set_type(static_cast<int>(EObjectType::ClusterNodeNode));
                req->set_ignore_existing(true);
                SyncExecuteVerb(rootService, req);
            }

            // Create "orchid" child.
            {
                auto req = TCypressYPathProxy::Create(nodePath + "/orchid");
                req->set_type(static_cast<int>(EObjectType::Orchid));
                req->set_ignore_existing(true);
                SyncExecuteVerb(rootService, req);
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error registering cluster node in Cypress");
        }

        UpdateNode(node, nodeAddresses);

        return node;
    }

    void UpdateNode(TNode* node, const TNodeAddressMap& nodeAddresses)
    {
        // NB: The default address must remain same, however others may change.
        node->SetNodeAddresses(nodeAddresses);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto rootService = objectManager->GetRootService();
        auto nodePath = GetNodePath(node);

        try {
            // Update "orchid" child.
            auto req = TYPathProxy::Set(nodePath + "/orchid&/@remote_addresses");
            req->set_value(ConvertToYsonString(node->GetAddressesOrThrow(EAddressType::InternalRpc)).ToString());
            SyncExecuteVerb(rootService, req);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error updating cluster node in Cypress");
        }
    }

    void UnregisterNode(TNode* node, bool propagate)
    {
        YT_PROFILE_TIMING("/node_tracker/node_unregister_time") {
            auto* transaction = UnregisterLeaseTransaction(node);
            if (IsObjectAlive(transaction)) {
                const auto& transactionManager = Bootstrap_->GetTransactionManager();
                // NB: This will trigger OnTransactionFinished, however we've already evicted the
                // lease so the latter call is no-op.
                transactionManager->AbortTransaction(transaction, true);
            }

            UpdateNodeCounters(node, -1);
            node->SetLocalState(ENodeState::Unregistered);
            node->ReportedHeartbeats().clear();

            for (const auto& locationUuid : node->LocationUuids()) {
                YT_VERIFY(RegisteredLocationUuids_.erase(locationUuid) > 0);
            }

            NodeUnregistered_.Fire(node);

            if (propagate) {
                if (IsLeader()) {
                    CommitDisposeNodeWithSemaphore(node);
                }

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                if (multicellManager->IsPrimaryMaster()) {
                    PostUnregisterNodeMutation(node);
                }
            }

            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node unregistered (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }
    }

    void DisposeNode(TNode* node)
    {
        YT_PROFILE_TIMING("/node_tracker/node_dispose_time") {
            node->SetLocalState(ENodeState::Offline);
            node->ReportedHeartbeats().clear();
            NodeDisposed_.Fire(node);

            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Node offline (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }
    }

    void EnsureNodeDisposed(TNode* node)
    {
        if (node->GetLocalState() == ENodeState::Registered ||
            node->GetLocalState() == ENodeState::Online)
        {
            UnregisterNode(node, false);
        }

        if (node->GetLocalState() == ENodeState::Unregistered) {
            DisposeNode(node);
        }
    }

    static void ValidateNoDuplicateLocationUuids(
        const TString& address,
        const std::vector<TLocationUuid>& uuids)
    {
        THashSet<TLocationUuid> uuidSet;
        for (auto uuid : uuids) {
            if (!uuidSet.insert(uuid).second) {
                THROW_ERROR_EXCEPTION("Duplicate location uuid %v reported by node %Qv",
                    uuid,
                    address);
            }
        }
    }


    void OnNodeStatesGossip(bool incremental)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

        TReqSetCellNodeDescriptors request;
        request.set_cell_tag(multicellManager->GetCellTag());
        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }

            auto state = node->GetLocalState();
            if (incremental && state == node->GetLastGossipState()) {
                continue;
            }

            auto* entry = request.add_entries();
            entry->set_node_id(node->GetId());
            auto descriptor = TCellNodeDescriptor{state, node->ComputeCellStatistics()};
            ToProto(entry->mutable_node_descriptor(), descriptor);
            node->SetLastGossipState(state);
        }

        if (request.entries_size() == 0) {
            return;
        }

        YT_LOG_INFO("Sending node states gossip message (Incremental: %v)",
            incremental);
        multicellManager->PostToPrimaryMaster(request, false);
    }


    void CommitMutationWithSemaphore(
        std::unique_ptr<TMutation> mutation,
        NRpc::IServiceContextPtr context,
        const TAsyncSemaphorePtr& semaphore)
    {
        auto handler = BIND([mutation = std::move(mutation), context = std::move(context)] (TAsyncSemaphoreGuard) {
            Y_UNUSED(WaitFor(mutation->CommitAndReply(context)));
        });

        semaphore->AsyncAcquire(handler, EpochAutomatonInvoker_);
    }

    void CommitDisposeNodeWithSemaphore(TNode* node)
    {
        TReqDisposeNode request;
        request.set_node_id(node->GetId());

        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TImpl::HydraDisposeNode,
            this);

        auto handler = BIND([mutation = std::move(mutation)] (TAsyncSemaphoreGuard) {
            Y_UNUSED(WaitFor(mutation->CommitAndLog(NodeTrackerServerLogger)));
        });

        DisposeNodeSemaphore_->AsyncAcquire(handler, EpochAutomatonInvoker_);
    }


    void PostRegisterNodeMutation(TNode* node)
    {
        TReqRegisterNode request;
        request.set_node_id(node->GetId());
        ToProto(request.mutable_node_addresses(), node->GetNodeAddresses());
        for (const auto& tag : node->NodeTags()) {
            request.add_tags(tag);
        }
        request.set_cypress_annotations(node->GetAnnotations().ToString());
        request.set_build_version(node->GetVersion());

        for (auto flavor : node->Flavors()) {
            request.add_flavors(static_cast<int>(flavor));
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToSecondaryMasters(request);
    }

    void PostUnregisterNodeMutation(TNode* node)
    {
        TReqUnregisterNode request;
        request.set_node_id(node->GetId());

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
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto nodes = GetValuesSortedByKey(NodeMap_);
        for (const auto* node : nodes) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            // NB: TReqRegisterNode+TReqUnregisterNode create an offline node at the secondary master.
            {
                TReqRegisterNode request;
                request.set_node_id(node->GetId());
                ToProto(request.mutable_node_addresses(), node->GetNodeAddresses());
                multicellManager->PostToMaster(request, cellTag);
            }
            {
                TReqUnregisterNode request;
                request.set_node_id(node->GetId());
                multicellManager->PostToMaster(request, cellTag);
            }
        }

        auto racks = GetValuesSortedByKey(RackMap_);
        for (auto* rack : racks) {
            if (!IsObjectAlive(rack)) {
                continue;
            }
            objectManager->ReplicateObjectCreationToSecondaryMaster(rack, cellTag);
        }

        auto dcs = GetValuesSortedByKey(DataCenterMap_);
        for (auto* dc : dcs) {
            if (!IsObjectAlive(dc)) {
                continue;
            }
            objectManager->ReplicateObjectCreationToSecondaryMaster(dc, cellTag);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto nodes = GetValuesSortedByKey(NodeMap_);
        for (auto* node : nodes) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            objectManager->ReplicateObjectAttributesToSecondaryMaster(node, cellTag);
        }

        auto racks = GetValuesSortedByKey(RackMap_);
        for (auto* rack : racks) {
            if (!IsObjectAlive(rack)) {
                continue;
            }
            objectManager->ReplicateObjectAttributesToSecondaryMaster(rack, cellTag);
        }

        auto dcs = GetValuesSortedByKey(DataCenterMap_);
        for (auto* dc : dcs) {
            if (!IsObjectAlive(dc)) {
                continue;
            }
            objectManager->ReplicateObjectAttributesToSecondaryMaster(dc, cellTag);
        }
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
        auto statistics = GetTotalNodeStatistics();

        buffer.AddGauge("/available_space", statistics.TotalSpace.Available);
        buffer.AddGauge("/used_space", statistics.TotalSpace.Used);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (const auto& [mediumIndex, space] : statistics.SpacePerMedium) {
            const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
            if (!medium) {
                continue;
            }

            buffer.PushTag({"medium", medium->GetName()});
            buffer.AddGauge("/available_space_per_medium", space.Available);
            buffer.AddGauge("/used_space_per_medium", space.Used);
            buffer.PopTag();
        }

        buffer.AddGauge("/chunk_replica_count", statistics.ChunkReplicaCount);

        buffer.AddGauge("/online_node_count", statistics.OnlineNodeCount);
        buffer.AddGauge("/offline_node_count", statistics.OfflineNodeCount);
        buffer.AddGauge("/banned_node_count", statistics.BannedNodeCount);
        buffer.AddGauge("/decommissioned_node_count", statistics.DecommissinedNodeCount);
        buffer.AddGauge("/with_alerts_node_count", statistics.WithAlertsNodeCount);
        buffer.AddGauge("/full_node_count", statistics.FullNodeCount);

        for (auto nodeRole : TEnumTraits<ENodeRole>::GetDomainValues()) {
            buffer.PushTag({"node_role", FormatEnum(nodeRole)});
            buffer.AddGauge(
                "/node_count",
                NodeListPerRole_[nodeRole].Nodes().size());
        }

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

        for (const auto& address : PendingRegisterNodeAddreses_) {
            auto groups = GetGroupsForNode(address);
            for (auto* group : groups) {
                ++group->PendingRegisterNodeMutationCount;
            }
        }
    }

    void ReconfigureGossipPeriods()
    {
        if (IncrementalNodeStatesGossipExecutor_) {
            IncrementalNodeStatesGossipExecutor_->SetPeriod(GetDynamicConfig()->IncrementalNodeStatesGossipPeriod);
        }
        if (FullNodeStatesGossipExecutor_) {
            FullNodeStatesGossipExecutor_->SetPeriod(GetDynamicConfig()->FullNodeStatesGossipPeriod);
        }
    }

    void ReconfigureNodeSemaphores()
    {
        HeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentClusterNodeHeartbeats);
        FullHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentFullHeartbeats);
        IncrementalHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentIncrementalHeartbeats);
        DisposeNodeSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentNodeUnregistrations);
    }

    void RebuildTotalNodeStatistics()
    {
        TotalNodeStatistics_ = TTotalNodeStatistics();
        for (auto [nodeId, node] : NodeMap_) {
            if (!IsObjectAlive(node)) {
                continue;
            }

            TotalNodeStatistics_.BannedNodeCount += node->GetBanned();
            TotalNodeStatistics_.DecommissinedNodeCount += node->GetDecommissioned();
            TotalNodeStatistics_.WithAlertsNodeCount += !node->Alerts().empty();

            if (node->GetAggregatedState() != ENodeState::Online) {
                ++TotalNodeStatistics_.OfflineNodeCount;
                continue;
            }
            TotalNodeStatistics_.OnlineNodeCount += 1;

            const auto& statistics = node->DataNodeStatistics();
            for (const auto& location : statistics.storage_locations()) {
                int mediumIndex = location.medium_index();
                if (!node->GetDecommissioned()) {
                    TotalNodeStatistics_.SpacePerMedium[mediumIndex].Available += location.available_space();
                    TotalNodeStatistics_.TotalSpace.Available += location.available_space();
                }
                TotalNodeStatistics_.SpacePerMedium[mediumIndex].Used += location.used_space();
                TotalNodeStatistics_.TotalSpace.Used += location.used_space();
            }
            TotalNodeStatistics_.ChunkReplicaCount += statistics.total_stored_chunk_count();
            TotalNodeStatistics_.FullNodeCount += statistics.full() ? 1 : 0;
        }

        TotalNodeStatisticsUpdateDeadline_ =
            NProfiling::GetCpuInstant() +
            DurationToCpuDuration(GetDynamicConfig()->TotalNodeStatisticsUpdatePeriod);
    }

    const TDynamicNodeTrackerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->NodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/ = nullptr)
    {
        RebuildNodeGroups();
        RecomputePendingRegisterNodeMutationCounters();
        ReconfigureGossipPeriods();
        ReconfigureNodeSemaphores();
        RebuildTotalNodeStatistics();
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker::TImpl, Node, TNode, NodeMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker::TImpl, Rack, TRack, RackMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker::TImpl, DataCenter, TDataCenter, DataCenterMap_)

////////////////////////////////////////////////////////////////////////////////

TNodeTracker::TNodeTracker(TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TNodeTracker::~TNodeTracker() = default;

void TNodeTracker::Initialize()
{
    Impl_->Initialize();
}

TObjectId TNodeTracker::ObjectIdFromNodeId(TNodeId nodeId)
{
    return Impl_->ObjectIdFromNodeId(nodeId);
}

TNode* TNodeTracker::FindNode(TNodeId id)
{
    return Impl_->FindNode(id);
}

TNode* TNodeTracker::GetNode(TNodeId id)
{
    return Impl_->GetNode(id);
}

TNode* TNodeTracker::GetNodeOrThrow(TNodeId id)
{
    return Impl_->GetNodeOrThrow(id);
}

TNode* TNodeTracker::FindNodeByAddress(const TString& address)
{
    return Impl_->FindNodeByAddress(address);
}

TNode* TNodeTracker::GetNodeByAddress(const TString& address)
{
    return Impl_->GetNodeByAddress(address);
}

TNode* TNodeTracker::GetNodeByAddressOrThrow(const TString& address)
{
    return Impl_->GetNodeByAddressOrThrow(address);
}

TNode* TNodeTracker::FindNodeByHostName(const TString& hostName)
{
    return Impl_->FindNodeByHostName(hostName);
}

std::vector<TNode*> TNodeTracker::GetRackNodes(const TRack* rack)
{
    return Impl_->GetRackNodes(rack);
}

std::vector<TRack*> TNodeTracker::GetDataCenterRacks(const TDataCenter* dc)
{
    return Impl_->GetDataCenterRacks(dc);
}

void TNodeTracker::UpdateLastSeenTime(TNode* node)
{
    Impl_->UpdateLastSeenTime(node);
}

void TNodeTracker::SetNodeBanned(TNode* node, bool value)
{
    Impl_->SetNodeBanned(node, value);
}

void TNodeTracker::SetNodeDecommissioned(TNode* node, bool value)
{
    Impl_->SetNodeDecommissioned(node, value);
}

void TNodeTracker::SetDisableWriteSessions(TNode* node, bool value)
{
    Impl_->SetDisableWriteSessions(node, value);
}

void TNodeTracker::SetDisableTabletCells(TNode* node, bool value)
{
    Impl_->SetDisableTabletCells(node, value);
}

void TNodeTracker::SetNodeRack(TNode* node, TRack* rack)
{
    Impl_->SetNodeRack(node, rack);
}

void TNodeTracker::SetNodeUserTags(TNode* node, const std::vector<TString>& tags)
{
    Impl_->SetNodeUserTags(node, tags);
}

std::unique_ptr<TMutation> TNodeTracker::CreateUpdateNodeResourcesMutation(
    const NProto::TReqUpdateNodeResources& request)
{
    return Impl_->CreateUpdateNodeResourcesMutation(request);
}

void TNodeTracker::RenameRack(TRack* rack, const TString& newName)
{
    Impl_->RenameRack(rack, newName);
}

TRack* TNodeTracker::FindRackByName(const TString& name)
{
    return Impl_->FindRackByName(name);
}

TRack* TNodeTracker::GetRackByNameOrThrow(const TString& name)
{
    return Impl_->GetRackByNameOrThrow(name);
}

void TNodeTracker::SetRackDataCenter(TRack* rack, TDataCenter* dc)
{
    return Impl_->SetRackDataCenter(rack, dc);
}

void TNodeTracker::RenameDataCenter(TDataCenter* dc, const TString& newName)
{
    Impl_->RenameDataCenter(dc, newName);
}

TDataCenter* TNodeTracker::FindDataCenterByName(const TString& name)
{
    return Impl_->FindDataCenterByName(name);
}

TDataCenter* TNodeTracker::GetDataCenterByNameOrThrow(const TString& name)
{
    return Impl_->GetDataCenterByNameOrThrow(name);
}

void TNodeTracker::ProcessRegisterNode(
    const TString& address,
    TCtxRegisterNodePtr context)
{
    Impl_->ProcessRegisterNode(address, context);
}

void TNodeTracker::ProcessHeartbeat(TCtxHeartbeatPtr context)
{
    Impl_->ProcessHeartbeat(context);
}

void TNodeTracker::ProcessFullHeartbeat(TCtxFullHeartbeatPtr context)
{
    Impl_->ProcessFullHeartbeat(context);
}

void TNodeTracker::ProcessIncrementalHeartbeat(TCtxIncrementalHeartbeatPtr context)
{
    Impl_->ProcessIncrementalHeartbeat(context);
}

TTotalNodeStatistics TNodeTracker::GetTotalNodeStatistics()
{
    return Impl_->GetTotalNodeStatistics();
}

int TNodeTracker::GetOnlineNodeCount()
{
    return Impl_->GetOnlineNodeCount();
}

void TNodeTracker::ZombifyNode(TNode* node)
{
    Impl_->ZombifyNode(node);
}

TRack* TNodeTracker::CreateRack(const TString& name, TObjectId hintId)
{
    return Impl_->CreateRack(name, hintId);
}

void TNodeTracker::ZombifyRack(TRack* rack)
{
    Impl_->ZombifyRack(rack);
}

TDataCenter* TNodeTracker::CreateDataCenter(const TString& name, TObjectId hintId)
{
    return Impl_->CreateDataCenter(name, hintId);
}

void TNodeTracker::ZombifyDataCenter(TDataCenter* dc)
{
    Impl_->ZombifyDataCenter(dc);
}

const std::vector<TNode*>& TNodeTracker::GetNodesForRole(ENodeRole nodeRole)
{
    return Impl_->GetNodesForRole(nodeRole);
}

const std::vector<TString>& TNodeTracker::GetNodeAddressesForRole(ENodeRole nodeRole)
{
    return Impl_->GetNodeAddressesForRole(nodeRole);
}

void TNodeTracker::OnNodeHeartbeat(TNode* node, ENodeHeartbeatType heartbeatType)
{
    return Impl_->OnNodeHeartbeat(node, heartbeatType);
}

void TNodeTracker::RequestNodeHeartbeat(TNodeId nodeId)
{
    return Impl_->RequestNodeHeartbeat(nodeId);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TNodeTracker, Node, TNode, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TNodeTracker, Rack, TRack, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TNodeTracker, DataCenter, TDataCenter, *Impl_)

DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeRegistered, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeOnline, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeUnregistered, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeDisposed, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeBanChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeDecommissionChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeDisableTabletCellsChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeTagsChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*, TRack*), NodeRackChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*, TDataCenter*), NodeDataCenterChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TDataCenter*), DataCenterCreated, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TDataCenter*), DataCenterRenamed, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TDataCenter*), DataCenterDestroyed, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
