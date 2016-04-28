#include "node_tracker.h"
#include "private.h"
#include "config.h"
#include "node.h"
#include "node_proxy.h"
#include "rack.h"
#include "rack_proxy.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/chunk_server/job.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/node_tracker_server/node_tracker.pb.h>

#include <yt/server/object_server/attribute_set.h>
#include <yt/server/object_server/object_manager.h>
#include <yt/server/object_server/type_handler_detail.h>

#include <yt/server/transaction_server/transaction.h>
#include <yt/server/transaction_server/transaction_manager.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/address.h>
#include <yt/core/misc/id_generator.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <deque>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NHydra;
using namespace NHive;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NNodeTrackerServer::NProto;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NodeTrackerServerLogger;
static const auto ProfilingPeriod = TDuration::Seconds(1);
static const auto TotalNodeStatisticsUpdatePeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker::TClusterNodeTypeHandler
    : public TObjectTypeHandlerWithMapBase<TNode>
{
public:
    explicit TClusterNodeTypeHandler(TImpl* owner);

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::ClusterNode;
    }

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TNode* node) override
    {
        return AllSecondaryCellTags();
    }

    virtual Stroka DoGetName(const TNode* node) override
    {
        return Format("node %v", node->GetDefaultAddress());
    }

    virtual IObjectProxyPtr DoGetProxy(TNode* node, TTransaction* transaction) override;

    virtual void DoZombifyObject(TNode* node) override;

    virtual void DoResetObject(TNode* node) override
    {
        TObjectTypeHandlerWithMapBase::DoResetObject(node);
        node->Reset();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker::TRackTypeHandler
    : public TObjectTypeHandlerWithMapBase<TRack>
{
public:
    explicit TRackTypeHandler(TImpl* owner);

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Rack;
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes,
        const NObjectClient::NProto::TObjectCreationExtensions& extensions) override;

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TRack* /*rack*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual Stroka DoGetName(const TRack* rack) override
    {
        return Format("rack %Qv", rack->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TRack* rack, TTransaction* transaction) override;

    virtual void DoZombifyObject(TRack* rack) override;

};

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(
        TNodeTrackerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap)
        , Config_(config)
        , FullHeartbeatSemaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentFullHeartbeats))
        , IncrementalHeartbeatSemaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentIncrementalHeartbeats))
        , DisposeNodeSemaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentNodeUnregistrations))
    {
        RegisterMethod(BIND(&TImpl::HydraRegisterNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnregisterNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraDisposeNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraFullHeartbeat, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraIncrementalHeartbeat, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetNodeStates, Unretained(this)));

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

        auto* profileManager = NProfiling::TProfileManager::Get();
        Profiler.TagIds().push_back(profileManager->RegisterTag("cell_tag", Bootstrap_->GetCellTag()));
    }

    void Initialize()
    {
        auto transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TClusterNodeTypeHandler>(this));
        objectManager->RegisterHandler(New<TRackTypeHandler>(this));

        if (Bootstrap_->IsPrimaryMaster()) {
            auto multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->SubscribeValidateSecondaryMasterRegistration(
                BIND(&TImpl::OnValidateSecondaryMasterRegistration, MakeWeak(this)));
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND(&TImpl::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND(&TImpl::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }

    void ProcessRegisterNode(TCtxRegisterNodePtr context)
    {
        if (PendingRegisterNodeMutationCount_ + LocalRegisteredNodeCount_ >= Config_->MaxConcurrentNodeRegistrations) {
            context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Node registration throttling is active"));
            return;
        }

        ++PendingRegisterNodeMutationCount_;
        CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TImpl::HydraRegisterNode,
            this)
            ->CommitAndReply(context);
    }

    void ProcessFullHeartbeat(TCtxFullHeartbeatPtr context)
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TImpl::HydraFullHeartbeat,
            this);
        CommitMutationWithSemaphore(mutation, context, FullHeartbeatSemaphore_);
   }

    void ProcessIncrementalHeartbeat(TCtxIncrementalHeartbeatPtr context)
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TImpl::HydraIncrementalHeartbeat,
            this);
        CommitMutationWithSemaphore(mutation, context, IncrementalHeartbeatSemaphore_);
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Node, TNode);
    DECLARE_ENTITY_MAP_ACCESSORS(Rack, TRack);

    DEFINE_SIGNAL(void(TNode* node), NodeRegistered);
    DEFINE_SIGNAL(void(TNode* node), NodeUnregistered);
    DEFINE_SIGNAL(void(TNode* node), NodeDisposed);
    DEFINE_SIGNAL(void(TNode* node), NodeBanChanged);
    DEFINE_SIGNAL(void(TNode* node), NodeDecommissionChanged);
    DEFINE_SIGNAL(void(TNode* node), NodeRackChanged);
    DEFINE_SIGNAL(void(TNode* node, TReqFullHeartbeat* request), FullHeartbeat);
    DEFINE_SIGNAL(void(TNode* node, TReqIncrementalHeartbeat* request, TRspIncrementalHeartbeat* response), IncrementalHeartbeat);


    void DestroyNode(TNode* node)
    {
        auto nodeMapProxy = GetNodeMap();
        auto nodeNodeProxy = nodeMapProxy->FindChild(ToString(node->GetDefaultAddress()));
        if (nodeNodeProxy) {
            auto cypressManager = Bootstrap_->GetCypressManager();
            cypressManager->AbortSubtreeTransactions(nodeNodeProxy);
            nodeMapProxy->RemoveChild(nodeNodeProxy);
        }

        // NB: This is typically redundant since it's not possible to remove a node unless
        // it is offline. Secondary masters, however, may receive a removal request from primaries
        // and must obey it regardless of the node's state.
        EnsureNodeDisposed(node);

        RemoveFromAddressMaps(node);
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

    TNode* FindNodeByAddress(const Stroka& address)
    {
        auto it = AddressToNodeMap_.find(address);
        return it == AddressToNodeMap_.end() ? nullptr : it->second;
    }

    TNode* GetNodeByAddress(const Stroka& address)
    {
        auto* node = FindNodeByAddress(address);
        YCHECK(node);
        return node;
    }

    TNode* GetNodeByAddressOrThrow(const Stroka& address)
    {
        auto* node = FindNodeByAddress(address);
        if (!node) {
            THROW_ERROR_EXCEPTION("No such cluster node %v", address);
        }
        return node;
    }

    TNode* FindNodeByHostName(const Stroka& hostName)
    {
        auto it = HostNameToNodeMap_.find(hostName);
        return it == HostNameToNodeMap_.end() ? nullptr : it->second;
    }

    std::vector<TNode*> GetRackNodes(const TRack* rack)
    {
        std::vector<TNode*> result;
        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            if (!IsObjectAlive(node))
                continue;
            if (node->GetRack() == rack) {
                result.push_back(node);
            }
        }
        return result;
    }


    void SetNodeBanned(TNode* node, bool value)
    {
        if (node->GetBanned() != value) {
            node->SetBanned(value);
            if (value) {
                LOG_INFO_UNLESS(IsRecovery(), "Node banned (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
                if (Bootstrap_->IsPrimaryMaster()) {
                    auto state = node->GetLocalState();
                    if (state == ENodeState::Online || state == ENodeState::Registered) {
                        UnregisterNode(node, true);
                    }
                }
            } else {
                LOG_INFO_UNLESS(IsRecovery(), "Node is no longer banned (NodeId: %v, Address: %v)",
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
                LOG_INFO_UNLESS(IsRecovery(), "Node decommissioned (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            } else {
                LOG_INFO_UNLESS(IsRecovery(), "Node is no longer decommissioned (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            }
            NodeDecommissionChanged_.Fire(node);
        }
    }

    void SetNodeRack(TNode* node, TRack* rack)
    {
        if (node->GetRack() != rack) {
            node->SetRack(rack);
            LOG_INFO_UNLESS(IsRecovery(), "Node rack changed (NodeId: %v, Address: %v, Rack: %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                rack ? MakeNullable(rack->GetName()) : Null);
            NodeRackChanged_.Fire(node);
        }
    }


    TRack* CreateRack(const Stroka& name, const TObjectId& hintId)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Rack name cannot be empty");
        }

        if (FindRackByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Rack %Qv already exists",
                name);
        }

        if (RackMap_.GetSize() >= MaxRackCount) {
            THROW_ERROR_EXCEPTION("Rack count limit %v is reached",
                MaxRackCount);
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Rack, hintId);

        auto rackHolder = std::make_unique<TRack>(id);
        rackHolder->SetName(name);
        rackHolder->SetIndex(AllocateRackIndex());

        auto* rack = RackMap_.Insert(id, std::move(rackHolder));
        YCHECK(NameToRackMap_.insert(std::make_pair(name, rack)).second);

        // Make the fake reference.
        YCHECK(rack->RefObject() == 1);

        return rack;
    }

    void DestroyRack(TRack* rack)
    {
        // Unbind nodes from this rack.
        for (auto* node : GetRackNodes(rack)) {
            SetNodeRack(node, nullptr);
        }

        // Remove rack from maps.
        YCHECK(NameToRackMap_.erase(rack->GetName()) == 1);
        FreeRackIndex(rack->GetIndex());
    }

    void RenameRack(TRack* rack, const Stroka& newName)
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
        YCHECK(NameToRackMap_.erase(rack->GetName()) == 1);
        YCHECK(NameToRackMap_.insert(std::make_pair(newName, rack)).second);
        rack->SetName(newName);
    }

    TRack* FindRackByName(const Stroka& name)
    {
        auto it = NameToRackMap_.find(name);
        return it == NameToRackMap_.end() ? nullptr : it->second;
    }

    TRack* GetRackByNameOrThrow(const Stroka& name)
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


    TTotalNodeStatistics GetTotalNodeStatistics()
    {
        auto now = NProfiling::GetCpuInstant();
        if (now < TotalNodeStatisticsUpdateDeadline_) {
            return TotalNodeStatistics_;
        }

        TotalNodeStatistics_ = TTotalNodeStatistics();
        for (const auto& pair : NodeMap_) {
            const auto* node = pair.second;
            if (node->GetAggregatedState() != ENodeState::Online) {
                continue;
            }
            const auto& statistics = node->Statistics();
            if (!node->GetDecommissioned()) {
                TotalNodeStatistics_.AvailableSpace += statistics.total_available_space();
            }
            TotalNodeStatistics_.UsedSpace += statistics.total_used_space();
            TotalNodeStatistics_.ChunkReplicaCount += statistics.total_stored_chunk_count();
            TotalNodeStatistics_.FullNodeCount += statistics.full() ? 1 : 0;
            TotalNodeStatistics_.OnlineNodeCount += 1;
        }
        TotalNodeStatisticsUpdateDeadline_ = now + NProfiling::DurationToCpuDuration(TotalNodeStatisticsUpdatePeriod);
        return TotalNodeStatistics_;
    }

    int GetOnlineNodeCount()
    {
        return AggregatedOnlineNodeCount_;
    }

private:
    friend class TClusterNodeTypeHandler;
    friend class TRackTypeHandler;

    const TNodeTrackerConfigPtr Config_;

    TPeriodicExecutorPtr ProfilingExecutor_;

    NProfiling::TProfiler Profiler = NodeTrackerServerProfiler;

    TIdGenerator NodeIdGenerator_;
    NHydra::TEntityMap<TNode> NodeMap_;
    NHydra::TEntityMap<TRack> RackMap_;

    int AggregatedOnlineNodeCount_ = 0;
    int LocalRegisteredNodeCount_ = 0;

    NProfiling::TCpuInstant TotalNodeStatisticsUpdateDeadline_ = 0;
    TTotalNodeStatistics TotalNodeStatistics_;

    TRackSet UsedRackIndexes_;

    yhash_map<Stroka, TNode*> AddressToNodeMap_;
    yhash_multimap<Stroka, TNode*> HostNameToNodeMap_;
    yhash_map<TTransaction*, TNode*> TransactionToNodeMap_;
    yhash_map<Stroka, TRack*> NameToRackMap_;

    TPeriodicExecutorPtr NodeStatesGossipExecutor_;

    int PendingRegisterNodeMutationCount_ = 0;

    TAsyncSemaphorePtr FullHeartbeatSemaphore_;
    TAsyncSemaphorePtr IncrementalHeartbeatSemaphore_;
    TAsyncSemaphorePtr DisposeNodeSemaphore_;


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

    TObjectId ObjectIdFromNodeId(TNodeId nodeId)
    {
        return NNodeTrackerClient::ObjectIdFromNodeId(
            nodeId,
            Bootstrap_->GetPrimaryCellTag());
    }


    static TYPath GetNodePath(const Stroka& address)
    {
        return "//sys/nodes/" + ToYPathLiteral(address);
    }

    static TYPath GetNodePath(TNode* node)
    {
        return GetNodePath(node->GetDefaultAddress());
    }

    IMapNodePtr GetNodeMap()
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto resolver = cypressManager->CreateResolver();
        auto node = resolver->ResolvePath("//sys/nodes");
        return node->AsMap();
    }


    void HydraRegisterNode(TCtxRegisterNodePtr context, TReqRegisterNode* request, TRspRegisterNode* response)
    {
        if (Bootstrap_->IsPrimaryMaster() && IsLeader()) {
            YCHECK(--PendingRegisterNodeMutationCount_ >= 0);
        }

        auto addresses = FromProto<TAddressMap>(request->addresses());
        const auto& address = GetDefaultAddress(addresses);
        const auto& statistics = request->statistics();
        auto leaseTransactionId = request->has_lease_transaction_id()
            ? FromProto<TTransactionId>(request->lease_transaction_id())
            : NullTransactionId;

        // Check lease transaction.
        TTransaction* leaseTransaction = nullptr;
        if (leaseTransactionId) {
            YCHECK(Bootstrap_->IsPrimaryMaster());
            auto transactionManager = Bootstrap_->GetTransactionManager();
            leaseTransaction = transactionManager->GetTransactionOrThrow(leaseTransactionId);
            if (leaseTransaction->GetPersistentState() != ETransactionState::Active) {
                leaseTransaction->ThrowInvalidState();
            }
        }

        // Kick-out any previous incarnation.
        auto* node = FindNodeByAddress(address);
        if (IsObjectAlive(node)) {
            if (node->GetBanned()) {
                THROW_ERROR_EXCEPTION("Node %v is banned", address);
            }

            if (Bootstrap_->IsPrimaryMaster()) {
                auto localState = node->GetLocalState();
                if (localState == ENodeState::Registered || localState == ENodeState::Online) {
                    LOG_INFO_UNLESS(IsRecovery(), "Kicking node out due to address conflict (NodeId: %v, Address: %v, State: %v)",
                        node->GetId(),
                        address,
                        localState);
                    UnregisterNode(node, true);
                }

                auto aggregatedState = node->GetAggregatedState();
                if (aggregatedState != ENodeState::Offline) {
                    THROW_ERROR_EXCEPTION("Node %v is still in %Qlv state; must wait for it to become fully offline",
                        node->GetDefaultAddress(),
                        aggregatedState);
                }
            } else {
                EnsureNodeDisposed(node);
            }

            UpdateNode(node, addresses);
        } else {
            auto nodeId = request->has_node_id() ? request->node_id() : GenerateNodeId();
            node = CreateNode(nodeId, addresses);
        }

        node->SetLocalState(ENodeState::Registered);
        node->Statistics() = statistics;

        UpdateLastSeenTime(node);
        UpdateRegisterTime(node);
        UpdateNodeCounters(node, +1);

        if (leaseTransaction) {
            node->SetLeaseTransaction(leaseTransaction);
            RegisterLeaseTransaction(node);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Node registered (NodeId: %v, Address: %v, LeaseTransactionId: %v, %v)",
            node->GetId(),
            address,
            leaseTransactionId,
            statistics);

        NodeRegistered_.Fire(node);

        if (Bootstrap_->IsPrimaryMaster()) {
            PostRegisterNodeMutation(node);
        }

        response->set_node_id(node->GetId());

        if (context) {
            context->SetResponseInfo("NodeId: %v",
                node->GetId());
        }
    }

    void HydraUnregisterNode(TReqUnregisterNode* request)
    {
        auto nodeId = request->node_id();

        auto* node = FindNode(nodeId);
        if (!IsObjectAlive(node))
            return;

        auto state = node->GetLocalState();
        if (state != ENodeState::Registered && state != ENodeState::Online)
            return;

        UnregisterNode(node, true);
    }

    void HydraDisposeNode(TReqDisposeNode* request)
    {
        auto nodeId = request->node_id();
        auto* node = FindNode(nodeId);
        if (!IsObjectAlive(node))
            return;

        if (node->GetLocalState() != ENodeState::Unregistered)
            return;

        DisposeNode(node);
    }

    void HydraFullHeartbeat(TCtxFullHeartbeatPtr /*context*/, TReqFullHeartbeat* request, TRspFullHeartbeat* /*response*/)
    {
        auto nodeId = request->node_id();
        const auto& statistics = request->statistics();

        auto* node = GetNodeOrThrow(nodeId);
        if (node->GetLocalState() != ENodeState::Registered) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process a full heartbeat in %Qlv state",
                node->GetLocalState());
        }

        PROFILE_TIMING ("/full_heartbeat_time") {
            LOG_DEBUG_UNLESS(IsRecovery(), "Processing full heartbeat (NodeId: %v, Address: %v, State: %v, %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            UpdateNodeCounters(node, -1);
            node->SetLocalState(ENodeState::Online);
            UpdateNodeCounters(node, +1);

            node->Statistics() = statistics;

            UpdateLastSeenTime(node);

            LOG_INFO_UNLESS(IsRecovery(), "Node online (NodeId: %v, Address: %v)",
                nodeId,
                node->GetDefaultAddress());

            FullHeartbeat_.Fire(node, request);
        }
    }

    void HydraIncrementalHeartbeat(TCtxIncrementalHeartbeatPtr context, TReqIncrementalHeartbeat* request, TRspIncrementalHeartbeat* response)
    {
        auto nodeId = request->node_id();
        const auto& statistics = request->statistics();

        auto* node = GetNodeOrThrow(nodeId);
        if (node->GetLocalState() != ENodeState::Online) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process an incremental heartbeat in %Qlv state",
                node->GetLocalState());
        }

        PROFILE_TIMING ("/incremental_heartbeat_time") {
            LOG_DEBUG_UNLESS(IsRecovery(), "Processing incremental heartbeat (NodeId: %v, Address: %v, State: %v, %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            node->Statistics() = statistics;
            node->Alerts() = FromProto<std::vector<TError>>(request->alerts());

            UpdateLastSeenTime(node);

            if (Bootstrap_->IsPrimaryMaster()) {
                if (node->GetRack()) {
                    response->set_rack(node->GetRack()->GetName());
                }

                *response->mutable_resource_limits_overrides() = node->ResourceLimitsOverrides();
            }

            IncrementalHeartbeat_.Fire(node, request, response);
        }
    }

    void HydraSetNodeStates(TReqSetNodeStates* request)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());

        auto cellTag = request->cell_tag();

        auto multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            LOG_ERROR_UNLESS(IsRecovery(), "Received node states gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Received node states gossip message (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
            auto* node = FindNode(entry.node_id());
            if (!IsObjectAlive(node))
                continue;

            UpdateNodeCounters(node, -1);
            node->MulticellStates()[cellTag] = ENodeState(entry.state());
            UpdateNodeCounters(node, +1);
        }
    }


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveKeys(context);
        RackMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        Save(context, NodeIdGenerator_);
        NodeMap_.SaveValues(context);
        RackMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        NodeMap_.LoadKeys(context);
        RackMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        Load(context, NodeIdGenerator_);
        NodeMap_.LoadValues(context);
        RackMap_.LoadValues(context);
    }


    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        NodeIdGenerator_.Reset();
        NodeMap_.Clear();
        RackMap_.Clear();

        AddressToNodeMap_.clear();
        HostNameToNodeMap_.clear();
        TransactionToNodeMap_.clear();

        NameToRackMap_.clear();

        AggregatedOnlineNodeCount_ = 0;
        LocalRegisteredNodeCount_ = 0;
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        AddressToNodeMap_.clear();
        HostNameToNodeMap_.clear();
        TransactionToNodeMap_.clear();

        AggregatedOnlineNodeCount_ = 0;
        LocalRegisteredNodeCount_ = 0;

        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;

            InitializeNodeStates(node);
            InsertToAddressMaps(node);
            UpdateNodeCounters(node, +1);

            if (node->GetLeaseTransaction()) {
                RegisterLeaseTransaction(node);
            }
        }

        UsedRackIndexes_.reset();
        for (const auto& pair : RackMap_) {
            auto* rack = pair.second;

            YCHECK(NameToRackMap_.insert(std::make_pair(rack->GetName(), rack)).second);

            auto rackIndex = rack->GetIndex();
            YCHECK(!UsedRackIndexes_.test(rackIndex));
            UsedRackIndexes_.set(rackIndex);
        }
    }

    virtual void OnRecoveryStarted() override
    {
        TMasterAutomatonPart::OnRecoveryStarted();

        Profiler.SetEnabled(false);
    }

    virtual void OnRecoveryComplete() override
    {
        TMasterAutomatonPart::OnRecoveryComplete();

        Profiler.SetEnabled(true);
    }

    virtual void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        // NB: Node states gossip is one way: secondary-to-primary.
        if (Bootstrap_->IsSecondaryMaster()) {
            NodeStatesGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
                BIND(&TImpl::OnNodeStatesGossip, MakeWeak(this)),
                Config_->NodeStatesGossipPeriod);
            NodeStatesGossipExecutor_->Start();
        }

        PendingRegisterNodeMutationCount_ = 0;

        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            if (node->GetLocalState() == ENodeState::Unregistered) {
                CommitDisposeNodeWithSemaphore(node);
            }
        }
    }

    virtual void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        if (NodeStatesGossipExecutor_) {
            NodeStatesGossipExecutor_->Stop();
            NodeStatesGossipExecutor_.Reset();
        }
    }


    void InitializeNodeStates(TNode* node)
    {
        auto cellTag = Bootstrap_->GetCellTag();
        const auto& secondaryCellTags = Bootstrap_->GetSecondaryCellTags();

        auto& multicellStates = node->MulticellStates();
        if (multicellStates.find(cellTag) == multicellStates.end()) {
            multicellStates[cellTag] = ENodeState::Offline;
        }

        for (auto secondaryCellTag : secondaryCellTags) {
            if (multicellStates.find(secondaryCellTag) == multicellStates.end()) {
                multicellStates[secondaryCellTag] = ENodeState::Offline;
            }
        }

        node->SetLocalStatePtr(&multicellStates[cellTag]);
    }

    void UpdateNodeCounters(TNode* node, int delta)
    {
        if (node->GetLocalState() == ENodeState::Registered) {
            LocalRegisteredNodeCount_ += delta;
        }
        if (node->GetAggregatedState() == ENodeState::Online) {
            AggregatedOnlineNodeCount_ += delta;
        }
    }


    void RegisterLeaseTransaction(TNode* node)
    {
        auto* transaction = node->GetLeaseTransaction();
        YCHECK(transaction);
        YCHECK(transaction->GetPersistentState() == ETransactionState::Active);
        YCHECK(TransactionToNodeMap_.insert(std::make_pair(transaction, node)).second);
    }

    TTransaction* UnregisterLeaseTransaction(TNode* node)
    {
        auto* transaction = node->GetLeaseTransaction();
        if (transaction) {
            YCHECK(TransactionToNodeMap_.erase(transaction) == 1);
        }
        node->SetLeaseTransaction(nullptr);
        return transaction;
    }

    void UpdateRegisterTime(TNode* node)
    {
        const auto* mutationContext = GetCurrentMutationContext();
        node->SetRegisterTime(mutationContext->GetTimestamp());
    }

    void UpdateLastSeenTime(TNode* node)
    {
        const auto* mutationContext = GetCurrentMutationContext();
        node->SetLastSeenTime(mutationContext->GetTimestamp());
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        auto it = TransactionToNodeMap_.find(transaction);
        if (it == TransactionToNodeMap_.end())
            return;

        auto* node = it->second;
        LOG_INFO_UNLESS(IsRecovery(), "Node lease transaction finished (NodeId: %v, Address: %v, TransactionId: %v)",
            node->GetId(),
            node->GetDefaultAddress(),
            transaction->GetId());

        UnregisterNode(node, true);
    }


    TNode* CreateNode(TNodeId nodeId, const TAddressMap& addresses)
    {
        auto objectId = ObjectIdFromNodeId(nodeId);

        auto nodeHolder = std::make_unique<TNode>(
            objectId,
            addresses);

        auto* node = NodeMap_.Insert(objectId, std::move(nodeHolder));

        // Make the fake reference.
        YCHECK(node->RefObject() == 1);

        InitializeNodeStates(node);
        InsertToAddressMaps(node);

        auto objectManager = Bootstrap_->GetObjectManager();
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

                auto attributes = CreateEphemeralAttributes();
                attributes->Set("remote_address", GetInterconnectAddress(addresses));
                ToProto(req->mutable_node_attributes(), *attributes);

                SyncExecuteVerb(rootService, req);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR_UNLESS(IsRecovery(), ex, "Error registering cluster node in Cypress");
        }

        return node;
    }

    void UpdateNode(TNode* node, const TAddressMap& addresses)
    {
        // NB: The default address must remain same, however others may change.
        node->SetAddresses(addresses);

        auto objectManager = Bootstrap_->GetObjectManager();
        auto rootService = objectManager->GetRootService();
        auto nodePath = GetNodePath(node);

        try {
            // Update "orchid" child.
            auto req = TYPathProxy::Set(nodePath + "/orchid/@remote_address");
            req->set_value(ConvertToYsonString(GetInterconnectAddress(addresses)).Data());

            SyncExecuteVerb(rootService, req);
        } catch (const std::exception& ex) {
            LOG_ERROR_UNLESS(IsRecovery(), ex, "Error updating cluster node in Cypress");
        }
    }

    void UnregisterNode(TNode* node, bool propagate)
    {
        PROFILE_TIMING ("/node_unregister_time") {
            auto* transaction = UnregisterLeaseTransaction(node);
            if (IsObjectAlive(transaction)) {
                auto transactionManager = Bootstrap_->GetTransactionManager();
                // NB: This will trigger OnTransactionFinished, however we've already evicted the
                // lease so the latter call is no-op.
                transactionManager->AbortTransaction(transaction, true);
            }

            UpdateNodeCounters(node, -1);
            node->SetLocalState(ENodeState::Unregistered);
            NodeUnregistered_.Fire(node);

            if (propagate) {
                if (IsLeader()) {
                    CommitDisposeNodeWithSemaphore(node);
                }
                if (Bootstrap_->IsPrimaryMaster()) {
                    PostUnregisterNodeMutation(node);
                }
            }

            LOG_INFO_UNLESS(IsRecovery(), "Node unregistered (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }
    }

    void DisposeNode(TNode* node)
    {
        PROFILE_TIMING ("/node_dispose_time") {
            node->SetLocalState(ENodeState::Offline);
            NodeDisposed_.Fire(node);

            LOG_INFO_UNLESS(IsRecovery(), "Node offline (NodeId: %v, Address: %v)",
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


    void OnNodeStatesGossip()
    {
        auto multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

        LOG_INFO("Sending node states gossip message");

        TReqSetNodeStates request;
        request.set_cell_tag(Bootstrap_->GetCellTag());
        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            if (!IsObjectAlive(node))
                continue;

            auto* entry = request.add_entries();
            entry->set_node_id(node->GetId());
            entry->set_state(static_cast<int>(node->GetLocalState()));
        }

        multicellManager->PostToMaster(request, PrimaryMasterCellTag, false);
    }


    void CommitMutationWithSemaphore(TMutationPtr mutation, NRpc::IServiceContextPtr context, TAsyncSemaphorePtr semaphore)
    {
        auto handler = BIND([=] (TAsyncSemaphoreGuard) {
            WaitFor(mutation->CommitAndReply(context));
        });

        auto invoker = Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker();

        semaphore->AsyncAcquire(handler, invoker);
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

        auto handler = BIND([=] (TAsyncSemaphoreGuard) {
            WaitFor(mutation->CommitAndLog(NodeTrackerServerLogger));
        });

        auto invoker = Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker();

        DisposeNodeSemaphore_->AsyncAcquire(handler, invoker);
    }


    void PostRegisterNodeMutation(TNode* node)
    {
        TReqRegisterNode request;
        request.set_node_id(node->GetId());
        ToProto(request.mutable_addresses(), node->GetAddresses());
        *request.mutable_statistics() = node->Statistics();

        auto multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToSecondaryMasters(request);
    }

    void PostUnregisterNodeMutation(TNode* node)
    {
        TReqUnregisterNode request;
        request.set_node_id(node->GetId());

        auto multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToSecondaryMasters(request);
    }


    int AllocateRackIndex()
    {
        for (int index = 0; index < UsedRackIndexes_.size(); ++index) {
            if (index == NullRackIndex) {
                continue;
            }
            if (!UsedRackIndexes_.test(index)) {
                UsedRackIndexes_.set(index);
                return index;
            }
        }
        YUNREACHABLE();
    }

    void FreeRackIndex(int index)
    {
        YCHECK(UsedRackIndexes_.test(index));
        UsedRackIndexes_.reset(index);
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
        auto multicellManager = Bootstrap_->GetMulticellManager();
        auto objectManager = Bootstrap_->GetObjectManager();

        auto nodes = GetValuesSortedByKey(NodeMap_);
        for (const auto* node : nodes) {
            // NB: TReqRegisterNode+TReqUnregisterNode create an offline node at the secondary master.
            {
                TReqRegisterNode request;
                request.set_node_id(node->GetId());
                ToProto(request.mutable_addresses(), node->GetAddresses());
                *request.mutable_statistics() = node->Statistics();
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
            objectManager->ReplicateObjectCreationToSecondaryMaster(rack, cellTag);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        auto objectManager = Bootstrap_->GetObjectManager();

        auto nodes = GetValuesSortedByKey(NodeMap_);
        for (auto* node : nodes) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(node, cellTag);
        }

        auto racks = GetValuesSortedByKey(RackMap_);
        for (auto* rack : racks) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(rack, cellTag);
        }
    }

    void InsertToAddressMaps(TNode* node)
    {
        YCHECK(AddressToNodeMap_.insert(std::make_pair(node->GetDefaultAddress(), node)).second);
        for (const auto& pair : node->GetAddresses()) {
            HostNameToNodeMap_.insert(std::make_pair(Stroka(GetServiceHostName(pair.second)), node));
        }
    }

    void RemoveFromAddressMaps(TNode* node)
    {
        YCHECK(AddressToNodeMap_.erase(node->GetDefaultAddress()) == 1);
        for (const auto& pair : node->GetAddresses()) {
            auto hostNameRange = HostNameToNodeMap_.equal_range(Stroka(GetServiceHostName(pair.second)));
            for (auto it = hostNameRange.first; it != hostNameRange.second; ++it) {
                if (it->second == node) {
                    HostNameToNodeMap_.erase(it);
                    break;
                }
            }
        }
    }


    void OnProfiling()
    {
        if (!IsLeader()) {
            return;
        }

        auto statistics = GetTotalNodeStatistics();
        Profiler.Enqueue("/available_space", statistics.AvailableSpace);
        Profiler.Enqueue("/used_space", statistics.UsedSpace);
        Profiler.Enqueue("/chunk_replica_count", statistics.ChunkReplicaCount);
        Profiler.Enqueue("/online_node_count", statistics.OnlineNodeCount);
        Profiler.Enqueue("/full_node_count", statistics.FullNodeCount);
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker::TImpl, Node, TNode, NodeMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker::TImpl, Rack, TRack, RackMap_)

///////////////////////////////////////////////////////////////////////////////

TNodeTracker::TNodeTracker(
    TNodeTrackerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

void TNodeTracker::Initialize()
{
    Impl_->Initialize();
}

TNodeTracker::~TNodeTracker() = default;

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

TNode* TNodeTracker::FindNodeByAddress(const Stroka& address)
{
    return Impl_->FindNodeByAddress(address);
}

TNode* TNodeTracker::GetNodeByAddress(const Stroka& address)
{
    return Impl_->GetNodeByAddress(address);
}

TNode* TNodeTracker::GetNodeByAddressOrThrow(const Stroka& address)
{
    return Impl_->GetNodeByAddressOrThrow(address);
}

TNode* TNodeTracker::FindNodeByHostName(const Stroka& hostName)
{
    return Impl_->FindNodeByHostName(hostName);
}

std::vector<TNode*> TNodeTracker::GetRackNodes(const TRack* rack)
{
    return Impl_->GetRackNodes(rack);
}

void TNodeTracker::SetNodeBanned(TNode* node, bool value)
{
    Impl_->SetNodeBanned(node, value);
}

void TNodeTracker::SetNodeDecommissioned(TNode* node, bool value)
{
    Impl_->SetNodeDecommissioned(node, value);
}

void TNodeTracker::SetNodeRack(TNode* node, TRack* rack)
{
    Impl_->SetNodeRack(node, rack);
}

TRack* TNodeTracker::CreateRack(const Stroka& name)
{
    return Impl_->CreateRack(name, NullObjectId);
}

void TNodeTracker::DestroyRack(TRack* rack)
{
    Impl_->DestroyRack(rack);
}

void TNodeTracker::RenameRack(TRack* rack, const Stroka& newName)
{
    Impl_->RenameRack(rack, newName);
}

TRack* TNodeTracker::FindRackByName(const Stroka& name)
{
    return Impl_->FindRackByName(name);
}

TRack* TNodeTracker::GetRackByNameOrThrow(const Stroka& name)
{
    return Impl_->GetRackByNameOrThrow(name);
}

void TNodeTracker::ProcessRegisterNode(TCtxRegisterNodePtr context)
{
    Impl_->ProcessRegisterNode(context);
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

DELEGATE_ENTITY_MAP_ACCESSORS(TNodeTracker, Node, TNode, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TNodeTracker, Rack, TRack, *Impl_)

DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeRegistered, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeUnregistered, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeDisposed, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeBanChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeDecommissionChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeRackChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*, TReqFullHeartbeat*), FullHeartbeat, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*, TReqIncrementalHeartbeat*, TRspIncrementalHeartbeat*), IncrementalHeartbeat, *Impl_);

///////////////////////////////////////////////////////////////////////////////

TNodeTracker::TClusterNodeTypeHandler::TClusterNodeTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->NodeMap_)
    , Owner_(owner)
{ }

IObjectProxyPtr TNodeTracker::TClusterNodeTypeHandler::DoGetProxy(
    TNode* node,
    TTransaction* /*transaction*/)
{
    return CreateClusterNodeProxy(Owner_->Bootstrap_, node);
}

void TNodeTracker::TClusterNodeTypeHandler::DoZombifyObject(TNode* node)
{
    TObjectTypeHandlerWithMapBase::DoZombifyObject(node);
    // NB: Destroy the node right away and do not wait for GC to prevent
    // dangling links from occuring in //sys/nodes.
    Owner_->DestroyNode(node);
}

///////////////////////////////////////////////////////////////////////////////

TNodeTracker::TRackTypeHandler::TRackTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->RackMap_)
    , Owner_(owner)
{ }

TObjectBase* TNodeTracker::TRackTypeHandler::CreateObject(
    const TObjectId& hintId,
    IAttributeDictionary* attributes,
    const NObjectClient::NProto::TObjectCreationExtensions& extensions)
{
    auto name = attributes->Get<Stroka>("name");
    attributes->Remove("name");

    return Owner_->CreateRack(name, hintId);
}

IObjectProxyPtr TNodeTracker::TRackTypeHandler::DoGetProxy(
    TRack* rack,
    TTransaction* /*transaction*/)
{
    return CreateRackProxy(Owner_->Bootstrap_, rack);
}

void TNodeTracker::TRackTypeHandler::DoZombifyObject(TRack* rack)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(rack);
    Owner_->DestroyRack(rack);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
