#include "node_tracker.h"
#include "private.h"
#include "config.h"
#include "node.h"
#include "node_proxy.h"
#include "rack.h"
#include "rack_proxy.h"
#include "data_center.h"
#include "data_center_proxy.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>
#include <yt/server/cell_master/config_manager.h>
#include <yt/server/cell_master/config.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/chunk_server/chunk_manager.h>
#include <yt/server/chunk_server/job.h>
#include <yt/server/chunk_server/medium.h>

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

#include <yt/core/net/address.h>

#include <yt/core/misc/id_generator.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NNodeTrackerServer {

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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NodeTrackerServerLogger;
static const auto ProfilingPeriod = TDuration::Seconds(10);
static const auto TotalNodeStatisticsUpdatePeriod = TDuration::Seconds(1);

static NProfiling::TAggregateCounter FullHeartbeatTimeCounter("/full_heartbeat_time");
static NProfiling::TAggregateCounter IncrementalHeartbeatTimeCounter("/incremental_heartbeat_time");
static NProfiling::TAggregateCounter NodeUnregisterTimeCounter("/node_unregister_time");
static NProfiling::TAggregateCounter NodeDisposeTimeCounter("/node_dispose_time");

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

    virtual TString DoGetName(const TNode* node) override
    {
        return Format("node %v", node->GetDefaultAddress());
    }

    virtual IObjectProxyPtr DoGetProxy(TNode* node, TTransaction* transaction) override;

    virtual void DoZombifyObject(TNode* node) override;
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
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TRack* /*rack*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual TString DoGetName(const TRack* rack) override
    {
        return Format("rack %Qv", rack->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TRack* rack, TTransaction* transaction) override;

    virtual void DoZombifyObject(TRack* rack) override;

};

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker::TDataCenterTypeHandler
    : public TObjectTypeHandlerWithMapBase<TDataCenter>
{
public:
    explicit TDataCenterTypeHandler(TImpl* owner);

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
        return EObjectType::DataCenter;
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TDataCenter* /*dc*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual TString DoGetName(const TDataCenter* dc) override
    {
        return Format("DC %Qv", dc->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TDataCenter* dc, TTransaction* transaction) override;

    virtual void DoZombifyObject(TDataCenter* dc) override;

};

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(
        TNodeTrackerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::NodeTracker)
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

        auto* profileManager = TProfileManager::Get();
        Profiler.TagIds().push_back(profileManager->RegisterTag("cell_tag", Bootstrap_->GetCellTag()));
    }

    void Initialize()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TImpl::OnConfigChanged, MakeWeak(this)));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TClusterNodeTypeHandler>(this));
        objectManager->RegisterHandler(New<TRackTypeHandler>(this));
        objectManager->RegisterHandler(New<TDataCenterTypeHandler>(this));

        if (Bootstrap_->IsPrimaryMaster()) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
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

        YCHECK(PendingRegisterNodeAddreses_.insert(address).second);
        for (auto* group : groups) {
            ++group->PendingRegisterNodeMutationCount;
        }

        LOG_DEBUG("Node register mutation scheduled (Address: %v, NodeGroups: %v)",
            address,
            MakeFormattableRange(groups, [] (auto* builder, const auto* group) {
                builder->AppendFormat("%v", group->Id);
            }));

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
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), FullHeartbeatSemaphore_);
   }

    void ProcessIncrementalHeartbeat(TCtxIncrementalHeartbeatPtr context)
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TImpl::HydraIncrementalHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), IncrementalHeartbeatSemaphore_);
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Node, TNode);
    DECLARE_ENTITY_MAP_ACCESSORS(Rack, TRack);
    DECLARE_ENTITY_MAP_ACCESSORS(DataCenter, TDataCenter);

    DEFINE_SIGNAL(void(TNode* node), NodeRegistered);
    DEFINE_SIGNAL(void(TNode* node), NodeUnregistered);
    DEFINE_SIGNAL(void(TNode* node), NodeDisposed);
    DEFINE_SIGNAL(void(TNode* node), NodeBanChanged);
    DEFINE_SIGNAL(void(TNode* node), NodeDecommissionChanged);
    DEFINE_SIGNAL(void(TNode* node), NodeDisableTabletCellsChanged);
    DEFINE_SIGNAL(void(TNode* node), NodeTagsChanged);
    DEFINE_SIGNAL(void(TNode* node, TRack*), NodeRackChanged);
    DEFINE_SIGNAL(void(TNode* node, TDataCenter*), NodeDataCenterChanged);
    DEFINE_SIGNAL(void(TNode* node, TReqFullHeartbeat* request), FullHeartbeat);
    DEFINE_SIGNAL(void(TNode* node, TReqIncrementalHeartbeat* request, TRspIncrementalHeartbeat* response), IncrementalHeartbeat);
    DEFINE_SIGNAL(void(TDataCenter*), DataCenterCreated);
    DEFINE_SIGNAL(void(TDataCenter*), DataCenterRenamed);
    DEFINE_SIGNAL(void(TDataCenter*), DataCenterDestroyed);

    void DestroyNode(TNode* node)
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
        YCHECK(node);
        return node;
    }

    TNode* GetNodeByAddressOrThrow(const TString& address)
    {
        auto* node = FindNodeByAddress(address);
        if (!node) {
            THROW_ERROR_EXCEPTION("No such cluster node %v", address);
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

    std::vector<TRack*> GetDataCenterRacks(const TDataCenter* dc)
    {
        std::vector<TRack*> result;
        for (const auto& pair : RackMap_) {
            auto* rack = pair.second;
            if (!IsObjectAlive(rack))
                continue;
            if (rack->GetDataCenter() == dc) {
                result.push_back(rack);
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

    void SetDisableWriteSessions(TNode* node, bool value)
    {
        if (node->GetDisableWriteSessions() != value) {
            node->SetDisableWriteSessions(value);
            if (value) {
                LOG_INFO_UNLESS(IsRecovery(), "Disabled write sessions on node (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            } else {
                LOG_INFO_UNLESS(IsRecovery(), "Enabled write sessions on node (NodeId: %v, Address: %v)",
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
                LOG_INFO_UNLESS(IsRecovery(), "Disabled tablet cells on node (NodeId: %v, Address: %v)",
                    node->GetId(),
                    node->GetDefaultAddress());
            } else {
                LOG_INFO_UNLESS(IsRecovery(), "Enabled tablet cells on node (NodeId: %v, Address: %v)",
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
            LOG_INFO_UNLESS(IsRecovery(), "Node rack changed (NodeId: %v, Address: %v, Rack: %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                rack ? MakeNullable(rack->GetName()) : Null);
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


    TRack* CreateRack(const TString& name, const TObjectId& hintId)
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

        if (RackCount_ >= MaxRackCount) {
            THROW_ERROR_EXCEPTION("Rack count limit %v is reached",
                MaxRackCount);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
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
        YCHECK(NameToRackMap_.erase(rack->GetName()) == 1);
        YCHECK(NameToRackMap_.insert(std::make_pair(newName, rack)).second);
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

            LOG_INFO_UNLESS(IsRecovery(), "Rack data center changed (Rack: %v, DataCenter: %v)",
                MakeNullable(rack->GetName()),
                dataCenter ? MakeNullable(dataCenter->GetName()) : Null);

            for (auto* node : nodes) {
                NodeDataCenterChanged_.Fire(node, oldDataCenter);
            }
        }
    }


    TDataCenter* CreateDataCenter(const TString& name, const TObjectId& hintId)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Data center name cannot be empty");
        }

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

        auto dcHolder = std::make_unique<TDataCenter>(id);
        dcHolder->SetName(name);

        auto* dc = DataCenterMap_.Insert(id, std::move(dcHolder));
        YCHECK(NameToDataCenterMap_.insert(std::make_pair(name, dc)).second);

        // Make the fake reference.
        YCHECK(dc->RefObject() == 1);

        DataCenterCreated_.Fire(dc);

        return dc;
    }

    void DestroyDataCenter(TDataCenter* dc)
    {
        // Unbind racks from this DC.
        for (auto* rack : GetDataCenterRacks(dc)) {
            SetRackDataCenter(rack, nullptr);
        }

        // Remove DC from maps.
        YCHECK(NameToDataCenterMap_.erase(dc->GetName()) == 1);

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
        YCHECK(NameToDataCenterMap_.erase(dc->GetName()) == 1);
        YCHECK(NameToDataCenterMap_.insert(std::make_pair(newName, dc)).second);
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

        TotalNodeStatistics_ = TTotalNodeStatistics();
        for (const auto& pair : NodeMap_) {
            const auto* node = pair.second;
            TotalNodeStatistics_.BannedNodeCount += node->GetBanned();
            TotalNodeStatistics_.DecommissinedNodeCount += node->GetDecommissioned();
            TotalNodeStatistics_.WithAlertsNodeCount += !node->Alerts().empty();

            if (node->GetAggregatedState() != ENodeState::Online) {
                ++TotalNodeStatistics_.OfflineNodeCount;
                continue;
            }
            TotalNodeStatistics_.OnlineNodeCount += 1;

            const auto& statistics = node->Statistics();
            for (const auto& location : statistics.locations()) {
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
        TotalNodeStatisticsUpdateDeadline_ = now + DurationToCpuDuration(TotalNodeStatisticsUpdatePeriod);
        return TotalNodeStatistics_;
    }

    int GetOnlineNodeCount()
    {
        return AggregatedOnlineNodeCount_;
    }

private:
    friend class TClusterNodeTypeHandler;
    friend class TRackTypeHandler;
    friend class TDataCenterTypeHandler;

    const TNodeTrackerConfigPtr Config_;

    TPeriodicExecutorPtr ProfilingExecutor_;

    TProfiler Profiler = NodeTrackerServerProfiler;

    TIdGenerator NodeIdGenerator_;
    NHydra::TEntityMap<TNode> NodeMap_;
    NHydra::TEntityMap<TRack> RackMap_;
    NHydra::TEntityMap<TDataCenter> DataCenterMap_;

    int AggregatedOnlineNodeCount_ = 0;
    int LocalRegisteredNodeCount_ = 0;

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

    TAsyncSemaphorePtr FullHeartbeatSemaphore_;
    TAsyncSemaphorePtr IncrementalHeartbeatSemaphore_;
    TAsyncSemaphorePtr DisposeNodeSemaphore_;

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

    using TNodeGroupList = SmallVector<TNodeGroup*, 4>;

private:
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


    static TYPath GetNodePath(const TString& address)
    {
        return "//sys/nodes/" + ToYPathLiteral(address);
    }

    static TYPath GetNodePath(TNode* node)
    {
        return GetNodePath(node->GetDefaultAddress());
    }

    IMapNodePtr GetNodeMap()
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->ResolvePathToNodeProxy("//sys/nodes")->AsMap();
    }


    void HydraRegisterNode(
        const TCtxRegisterNodePtr& context,
        TReqRegisterNode* request,
        TRspRegisterNode* response)
    {
        auto nodeAddresses = FromProto<TNodeAddressMap>(request->node_addresses());
        const auto& addresses = GetAddressesOrThrow(nodeAddresses, EAddressType::InternalRpc);
        const auto& address = GetDefaultAddress(addresses);
        auto& statistics = *request->mutable_statistics();
        auto leaseTransactionId = FromProto<TTransactionId>(request->lease_transaction_id());
        auto tags = FromProto<std::vector<TString>>(request->tags());

        if (Bootstrap_->IsPrimaryMaster() && IsLeader()) {
            YCHECK(PendingRegisterNodeAddreses_.erase(address) == 1);
        }

        // Check lease transaction.
        TTransaction* leaseTransaction = nullptr;
        if (leaseTransactionId) {
            YCHECK(Bootstrap_->IsPrimaryMaster());
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            leaseTransaction = transactionManager->GetTransactionOrThrow(leaseTransactionId);
            if (leaseTransaction->GetPersistentState() != ETransactionState::Active) {
                leaseTransaction->ThrowInvalidState();
            }
        }

        // Kick-out any previous incarnation.
        auto* node = FindNodeByAddress(address);
        if (IsObjectAlive(node)) {
            node->ValidateNotBanned();

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

            UpdateNode(node, nodeAddresses);
        } else {
            auto nodeId = request->has_node_id() ? request->node_id() : GenerateNodeId();
            node = CreateNode(nodeId, nodeAddresses);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Node registered (NodeId: %v, Address: %v, Tags: %v, LeaseTransactionId: %v, %v)",
            node->GetId(),
            address,
            tags,
            leaseTransactionId,
            statistics);

        UpdateNodeCounters(node, -1);
        node->SetLocalState(ENodeState::Registered);
        node->SetNodeTags(tags);
        UpdateNodeCounters(node, +1);

        node->SetStatistics(std::move(statistics));

        UpdateLastSeenTime(node);
        UpdateRegisterTime(node);

        if (leaseTransaction) {
            node->SetLeaseTransaction(leaseTransaction);
            RegisterLeaseTransaction(node);
        }

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

    void HydraFullHeartbeat(
        const TCtxFullHeartbeatPtr& /*context*/,
        TReqFullHeartbeat* request,
        TRspFullHeartbeat* /*response*/)
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

        PROFILE_AGGREGATED_TIMING (FullHeartbeatTimeCounter) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Processing full heartbeat (NodeId: %v, Address: %v, State: %v, %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            UpdateNodeCounters(node, -1);
            node->SetLocalState(ENodeState::Online);
            UpdateNodeCounters(node, +1);

            node->SetStatistics(std::move(statistics));

            UpdateLastSeenTime(node);

            LOG_INFO_UNLESS(IsRecovery(), "Node online (NodeId: %v, Address: %v)",
                nodeId,
                node->GetDefaultAddress());

            FullHeartbeat_.Fire(node, request);
        }
    }

    void HydraIncrementalHeartbeat(
        const TCtxIncrementalHeartbeatPtr& context,
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

        PROFILE_AGGREGATED_TIMING (IncrementalHeartbeatTimeCounter) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Processing incremental heartbeat (NodeId: %v, Address: %v, State: %v, %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            node->SetStatistics(std::move(statistics));
            node->Alerts() = FromProto<std::vector<TError>>(request->alerts());

            UpdateLastSeenTime(node);

            if (Bootstrap_->IsPrimaryMaster()) {
                if (auto* rack = node->GetRack()) {
                    response->set_rack(rack->GetName());
                    if (auto* dc = rack->GetDataCenter()) {
                        response->set_data_center(dc->GetName());
                    }
                }

                *response->mutable_resource_limits_overrides() = node->ResourceLimitsOverrides();
                response->set_disable_scheduler_jobs(node->GetDisableSchedulerJobs());
            }

            IncrementalHeartbeat_.Fire(node, request, response);
        }
    }

    void HydraSetNodeStates(TReqSetNodeStates* request)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());

        auto cellTag = request->cell_tag();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
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
            node->SetState(cellTag, ENodeState(entry.state()));
            UpdateNodeCounters(node, +1);
        }
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
        NodeMap_.SaveValues(context);
        RackMap_.SaveValues(context);
        DataCenterMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        NodeMap_.LoadKeys(context);
        RackMap_.LoadKeys(context);
        // COMPAT(shakurov)
        if (context.GetVersion() >= 400) {
            DataCenterMap_.LoadKeys(context);
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        Load(context, NodeIdGenerator_);
        NodeMap_.LoadValues(context);
        RackMap_.LoadValues(context);
        // COMPAT(shakurov)
        if (context.GetVersion() >= 400) {
            DataCenterMap_.LoadValues(context);
        }
    }


    virtual void Clear() override
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
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        AddressToNodeMap_.clear();
        HostNameToNodeMap_.clear();
        TransactionToNodeMap_.clear();

        AggregatedOnlineNodeCount_ = 0;

        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;

            node->RebuildTags();
            InitializeNodeStates(node);
            InsertToAddressMaps(node);
            UpdateNodeCounters(node, +1);

            if (node->GetLeaseTransaction()) {
                RegisterLeaseTransaction(node);
            }
        }

        UsedRackIndexes_.reset();
        RackCount_ = 0;
        for (const auto& pair : RackMap_) {
            auto* rack = pair.second;

            YCHECK(NameToRackMap_.insert(std::make_pair(rack->GetName(), rack)).second);

            auto rackIndex = rack->GetIndex();
            YCHECK(!UsedRackIndexes_.test(rackIndex));
            UsedRackIndexes_.set(rackIndex);
            ++RackCount_;
        }

        for (const auto& pair : DataCenterMap_) {
            auto* dc = pair.second;

            YCHECK(NameToDataCenterMap_.insert(std::make_pair(dc->GetName(), dc)).second);
        }

        OnConfigChanged();
    }

    virtual void OnRecoveryStarted() override
    {
        TMasterAutomatonPart::OnRecoveryStarted();

        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            node->Reset();
        }

        Profiler.SetEnabled(false);
    }

    virtual void OnRecoveryComplete() override
    {
        TMasterAutomatonPart::OnRecoveryComplete();

        OnConfigChanged();
        Profiler.SetEnabled(true);
    }

    virtual void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        // NB: Node states gossip is one way: secondary-to-primary.
        if (Bootstrap_->IsSecondaryMaster()) {
            IncrementalNodeStatesGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
                BIND(&TImpl::OnNodeStatesGossip, MakeWeak(this), true),
                Config_->IncrementalNodeStatesGossipPeriod);
            IncrementalNodeStatesGossipExecutor_->Start();

            FullNodeStatesGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
                BIND(&TImpl::OnNodeStatesGossip, MakeWeak(this), false),
                Config_->FullNodeStatesGossipPeriod);
            FullNodeStatesGossipExecutor_->Start();
        }

        PendingRegisterNodeAddreses_.clear();
        for (auto& group : NodeGroups_) {
            group.PendingRegisterNodeMutationCount = 0;
        }

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
        node->InitializeStates(Bootstrap_->GetCellTag(), Bootstrap_->GetSecondaryCellTags());
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


    TNode* CreateNode(TNodeId nodeId, const TNodeAddressMap& nodeAddresses)
    {
        auto objectId = ObjectIdFromNodeId(nodeId);

        auto nodeHolder = std::make_unique<TNode>(objectId);
        auto* node = NodeMap_.Insert(objectId, std::move(nodeHolder));

        // Make the fake reference.
        YCHECK(node->RefObject() == 1);

        InitializeNodeStates(node);

        node->SetNodeAddresses(nodeAddresses);
        InsertToAddressMaps(node);

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
            LOG_ERROR_UNLESS(IsRecovery(), ex, "Error registering cluster node in Cypress");
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
            req->set_value(ConvertToYsonString(node->GetAddressesOrThrow(EAddressType::InternalRpc)).GetData());
            SyncExecuteVerb(rootService, req);
        } catch (const std::exception& ex) {
            LOG_ERROR_UNLESS(IsRecovery(), ex, "Error updating cluster node in Cypress");
        }
    }

    void UnregisterNode(TNode* node, bool propagate)
    {
        PROFILE_AGGREGATED_TIMING (NodeUnregisterTimeCounter) {
            auto* transaction = UnregisterLeaseTransaction(node);
            if (IsObjectAlive(transaction)) {
                const auto& transactionManager = Bootstrap_->GetTransactionManager();
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
        PROFILE_AGGREGATED_TIMING (NodeDisposeTimeCounter) {
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


    void OnNodeStatesGossip(bool incremental)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

        TReqSetNodeStates request;
        request.set_cell_tag(Bootstrap_->GetCellTag());
        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            if (!IsObjectAlive(node)) {
                continue;
            }

            auto state = node->GetLocalState();
            if (incremental && state == node->GetLastGossipState()) {
                continue;
            }

            auto* entry = request.add_entries();
            entry->set_node_id(node->GetId());
            entry->set_state(static_cast<int>(state));
            node->SetLastGossipState(state);
        }

        if (request.entries_size() == 0) {
            return;
        }

        LOG_INFO("Sending node states gossip message (Incremental: %v)",
            incremental);
        multicellManager->PostToMaster(request, PrimaryMasterCellTag, false);
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
        *request.mutable_statistics() = node->Statistics();

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
        for (int index = 0; index < UsedRackIndexes_.size(); ++index) {
            if (index == NullRackIndex) {
                continue;
            }
            if (!UsedRackIndexes_.test(index)) {
                UsedRackIndexes_.set(index);
                ++RackCount_;
                return index;
            }
        }
        Y_UNREACHABLE();
    }

    void FreeRackIndex(int index)
    {
        YCHECK(UsedRackIndexes_.test(index));
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
            // NB: TReqRegisterNode+TReqUnregisterNode create an offline node at the secondary master.
            {
                TReqRegisterNode request;
                request.set_node_id(node->GetId());
                ToProto(request.mutable_node_addresses(), node->GetNodeAddresses());
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

        auto dcs = GetValuesSortedByKey(DataCenterMap_);
        for (auto* dc : dcs) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(dc, cellTag);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto nodes = GetValuesSortedByKey(NodeMap_);
        for (auto* node : nodes) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(node, cellTag);
        }

        auto racks = GetValuesSortedByKey(RackMap_);
        for (auto* rack : racks) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(rack, cellTag);
        }

        auto dcs = GetValuesSortedByKey(DataCenterMap_);
        for (auto* dc : dcs) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(dc, cellTag);
        }
    }

    void InsertToAddressMaps(TNode* node)
    {
        YCHECK(AddressToNodeMap_.insert(std::make_pair(node->GetDefaultAddress(), node)).second);
        for (const auto& pair : node->GetAddressesOrThrow(EAddressType::InternalRpc)) {
            HostNameToNodeMap_.insert(std::make_pair(TString(GetServiceHostName(pair.second)), node));
        }
    }

    void RemoveFromAddressMaps(TNode* node)
    {
        YCHECK(AddressToNodeMap_.erase(node->GetDefaultAddress()) == 1);
        for (const auto& pair : node->GetAddressesOrThrow(EAddressType::InternalRpc)) {
            auto hostNameRange = HostNameToNodeMap_.equal_range(TString(GetServiceHostName(pair.second)));
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
        if (!Bootstrap_->IsPrimaryMaster() || !IsLeader()) {
            return;
        }

        auto statistics = GetTotalNodeStatistics();

        Profiler.Enqueue("/available_space", statistics.TotalSpace.Available, EMetricType::Gauge);
        Profiler.Enqueue("/used_space", statistics.TotalSpace.Used, EMetricType::Gauge);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (const auto& pair : chunkManager->Media()) {
            const auto* medium = pair.second;
            auto tag = TProfileManager::Get()->RegisterTag("medium", medium->GetName());
            int mediumIndex = medium->GetIndex();
            Profiler.Enqueue("/available_space_per_medium", statistics.SpacePerMedium[mediumIndex].Available, EMetricType::Gauge, {tag});
            Profiler.Enqueue("/used_space_per_medium", statistics.SpacePerMedium[mediumIndex].Used, EMetricType::Gauge, {tag});
        }

        Profiler.Enqueue("/chunk_replica_count", statistics.ChunkReplicaCount, EMetricType::Gauge);

        Profiler.Enqueue("/online_node_count", statistics.OnlineNodeCount, EMetricType::Gauge);
        Profiler.Enqueue("/offline_node_count", statistics.OfflineNodeCount, EMetricType::Gauge);
        Profiler.Enqueue("/banned_node_count", statistics.BannedNodeCount, EMetricType::Gauge);
        Profiler.Enqueue("/decommissioned_node_count", statistics.DecommissinedNodeCount, EMetricType::Gauge);
        Profiler.Enqueue("/with_alerts_node_count", statistics.WithAlertsNodeCount, EMetricType::Gauge);
        Profiler.Enqueue("/full_node_count", statistics.FullNodeCount, EMetricType::Gauge);
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
            YCHECK(DefaultNodeGroup_);
            return {DefaultNodeGroup_}; // default is the last one
        }
        return GetGroupsForNode(node);
    }

    void OnConfigChanged()
    {
        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            UpdateNodeCounters(node, -1);
        }

        NodeGroups_.clear();

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->NodeTracker;
        for (const auto& pair : config->NodeGroups) {
            NodeGroups_.emplace_back();
            auto& group = NodeGroups_.back();
            group.Id = pair.first;
            group.Config = pair.second;
        }

        {
            NodeGroups_.emplace_back();
            DefaultNodeGroup_ = &NodeGroups_.back();
            DefaultNodeGroup_->Id = "default";
            DefaultNodeGroup_->Config = New<TNodeGroupConfig>();
            DefaultNodeGroup_->Config->MaxConcurrentNodeRegistrations = Config_->MaxConcurrentNodeRegistrations;
        }

        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            UpdateNodeCounters(node, +1);
        }

        for (const auto& address : PendingRegisterNodeAddreses_) {
            auto groups = GetGroupsForNode(address);
            for (auto* group : groups) {
                ++group->PendingRegisterNodeMutationCount;
            }
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker::TImpl, Node, TNode, NodeMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker::TImpl, Rack, TRack, RackMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker::TImpl, DataCenter, TDataCenter, DataCenterMap_)

////////////////////////////////////////////////////////////////////////////////

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

TRack* TNodeTracker::CreateRack(const TString& name)
{
    return Impl_->CreateRack(name, NullObjectId);
}

void TNodeTracker::DestroyRack(TRack* rack)
{
    Impl_->DestroyRack(rack);
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

TDataCenter* TNodeTracker::CreateDataCenter(const TString& name)
{
    return Impl_->CreateDataCenter(name, NullObjectId);
}

void TNodeTracker::DestroyDataCenter(TDataCenter* dc)
{
    Impl_->DestroyDataCenter(dc);
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
DELEGATE_ENTITY_MAP_ACCESSORS(TNodeTracker, DataCenter, TDataCenter, *Impl_)

DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeRegistered, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeUnregistered, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeDisposed, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeBanChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeDecommissionChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeDisableTabletCellsChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeTagsChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*, TRack*), NodeRackChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*, TDataCenter*), NodeDataCenterChanged, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*, TReqFullHeartbeat*), FullHeartbeat, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*, TReqIncrementalHeartbeat*, TRspIncrementalHeartbeat*), IncrementalHeartbeat, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TDataCenter*), DataCenterCreated, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TDataCenter*), DataCenterRenamed, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TDataCenter*), DataCenterDestroyed, *Impl_);

////////////////////////////////////////////////////////////////////////////////

TNodeTracker::TClusterNodeTypeHandler::TClusterNodeTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->NodeMap_)
    , Owner_(owner)
{ }

IObjectProxyPtr TNodeTracker::TClusterNodeTypeHandler::DoGetProxy(
    TNode* node,
    TTransaction* /*transaction*/)
{
    return CreateClusterNodeProxy(Owner_->Bootstrap_, &Metadata_, node);
}

void TNodeTracker::TClusterNodeTypeHandler::DoZombifyObject(TNode* node)
{
    TObjectTypeHandlerWithMapBase::DoZombifyObject(node);
    // NB: Destroy the node right away and do not wait for GC to prevent
    // dangling links from occuring in //sys/nodes.
    Owner_->DestroyNode(node);
}

////////////////////////////////////////////////////////////////////////////////

TNodeTracker::TRackTypeHandler::TRackTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->RackMap_)
    , Owner_(owner)
{ }

TObjectBase* TNodeTracker::TRackTypeHandler::CreateObject(
    const TObjectId& hintId,
    IAttributeDictionary* attributes)
{
    auto name = attributes->GetAndRemove<TString>("name");

    return Owner_->CreateRack(name, hintId);
}

IObjectProxyPtr TNodeTracker::TRackTypeHandler::DoGetProxy(
    TRack* rack,
    TTransaction* /*transaction*/)
{
    return CreateRackProxy(Owner_->Bootstrap_, &Metadata_, rack);
}

void TNodeTracker::TRackTypeHandler::DoZombifyObject(TRack* rack)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(rack);
    Owner_->DestroyRack(rack);
}

////////////////////////////////////////////////////////////////////////////////

TNodeTracker::TDataCenterTypeHandler::TDataCenterTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->DataCenterMap_)
    , Owner_(owner)
{ }

TObjectBase* TNodeTracker::TDataCenterTypeHandler::CreateObject(
    const TObjectId& hintId,
    IAttributeDictionary* attributes)
{
    auto name = attributes->Get<TString>("name");
    attributes->Remove("name");

    return Owner_->CreateDataCenter(name, hintId);
}

IObjectProxyPtr TNodeTracker::TDataCenterTypeHandler::DoGetProxy(
    TDataCenter* dc,
    TTransaction* /*transaction*/)
{
    return CreateDataCenterProxy(Owner_->Bootstrap_, &Metadata_, dc);
}

void TNodeTracker::TDataCenterTypeHandler::DoZombifyObject(TDataCenter* dc)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(dc);
    Owner_->DestroyDataCenter(dc);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
