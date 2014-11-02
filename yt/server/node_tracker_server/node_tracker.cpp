#include "stdafx.h"
#include "node_tracker.h"
#include "config.h"
#include "node.h"
#include "private.h"

#include <core/misc/id_generator.h>
#include <core/misc/address.h>

#include <core/ytree/convert.h>
#include <core/ytree/ypath_client.h>

#include <core/ypath/token.h>

#include <core/concurrency/scheduler.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/object_client/helpers.h>

#include <server/chunk_server/job.h>

#include <server/node_tracker_server/node_tracker.pb.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/transaction_server/transaction_manager.h>
#include <server/transaction_server/transaction.h>

#include <server/object_server/object_manager.h>
#include <server/object_server/attribute_set.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/serialize.h>

#include <deque>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NHydra;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NNodeTrackerServer::NProto;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NodeTrackerServerLogger;
static auto& Profiler = NodeTrackerServerProfiler;

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
    {
        RegisterMethod(BIND(&TImpl::HydraRegisterNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnregisterNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRemoveNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraFullHeartbeat, Unretained(this), nullptr));
        RegisterMethod(BIND(&TImpl::HydraIncrementalHeartbeat, Unretained(this), nullptr, nullptr));

        RegisterLoader(
            "NodeTracker.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "NodeTracker.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "NodeTracker.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "NodeTracker.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        SubscribeNodeConfigUpdated(BIND(&TImpl::OnNodeConfigUpdated, Unretained(this)));
    }

    void Initialize()
    {
        auto transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));
    }


    TMutationPtr CreateRegisterNodeMutation(
        const TReqRegisterNode& request)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request);
    }

    TMutationPtr CreateUnregisterNodeMutation(
        const TReqUnregisterNode& request)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request);
    }

    TMutationPtr CreateRemoveNodeMutation(
        const TReqRemoveNode& request)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request);
    }

    TMutationPtr CreateFullHeartbeatMutation(
        TCtxFullHeartbeatPtr context)
    {
        return CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager())
            ->SetRequestData(context->GetRequestBody(), context->Request().GetTypeName())
            ->SetAction(BIND(
                &TImpl::HydraFullHeartbeat,
                MakeStrong(this),
                context,
                ConstRef(context->Request())));
   }

    TMutationPtr CreateIncrementalHeartbeatMutation(
        TCtxIncrementalHeartbeatPtr context)
    {
        return CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager())
            ->SetRequestData(context->GetRequestBody(), context->Request().GetTypeName())
            ->SetAction(BIND(
                &TImpl::HydraIncrementalHeartbeat,
                MakeStrong(this),
                context,
                &context->Response(),
                ConstRef(context->Request())));
    }


    void RefreshNodeConfig(TNode* node)
    {
        auto attributes = DoFindNodeConfig(node->GetAddress());
        if (!attributes)
            return;

        if (!ReconfigureYsonSerializable(node->GetConfig(), attributes))
            return;

        LOG_INFO_UNLESS(IsRecovery(), "Node configuration updated (Address: %v)", node->GetAddress());

        NodeConfigUpdated_.Fire(node);
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Node, TNode, TNodeId);

    DEFINE_SIGNAL(void(TNode* node), NodeRegistered);
    DEFINE_SIGNAL(void(TNode* node), NodeUnregistered);
    DEFINE_SIGNAL(void(TNode* node), NodeRemoved);
    DEFINE_SIGNAL(void(TNode* node), NodeConfigUpdated);
    DEFINE_SIGNAL(void(TNode* node, const TReqFullHeartbeat& request), FullHeartbeat);
    DEFINE_SIGNAL(void(TNode* node, const TReqIncrementalHeartbeat& request, TRspIncrementalHeartbeat* response), IncrementalHeartbeat);


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

    TNode* FindNodeByHostName(const Stroka& hostName)
    {
        auto it = HostNameToNodeMap_.find(hostName);
        return it == AddressToNodeMap_.end() ? nullptr : it->second;
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


    TNodeConfigPtr FindNodeConfigByAddress(const Stroka& address)
    {
        auto attributes = DoFindNodeConfig(address);
        if (!attributes) {
            return nullptr;
        }

        try {
            return ConvertTo<TNodeConfigPtr>(attributes);
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Error parsing configuration of node %v, defaults will be used", address);
            return nullptr;
        }
    }

    TNodeConfigPtr GetNodeConfigByAddress(const Stroka& address)
    {
        auto config = FindNodeConfigByAddress(address);
        return config ? config : New<TNodeConfig>();
    }

    
    TTotalNodeStatistics GetTotalNodeStatistics()
    {
        TTotalNodeStatistics result;
        for (const auto& pair : NodeMap_) {
            const auto* node = pair.second;
            const auto& statistics = node->Statistics();
            result.AvailableSpace += statistics.total_available_space();
            result.UsedSpace += statistics.total_used_space();
            result.ChunkCount += statistics.total_chunk_count();
            result.OnlineNodeCount++;
        }
        return result;
    }

    int GetRegisteredNodeCount()
    {
        return RegisteredNodeCount_;
    }

    int GetOnlineNodeCount()
    {
        return OnlineNodeCount_;
    }

private:
    TNodeTrackerConfigPtr Config_;

    TIdGenerator NodeIdGenerator_;
    NHydra::TEntityMap<TNodeId, TNode> NodeMap_;

    int OnlineNodeCount_ = 0;
    int RegisteredNodeCount_ = 0;

    yhash_map<Stroka, TNode*> AddressToNodeMap_;
    yhash_multimap<Stroka, TNode*> HostNameToNodeMap_;
    yhash_map<TTransaction*, TNode*> TransactionToNodeMap_;

    std::deque<TNode*> NodeRemovalQueue_;
    int PendingRemoveNodeMutationCount_ = 0;


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

    IMapNodePtr DoFindNodeConfig(const Stroka& address)
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto resolver = cypressManager->CreateResolver();
        auto nodesMap = resolver->ResolvePath("//sys/nodes")->AsMap();
        auto nodeNode = nodesMap->FindChild(address);
        if (!nodeNode) {
            return nullptr;
        }
        return nodeNode->Attributes().ToMap();
    }

    static TYPath GetNodePath(TNode* node)
    {
        return "//sys/nodes/" + ToYPathLiteral(node->GetAddress());
    }


    TRspRegisterNode HydraRegisterNode(const TReqRegisterNode& request)
    {
        auto descriptor = FromProto<NNodeTrackerClient::TNodeDescriptor>(request.node_descriptor());
        const auto& statistics = request.statistics();
        const auto& address = descriptor.GetDefaultAddress();

        // Kick-out any previous incarnation.
        {
            auto* existingNode = FindNodeByAddress(descriptor.GetDefaultAddress());
            if (existingNode) {
                LOG_INFO_UNLESS(IsRecovery(), "Node kicked out due to address conflict (Address: %v, ExistingId: %v)",
                    address,
                    existingNode->GetId());
                DoUnregisterNode(existingNode, false);
                DoRemoveNode(existingNode);
            }
        }

        auto* node = DoRegisterNode(descriptor, statistics);

        TRspRegisterNode response;
        response.set_node_id(node->GetId());
        return response;
    }

    void HydraUnregisterNode(const TReqUnregisterNode& request)
    {
        auto nodeId = request.node_id();

        auto* node = FindNode(nodeId);
        if (!node)
            return;
        if (node->GetState() != ENodeState::Registered && node->GetState() != ENodeState::Online)
            return;

        DoUnregisterNode(node, true);
    }

    void HydraRemoveNode(const TReqRemoveNode& request)
    {
        auto nodeId = request.node_id();

        auto* node = FindNode(nodeId);
        if (!node)
            return;
        if (node->GetState() != ENodeState::Unregistered)
            return;

        DoRemoveNode(node);
    }

    void HydraFullHeartbeat(
        TCtxFullHeartbeatPtr context,
        const TReqFullHeartbeat& request)
    {
        auto nodeId = request.node_id();
        const auto& statistics = request.statistics();

        auto* node = FindNode(nodeId);
        if (!node)
            return;
        if (node->GetState() != ENodeState::Registered)
            return;

        PROFILE_TIMING ("/full_heartbeat_time") {
            LOG_DEBUG_UNLESS(IsRecovery(), "Processing full heartbeat (NodeId: %v, Address: %v, State: %v, %v)",
                nodeId,
                node->GetAddress(),
                node->GetState(),
                statistics);

            UpdateNodeCounters(node, -1);
            node->SetState(ENodeState::Online);
            UpdateNodeCounters(node, +1);

            node->Statistics() = statistics;

            RenewNodeLease(node);

            LOG_INFO_UNLESS(IsRecovery(), "Node online (NodeId: %v, Address: %v)",
                nodeId,
                node->GetAddress());

            FullHeartbeat_.Fire(node, request);
        }
    }

    void HydraIncrementalHeartbeat(
        TCtxIncrementalHeartbeatPtr context,
        TRspIncrementalHeartbeat* response,
        const TReqIncrementalHeartbeat& request)
    {
        auto nodeId = request.node_id();
        const auto& statistics = request.statistics();

        auto* node = FindNode(nodeId);
        if (!node)
            return;
        if (node->GetState() != ENodeState::Online)
            return;

        PROFILE_TIMING ("/incremental_heartbeat_time") {
            LOG_DEBUG_UNLESS(IsRecovery(), "Processing incremental heartbeat (NodeId: %v, Address: %v, State: %v, %v)",
                nodeId,
                node->GetAddress(),
                node->GetState(),
                statistics);

            node->Statistics() = statistics;
            node->Alerts() = FromProto<Stroka>(request.alerts());

            RenewNodeLease(node);
            
            IncrementalHeartbeat_.Fire(node, request, response);
        }
    }


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        Save(context, NodeIdGenerator_);
        NodeMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        NodeMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        Load(context, NodeIdGenerator_);
        NodeMap_.LoadValues(context);
    }

    virtual void Clear() override
    {
        NodeIdGenerator_.Reset();
        NodeMap_.Clear();

        AddressToNodeMap_.clear();
        HostNameToNodeMap_.clear();
        TransactionToNodeMap_.clear();

        OnlineNodeCount_ = 0;
        RegisteredNodeCount_ = 0;

        NodeRemovalQueue_.clear();
        PendingRemoveNodeMutationCount_ = 0;
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        AddressToNodeMap_.clear();
        HostNameToNodeMap_.clear();
        TransactionToNodeMap_.clear();

        OnlineNodeCount_ = 0;
        RegisteredNodeCount_ = 0;

        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            const auto& address = node->GetAddress();

            YCHECK(AddressToNodeMap_.insert(std::make_pair(address, node)).second);
            HostNameToNodeMap_.insert(std::make_pair(Stroka(GetServiceHostName(address)), node));

            UpdateNodeCounters(node, +1);

            if (node->GetTransaction()) {
                RegisterLeaseTransaction(node);
            }
        }
    }

    virtual void OnRecoveryStarted() override
    {
        Profiler.SetEnabled(false);

        // Reset runtime info.
        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            node->ResetHints();
            node->ClearChunkRemovalQueue();
            node->ClearChunkReplicationQueues();
            node->ClearChunkSealQueue();
        }
    }

    virtual void OnRecoveryComplete() override
    {
        Profiler.SetEnabled(true);

        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            RefreshNodeConfig(node);
        }
    }

    virtual void OnLeaderActive() override
    {
        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;
            if (node->GetState() == ENodeState::Unregistered) {
                NodeRemovalQueue_.push_back(node);
            }
        }

        MaybePostRemoveNodeMutations();
    }


    void UpdateNodeCounters(TNode* node, int delta)
    {
        switch (node->GetState()) {
            case ENodeState::Registered:
                RegisteredNodeCount_ += delta;
                break;
            case ENodeState::Online:
                OnlineNodeCount_ += delta;
                break;
            default:
                break;
        }
    }


    void RegisterLeaseTransaction(TNode* node)
    {
        auto* transaction = node->GetTransaction();
        YCHECK(transaction);
        YCHECK(TransactionToNodeMap_.insert(std::make_pair(transaction, node)).second);
    }

    TTransaction* UnregisterLeaseTransaction(TNode* node)
    {
        auto* transaction = node->GetTransaction();
        if (transaction) {
            YCHECK(TransactionToNodeMap_.erase(transaction) == 1);
        }
        node->SetTransaction(nullptr);
        return transaction;
    }

    void RenewNodeLease(TNode* node)
    {
        auto* transaction = node->GetTransaction();
        if (!transaction)
            return;

        auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        const auto* mutationContext = hydraManager->GetMutationContext();

        auto timeout = GetNodeLeaseTimeout(node);
        transaction->SetTimeout(timeout);

        try {
            auto objectManager = Bootstrap_->GetObjectManager();
            auto rootService = objectManager->GetRootService();
            auto nodePath = GetNodePath(node);
            SyncYPathSet(rootService, nodePath + "/@last_seen_time", ConvertToYsonString(mutationContext->GetTimestamp()));
        } catch (const std::exception& ex) {
            LOG_ERROR_UNLESS(IsRecovery(), ex, "Error updating node properties in Cypress");
        }

        if (IsLeader()) {
            auto transactionManager = Bootstrap_->GetTransactionManager();
            transactionManager->PingTransaction(transaction);
        }
    }

    TDuration GetNodeLeaseTimeout(TNode* node)
    {
        switch (node->GetState()) {
            case ENodeState::Registered:
                return Config_->RegisteredNodeTimeout;
            case ENodeState::Online:
                return Config_->OnlineNodeTimeout;
            default:
                YUNREACHABLE();
        }
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        auto it = TransactionToNodeMap_.find(transaction);
        if (it == TransactionToNodeMap_.end())
            return;

        auto* node = it->second;
        LOG_INFO_UNLESS(IsRecovery(), "Node lease expired (NodeId: %v, Address: %v)",
            node->GetId(),
            node->GetAddress());

        DoUnregisterNode(node, true);
    }


    TNode* DoRegisterNode(const TNodeDescriptor& descriptor, const TNodeStatistics& statistics)
    {
        PROFILE_TIMING ("/node_register_time") {
            const auto& address = descriptor.GetDefaultAddress();
            auto config = GetNodeConfigByAddress(address);
            auto nodeId = GenerateNodeId();

            auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            const auto* mutationContext = hydraManager->GetMutationContext();

            auto* node = new TNode(
                nodeId,
                descriptor,
                config,
                mutationContext->GetTimestamp());
            node->SetState(ENodeState::Registered);
            node->Statistics() = statistics;

            NodeMap_.Insert(nodeId, node);
            AddressToNodeMap_.insert(std::make_pair(address, node));
            HostNameToNodeMap_.insert(std::make_pair(Stroka(GetServiceHostName(address)), node));
            
            UpdateNodeCounters(node, +1);

            auto transactionManager = Bootstrap_->GetTransactionManager();
            auto objectManager = Bootstrap_->GetObjectManager();
            auto rootService = objectManager->GetRootService();
            auto nodePath = GetNodePath(node);

            // Create lease transaction.
            TTransaction* transaction;
            {
                auto timeout = GetNodeLeaseTimeout(node);
                transaction = transactionManager->StartTransaction(nullptr, timeout);
                node->SetTransaction(transaction);
                RegisterLeaseTransaction(node);
            }

            try {
                // Set "title" attribute.
                {
                    auto path = FromObjectId(transaction->GetId()) + "/@title";
                    auto title = Format("Lease for node %v", node->GetAddress());
                    SyncYPathSet(rootService, path, ConvertToYsonString(title));
                }

                // Create Cypress node.
                {
                    auto req = TCypressYPathProxy::Create(nodePath);
                    req->set_type(EObjectType::CellNode);
                    req->set_ignore_existing(true);

                    auto defaultAttributes = ConvertToAttributes(New<TNodeConfig>());
                    ToProto(req->mutable_node_attributes(), *defaultAttributes);

                    auto rsp = SyncExecuteVerb(rootService, req);
                    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
                }

                // Create "orchid" child.
                {
                    auto req = TCypressYPathProxy::Create(nodePath + "/orchid");
                    req->set_type(EObjectType::Orchid);
                    req->set_ignore_existing(true);

                    auto attributes = CreateEphemeralAttributes();
                    attributes->Set("remote_address", address);
                    ToProto(req->mutable_node_attributes(), *attributes);

                    auto rsp = SyncExecuteVerb(rootService, req);
                    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
                }
            } catch (const std::exception& ex) {
                LOG_ERROR_UNLESS(IsRecovery(), ex, "Error registering node in Cypress");
            }

            // Make the initial lease renewal (and also set "last_seen_time" attribute).
            RenewNodeLease(node);

            // Lock Cypress node.
            {
                auto req = TCypressYPathProxy::Lock(nodePath);
                req->set_mode(ELockMode::Shared);
                SetTransactionId(req, transaction->GetId());

                auto rsp = SyncExecuteVerb(rootService, req);
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
            }

            LOG_INFO_UNLESS(IsRecovery(), "Node registered (NodeId: %v, Address: %v, %v)",
                nodeId,
                address,
                statistics);

            NodeRegistered_.Fire(node);

            return node;
        }
    }

    void DoUnregisterNode(TNode* node, bool scheduleRemoval)
    {
        PROFILE_TIMING ("/node_unregister_time") {
            auto* transaction = UnregisterLeaseTransaction(node);
            if (transaction && transaction->GetState() == ETransactionState::Active) {
                auto transactionManager = Bootstrap_->GetTransactionManager();
                // NB: This will trigger OnTransactionFinished, however we've already evicted the
                // lease so the latter call is no-op.
                transactionManager->AbortTransaction(transaction, false);
            }

            const auto& address = node->GetAddress();
            YCHECK(AddressToNodeMap_.erase(address) == 1);
            {
                auto hostNameRange = HostNameToNodeMap_.equal_range(Stroka(GetServiceHostName(address)));
                for (auto it = hostNameRange.first; it != hostNameRange.second; ++it) {
                    if (it->second == node) {
                        HostNameToNodeMap_.erase(it);
                        break;
                    }
                }
            }

            UpdateNodeCounters(node, -1);
            node->SetState(ENodeState::Unregistered);
            NodeUnregistered_.Fire(node);

            if (scheduleRemoval && IsLeader()) {
                NodeRemovalQueue_.push_back(node);
                MaybePostRemoveNodeMutations();
            }

            LOG_INFO_UNLESS(IsRecovery(), "Node unregistered (NodeId: %v, Address: %v)",
                node->GetId(),
                node->GetAddress());
        }
    }

    void DoRemoveNode(TNode* node)
    {
        PROFILE_TIMING ("/node_remove_time") {
            // Make copies, node will die soon.
            auto nodeId = node->GetId();
            auto address = node->GetAddress();

            NodeRemoved_.Fire(node);

            NodeMap_.Remove(nodeId);

            LOG_INFO_UNLESS(IsRecovery(), "Node removed (NodeId: %v, Address: %v)",
                nodeId,
                address);

            if (IsLeader()) {
                YCHECK(--PendingRemoveNodeMutationCount_ >= 0);
                MaybePostRemoveNodeMutations();
            }
        }
    }


    void PostUnregisterNodeMutation(TNode* node)
    {
        TReqUnregisterNode request;
        request.set_node_id(node->GetId());

        auto mutation = CreateUnregisterNodeMutation(request);
        Bootstrap_
            ->GetHydraFacade()
            ->GetEpochAutomatonInvoker()
            ->Invoke(BIND(IgnoreResult(&TMutation::Commit), mutation));
    }

    void MaybePostRemoveNodeMutations()
    {
        while (
            !NodeRemovalQueue_.empty() &&
            PendingRemoveNodeMutationCount_ < Config_->MaxConcurrentNodeRemoveMutations)
        {
            const auto* node = NodeRemovalQueue_.front();
            NodeRemovalQueue_.pop_front();

            TReqRemoveNode request;
            request.set_node_id(node->GetId());

            auto mutation = CreateRemoveNodeMutation(request);
            Bootstrap_
                ->GetHydraFacade()
                ->GetEpochAutomatonInvoker()
                ->Invoke(BIND(IgnoreResult(&TMutation::Commit), mutation));

            ++PendingRemoveNodeMutationCount_;
        }
    }


    void OnNodeConfigUpdated(TNode* node)
    {
        if (node->GetConfig()->Banned) {
            LOG_INFO_UNLESS(IsRecovery(), "Node banned (Address: %v)",
                node->GetAddress());
            if (IsLeader()) {
                PostUnregisterNodeMutation(node);
            }
        }
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TNodeTracker::TImpl, Node, TNode, TNodeId, NodeMap_)

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

TNodeTracker::~TNodeTracker()
{ }

TNode* TNodeTracker::FindNodeByAddress(const Stroka& address)
{
    return Impl_->FindNodeByAddress(address);
}

TNode* TNodeTracker::GetNodeByAddress(const Stroka& address)
{
    return Impl_->GetNodeByAddress(address);
}

TNode* TNodeTracker::FindNodeByHostName(const Stroka& hostName)
{
    return Impl_->FindNodeByHostName(hostName);
}

TNode* TNodeTracker::GetNodeOrThrow(TNodeId id)
{
    return Impl_->GetNodeOrThrow(id);
}

TNodeConfigPtr TNodeTracker::FindNodeConfigByAddress(const Stroka& address)
{
    return Impl_->FindNodeConfigByAddress(address);
}

TNodeConfigPtr TNodeTracker::GetNodeConfigByAddress(const Stroka& address)
{
    return Impl_->GetNodeConfigByAddress(address);
}

TMutationPtr TNodeTracker::CreateRegisterNodeMutation(
    const TReqRegisterNode& request)
{
    return Impl_->CreateRegisterNodeMutation(request);
}

TMutationPtr TNodeTracker::CreateUnregisterNodeMutation(
    const TReqUnregisterNode& request)
{
    return Impl_->CreateUnregisterNodeMutation(request);
}

TMutationPtr TNodeTracker::CreateFullHeartbeatMutation(
    TCtxFullHeartbeatPtr context)
{
    return Impl_->CreateFullHeartbeatMutation(context);
}

TMutationPtr TNodeTracker::CreateIncrementalHeartbeatMutation(
    TCtxIncrementalHeartbeatPtr context)
{
    return Impl_->CreateIncrementalHeartbeatMutation(context);
}

void TNodeTracker::RefreshNodeConfig(TNode* node)
{
    return Impl_->RefreshNodeConfig(node);
}

TTotalNodeStatistics TNodeTracker::GetTotalNodeStatistics()
{
    return Impl_->GetTotalNodeStatistics();
}

int TNodeTracker::GetRegisteredNodeCount()
{
    return Impl_->GetRegisteredNodeCount();
}

int TNodeTracker::GetOnlineNodeCount()
{
    return Impl_->GetOnlineNodeCount();
}

DELEGATE_ENTITY_MAP_ACCESSORS(TNodeTracker, Node, TNode, TNodeId, *Impl_)

DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeRegistered, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeUnregistered, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeRemoved, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*), NodeConfigUpdated, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*, const TReqFullHeartbeat&), FullHeartbeat, *Impl_);
DELEGATE_SIGNAL(TNodeTracker, void(TNode*, const TReqIncrementalHeartbeat&, TRspIncrementalHeartbeat*), IncrementalHeartbeat, *Impl_);

///////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
