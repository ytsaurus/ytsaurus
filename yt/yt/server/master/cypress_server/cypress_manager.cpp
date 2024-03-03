#include "cypress_manager.h"

#include "access_control_object_namespace_type_handler.h"
#include "access_control_object_type_handler.h"
#include "access_tracker.h"
#include "config.h"
#include "cypress_integration.h"
#include "cypress_traverser.h"
#include "document_node_type_handler.h"
#include "expiration_tracker.h"
#include "grafting_manager.h"
#include "helpers.h"
#include "link_node_type_handler.h"
#include "lock_proxy.h"
#include "node_detail.h"
#include "node_proxy_detail.h"
#include "portal_entrance_node.h"
#include "portal_entrance_type_handler.h"
#include "portal_exit_node.h"
#include "portal_exit_type_handler.h"
#include "portal_manager.h"
#include "portal_node_map_type_handler.h"
#include "private.h"
#include "resolve_cache.h"
#include "rootstock_map_type_handler.h"
#include "rootstock_node.h"
#include "rootstock_type_handler.h"
#include "scion_map_type_handler.h"
#include "scion_node.h"
#include "scion_type_handler.h"
#include "shard_map_type_handler.h"
#include "shard_type_handler.h"
#include "shard.h"

// COMPAT(h0pless): RecomputeMasterTableSchemaRefCounters
#include <yt/yt/server/master/table_server/config.h>

// COMPAT(babenko)
#include <yt/yt/server/master/journal_server/journal_node.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>

// COMPAT(h0pless): Used for schema migration
#include <yt/yt/server/master/chaos_server/chaos_replicated_table_node.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_server/cypress_integration.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/cell_server/cell_map_type_handler.h>

#include <yt/yt/server/master/chunk_server/cypress_integration.h>

#include <yt/yt/server/master/file_server/file_node_type_handler.h>

#include <yt/yt/server/master/journal_server/journal_node_type_handler.h>

#include <yt/yt/server/master/maintenance_tracker_server/cluster_proxy_node_type_handler.h>

#include <yt/yt/server/master/node_tracker_server/cypress_integration.h>

#include <yt/yt/server/master/object_server/cypress_integration.h>
#include <yt/yt/server/master/object_server/object_detail.h>
#include <yt/yt/server/master/object_server/sys_node_type_handler.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/orchid_server/cypress_integration.h>

#include <yt/yt/server/master/scheduler_pool_server/cypress_integration.h>

#include <yt/yt/server/master/security_server/access_log.h>
#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/cypress_integration.h>
#include <yt/yt/server/master/security_server/group.h>
#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/table_server/cypress_integration.h>
#include <yt/yt/server/master/table_server/master_table_schema.h>
#include <yt/yt/server/master/table_server/replicated_table_node_type_handler.h>
#include <yt/yt/server/master/table_server/table_manager.h>
#include <yt/yt/server/master/table_server/table_node.h>
#include <yt/yt/server/master/table_server/table_node_type_handler.h>

#include <yt/yt/server/master/tablet_server/cypress_integration.h>
#include <yt/yt/server/master/tablet_server/hunk_storage_node_type_handler.h>

#include <yt/yt/server/master/transaction_server/cypress_integration.h>

#include <yt/yt/server/master/zookeeper_server/cypress_integration.h>

#include <yt/yt/server/lib/hydra/hydra_context.h>

#include <yt/yt/ytlib/api/native/proto/transaction_actions.pb.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>
#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/sync_expiring_cache.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <yt/yt/core/ypath/token.h>

#include <library/cpp/yt/small_containers/compact_set.h>
#include <library/cpp/yt/small_containers/compact_queue.h>

#include <library/cpp/yt/misc/variant.h>

#include <util/generic/algorithm.h>

namespace NYT::NCypressServer {

using namespace NBus;
using namespace NCellarClient;
using namespace NCellMaster;
using namespace NCellServer;
using namespace NChunkServer;
using namespace NCypressClient::NProto;
using namespace NFileServer;
using namespace NHydra;
using namespace NJournalServer;
using namespace NMaintenanceTrackerServer;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NObjectServer;
using namespace NOrchidServer;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
using namespace NTableClient;
using namespace NTableServer;
using namespace NSchedulerPoolServer;
using namespace NTabletServer;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NZookeeperServer;

// COMPAT(h0pless): RecomputeMasterTableSchemaRefCounters
using namespace NLogging;

using TYPath = NYPath::TYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;
static const INodeTypeHandlerPtr NullTypeHandler;

////////////////////////////////////////////////////////////////////////////////

class TCypressManager;

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
    : public TTransactionalNodeFactoryBase
    , public ICypressNodeFactory
{
public:
    TNodeFactory(
        TBootstrap* bootstrap,
        TCypressShard* shard,
        TTransaction* transaction,
        TAccount* account,
        const TNodeFactoryOptions& options,
        TCypressNode* serviceTrunkNode,
        TYPath unresolvedPathSuffix)
        : Bootstrap_(bootstrap)
        , Shard_(shard)
        , Transaction_(transaction)
        , Account_(account)
        , Options_(options)
        , ServiceTrunkNode_(serviceTrunkNode)
        , UnresolvedPathSuffix_(std::move(unresolvedPathSuffix))
    {
        YT_VERIFY(Bootstrap_);
        YT_VERIFY(Account_);
    }

    ~TNodeFactory() override
    {
        RollbackIfNeeded();
    }

    void Commit() noexcept override
    {
        TTransactionalNodeFactoryBase::Commit();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        if (Transaction_) {
            for (auto* node : CreatedNodes_) {
                transactionManager->StageNode(Transaction_, node);
            }
        }

        YT_VERIFY(CreatedOpaqueChildren_.empty() || Transaction_);

        for (auto* child : CreatedOpaqueChildren_) {
            transactionManager->StageNode(Transaction_, child);
        }

        const auto& portalManager = Bootstrap_->GetPortalManager();
        for (const auto& entrance : CreatedPortalEntrances_) {
            portalManager->RegisterEntranceNode(
                entrance.Node,
                *entrance.InheritedAttributes,
                *entrance.ExplicitAttributes);
        }

        const auto& graftingManager = Bootstrap_->GetGraftingManager();
        for (const auto& rootstock : CreatedRootstocks_) {
            graftingManager->OnRootstockCreated(
                rootstock.Node,
                *rootstock.InheritedAttributes,
                *rootstock.ExplicitAttributes);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        for (const auto& clone : ClonedExternalNodes_) {
            NProto::TReqCloneForeignNode protoRequest;
            ToProto(protoRequest.mutable_source_node_id(), clone.SourceNodeId.ObjectId);
            if (clone.SchemaIdHint) {
                ToProto(protoRequest.mutable_schema_id_hint(), clone.SchemaIdHint);
            }
            if (clone.SourceNodeId.TransactionId) {
                ToProto(protoRequest.mutable_source_transaction_id(), clone.SourceNodeId.TransactionId);
            }
            ToProto(protoRequest.mutable_cloned_node_id(), clone.ClonedNodeId.ObjectId);
            if (clone.ClonedNodeId.TransactionId) {
                ToProto(protoRequest.mutable_cloned_transaction_id(), clone.ClonedNodeId.TransactionId);
            }
            protoRequest.set_mode(static_cast<int>(clone.Mode));
            ToProto(protoRequest.mutable_account_id(), clone.CloneAccountId);
            protoRequest.set_native_content_revision(clone.NativeContentRevision);
            multicellManager->PostToMaster(protoRequest, clone.ExternalCellTag);
        }

        for (const auto& externalNode : CreatedExternalNodes_) {
            NProto::TReqCreateForeignNode protoRequest;
            ToProto(protoRequest.mutable_node_id(), externalNode.NodeId.ObjectId);
            if (externalNode.NodeId.TransactionId) {
                ToProto(protoRequest.mutable_transaction_id(), externalNode.NodeId.TransactionId);
            }
            protoRequest.set_type(static_cast<int>(externalNode.NodeType));
            ToProto(protoRequest.mutable_explicit_node_attributes(), *externalNode.ReplicationExplicitAttributes);
            ToProto(protoRequest.mutable_inherited_node_attributes(), *externalNode.ReplicationInheritedAttributes);
            ToProto(protoRequest.mutable_account_id(), externalNode.AccountId);
            protoRequest.set_native_content_revision(externalNode.NativeContentRevision);
            multicellManager->PostToMaster(protoRequest, externalNode.ExternalCellTag);
        }

        ReleaseStagedObjects();
    }

    void Rollback() noexcept override
    {
        TTransactionalNodeFactoryBase::Rollback();

        ReleaseStagedObjects();
    }

    IStringNodePtr CreateString() override
    {
        return CreateNode(EObjectType::StringNode)->AsString();
    }

    IInt64NodePtr CreateInt64() override
    {
        return CreateNode(EObjectType::Int64Node)->AsInt64();
    }

    IUint64NodePtr CreateUint64() override
    {
        return CreateNode(EObjectType::Uint64Node)->AsUint64();
    }

    IDoubleNodePtr CreateDouble() override
    {
        return CreateNode(EObjectType::DoubleNode)->AsDouble();
    }

    IBooleanNodePtr CreateBoolean() override
    {
        return CreateNode(EObjectType::BooleanNode)->AsBoolean();
    }

    IMapNodePtr CreateMap() override
    {
        return CreateNode(EObjectType::MapNode)->AsMap();
    }

    IListNodePtr CreateList() override
    {
        return CreateNode(EObjectType::ListNode)->AsList();
    }

    IEntityNodePtr CreateEntity() override
    {
        THROW_ERROR_EXCEPTION("Entity nodes cannot be created inside Cypress");
    }

    NTransactionServer::TTransaction* GetTransaction() const override
    {
        return Transaction_;
    }

    bool ShouldPreserveCreationTime() const override
    {
        return Options_.PreserveCreationTime;
    }

    bool ShouldPreserveModificationTime() const override
    {
        return Options_.PreserveModificationTime;
    }

    bool ShouldPreserveExpirationTime() const override
    {
        return Options_.PreserveExpirationTime;
    }

    bool ShouldPreserveExpirationTimeout() const override
    {
        return Options_.PreserveExpirationTimeout;
    }

    bool ShouldPreserveOwner() const override
    {
        return Options_.PreserveOwner;
    }

    bool ShouldPreserveAcl() const override
    {
        return Options_.PreserveAcl;
    }

    TAccount* GetNewNodeAccount() const override
    {
        return Account_;
    }

    TAccount* GetClonedNodeAccount(TAccount* sourceAccount) const override
    {
        return Options_.PreserveAccount ? sourceAccount : Account_;
    }

    void ValidateClonedAccount(
        ENodeCloneMode mode,
        TAccount* sourceAccount,
        TClusterResources sourceResourceUsage,
        TAccount* clonedAccount) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->ValidateObjectLifeStage(clonedAccount);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(clonedAccount, EPermission::Use);

        // Resource limit check must be suppressed when moving nodes
        // without altering the account.
        if (mode != ENodeCloneMode::Move || clonedAccount != sourceAccount) {
            TClusterResources resourceUsageIncrease;
            if (Options_.PessimisticQuotaCheck && clonedAccount != sourceAccount) {
                resourceUsageIncrease = std::move(sourceResourceUsage)
                    .SetTabletCount(0)
                    .SetTabletStaticMemory(0);
            } else {
                resourceUsageIncrease = TClusterResources()
                    .SetNodeCount(1)
                    .SetDetailedMasterMemory(EMasterMemoryType::Nodes, 1);
            }
            securityManager->ValidateResourceUsageIncrease(clonedAccount, resourceUsageIncrease);
        }
    }

    ICypressNodeProxyPtr CreateNode(
        EObjectType type,
        TNodeId hintId = NullObjectId,
        IAttributeDictionary* inheritedAttributes = nullptr,
        IAttributeDictionary* explicitAttributes = nullptr) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& handler = cypressManager->FindHandler(type);
        if (!handler) {
            THROW_ERROR_EXCEPTION("Unknown object type %Qlv",
                type);
        }

        if (None(handler->GetFlags() & ETypeFlags::Creatable)) {
            THROW_ERROR_EXCEPTION("Nodes of type %Qlv cannot be created explicitly",
                type);
        }

        IAttributeDictionaryPtr explicitAttributeHolder;
        if (!explicitAttributes) {
            explicitAttributeHolder = CreateEphemeralAttributes();
            explicitAttributes = explicitAttributeHolder.Get();
        }
        IAttributeDictionaryPtr inheritedAttributeHolder;
        if (!inheritedAttributes) {
            inheritedAttributeHolder = CreateEphemeralAttributes();
            inheritedAttributes = inheritedAttributeHolder.Get();
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = GetNewNodeAccount();
        securityManager->ValidatePermission(account, EPermission::Use);
        auto deltaResources = TClusterResources()
            .SetNodeCount(1)
            .SetDetailedMasterMemory(EMasterMemoryType::Nodes, 1);
        securityManager->ValidateResourceUsageIncrease(account, deltaResources);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto currentCellRoles = multicellManager->GetMasterCellRoles(multicellManager->GetCellTag());

        bool isExternalizable = Any(handler->GetFlags() & ETypeFlags::Externalizable);
        bool isSequoiaNodeHost = Any(currentCellRoles & EMasterCellRoles::SequoiaNodeHost);
        bool isPortal = Shard_ && Shard_->GetRoot() && Shard_->GetRoot()->GetType() == EObjectType::PortalExit;
        bool isMulticell = !multicellManager->GetRegisteredMasterCellTags().empty();

        bool defaultExternal =
            isMulticell && isExternalizable &&
            (multicellManager->IsPrimaryMaster() || isPortal || isSequoiaNodeHost);
        ValidateCreateNonExternalNode(explicitAttributes);
        bool external = explicitAttributes->GetAndRemove<bool>("external", defaultExternal);

        const auto& dynamicConfig = GetDynamicConfig();
        double externalCellBias = explicitAttributes->GetAndRemove<double>("external_cell_bias", dynamicConfig->DefaultExternalCellBias);
        if (externalCellBias < 0.0 || externalCellBias > MaxExternalCellBias) {
            THROW_ERROR_EXCEPTION("\"external_cell_bias\" must be in range [0, %v]",
                MaxExternalCellBias);
        }

        auto optionalExternalCellTag = explicitAttributes->FindAndRemove<TCellTag>("external_cell_tag");

        auto externalCellTag = NotReplicatedCellTagSentinel;
        if (external) {
            if (None(handler->GetFlags() & ETypeFlags::Externalizable)) {
                THROW_ERROR_EXCEPTION("Type %Qlv is not externalizable",
                    handler->GetObjectType());
            }
            if (optionalExternalCellTag) {
                externalCellTag = *optionalExternalCellTag;
            } else {
                externalCellTag = multicellManager->PickSecondaryChunkHostCell(externalCellBias);
                if (externalCellTag == InvalidCellTag) {
                    if (Any(currentCellRoles & EMasterCellRoles::ChunkHost)) {
                        external = false;
                        externalCellTag = NotReplicatedCellTagSentinel;
                    } else {
                        THROW_ERROR_EXCEPTION("No secondary masters with a chunk host role were found");
                    }
                }
            }
        }

        if (externalCellTag == multicellManager->GetCellTag()) {
            external = false;
            externalCellTag = NotReplicatedCellTagSentinel;
        }

        if (externalCellTag == multicellManager->GetPrimaryCellTag()) {
            THROW_ERROR_EXCEPTION("Cannot place externalizable nodes at primary cell");
        }

        if (externalCellTag != NotReplicatedCellTagSentinel) {
            if (!multicellManager->IsRegisteredMasterCell(externalCellTag)) {
                THROW_ERROR_EXCEPTION("Unknown cell tag %v", externalCellTag);
            }

            auto cellRoles = multicellManager->GetMasterCellRoles(externalCellTag);
            if (None(cellRoles & EMasterCellRoles::ChunkHost) && None(cellRoles & EMasterCellRoles::DedicatedChunkHost)) {
                THROW_ERROR_EXCEPTION("Cell with tag %v cannot host chunks", externalCellTag);
            }
        }

        // INodeTypeHandler::Create and ::FillAttributes may modify the attributes.
        IAttributeDictionaryPtr replicationExplicitAttributes;
        IAttributeDictionaryPtr replicationInheritedAttributes;
        if (external) {
            replicationExplicitAttributes = explicitAttributes->Clone();
            replicationInheritedAttributes = inheritedAttributes->Clone();
        }

        auto* trunkNode = cypressManager->CreateNode(
            handler,
            hintId,
            TCreateNodeContext{
                .ExternalCellTag = externalCellTag,
                .Transaction = Transaction_,
                .InheritedAttributes = inheritedAttributes,
                .ExplicitAttributes = explicitAttributes,
                .Account = account,
                .Shard = Shard_,
                .ServiceTrunkNode = ServiceTrunkNode_,
                .UnresolvedPathSuffix = UnresolvedPathSuffix_
            });

        if (Shard_) {
            cypressManager->SetShard(trunkNode, Shard_);
        }

        RegisterCreatedNode(trunkNode);

        handler->FillAttributes(trunkNode, inheritedAttributes, explicitAttributes);

        auto* node = cypressManager->LockNode(
            trunkNode,
            Transaction_,
            ELockMode::Exclusive,
            false,
            true);

        if (external) {
            if (IsTableType(trunkNode->GetType())) {
                auto* table = trunkNode->As<TTableNode>();
                auto* schema = table->GetSchema();
                auto schemaId = schema->GetId();
                replicationExplicitAttributes->Remove("schema");
                replicationExplicitAttributes->Set("schema_id", schemaId);
                replicationExplicitAttributes->Set("schema_mode", table->GetSchemaMode());
            }

            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto externalCellTag = node->GetExternalCellTag();
            auto externalizedTransactionId = transactionManager->ExternalizeTransaction(node->GetTransaction(), {externalCellTag});
            CreatedExternalNodes_.push_back(TCreatedExternalNode{
                .NodeType = trunkNode->GetType(),
                .NodeId = TVersionedNodeId(trunkNode->GetId(), externalizedTransactionId),
                .AccountId = account->GetId(),
                .ReplicationExplicitAttributes = std::move(replicationExplicitAttributes),
                .ReplicationInheritedAttributes = std::move(replicationInheritedAttributes),
                .ExternalCellTag = externalCellTag,
                .NativeContentRevision = trunkNode->GetContentRevision()
            });
        }

        if (type == EObjectType::PortalEntrance)  {
            CreatedPortalEntrances_.push_back(TCreatedPortalEntrance{
                .Node = StageNode(node->As<TPortalEntranceNode>()),
                .InheritedAttributes = inheritedAttributes->Clone(),
                .ExplicitAttributes = explicitAttributes->Clone()
            });
        } else if (type == EObjectType::Rootstock) {
            CreatedRootstocks_.push_back(TCreatedRootstock{
                .Node = StageNode(node->As<TRootstockNode>()),
                .InheritedAttributes = inheritedAttributes->Clone(),
                .ExplicitAttributes = explicitAttributes->Clone(),
            });
        }

        securityManager->UpdateMasterMemoryUsage(trunkNode);

        return cypressManager->GetNodeProxy(trunkNode, Transaction_);
    }

    TCypressNode* InstantiateNode(
        TNodeId id,
        TCellTag externalCellTag) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->InstantiateNode(id, externalCellTag);

        if (Shard_) {
            cypressManager->SetShard(node, Shard_);
        }

        RegisterCreatedNode(node);

        return node;
    }

    TCypressNode* CloneNode(
        TCypressNode* sourceNode,
        ENodeCloneMode mode,
        TNodeId hintId = NullObjectId) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* clonedTrunkNode = cypressManager->CloneNode(sourceNode, this, mode, hintId);
        auto* clonedNode = cypressManager->LockNode(
            clonedTrunkNode,
            Transaction_,
            ELockMode::Exclusive,
            false,
            true);

        // NB: No need to call RegisterCreatedNode since
        // cloning a node involves calling ICypressNodeFactory::InstantiateNode,
        // which calls RegisterCreatedNode.

        if (clonedNode->IsExternal()) {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto externalCellTag = clonedNode->GetExternalCellTag();
            auto externalizedSourceTransactionId = transactionManager->ExternalizeTransaction(sourceNode->GetTransaction(), {externalCellTag});
            auto externalizedClonedTransactionId = transactionManager->ExternalizeTransaction(clonedNode->GetTransaction(), {externalCellTag});

            ClonedExternalNodes_.push_back(TClonedExternalNode{
                .Mode = mode,
                .SourceNodeId = TVersionedObjectId(sourceNode->GetId(), externalizedSourceTransactionId),
                .ClonedNodeId = TVersionedObjectId(clonedNode->GetId(), externalizedClonedTransactionId),
                .CloneAccountId = clonedNode->Account()->GetId(),
                .ExternalCellTag = externalCellTag,
                .NativeContentRevision = clonedNode->GetContentRevision()
            });
        }

        return clonedNode;
    }

    TCypressNode* EndCopyNodeCore(
        TCypressNode* trunkNode,
        TEndCopyContext* context,
        TNodeId sourceNodeId)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* clonedNode = cypressManager->LockNode(
            trunkNode,
            Transaction_,
            ELockMode::Exclusive,
            false,
            true);

        // NB: No need to call RegisterCreatedNode since
        // cloning a node involves calling ICypressNodeFactory::InstantiateNode,
        // which calls RegisterCreatedNode.

        if (clonedNode->IsExternal()) {
            auto transactionId = Transaction_->GetId();

            // NB: source node has been locked during BeginCopy, from source node's native cell.
            // Make sure to use correct transaction for copying.
            auto sourceNodeNativeCellTag = CellTagFromId(sourceNodeId);
            auto sourceNodeExternalizedTransactionId = transactionId;
            if (CellTagFromId(transactionId) != sourceNodeNativeCellTag) {
                sourceNodeExternalizedTransactionId =
                    NTransactionClient::MakeExternalizedTransactionId(transactionId, sourceNodeNativeCellTag);
            }

            TMasterTableSchemaId schemaId;
            if (IsTableType(trunkNode->GetType())) {
                auto* table = trunkNode->As<TTableNode>();
                auto* schema = table->GetSchema();
                schemaId = schema->GetId();
            }

            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto externalCellTag = clonedNode->GetExternalCellTag();
            auto clonedNodeExternalizedTransactionId = transactionManager->ExternalizeTransaction(Transaction_, {externalCellTag});
            ClonedExternalNodes_.push_back(TClonedExternalNode{
                .Mode = context->GetMode(),
                .SourceNodeId = TVersionedObjectId(sourceNodeId, sourceNodeExternalizedTransactionId),
                .ClonedNodeId = TVersionedObjectId(clonedNode->GetId(), clonedNodeExternalizedTransactionId),
                .CloneAccountId = clonedNode->Account()->GetId(),
                .ExternalCellTag = externalCellTag,
                .SchemaIdHint = schemaId
            });
        }

        return clonedNode;
    }

    TCypressNode* EndCopyNode(TEndCopyContext* context) override
    {
        // See BeginCopyCore.
        using NYT::Load;
        auto sourceNodeId = Load<TNodeId>(*context);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* clonedTrunkNode = cypressManager->EndCopyNode(context, this, sourceNodeId);

        if (context->IsOpaqueChild()) {
            if (!Transaction_) {
                const auto& objectManager = Bootstrap_->GetObjectManager();
                const auto& handler = objectManager->GetHandler(clonedTrunkNode);
                handler->ZombifyObject(clonedTrunkNode);
                handler->DestroyObject(clonedTrunkNode);

                THROW_ERROR_EXCEPTION("Opaque child cannot be created without transaction");
            }

            RegisterCreatedOpaqueChild(clonedTrunkNode);
            return clonedTrunkNode;
        }

        return EndCopyNodeCore(clonedTrunkNode, context, sourceNodeId);
    }

    TCypressNode* EndCopyNodeInplace(
        TCypressNode* trunkNode,
        TEndCopyContext* context) override
    {
        // See BeginCopyCore.
        using NYT::Load;
        auto sourceNodeId = Load<TNodeId>(*context);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->EndCopyNodeInplace(trunkNode, context, this, sourceNodeId);

        return EndCopyNodeCore(trunkNode, context, sourceNodeId);
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    TCypressShard* const Shard_;
    TTransaction* const Transaction_;
    TAccount* const Account_;
    const TNodeFactoryOptions Options_;
    TCypressNode* ServiceTrunkNode_;
    const TYPath UnresolvedPathSuffix_;

    std::vector<TCypressNode*> CreatedNodes_;
    std::vector<TCypressNode*> CreatedOpaqueChildren_;
    std::vector<TObject*> StagedObjects_;

    struct TCreatedPortalEntrance
    {
        TPortalEntranceNode* Node;
        IAttributeDictionaryPtr InheritedAttributes;
        IAttributeDictionaryPtr ExplicitAttributes;
    };
    std::vector<TCreatedPortalEntrance> CreatedPortalEntrances_;

    struct TCreatedRootstock
    {
        TRootstockNode* Node;
        IAttributeDictionaryPtr InheritedAttributes;
        IAttributeDictionaryPtr ExplicitAttributes;
    };
    std::vector<TCreatedRootstock> CreatedRootstocks_;

    struct TClonedExternalNode
    {
        ENodeCloneMode Mode;
        TVersionedNodeId SourceNodeId;
        TVersionedNodeId ClonedNodeId;
        TAccountId CloneAccountId;
        TCellTag ExternalCellTag;
        NHydra::TRevision NativeContentRevision;
        TMasterTableSchemaId SchemaIdHint;
    };
    std::vector<TClonedExternalNode> ClonedExternalNodes_;

    struct TCreatedExternalNode
    {
        EObjectType NodeType;
        TVersionedNodeId NodeId;
        TAccountId AccountId;
        IAttributeDictionaryPtr ReplicationExplicitAttributes;
        IAttributeDictionaryPtr ReplicationInheritedAttributes;
        TCellTag ExternalCellTag;
        NHydra::TRevision NativeContentRevision;
    };
    std::vector<TCreatedExternalNode> CreatedExternalNodes_;


    void RegisterCreatedNode(TCypressNode* trunkNode)
    {
        YT_ASSERT(trunkNode->IsTrunk());
        StageNode(trunkNode);
        CreatedNodes_.push_back(trunkNode);
    }

    void RegisterCreatedOpaqueChild(TCypressNode* opaqueChild)
    {
        YT_ASSERT(opaqueChild->IsTrunk());
        StageNode(opaqueChild);
        CreatedOpaqueChildren_.push_back(opaqueChild);
    }

    template <class T>
    T* StageNode(T* node)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* trunkNode = node->GetTrunkNode();
        objectManager->RefObject(trunkNode);
        StagedObjects_.push_back(trunkNode);
        return node;
    }

    void ReleaseStagedObjects()
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* object : StagedObjects_) {
            objectManager->UnrefObject(object);
        }
        StagedObjects_.clear();
    }

    void ValidateCreateNonExternalNode(IAttributeDictionary* explicitAttributes) const
    {
        auto explicitExternal = explicitAttributes->Find<bool>("external");
        if (!explicitExternal || explicitExternal.value()) {
            return;
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        const auto* user = securityManager->GetAuthenticatedUser();

        if (user->RecursiveMemberOf().contains(securityManager->GetSuperusersGroup())) {
            return;
        }

        static const TString AllowExternalFalseKey("allow_external_false");
        auto attribute  = user->FindAttribute(AllowExternalFalseKey);
        if (attribute && ConvertTo<bool>(*attribute)) {
            return;
        }

        THROW_ERROR_EXCEPTION(
            "User %Qv is not allowed to create explicitly non-external nodes. "
            "Check for `external=%%false' in object attributes and do not specify "
            "this attribute",
            user->GetName());
    }

    const TDynamicCypressManagerConfigPtr& GetDynamicConfig()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->CypressManager;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNodeTypeHandler
    : public TObjectTypeHandlerBase<TCypressNode>
{
public:
    TNodeTypeHandler(
        TCypressManager* owner,
        INodeTypeHandlerPtr underlyingHandler);

    ETypeFlags GetFlags() const override
    {
        return UnderlyingHandler_->GetFlags();
    }

    EObjectType GetType() const override
    {
        return UnderlyingHandler_->GetObjectType();
    }

    TObject* FindObject(TObjectId id) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->FindNode(TVersionedNodeId(id));
    }

    std::unique_ptr<TObject> InstantiateObject(TObjectId /*id*/) override
    {
        YT_ABORT();
    }

    TObject* CreateObject(
        TObjectId /*hintId*/,
        IAttributeDictionary* /*attributes*/) override
    {
        THROW_ERROR_EXCEPTION("Cypress nodes cannot be created via this call");
    }

private:
    TCypressManager* const Owner_;
    const INodeTypeHandlerPtr UnderlyingHandler_;


    TCellTagList DoGetReplicationCellTags(const TCypressNode* node) override
    {
        auto externalCellTag = node->GetExternalCellTag();
        return externalCellTag == NotReplicatedCellTagSentinel ? TCellTagList() : TCellTagList{externalCellTag};
    }

    TString DoGetName(const TCypressNode* node) override;
    TString DoGetPath(const TCypressNode* node) override;

    IObjectProxyPtr DoGetProxy(
        TCypressNode* node,
        TTransaction* transaction) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->GetNodeProxy(node, transaction);
    }

    TAccessControlDescriptor* DoFindAcd(TCypressNode* node) override
    {
        return &node->GetTrunkNode()->Acd();
    }

    TAcdList DoListAcds(TCypressNode* node) override
    {
        return UnderlyingHandler_->ListAcds(node);
    }

    std::optional<std::vector<TString>> DoListColumns(TCypressNode* node) override
    {
        return UnderlyingHandler_->ListColumns(node);
    }

    TObject* DoGetParent(TCypressNode* node) override
    {
        return node->GetParent();
    }

    void DoDestroySequoiaObject(
        TCypressNode* node,
        const ISequoiaTransactionPtr& transaction) noexcept override
    {
        return UnderlyingHandler_->DestroySequoiaObject(
            node,
            transaction);
    }

    void CheckInvariants(TBootstrap* bootstrap) override
    {
        for (auto [nodeId, node] : bootstrap->GetCypressManager()->Nodes()) {
            if (node->GetType() == GetType()) {
                node->CheckInvariants(bootstrap);
            }
        }
    }

    void DoZombifyObject(TCypressNode* node) noexcept override;
    void DoDestroyObject(TCypressNode* node) noexcept override;
    void DoRecreateObjectAsGhost(TCypressNode* node) noexcept override;
};

////////////////////////////////////////////////////////////////////////////////

class TLockTypeHandler
    : public TObjectTypeHandlerWithMapBase<TLock>
{
public:
    explicit TLockTypeHandler(TCypressManager* owner);

    EObjectType GetType() const override
    {
        return EObjectType::Lock;
    }

private:
    IObjectProxyPtr DoGetProxy(
        TLock* lock,
        TTransaction* /*transaction*/) override
    {
        return CreateLockProxy(Bootstrap_, &Metadata_, lock);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TResourceUsageVisitor
    : public ICypressNodeVisitor
{
public:
    TResourceUsageVisitor(
        TBootstrap* bootstrap,
        TCypressNode* trunkRootNode,
        TTransaction* rootNodeTransaction)
        : Bootstrap_(bootstrap)
        , TrunkRootNode_(trunkRootNode)
        , RootNodeTransaction_(rootNodeTransaction)
    { }

    TPromise<TYsonString> Run()
    {
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        auto invoker = hydraFacade->IsAutomatonLocked()
            ? hydraFacade->CreateEpochInvoker(GetCurrentInvoker())
            : hydraFacade->GetEpochAutomatonInvoker(EAutomatonThreadQueue::CypressTraverser);
        TraverseCypress(
            Bootstrap_->GetCypressManager(),
            Bootstrap_->GetTransactionManager(),
            Bootstrap_->GetObjectManager(),
            Bootstrap_->GetSecurityManager(),
            std::move(invoker),
            TrunkRootNode_,
            RootNodeTransaction_,
            this);
        return Promise_;
    }

private:
    TBootstrap* const Bootstrap_;
    TCypressNode* const TrunkRootNode_;
    TTransaction* const RootNodeTransaction_;

    const TPromise<TYsonString> Promise_ = NewPromise<TYsonString>();
    TRichClusterResources LocalCellResourceUsage_;
    std::vector<TFuture<TRichClusterResources>> RemoteCellResourceUsage_;

    void OnNode(TCypressNode* trunkNode, TTransaction* transaction) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->GetVersionedNode(trunkNode, transaction);
        if (node->GetType() == EObjectType::PortalEntrance) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            const auto& handler = objectManager->GetHandler(node);
            auto proxy = handler->GetProxy(node, transaction);

            auto asyncResourceUsage = proxy->GetBuiltinAttributeAsync(EInternedAttributeKey::RecursiveResourceUsage)
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TYsonString& ysonResourceUsage) {
                    TRichClusterResources result;
                    DeserializeRichClusterResources(result, ConvertToNode(ysonResourceUsage), Bootstrap_);
                    return result;
                }).AsyncVia(GetCurrentInvoker()));

            RemoteCellResourceUsage_.push_back(std::move(asyncResourceUsage));
        } else {
            LocalCellResourceUsage_ += GetNodeResourceUsage(node);
        }
    }

    void OnError(const TError& error) override
    {
        auto wrappedError = TError("Error computing recursive resource usage")
            << error;
        Promise_.Set(wrappedError);
    }

    void OnCompleted() override
    {
        AllSucceeded(std::move(RemoteCellResourceUsage_))
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TRichClusterResources>>& resourceUsageOrError) {
                if (!resourceUsageOrError.IsOK()) {
                    OnError(resourceUsageOrError);
                    return;
                }

                const auto& resourceUsage = resourceUsageOrError.Value();
                auto result = std::accumulate(resourceUsage.begin(), resourceUsage.end(), LocalCellResourceUsage_);

                TStringStream output;
                TYsonWriter writer(&output, EYsonFormat::Binary);
                SerializeRichClusterResources(result, &writer, Bootstrap_);
                writer.Flush();
                Promise_.Set(TYsonString(output.Str()));
            }).Via(GetCurrentInvoker()));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager
    : public ICypressManager
    , public TMasterAutomatonPart
{
public:
    explicit TCypressManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::CypressManager)
        , AccessTracker_(New<TAccessTracker>(bootstrap))
        , ExpirationTracker_(New<TExpirationTracker>(bootstrap))
        , NodeMap_(TNodeMapTraits(this))
        , RecursiveResourceUsageCache_(New<TRecursiveResourceUsageCache>(
            BIND_NO_PROPAGATE(&TCypressManager::DoComputeRecursiveResourceUsage, MakeStrong(this)),
            std::nullopt,
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::RecursiveResourceUsageCache)))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        RootNodeId_ = MakeWellKnownId(EObjectType::MapNode, multicellManager->GetCellTag());
        RootShardId_ = MakeCypressShardId(RootNodeId_);
        ResolveCache_ = New<TResolveCache>(RootNodeId_, multicellManager->IsPrimaryMaster());

        RegisterHandler(New<TStringNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TInt64NodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TUint64NodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TDoubleNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TBooleanNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TCypressMapNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TListNodeTypeHandler>(Bootstrap_));
        RegisterHandler(CreateLinkNodeTypeHandler(Bootstrap_));
        RegisterHandler(CreateDocumentNodeTypeHandler(Bootstrap_));
        RegisterHandler(CreateShardMapTypeHandler(Bootstrap_));
        RegisterHandler(CreatePortalEntranceTypeHandler(Bootstrap_));
        RegisterHandler(CreatePortalExitTypeHandler(Bootstrap_));
        RegisterHandler(CreatePortalEntranceMapTypeHandler(Bootstrap_));
        RegisterHandler(CreatePortalExitMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateRootstockTypeHandler(Bootstrap_));
        RegisterHandler(CreateScionTypeHandler(Bootstrap_));
        RegisterHandler(CreateRootstockMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateScionMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateClusterProxyNodeTypeHandler(Bootstrap_));
        RegisterHandler(New<TSequoiaMapNodeTypeHandler>(Bootstrap_));

        RegisterHandler(CreateSysNodeTypeHandler(Bootstrap_));
        RegisterHandler(CreateChunkLocationMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::ChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LostChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LostVitalChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::PrecariousChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::PrecariousVitalChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::UnderreplicatedChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::OverreplicatedChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::DataMissingChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::ParityMissingChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::OldestPartMissingChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::QuorumMissingChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::UnsafelyPlacedChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::InconsistentlyPlacedChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::UnexpectedOverreplicatedChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::ReplicaTemporarilyUnavailableChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::ForeignChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalLostChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalLostVitalChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalPrecariousChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalPrecariousVitalChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalUnderreplicatedChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalOverreplicatedChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalDataMissingChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalParityMissingChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalOldestPartMissingChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalQuorumMissingChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalUnsafelyPlacedChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalInconsistentlyPlacedChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalUnexpectedOverreplicatedChunkMap));
        RegisterHandler(CreateChunkMapTypeHandler(Bootstrap_, EObjectType::LocalReplicaTemporarilyUnavailableChunkMap));
        RegisterHandler(CreateChunkViewMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateChunkListMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateMediumMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateTransactionMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateTopmostTransactionMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateLockMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateOrchidTypeHandler(Bootstrap_));
        RegisterHandler(CreateClusterNodeNodeTypeHandler(Bootstrap_));
        RegisterHandler(CreateHostMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateRackMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateClusterNodeMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateFlavoredNodeMapTypeHandler(Bootstrap_, EObjectType::DataNodeMap));
        RegisterHandler(CreateFlavoredNodeMapTypeHandler(Bootstrap_, EObjectType::ExecNodeMap));
        RegisterHandler(CreateFlavoredNodeMapTypeHandler(Bootstrap_, EObjectType::TabletNodeMap));
        RegisterHandler(CreateFlavoredNodeMapTypeHandler(Bootstrap_, EObjectType::ChaosNodeMap));
        RegisterHandler(CreateDataCenterMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateFileTypeHandler(Bootstrap_));
        RegisterHandler(CreateMasterTableSchemaMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateTableTypeHandler(Bootstrap_));
        RegisterHandler(CreateReplicatedTableTypeHandler(Bootstrap_));
        RegisterHandler(CreateReplicationLogTableTypeHandler(Bootstrap_));
        RegisterHandler(CreateJournalTypeHandler(Bootstrap_));
        RegisterHandler(CreateAccountMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateAccountResourceUsageLeaseMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateUserMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateGroupMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateNetworkProjectMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateProxyRoleMapTypeHandler(Bootstrap_, EProxyKind::Http));
        RegisterHandler(CreateProxyRoleMapTypeHandler(Bootstrap_, EProxyKind::Rpc));
        RegisterHandler(CreatePoolTreeMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateCellNodeTypeHandler(Bootstrap_));
        RegisterHandler(CreateCellBundleMapTypeHandler(Bootstrap_, ECellarType::Chaos));
        RegisterHandler(CreateCellMapTypeHandler(Bootstrap_, ECellarType::Chaos));
        RegisterHandler(CreateVirtualCellMapTypeHandler(Bootstrap_, ECellarType::Chaos));
        RegisterHandler(CreateCellBundleMapTypeHandler(Bootstrap_, ECellarType::Tablet));
        RegisterHandler(CreateCellMapTypeHandler(Bootstrap_, ECellarType::Tablet));
        RegisterHandler(CreateVirtualCellMapTypeHandler(Bootstrap_, ECellarType::Tablet));
        RegisterHandler(CreateTabletMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateTabletActionMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateAreaMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateHunkStorageTypeHandler(Bootstrap_));
        RegisterHandler(CreateEstimatedCreationTimeMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateAccessControlObjectNamespaceMapTypeHandler(Bootstrap_));
        RegisterHandler(CreateZookeeperShardMapTypeHandler(Bootstrap_));

        RegisterLoader(
            "CypressManager.Keys",
            BIND(&TCypressManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "CypressManager.Values",
            BIND(&TCypressManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "CypressManager.Keys",
            BIND(&TCypressManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "CypressManager.Values",
            BIND(&TCypressManager::SaveValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraUpdateAccessStatistics, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraTouchNodes, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraCreateForeignNode, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraCloneForeignNode, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraRemoveExpiredNodes, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraLockForeignNode, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraUnlockForeignNode, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraImportTableSchema, Unretained(this)));
        // COMPAT(shakurov)
        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraFixNodeStatistics, Unretained(this)));
        // COMPAT(h0pless): Remove this after schema migration is complete.
        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraSetTableSchemas, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCypressManager::HydraSetAttributeOnTransactionCommit, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND_NO_PROPAGATE(
            &TCypressManager::OnTransactionCommitted,
            MakeStrong(this)));
        transactionManager->SubscribeTransactionAborted(BIND_NO_PROPAGATE(
            &TCypressManager::OnTransactionAborted,
            MakeStrong(this)));

        transactionManager->RegisterTransactionActionHandlers<NApi::NNative::NProto::TReqSetAttributeOnTransactionCommit>({
            .Prepare = BIND_NO_PROPAGATE(&TCypressManager::HydraPrepareSetAttributeOnTransactionCommit, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TCypressManager::HydraCommitSetAttributeOnTransactionCommit, Unretained(this)),
        });

        transactionManager->RegisterTransactionActionHandlers<NApi::NNative::NProto::TReqMergeToTrunkAndUnlockNode>({
            .Commit = BIND_NO_PROPAGATE(&TCypressManager::HydraCommitMergeToTrunkAndUnlockNode, Unretained(this)),
        });

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TLockTypeHandler>(this));
        objectManager->RegisterHandler(CreateShardTypeHandler(Bootstrap_, &ShardMap_));
        objectManager->RegisterHandler(CreateAccessControlObjectTypeHandler(Bootstrap_, &AccessControlObjectMap_));
        objectManager->RegisterHandler(CreateAccessControlObjectNamespaceTypeHandler(Bootstrap_, &AccessControlObjectNamespaceMap_));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TCypressManager::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND_NO_PROPAGATE(&TCypressManager::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND_NO_PROPAGATE(&TCypressManager::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }
    }

    void HydraFixNodeStatistics(NProto::TReqFixNodeStatistics* request)
    {
        for (auto i = 0; i < request->node_ids_size(); ++i) {
            auto nodeId = FromProto<TNodeId>(request->node_ids(i));
            auto* node = FindNode(TVersionedNodeId(nodeId));
            if (!IsObjectAlive(node)) {
                continue;
            }

            if (!IsChunkOwnerType(node->GetType())) {
                continue;
            }

            auto* chunkOwner = node->As<TChunkOwnerBase>();
            chunkOwner->FixStatistics();
        }
    }

    void OnReplicateKeysToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto accessControlObjectNamespaces = GetValuesSortedByKey(AccessControlObjectNamespaceMap_);
        for (auto* accessControlObjectNamespace : accessControlObjectNamespaces) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(accessControlObjectNamespace, cellTag);
        }

        auto accessControlObjects = GetValuesSortedByKey(AccessControlObjectMap_);
        for (auto* accessControlObject : accessControlObjects) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(accessControlObject, cellTag);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto accessControlObjectNamespaces = GetValuesSortedByKey(AccessControlObjectNamespaceMap_);
        for (auto* accessControlObjectNamespace : accessControlObjectNamespaces) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(accessControlObjectNamespace, cellTag);
        }

        auto accessControlObjects = GetValuesSortedByKey(AccessControlObjectMap_);
        for (auto* accessControlObject : accessControlObjects) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(accessControlObject, cellTag);
        }
    }

    void RegisterHandler(INodeTypeHandlerPtr handler) override
    {
        // No thread affinity is given here.
        // This will be called during init-time only.
        YT_VERIFY(handler);

        auto type = handler->GetObjectType();
        YT_VERIFY(IsVersionedType(type));
        YT_VERIFY(!TypeToHandler_[type]);
        TypeToHandler_[type] = handler;

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TNodeTypeHandler>(this, handler));
    }

    const INodeTypeHandlerPtr& FindHandler(EObjectType type) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (type < TEnumTraits<EObjectType>::GetMinValue() || type > TEnumTraits<EObjectType>::GetMaxValue()) {
            return NullTypeHandler;
        }

        return TypeToHandler_[type];
    }

    const INodeTypeHandlerPtr& GetHandler(EObjectType type) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& handler = FindHandler(type);
        YT_VERIFY(handler);
        return handler;
    }

    const INodeTypeHandlerPtr& GetHandler(const TCypressNode* node) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetHandler(node->GetType());
    }


    TCypressShard* CreateShard(TCypressShardId shardId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto shardHolder = TPoolAllocator::New<TCypressShard>(shardId);
        auto* shard = shardHolder.get();
        ShardMap_.Insert(shardId, std::move(shardHolder));
        return shard;
    }

    void SetShard(TCypressNode* node, TCypressShard* shard) override
    {
        YT_ASSERT(node->IsTrunk());
        YT_ASSERT(!node->GetShard());

        node->SetShard(shard);

        if (auto* account = node->Account().Get()) {
            UpdateShardNodeCount(shard, account, +1);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(shard);
    }

    void ResetShard(TCypressNode* node) override
    {
        YT_ASSERT(node->IsTrunk());

        auto* shard = node->GetShard();
        if (!shard) {
            return;
        }

        node->SetShard(nullptr);

        if (auto* account = node->Account().Get()) {
            UpdateShardNodeCount(shard, account, -1);
        }

        if (shard->GetRoot() == node) {
            shard->SetRoot(nullptr);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(shard);
    }

    void UpdateShardNodeCount(
        TCypressShard* shard,
        TAccount* account,
        int delta) override
    {
        auto it = shard->AccountStatistics().find(account);
        if (it == shard->AccountStatistics().end()) {
            it = shard->AccountStatistics().emplace(account, TCypressShardAccountStatistics()).first;
        }
        auto& statistics = it->second;
        statistics.NodeCount += delta;
        if (statistics.IsZero()) {
            shard->AccountStatistics().erase(it);
        }
    }


    std::unique_ptr<ICypressNodeFactory> CreateNodeFactory(
        TCypressShard* shard,
        TTransaction* transaction,
        TAccount* account,
        const TNodeFactoryOptions& options,
        TCypressNode* serviceTrunkNode,
        TYPath unresolvedPathSuffix) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return std::make_unique<TNodeFactory>(
            Bootstrap_,
            shard,
            transaction,
            account,
            options,
            serviceTrunkNode,
            std::move(unresolvedPathSuffix));
    }

    TCypressNode* CreateNode(
        const INodeTypeHandlerPtr& handler,
        TNodeId hintId,
        const TCreateNodeContext& context) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(handler);
        YT_VERIFY(context.Account);
        YT_VERIFY(context.InheritedAttributes);
        YT_VERIFY(context.ExplicitAttributes);

        ValidateCreatedNodeTypePermission(handler->GetObjectType());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        securityManager->ValidatePermission(context.Account, user, NSecurityServer::EPermission::Use);

        auto nodeHolder = handler->Create(hintId, context);
        auto* node = RegisterNode(std::move(nodeHolder));

        // Set owner.
        node->Acd().SetOwner(user);

        NodeCreated_.Fire(node);

        return node;
    }

    TCypressNode* InstantiateNode(
        TNodeId id,
        TCellTag externalCellTag) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto type = TypeFromId(id);
        const auto& handler = GetHandler(type);
        auto nodeHolder = handler->Instantiate(
            TVersionedNodeId(id),
            externalCellTag);
        return RegisterNode(std::move(nodeHolder));
    }

    TCypressNode* CloneNode(
        TCypressNode* sourceNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        TNodeId hintId = NullObjectId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(sourceNode);
        YT_VERIFY(factory);

        ValidateCreatedNodeTypePermission(sourceNode->GetType());

        auto* clonedAccount = factory->GetClonedNodeAccount(sourceNode->Account().Get());
        factory->ValidateClonedAccount(
            mode,
            sourceNode->Account().Get(),
            sourceNode->GetTotalResourceUsage(),
            clonedAccount);

        return DoCloneNode(
            sourceNode,
            factory,
            hintId,
            mode);
    }

    TCypressNode* EndCopyNode(
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(context);
        YT_VERIFY(factory);

        // See BeginCopyCore.
        auto type = Load<EObjectType>(*context);
        ValidateCreatedNodeTypePermission(type);

        const auto& handler = GetHandler(type);
        return handler->EndCopy(context, factory, sourceNodeId);
    }

    void EndCopyNodeInplace(
        TCypressNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(context);
        YT_VERIFY(factory);
        YT_VERIFY(trunkNode->IsTrunk());

        // See BeginCopyCore.
        auto type = Load<EObjectType>(*context);
        if (type != trunkNode->GetType() &&
           !(type == EObjectType::MapNode && trunkNode->GetType() == EObjectType::PortalExit))
        {
            THROW_ERROR_EXCEPTION("Cannot inplace copy node %v of type %Qlv to node %v of type %Qlv",
                sourceNodeId,
                type,
                trunkNode->GetId(),
                trunkNode->GetType());
        }

        const auto& handler = GetHandler(type);
        return handler->EndCopyInplace(trunkNode, context, factory, sourceNodeId);
    }


    TCypressMapNode* GetRootNode() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return RootNode_;
    }

    TCypressShard* GetRootCypressShard() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return RootShard_;
    }

    bool IsShardRoot(const TObject* object) const override
    {
        return object == GetRootNode() ||
            object->GetType() == EObjectType::PortalExit;
    }

    TCypressNode* GetNodeOrThrow(TVersionedNodeId id) override
    {
        Bootstrap_->VerifyPersistentStateRead();

        auto* node = FindNode(id);
        // NB: Branches always have zero ref counter.
        if (!node || (!id.TransactionId && !IsObjectAlive(node))) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such node %v",
                id);
        }

        return node;
    }

    TYPath GetNodePath(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        EPathRootType* pathRootType = nullptr) override
    {
        auto fallbackToId = [&] {
            if (pathRootType) {
                *pathRootType = EPathRootType::Other;
            }
            return FromObjectId(trunkNode->GetId());
        };

        using TToken = std::variant<TStringBuf, int>;
        TCompactVector<TToken, 64> tokens;

        auto* currentNode = GetVersionedNode(trunkNode, transaction);
        while (true) {
            auto* currentTrunkNode = currentNode->GetTrunkNode();
            auto* currentParentTrunkNode = currentNode->GetParent();
            if (!currentParentTrunkNode) {
                break;
            }
            auto* currentParentNode = GetVersionedNode(currentParentTrunkNode, transaction);
            switch (currentParentTrunkNode->GetNodeType()) {
                case ENodeType::Map: {
                    auto key = FindMapNodeChildKey(currentParentNode->As<TCypressMapNode>(), currentTrunkNode);
                    if (!key.data()) {
                        return fallbackToId();
                    }
                    tokens.emplace_back(key);
                    break;
                }
                case ENodeType::List: {
                    auto index = FindListNodeChildIndex(currentParentNode->As<TListNode>(), currentTrunkNode);
                    if (index < 0) {
                        return fallbackToId();
                    }
                    tokens.emplace_back(index);
                    break;
                }
                default:
                    YT_ABORT();
            }
            currentNode = currentParentNode;
        }

        TStringBuilder builder;

        if (currentNode->GetTrunkNode() == RootNode_) {
            builder.AppendChar('/');
            if (pathRootType) {
                *pathRootType = EPathRootType::RootNode;
            }
        } else if (currentNode->GetType() == EObjectType::PortalExit) {
            const auto* portalExit = currentNode->GetTrunkNode()->As<TPortalExitNode>();
            builder.AppendString(portalExit->GetPath());
            if (pathRootType) {
                *pathRootType = EPathRootType::PortalExit;
            }
        } else if (currentNode->ImmutableSequoiaProperties()) {
            builder.AppendString(currentNode->ImmutableSequoiaProperties()->Path);
            if (pathRootType) {
                *pathRootType = EPathRootType::SequoiaNode;
            }
        } else {
            return fallbackToId();
        }

        for (auto it = tokens.rbegin(); it != tokens.rend(); ++it) {
            auto token = *it;
            builder.AppendChar('/');
            Visit(token,
                [&] (TStringBuf value) {
                    AppendYPathLiteral(&builder, value);
                },
                [&] (int value) {
                    AppendYPathLiteral(&builder, value);
                });
        }

        return builder.Flush();
    }

    TYPath GetNodePath(const ICypressNodeProxy* nodeProxy, EPathRootType* pathRootType = nullptr) override
    {
        return GetNodePath(nodeProxy->GetTrunkNode(), nodeProxy->GetTransaction(), pathRootType);
    }

    TCypressNode* ResolvePathToTrunkNode(const TYPath& path, TTransaction* transaction) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* object = objectManager->ResolvePathToObject(
            path,
            transaction,
            IObjectManager::TResolvePathOptions{});
        if (!IsVersionedType(object->GetType())) {
            THROW_ERROR_EXCEPTION("Path %v points to a nonversioned %Qlv object instead of a node",
                path,
                object->GetType());
        }
        return object->As<TCypressNode>();
    }

    ICypressNodeProxyPtr ResolvePathToNodeProxy(const TYPath& path, TTransaction* transaction) override
    {
        auto* trunkNode = ResolvePathToTrunkNode(path, transaction);
        return GetNodeProxy(trunkNode, transaction);
    }

    TCypressNode* FindNode(
        TCypressNode* trunkNode,
        TTransaction* transaction) override
    {
        Bootstrap_->VerifyPersistentStateRead();

        YT_ASSERT(trunkNode->IsTrunk());

        // Fast path -- no transaction.
        if (!transaction) {
            return trunkNode;
        }

        TVersionedNodeId versionedId(trunkNode->GetId(), GetObjectId(transaction));
        return FindNode(versionedId);
    }

    TCypressNode* GetVersionedNode(
        TCypressNode* trunkNode,
        TTransaction* transaction) override
    {
        Bootstrap_->VerifyPersistentStateRead();

        YT_ASSERT(trunkNode->IsTrunk());

        auto* currentTransaction = transaction;
        while (true) {
            auto* currentNode = FindNode(trunkNode, currentTransaction);
            if (currentNode) {
                return currentNode;
            }
            currentTransaction = currentTransaction->GetParent();
        }
    }

    ICypressNodeProxyPtr GetNodeProxy(
        TCypressNode* trunkNode,
        TTransaction* transaction) override
    {
        Bootstrap_->VerifyPersistentStateRead();

        YT_ASSERT(trunkNode->IsTrunk());

        const auto& handler = GetHandler(trunkNode);
        return handler->GetProxy(trunkNode, transaction);
    }


    TCypressNode* LockNode(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        bool recursive = false,
        bool dontLockForeign = false) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());
        YT_VERIFY(request.Mode != ELockMode::None);
        YT_VERIFY(!recursive || request.Key.Kind == ELockKeyKind::None);
        YT_VERIFY(!transaction || IsObjectAlive(transaction));

        auto error = CheckLock(
            trunkNode,
            transaction,
            request,
            recursive);
        error.ThrowOnError();

        if (IsLockRedundant(trunkNode, transaction, request)) {
            return GetVersionedNode(trunkNode, transaction);
        }

        TCypressNode* lockedNode = nullptr;
        ForEachSubtreeNode(trunkNode, transaction, recursive, [&] (TCypressNode* child) {
            auto* lock = DoCreateLock(child, transaction, request, true);
            auto* lockedChild = DoAcquireLock(lock, dontLockForeign);
            if (child == trunkNode) {
                lockedNode = lockedChild;
            }
        });

        YT_VERIFY(lockedNode);
        return lockedNode;
    }

    void UnlockNode(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        bool recursive,
        bool explicitOnly)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        auto error = CheckUnlock(trunkNode, transaction, recursive, explicitOnly);
        error.ThrowOnError();

        ForEachSubtreeNode(trunkNode, transaction, recursive, [&] (TCypressNode* trunkChild) {
            if (IsUnlockRedundant(trunkChild, transaction, explicitOnly)) {
                return;
            }

            DoUnlockNode(trunkChild, transaction, explicitOnly);
        });
    }

    void UnlockNode(
        TCypressNode* trunkNode,
        TTransaction* transaction) override
    {
        UnlockNode(
            trunkNode,
            transaction,
            /*recursive*/ false,
            /*explicitOnly*/ true);
    }

    void DoUnlockNode(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        bool explicitOnly)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(transaction);
        YT_VERIFY(!transaction->Locks().empty());

        // Nodes must be unbranched before locks are released (because unbranching uses them).
        // The effect of releasing locks must therefore be predicted without actually affecting them.
        // Make sure this prediction is consistent with what actually releasing locks does.

        auto strongestLockModeBefore = ELockMode::None;
        auto strongestLockModeAfter = ELockMode::None;
        for (auto* lock : transaction->Locks()) {
            YT_ASSERT(lock->GetTransaction() == transaction);

            if (lock->GetTrunkNode() != trunkNode) {
                continue;
            }

            if (lock->GetState() != ELockState::Acquired) {
                continue;
            }

            auto lockMode = lock->Request().Mode;

            if (lockMode > strongestLockModeBefore) {
                strongestLockModeBefore = lockMode;
            }

            // This lock won't be destroyed and thus will affect the "after" mode.
            if (!ShouldRemoveLockOnUnlock(lock, explicitOnly) && lockMode > strongestLockModeAfter) {
                strongestLockModeAfter = lockMode;
            }

            if (strongestLockModeBefore == ELockMode::Exclusive &&
                strongestLockModeAfter == ELockMode::Exclusive)
            {
                // As strong as it gets.
                break;
            }
        }

        // Again, the order is crucial: first unbranch (or update) nodes, then destroy locks.

        if (strongestLockModeBefore != strongestLockModeAfter) {
            RemoveOrUpdateNodesOnLockModeChange(trunkNode, transaction, strongestLockModeBefore, strongestLockModeAfter);
        }

        RemoveTransactionNodeLocks(trunkNode, transaction, explicitOnly);

        if (trunkNode->IsExternal() && trunkNode->IsNative()) {
            PostUnlockForeignNodeRequest(trunkNode, transaction, explicitOnly);
        }

        YT_LOG_DEBUG("Node explicitly unlocked (NodeId: %v, TransactionId: %v)",
            trunkNode->GetId(),
            transaction->GetId());

        CheckPendingLocks(trunkNode);
    }

    void RemoveTransactionNodeLocks(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        bool explicitOnly)
    {
        if (trunkNode->LockingState().IsEmpty()) {
            return;
        }

        auto isLockRelevant = [&] (TLock* l) { return l->GetTransaction() == transaction; };

        auto maybeRemoveLock = [&] (TLock* lock) {
            YT_ASSERT(lock->GetTrunkNode() == trunkNode);
            if (isLockRelevant(lock) && ShouldRemoveLockOnUnlock(lock, explicitOnly)) {
                DoRemoveLock(lock, false /*resetEmptyLockingState*/);
            }
        };

        auto& acquiredLocks = trunkNode->MutableLockingState()->AcquiredLocks;
        for (auto it = acquiredLocks.begin(); it != acquiredLocks.end(); ) {
            auto* lock = *it++; // Removing a lock invalidates the iterator.
            maybeRemoveLock(lock);
        }

        auto& pendingLocks = trunkNode->MutableLockingState()->PendingLocks;
        for (auto it = pendingLocks.begin(); it != pendingLocks.end(); ) {
            auto* lock = *it++; // Removing a lock invalidates the iterator.
            maybeRemoveLock(lock);
        }

        auto nodeFullyUnlocked =
            std::find_if(acquiredLocks.begin(), acquiredLocks.end(), isLockRelevant) == acquiredLocks.end();

        if (nodeFullyUnlocked) {
            transaction->LockedNodes().erase(trunkNode);
        }

        trunkNode->ResetLockingStateIfEmpty();
    }

    bool ShouldRemoveLockOnUnlock(TLock* lock, bool explicitOnly)
    {
        return !explicitOnly || !lock->GetImplicit();
    }

    // Returns the mode of the strongest lock among all of the descendants
    // (branches, subbranches, subsubbranches...) of the node.
    // NB: doesn't distinguish ELockMode::None and ELockMode::Snapshot - returns
    // the latter even if there're no locks at all.
    // (This is because snapshot locks may lie arbitrarily deep without
    // affecting ancestors, and we want to avoid traversing the whole subtree.)
    ELockMode GetStrongestLockModeOfNestedTransactions(TCypressNode* trunkNode, TTransaction* transaction)
    {
        YT_VERIFY(transaction);

        auto result = ELockMode::Snapshot;
        for (auto* nestedTransaction : transaction->NestedTransactions()) {
            for (auto* branchedNode : nestedTransaction->BranchedNodes()) {
                if (branchedNode->GetTrunkNode() == trunkNode) {
                    auto lockMode = branchedNode->GetLockMode();
                    YT_VERIFY(lockMode != ELockMode::None);

                    if (result < lockMode) {
                        result = lockMode;

                        if (result == ELockMode::Exclusive) {
                            // As strong as it gets.
                            return result;
                        }
                    }

                }
            }
        }

        return result;
    }

    //! Patches the version tree of the #trunkNode, performing modifications
    //! necessary when the lock mode of the branch corresponding to #transaction
    //! is about to be changed.
    /*!
     *  The lock mode change may happen:
     *    - when someone explicitly unlocks a node;
     *    - on transaction abort.
     *
     *  In either case it may be necessary:
     *    - to unbranch the branch created by #transaction or
     *    - to update that branch's lock mode;
     *    - to unbranch branches created by #transaction's ancestors or
     *    - to update those branches' lock modes.
     *
     *  In the case of an explicit unlock, the need to perform the patch-up is self-evident.
     *  In the case of transaction abort, the need for the patch-up is argued in YT-10796.
     */
    void RemoveOrUpdateNodesOnLockModeChange(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode strongestLockModeBefore,
        ELockMode strongestLockModeAfter)
    {
        auto mustUnbranchThisNode = strongestLockModeAfter <= ELockMode::None;
        auto mustUpdateThisNode = !mustUnbranchThisNode;
        auto mustUnbranchAboveNodes = strongestLockModeAfter <= ELockMode::Snapshot && strongestLockModeBefore > ELockMode::Snapshot;
        auto mustUpdateAboveNodes = strongestLockModeAfter > ELockMode::Snapshot;

        auto strongestLockModeBelow = GetStrongestLockModeOfNestedTransactions(trunkNode, transaction);

        auto* branchedNode = GetNode(TVersionedNodeId{trunkNode->GetId(), transaction->GetId()});
        YT_VERIFY(branchedNode->GetLockMode() != ELockMode::None);

        auto newLockMode = strongestLockModeAfter;
        if (strongestLockModeBelow > ELockMode::Snapshot) {
            newLockMode = std::max(newLockMode, strongestLockModeBelow);

            mustUnbranchThisNode = false;
            mustUnbranchAboveNodes = false;

            if (branchedNode->GetLockMode() == newLockMode) {
                mustUpdateThisNode = false;
                mustUpdateAboveNodes = false;
            } else {
                mustUpdateThisNode = true;
                mustUpdateAboveNodes = true;
            }
        }

        TCompactSet<TCypressNode*, 8> unbranchedNodes;
        auto unbranchNode = [&] (TCypressNode* node) -> TCypressNode* {
            auto* originator = node->GetOriginator();
            auto* transaction = node->GetTransaction();

            DestroyBranchedNode(transaction, node);

            auto& branchedNodes = transaction->BranchedNodes();
            auto it = std::remove(branchedNodes.begin(), branchedNodes.end(), node);
            branchedNodes.erase(it, branchedNodes.end());

            unbranchedNodes.insert(node);

            return originator;
        };
        YT_VERIFY(!mustUnbranchThisNode || !mustUpdateThisNode);

        // Store the originator before (maybe) unbranching.
        TCypressNode* newOriginator = branchedNode->GetOriginator();
        if (mustUnbranchThisNode) {
            YT_VERIFY(unbranchNode(branchedNode) == newOriginator);
        }

        if (mustUpdateThisNode) {
            YT_VERIFY(!mustUnbranchThisNode);
            branchedNode->SetLockMode(newLockMode);
        }

        YT_VERIFY(!mustUnbranchAboveNodes || !mustUpdateAboveNodes);
        if (mustUnbranchAboveNodes || mustUpdateAboveNodes) {
            // Process nodes above until another lock is met.
            for (auto* aboveNode = newOriginator; aboveNode != trunkNode; ) {
                // Make sure to get the originator of the current node *before* it's unbranched.
                auto* nextOriginator = aboveNode->GetOriginator();

                auto* aboveNodeTransaction = aboveNode->GetTransaction();
                if (aboveNodeTransaction->LockedNodes().contains(trunkNode)) {
                    break;
                }

                auto strongestLockModeBelow = GetStrongestLockModeOfNestedTransactions(trunkNode, aboveNodeTransaction);

                auto updateNode = [&] () {
                    aboveNode->SetLockMode(strongestLockModeBelow == ELockMode::Snapshot
                        ? strongestLockModeBelow = ELockMode::None
                        : strongestLockModeBelow);
                };

                if (mustUpdateAboveNodes) {
                    updateNode();
                }

                if (mustUnbranchAboveNodes) {
                    // Be careful: it's not always possible to continue unbranching - some sibling may have a branch.
                    if (strongestLockModeBelow <= ELockMode::Snapshot) {
                        newOriginator = unbranchNode(aboveNode);
                    } else {
                        // Switch to updating.
                        updateNode();
                        mustUnbranchAboveNodes = false;
                        mustUpdateAboveNodes = true;
                    }
                }

                aboveNode = nextOriginator;
            }
        }

        if (!unbranchedNodes.empty()) {
            // Nested arbitrarily deeply in the transaction tree, there may lie
            // snapshot-locked branched nodes referencing the nodes we've unbranched
            // as its originator. We must update these references to avoid dangling pointers.
            auto* newOriginatorTransaction = newOriginator->GetTransaction();

            VisitTransactionTree(
                newOriginatorTransaction
                    ? newOriginatorTransaction
                    : transaction->GetTopmostTransaction(),
                [&] (TTransaction* t) {
                    // Locks are released later, so be sure to skip the node we've already unbranched.
                    if (t == transaction) {
                        return;
                    }

                    for (auto* branchedNode : t->BranchedNodes()) {
                        auto* branchedNodeOriginator = branchedNode->GetOriginator();
                        if (unbranchedNodes.count(branchedNodeOriginator) != 0) {
                            branchedNode->SetOriginator(newOriginator);
                        }
                    }
                });
        }
    }

    //! Traverses a transaction tree. The root transaction does not have to be topmost.
    template <class F>
    void VisitTransactionTree(TTransaction* rootTransaction, F&& processTransaction)
    {
        // BFS queue.
        TCompactQueue<TTransaction*, 64> queue;
        queue.Push(rootTransaction);

        while (!queue.Empty()) {
            auto* transaction = queue.Pop();

            for (auto* transaction : transaction->NestedTransactions()) {
                queue.Push(transaction);
            }

            processTransaction(transaction);
        }
    }

    //! Destroys a single branched node.
    void DestroyBranchedNode(
        TTransaction* transaction,
        TCypressNode* branchedNode)
    {
        YT_ASSERT(branchedNode->GetTransaction() == transaction);

        const auto& objectManager = Bootstrap_->GetObjectManager();

        const auto& handler = GetHandler(branchedNode);

        auto* trunkNode = branchedNode->GetTrunkNode();
        auto branchedNodeId = branchedNode->GetVersionedId();

        // Drop the implicit reference to the originator.
        objectManager->UnrefObject(trunkNode);

        if (branchedNode->GetLockMode() != ELockMode::Snapshot) {
            // Cleanup the branched node.
            auto* originatingNode = branchedNode->GetOriginator();
            handler->Unbranch(originatingNode, branchedNode);
        }

        // Remove the node.
        handler->Zombify(branchedNode);
        handler->Destroy(branchedNode);
        NodeMap_.Remove(branchedNodeId);

        YT_LOG_DEBUG("Branched node removed (NodeId: %v)", branchedNodeId);
    }

    TCreateLockResult CreateLock(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool waitable) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());
        YT_VERIFY(transaction);
        YT_VERIFY(request.Mode != ELockMode::None);

        if (waitable && !transaction) {
            THROW_ERROR_EXCEPTION("Waitable lock requires a transaction");
        }

        if (request.Mode == ELockMode::Snapshot && !transaction) {
            THROW_ERROR_EXCEPTION("%Qlv lock requires a transaction",
                request.Mode);
        }

        // Try to lock without waiting in the queue.
        auto error = CheckLock(
            trunkNode,
            transaction,
            request,
            false);

        // Is it OK?
        if (error.IsOK()) {
            auto* lock = DoCreateLock(trunkNode, transaction, request, false);
            auto* branchedNode = DoAcquireLock(lock);
            return {lock, branchedNode};
        }

        // Should we wait?
        if (!waitable) {
            THROW_ERROR error;
        }

        // Will wait.
        return {DoCreateLock(trunkNode, transaction, request, false), nullptr};
    }

    void SetModified(TCypressNode* node, EModificationType modificationType) override
    {
        node->SetModified(modificationType);

        YT_LOG_ACCESS(
            node->GetId(),
            GetNodePath(node->GetTrunkNode(), node->GetTransaction()),
            node->GetTransaction(),
            "Revise",
            {
                {"revision_type", FormatEnum(modificationType)},
                {"revision", ToString(node->GetRevision())}
            });
    }

    void SetAccessed(TCypressNode* trunkNode) override
    {
        Bootstrap_->VerifyPersistentStateRead();

        YT_ASSERT(trunkNode->IsTrunk());

        if (HydraManager_->IsLeader() || HydraManager_->IsFollower() && !HasMutationContext()) {
            AccessTracker_->SetAccessed(trunkNode);
        }
    }

    void SetTouched(TCypressNode* trunkNode) override
    {
        Bootstrap_->VerifyPersistentStateRead();

        YT_ASSERT(trunkNode->IsTrunk());

        if (!trunkNode->TryGetExpirationTimeout()) {
            return;
        }

        if (HydraManager_->IsLeader() || HydraManager_->IsFollower() && !HasMutationContext()) {
            AccessTracker_->SetTouched(trunkNode);
        }
    }

    void SetExpirationTime(TCypressNode* node, std::optional<TInstant> time) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto oldExpirationTime = node->TryGetExpirationTime();

        if (time) {
            node->SetExpirationTime(*time);
        } else {
            node->RemoveExpirationTime();
        }

        if (node->IsTrunk() && node->TryGetExpirationTime() != oldExpirationTime) {
            ExpirationTracker_->OnNodeExpirationTimeUpdated(node);
        } // Otherwise the tracker will be notified when and if the node is merged in.
    }

    void MergeExpirationTime(TCypressNode* originatingNode, TCypressNode* branchedNode) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto oldExpirationTime = originatingNode->TryGetExpirationTime();
        originatingNode->MergeExpirationTime(branchedNode);

        if (originatingNode->IsTrunk() &&
            originatingNode->TryGetExpirationTime() != oldExpirationTime)
        {
            ExpirationTracker_->OnNodeExpirationTimeUpdated(originatingNode);
        }
    }

    void SetExpirationTimeout(TCypressNode* node, std::optional<TDuration> timeout) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto oldExpirationTimeout = node->TryGetExpirationTimeout();

        if (timeout) {
            node->SetExpirationTimeout(*timeout);

            if (node->IsTrunk() && !node->GetTouchTime()) {
                auto* context = GetCurrentMutationContext();
                YT_VERIFY(context);
                node->SetTouchTime(context->GetTimestamp());
            }

            // NB: Touch time should not be updated here. This might've been
            // suppressed. Otherwise the node will be touched as usual (as part
            // of Invoke).
        } else {
            node->RemoveExpirationTimeout();

            if (node->IsTrunk()) {
                node->SetTouchTime(TInstant::Zero());
            }
        }

        if (node->IsTrunk() &&
            node->TryGetExpirationTimeout() != oldExpirationTimeout)
        {
            ExpirationTracker_->OnNodeExpirationTimeoutUpdated(node);
        } // Otherwise the tracker will be notified when and if the node is merged in.
    }

    void MergeExpirationTimeout(TCypressNode* originatingNode, TCypressNode* branchedNode) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto oldExpirationTimeout = originatingNode->TryGetExpirationTimeout();

        originatingNode->MergeExpirationTimeout(branchedNode);

        if (originatingNode->IsTrunk()) {
            // Touching a node upon merging is not suppressible by design.
            // NB: Changing this requires tracking touch time for branched nodes.
            if (originatingNode->TryGetExpirationTimeout()) {
                auto* context = GetCurrentMutationContext();
                YT_VERIFY(context);
                originatingNode->SetTouchTime(context->GetTimestamp());
            } else {
                YT_VERIFY(HasMutationContext());
                originatingNode->SetTouchTime(TInstant::Zero());
            }
        }

        if (originatingNode->IsTrunk() &&
            originatingNode->TryGetExpirationTimeout() != oldExpirationTimeout)
        {
            ExpirationTracker_->OnNodeExpirationTimeoutUpdated(originatingNode);
        }
    }

    TSubtreeNodes ListSubtreeNodes(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        bool includeRoot) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        TSubtreeNodes result;
        ListSubtreeNodes(trunkNode, transaction, includeRoot, &result);
        return result;
    }

    bool IsOrphaned(TCypressNode* trunkNode) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        auto* currentNode = trunkNode;
        while (true) {
            if (!IsObjectAlive(currentNode)) {
                return true;
            }
            if (currentNode == RootNode_ || currentNode->GetType() == EObjectType::PortalExit) {
                return false;
            }
            currentNode = currentNode->GetParent();
        }
    }


    TCypressNodeList GetNodeOriginators(
        TTransaction* transaction,
        TCypressNode* trunkNode) override
    {
        Bootstrap_->VerifyPersistentStateRead();

        YT_ASSERT(trunkNode->IsTrunk());

        // Fast path.
        if (!transaction) {
            return TCypressNodeList(1, trunkNode);
        }

        // Slow path.
        TCypressNodeList result;
        auto* currentNode = GetVersionedNode(trunkNode, transaction);
        while (currentNode) {
            result.push_back(currentNode);
            currentNode = currentNode->GetOriginator();
        }

        return result;
    }

    TCypressNodeList GetNodeReverseOriginators(
        TTransaction* transaction,
        TCypressNode* trunkNode) override
    {
        Bootstrap_->VerifyPersistentStateRead();

        YT_ASSERT(trunkNode->IsTrunk());

        auto result = GetNodeOriginators(transaction, trunkNode);
        std::reverse(result.begin(), result.end());
        return result;
    }

    const TResolveCachePtr& GetResolveCache() const override
    {
        return ResolveCache_;
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Node, TCypressNode);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Lock, TLock);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Shard, TCypressShard);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(AccessControlObject, TAccessControlObject);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(AccessControlObjectNamespace, TAccessControlObjectNamespace);

    TAccessControlObjectNamespace* CreateAccessControlObjectNamespace(
        const TString& name,
        TObjectId hintId = NullObjectId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ValidateAccessControlObjectNamespaceName(name);

        if (FindAccessControlObjectNamespaceByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Access control object namespace %Qv already exists",
                name);
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::AccessControlObjectNamespace, hintId);

        auto objectHolder = TPoolAllocator::New<TAccessControlObjectNamespace>(id);
        objectHolder->SetName(name);
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        objectHolder->Acd().SetOwner(user);
        auto* object = AccessControlObjectNamespaceMap_.Insert(id, std::move(objectHolder));

        RegisterAccessControlObjectNamespace(object);

        // Make the fake reference.
        YT_VERIFY(object->RefObject() == 1);

        YT_LOG_DEBUG("Access control object namespace registered (Id: %v, Name: %v)",
            object->GetId(),
            object->GetName());

        return object;
    }

    TAccessControlObjectNamespace* FindAccessControlObjectNamespaceByName(
        const TString& name) const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        auto it = NameToAccessControlObjectNamespaceMap_.find(name);
        return it == NameToAccessControlObjectNamespaceMap_.end() ? nullptr : it->second;
    }

    void ZombifyAccessControlObjectNamespace(TAccessControlObjectNamespace* object) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(!IsObjectAlive(object));
        YT_VERIFY(object->Members().empty());

        UnregisterAccessControlObjectNamespace(object);

        YT_LOG_DEBUG("Access control object namespace unregistered (Id: %v, Name: %v)",
            object->GetId(),
            object->GetName());
    }

    int GetAccessControlObjectNamespaceCount() const override
    {
        return ssize(NameToAccessControlObjectNamespaceMap_);
    }

    void RegisterAccessControlObjectNamespace(TAccessControlObjectNamespace* object)
    {
        YT_VERIFY(NameToAccessControlObjectNamespaceMap_.emplace(object->GetName(), object).second);
    }

    void UnregisterAccessControlObjectNamespace(TAccessControlObjectNamespace* object)
    {
        YT_VERIFY(NameToAccessControlObjectNamespaceMap_.erase(object->GetName()) == 1);
    }

    TAccessControlObject* CreateAccessControlObject(
        const TString& name,
        const TString& namespace_,
        TObjectId hintId = NullObjectId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ValidateAccessControlObjectName(name);

        auto* namespaceObject = FindAccessControlObjectNamespaceByName(namespace_);
        if (!namespaceObject) {
            THROW_ERROR_EXCEPTION(
                "Access control object namespace %Qv not found",
                namespace_);
        }

        if (namespaceObject->FindMember(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Access control object %Qv/%Qv already exists",
                namespaceObject->GetName(),
                name);
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::AccessControlObject, hintId);

        auto objectHolder = TPoolAllocator::New<TAccessControlObject>(id);
        objectHolder->SetName(name);
        objectHolder->Namespace() = TAccessControlObjectNamespacePtr(namespaceObject);
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        objectHolder->Acd().SetOwner(user);
        objectHolder->PrincipalAcd().SetOwner(user);
        auto* object = AccessControlObjectMap_.Insert(id, std::move(objectHolder));

        namespaceObject->RegisterMember(object);

        // Make the fake reference.
        YT_VERIFY(object->RefObject() == 1);

        YT_LOG_DEBUG("Access control object registered (Id: %v, Namespace: %v, Name: %v)",
            object->GetId(),
            namespaceObject->GetName(),
            object->GetName());

        return object;
    }

    void ZombifyAccessControlObject(TAccessControlObject* object) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(!IsObjectAlive(object));

        auto& namespaceObject = object->Namespace();
        namespaceObject->UnregisterMember(object);

        YT_LOG_DEBUG("Access control object unregistered (Id: %v, Namespace: %v, Name: %v)",
            object->GetId(),
            namespaceObject->GetName(),
            object->GetName());

        namespaceObject.Reset();
    }

    DEFINE_SIGNAL_OVERRIDE(void(TCypressNode*), NodeCreated);

    TFuture<TYsonString> ComputeRecursiveResourceUsage(
        TCypressNode* trunkNode,
        TTransaction* transaction) override
    {
        return RecursiveResourceUsageCache_->Get(TVersionedNodeId(
            trunkNode->GetId(),
            GetObjectId(transaction)));
    }

private:
    friend class TNodeTypeHandler;
    friend class TLockTypeHandler;

    class TNodeMapTraits
    {
    public:
        explicit TNodeMapTraits(TCypressManager* owner);

        std::unique_ptr<TCypressNode> Create(TVersionedNodeId id) const;

    private:
        TCypressManager* const Owner_;
    };

    const TAccessTrackerPtr AccessTracker_;
    const TExpirationTrackerPtr ExpirationTracker_;

    TResolveCachePtr ResolveCache_;

    NHydra::TEntityMap<TCypressNode, TNodeMapTraits> NodeMap_;
    NHydra::TEntityMap<TLock> LockMap_;
    NHydra::TEntityMap<TCypressShard> ShardMap_;
    NHydra::TEntityMap<TAccessControlObject> AccessControlObjectMap_;
    NHydra::TEntityMap<TAccessControlObjectNamespace> AccessControlObjectNamespaceMap_;

    using TNameToAccessControlObjectNamespace = THashMap<TString, TAccessControlObjectNamespace*>;
    TNameToAccessControlObjectNamespace NameToAccessControlObjectNamespaceMap_;

    TEnumIndexedArray<NObjectClient::EObjectType, INodeTypeHandlerPtr> TypeToHandler_;

    TNodeId RootNodeId_;
    TCypressMapNode* RootNode_ = nullptr;

    TCypressShardId RootShardId_;
    TCypressShard* RootShard_ = nullptr;

    // COMPAT(shakurov)
    bool NeedFixNodeStatistics_ = false;

    // COMPAT(h0pless): Remove this after schema migration is complete.
    ESchemaMigrationMode SchemaExportMode_ = ESchemaMigrationMode::None;

    // COMPAT(h0pless): RecomputeMasterTableSchemaRefCounters, RefactorSchemaExport
    bool NeedRecomputeMasterTableSchemaRefCounters_ = false;
    bool NeedRecomputeMasterTableSchemaExportRefCounters_ = false;

    using TRecursiveResourceUsageCache = TSyncExpiringCache<TVersionedNodeId, TFuture<TYsonString>>;
    using TRecursiveResourceUsageCachePtr = TIntrusivePtr<TRecursiveResourceUsageCache>;
    const TRecursiveResourceUsageCachePtr RecursiveResourceUsageCache_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveKeys(context);
        LockMap_.SaveKeys(context);
        ShardMap_.SaveKeys(context);
        AccessControlObjectNamespaceMap_.SaveKeys(context);
        AccessControlObjectMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveValues(context);
        LockMap_.SaveValues(context);
        ShardMap_.SaveValues(context);
        AccessControlObjectNamespaceMap_.SaveValues(context);
        AccessControlObjectMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NodeMap_.LoadKeys(context);
        LockMap_.LoadKeys(context);
        ShardMap_.LoadKeys(context);
        AccessControlObjectNamespaceMap_.LoadKeys(context);
        AccessControlObjectMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NodeMap_.LoadValues(context);
        LockMap_.LoadValues(context);
        ShardMap_.LoadValues(context);

        AccessControlObjectNamespaceMap_.LoadValues(context);
        for (auto [id, object] : AccessControlObjectNamespaceMap_) {
            if (!IsObjectAlive(object)) {
                continue;
            }
            RegisterAccessControlObjectNamespace(object);
        }

        AccessControlObjectMap_.LoadValues(context);
        for (auto [id, object] : AccessControlObjectMap_) {
            if (!IsObjectAlive(object)) {
                continue;
            }
            object->Namespace()->RegisterMember(object);
        }

        NeedFixNodeStatistics_ = context.GetVersion() < EMasterReign::FixClonedTrunkNodeStatistics;
        if (context.GetVersion() < EMasterReign::ExportMasterTableSchemas) {
            SchemaExportMode_ = ESchemaMigrationMode::AllSchemas;
        } else if (context.GetVersion() < EMasterReign::ExportEmptyMasterTableSchemas) {
            SchemaExportMode_ = ESchemaMigrationMode::EmptySchemaOnly;
        }

        if (context.GetVersion() < EMasterReign::RefactorSchemaExport) {
            NeedRecomputeMasterTableSchemaRefCounters_ = true;

            if (EMasterReign::ExportEmptyMasterTableSchemas < context.GetVersion()) {
                NeedRecomputeMasterTableSchemaExportRefCounters_ = true;
            }
        }
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        ExpirationTracker_->Clear();

        NodeMap_.Clear();
        LockMap_.Clear();
        ShardMap_.Clear();

        AccessControlObjectNamespaceMap_.Clear();
        NameToAccessControlObjectNamespaceMap_.clear();

        AccessControlObjectMap_.Clear();

        RootNode_ = nullptr;
        RootShard_ = nullptr;

        RecursiveResourceUsageCache_->Clear();

        NeedFixNodeStatistics_ = false;
        SchemaExportMode_ = ESchemaMigrationMode::None;

        NeedRecomputeMasterTableSchemaRefCounters_ = false;
        NeedRecomputeMasterTableSchemaExportRefCounters_ = false;
    }

    void SetZeroState() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::SetZeroState();

        InitBuiltins();
    }

    void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        YT_LOG_INFO("Started initializing locks");
        for (auto [lockId, lock] : LockMap_) {
            if (!IsObjectAlive(lock)) {
                continue;
            }

            // Reconstruct iterators.
            if (lock->GetState() == ELockState::Acquired) {
                RegisterLock(lock);
            }
        }
        YT_LOG_INFO("Finished initializing locks");

        YT_LOG_INFO("Started initializing nodes");
        for (auto [nodeId, node] : NodeMap_) {
            // Reconstruct immediate ancestor sets.
            if (auto* parent = node->GetParent()) {
                YT_VERIFY(parent->ImmediateDescendants().insert(node).second);
            }

            // Reconstruct TrunkNode and Transaction.
            if (auto transactionId = node->GetVersionedId().TransactionId) {
                node->SetTrunkNode(GetNode(TVersionedNodeId(node->GetId())));
                node->SetTransaction(transactionManager->GetTransaction(transactionId));
            }

            // Compute originators.
            if (!node->IsTrunk()) {
                auto* parentTransaction = node->GetTransaction()->GetParent();
                auto* originator = GetVersionedNode(node->GetTrunkNode(), parentTransaction);
                node->SetOriginator(originator);
            }

            // Reconstruct iterators.
            if (node->HasLockingState()) {
                auto* lockingState = node->MutableLockingState();

                for (auto it = lockingState->AcquiredLocks.begin(); it != lockingState->AcquiredLocks.end(); ++it) {
                    auto* lock = *it;
                    lock->SetLockListIterator(it);
                }

                for (auto it = lockingState->PendingLocks.begin(); it != lockingState->PendingLocks.end(); ++it) {
                    auto* lock = *it;
                    lock->SetLockListIterator(it);
                }
            }

            if (node->IsTrunk() && node->TryGetExpirationTime()) {
                ExpirationTracker_->OnNodeExpirationTimeUpdated(node);
            }

            // COMPAT(shakurov)
            if (node->TryGetExpirationTimeout()) {
                if (node->IsTrunk() && !node->GetTouchTime()) {
                    const auto* hydraContext = GetCurrentHydraContext();
                    node->SetTouchTime(hydraContext->GetTimestamp());
                } else if (!node->IsTrunk() && node->GetTouchTime(/*branchIsOk*/ true)) {
                    node->SetTouchTime(TInstant::Zero(), /*branchIsOk*/ true);
                }
            } else {
                node->SetTouchTime(TInstant::Zero(), /*branchIsOk*/ true);
            }

            if (node->IsTrunk() && node->TryGetExpirationTimeout()) {
                ExpirationTracker_->OnNodeExpirationTimeoutUpdated(node);
            }
        }
        YT_LOG_INFO("Finished initializing nodes");

        InitBuiltins();

        // COMPAT(shakurov)
        if (NeedFixNodeStatistics_) {
            for (auto [nodeId, node] : NodeMap_) {
                if (!IsObjectAlive(node)) {
                    continue;
                }

                if (!node->IsTrunk()) {
                    continue;
                }

                if (!IsChunkOwnerType(node->GetType())) {
                    continue;
                }

                auto* chunkOwner = node->As<TChunkOwnerBase>();
                if (chunkOwner->IsStatisticsFixNeeded()) {
                    YT_LOG_ALERT("Fixing chunk owner statistics (ChunkOwnerId: %v, SnapshotStatistics: %v, DeltaStatistics: %v)",
                        chunkOwner->GetId(),
                        chunkOwner->SnapshotStatistics(),
                        chunkOwner->DeltaStatistics());
                    chunkOwner->FixStatistics();
                }
            }
        }

        // NB: Referencing accounts are transient,
        // thus only ref counters and export ref counters need to be recalculated here.
        if (NeedRecomputeMasterTableSchemaRefCounters_) {
            auto logLevel = Bootstrap_->GetConfig()->TableManager->AlertOnMasterTableSchemaRefCounterMismatch
                ? ELogLevel::Alert
                : ELogLevel::Error;

            const auto& tableManager = Bootstrap_->GetTableManager();
            if (NeedRecomputeMasterTableSchemaExportRefCounters_) {
                tableManager->RecomputeMasterTableSchemaExportRefCounters(logLevel);
            }
            tableManager->RecomputeMasterTableSchemaRefCounters(logLevel);
        }

        // This needs to be done for 2 reasons:
        //   - Empty schema has artificial export counters to all cells, which need to be dealt with.
        //   - Schema updates will use the same protocol as new migration, which also export refs it.
        if (SchemaExportMode_ == ESchemaMigrationMode::EmptySchemaOnly) {
            const auto& tableManager = Bootstrap_->GetTableManager();
            auto* emptySchema = tableManager->GetEmptyMasterTableSchema();
            emptySchema->ResetExportRefCounters();
        }

        // COMPAT(h0pless): Remove this after schema migration is complete.
        if (SchemaExportMode_ != ESchemaMigrationMode::None) {
            const auto& tableManager = Bootstrap_->GetTableManager();
            if (SchemaExportMode_ == ESchemaMigrationMode::AllSchemas) {
                // This has been done during the first export.
                tableManager->TransformForeignSchemaIdsToNative();
            }

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            auto* emptySchema = Bootstrap_->GetTableManager()->GetEmptyMasterTableSchema();

            auto maybeUpdateEmptySchema = [&] (TSchemafulNode* schemafulNode) {
                auto* schema = schemafulNode->GetSchema();
                if (!multicellManager->IsPrimaryMaster() && schema && *schema->AsTableSchema() == *emptySchema->AsTableSchema()) {
                    tableManager->SetTableSchema(schemafulNode, emptySchema);
                    return emptySchema;
                }
                return schema;
            };

            THashMap<TVersionedNodeId, TMasterTableSchema*> nodeIdToSchema;
            THashMap<TCellTag, std::vector<TVersionedNodeId>> cellTagToNodeIds;
            for (auto [nodeId, node] : NodeMap_) {
                // We need to export all nodes, even zombies because schemas are unexported during node destruction.
                if (!node->IsNative()) {
                    continue;
                }

                auto nodeType = node->GetType();
                // Updating schemas on chaos replicated tables separately.
                // Thankfully, chaos replicated tables are never externalized.
                if (nodeType == EObjectType::ChaosReplicatedTable) {
                    auto* table = node->As<NChaosServer::TChaosReplicatedTableNode>();
                    auto* schema = table->GetSchema();
                    if (SchemaExportMode_ == ESchemaMigrationMode::EmptySchemaOnly) {
                        maybeUpdateEmptySchema(table);
                    }

                    if (!schema) {
                        tableManager->SetTableSchema(table, emptySchema);
                    }
                    continue;
                }

                if (!IsTableType(node->GetType())) {
                    continue;
                }

                auto* table = node->As<TTableNode>();
                auto* schema = table->GetSchema();

                // Possible on opaque tables.
                if (!schema) {
                    continue;
                }

                YT_VERIFY(IsObjectAlive(schema));
                if (SchemaExportMode_ == ESchemaMigrationMode::EmptySchemaOnly) {
                    schema = maybeUpdateEmptySchema(table);
                }

                if (!node->IsExternal()) {
                    continue;
                }

                if (SchemaExportMode_ == ESchemaMigrationMode::EmptySchemaOnly &&
                    *schema->AsTableSchema() != *emptySchema->AsTableSchema()) {
                    continue;
                }

                EmplaceOrCrash(nodeIdToSchema, nodeId, schema);
                cellTagToNodeIds[table->GetExternalCellTag()].push_back(nodeId);
            }

            auto sendRequestAndLog = [&] (TCellTag cellTag, NProto::TReqSetTableSchemas& req) {
                YT_LOG_DEBUG("Sending SetTableSchemas request (DestinationCellTag: %v, RequestSize: %v",
                    cellTag, req.subrequests_size());
                multicellManager->PostToMaster(req, cellTag);
                req.clear_subrequests();
            };

            const auto maxBatchSize = 10000;
            NProto::TReqSetTableSchemas req;
            for (auto [cellTag, nodeIds] : cellTagToNodeIds) {
                YT_LOG_DEBUG("Started schema migration (DestinationCellTag: %v)", cellTag);

                auto batchSize = 0;
                auto previousId = NullObjectId;
                std::sort(nodeIds.begin(), nodeIds.end());
                for (auto nodeId : nodeIds) {
                    if (batchSize >= maxBatchSize && previousId != nodeId.ObjectId) {
                        sendRequestAndLog(cellTag, req);
                        batchSize = 0;
                    }

                    auto* subrequest = req.add_subrequests();
                    auto* schema = nodeIdToSchema.find(nodeId)->second;

                    YT_VERIFY(SchemaExportMode_ == ESchemaMigrationMode::AllSchemas || schema == emptySchema);

                    ToProto(subrequest->mutable_table_node_id(), nodeId.ObjectId);
                    ToProto(subrequest->mutable_transaction_id(), nodeId.TransactionId);
                    ToProto(subrequest->mutable_schema_id(), schema->GetId());

                    tableManager->ExportMasterTableSchema(schema, cellTag);

                    previousId = nodeId.ObjectId;
                    ++batchSize;
                }
                if (batchSize > 0) {
                    sendRequestAndLog(cellTag, req);
                }
            }
        }
    }

    void InitBuiltins()
    {
        if (auto* untypedRootNode = FindNode(TVersionedNodeId(RootNodeId_))) {
            // Root node already exists.
            RootNode_ = untypedRootNode->As<TCypressMapNode>();
        } else {
            // Create the root node.
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto rootNodeHolder = TPoolAllocator::New<TCypressMapNode>(TVersionedNodeId(RootNodeId_));
            rootNodeHolder->SetTrunkNode(rootNodeHolder.get());
            rootNodeHolder->Account().Assign(securityManager->GetSysAccount());
            rootNodeHolder->Acd().SetInherit(false);
            rootNodeHolder->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetEveryoneGroup(),
                EPermission::Read));
            rootNodeHolder->Acd().SetOwner(securityManager->GetRootUser());

            RootNode_ = rootNodeHolder.get();
            NodeMap_.Insert(TVersionedNodeId(RootNodeId_), std::move(rootNodeHolder));
            YT_VERIFY(RootNode_->RefObject() == 1);
        }

        RootShard_ = FindShard(RootShardId_);
        if (!RootShard_) {
            // Create the root shard.
            auto rootShardHolder = TPoolAllocator::New<TCypressShard>(RootShardId_);
            rootShardHolder->SetRoot(RootNode_);

            RootShard_ = rootShardHolder.get();
            RootShard_->SetName(SuggestCypressShardName(RootShard_));
            ShardMap_.Insert(RootShardId_, std::move(rootShardHolder));

            SetShard(RootNode_, RootShard_);
        }
    }


    void OnRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnRecoveryComplete();

        AccessTracker_->Start();
    }

    void OnLeaderRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderRecoveryComplete();

        ExpirationTracker_->Start();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        ExpirationTracker_->Stop();
        OnStopEpoch();
    }

    void OnStopFollowing() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopFollowing();
        OnStopEpoch();
    }

    void OnStopEpoch()
    {
        AccessTracker_->Stop();
        ResolveCache_->Clear();
    }


    TCypressNode* RegisterNode(std::unique_ptr<TCypressNode> trunkNodeHolder)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(trunkNodeHolder->IsTrunk());

        const auto nodeId = trunkNodeHolder->GetId();
        auto* node = NodeMap_.Insert(TVersionedNodeId(nodeId), std::move(trunkNodeHolder));

        const auto* hydraContext = GetCurrentHydraContext();
        node->SetCreationTime(hydraContext->GetTimestamp());
        node->SetAccessTime(hydraContext->GetTimestamp());

        node->SetModified(EModificationType::Content);
        node->SetModified(EModificationType::Attributes);

        node->RememberAevum();

        if (node->IsExternal()) {
            YT_LOG_DEBUG("External node registered (NodeId: %v, Type: %v, ExternalCellTag: %v)",
                node->GetId(),
                node->GetType(),
                node->GetExternalCellTag());
        } else {
            YT_LOG_DEBUG("%v node registered (NodeId: %v, Type: %v)",
                node->IsForeign() ? "Foreign" : "Local",
                node->GetId(),
                node->GetType());
        }

        return node;
    }

    void ZombifyNode(TCypressNode* trunkNode)
    {
        const auto& handler = GetHandler(trunkNode);
        handler->Zombify(trunkNode);
    }

    void DestroyNode(TCypressNode* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        const auto& lockingState = trunkNode->LockingState();

        for (auto* lock : lockingState.AcquiredLocks) {
            lock->SetTrunkNode(nullptr);
            // NB: Transaction may have more than one lock for a given node.
            lock->GetTransaction()->LockedNodes().erase(trunkNode);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* lock : lockingState.PendingLocks) {
            YT_LOG_DEBUG("Lock orphaned due to node removal (LockId: %v, NodeId: %v)",
                lock->GetId(),
                trunkNode->GetId());
            lock->SetTrunkNode(nullptr);

            auto* transaction = lock->GetTransaction();
            transaction->DetachLock(lock, objectManager);
        }

        trunkNode->ResetLockingState();

        ExpirationTracker_->OnNodeDestroyed(trunkNode);

        const auto& handler = GetHandler(trunkNode);
        handler->Destroy(trunkNode);

        // Remove the object from the map but keep it alive.
        Y_UNUSED(NodeMap_.Release(trunkNode->GetVersionedId()).release());
    }

    void RecreateNodeAsGhost(TCypressNode* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        const auto& handler = GetHandler(trunkNode);
        handler->RecreateAsGhost(trunkNode);
    }


    void OnTransactionCommitted(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        MergeBranchedNodes(transaction);
        ReleaseLocks(transaction, transaction->GetParent());
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(transaction->NestedTransactions().empty());

        RemoveBranchedNodes(transaction);
        ReleaseLocks(transaction, false);
    }

    template <typename F>
    TError CheckSubtreeTrunkNodes(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        bool recursive,
        F doCheck)
    {
        TSubtreeNodes nodes;
        if (recursive) {
            ListSubtreeNodes(trunkNode, transaction, true, &nodes);
        } else {
            nodes.push_back(trunkNode);
        }

        Sort(nodes, TCypressNodeIdComparer());

        for (auto* node : nodes) {
            auto* trunkNode = node->GetTrunkNode();

            auto error = doCheck(trunkNode);
            if (!error.IsOK()) {
                return error;
            }
        }

        return TError();
    }

    template <typename F>
    void ForEachSubtreeNode(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        bool recursive,
        F processNode)
    {
        TSubtreeNodes nodes;
        if (recursive) {
            ListSubtreeNodes(trunkNode, transaction, true, &nodes);

            // For determinism.
            Sort(nodes, TCypressNodeIdComparer());

            for (auto* node : nodes) {
                processNode(node);
            }
        } else {
            processNode(trunkNode);
        }
    }

    TError CheckLock(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        bool recursive)
    {
        auto doCheck = [&](TCypressNode* trunkNode) {
            return DoCheckLock(trunkNode, transaction, request);
        };


        // Validate all potential locks to see if we need to take at least one of them.
        // This throws an exception in case the validation fails.
        return CheckSubtreeTrunkNodes(trunkNode, transaction, recursive, doCheck);
    }

    TError DoCheckLock(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request)
    {
        YT_ASSERT(trunkNode->IsTrunk());
        YT_VERIFY(transaction || request.Mode != ELockMode::Snapshot);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& handler = objectManager->GetHandler(trunkNode);
        if (Any(handler->GetFlags() & ETypeFlags::ForbidLocking)) {
            THROW_ERROR_EXCEPTION("Cannot lock objects of type %Qlv",
                trunkNode->GetType());
        }

        const auto& lockingState = trunkNode->LockingState();
        const auto& transactionAndKeyToSharedLocks = lockingState.TransactionAndKeyToSharedLocks;
        const auto& keyToSharedLocks = lockingState.KeyToSharedLocks;
        const auto& transactionToExclusiveLocks = lockingState.TransactionToExclusiveLocks;

        // Handle snapshot locks.
        if (transaction && lockingState.HasSnapshotLock(transaction)) {
            if (request.Mode == ELockMode::Snapshot) {
                // Already taken by this transaction.
                return TError();
            } else {
                // Cannot take non-snapshot lock when a snapshot lock is already taken.
                return TError(
                    NCypressClient::EErrorCode::SameTransactionLockConflict,
                    "Cannot take %Qlv lock for node %v since %Qlv lock is already taken by same transaction %v",
                    request.Mode,
                    GetNodePath(trunkNode, transaction),
                    ELockMode::Snapshot,
                    transaction->GetId());
            }
        }

        // New snapshot lock.
        if (request.Mode == ELockMode::Snapshot) {
            if (transaction) {
                // Check if a non-snapshot lock is already taken by this transaction.
                if (lockingState.HasExclusiveLock(transaction)) {
                    return TError(
                        NCypressClient::EErrorCode::SameTransactionLockConflict,
                        "Cannot take %Qlv lock for node %v since %Qlv lock is already taken by same transaction %v",
                        request.Mode,
                        GetNodePath(trunkNode, transaction),
                        ELockMode::Exclusive,
                        transaction->GetId());
                }
                if (lockingState.HasSharedLock(transaction)) {
                    return TError(
                        NCypressClient::EErrorCode::SameTransactionLockConflict,
                        "Cannot take %Qlv lock for node %v since %Qlv lock is already taken by same transaction %v",
                        request.Mode,
                        GetNodePath(trunkNode, transaction),
                        ELockMode::Shared,
                        transaction->GetId());
                }

                // Check if any nested transaction has taken a non-snapshot lock.
                auto lockModeBelow = GetStrongestLockModeOfNestedTransactions(trunkNode, transaction);
                if (lockModeBelow > ELockMode::Snapshot) {
                    return TError(
                        NCypressClient::EErrorCode::DescendantTransactionLockConflict,
                        "Cannot take %Qlv lock for node %v since %Qlv lock is already taken by a descendant transaction",
                        request.Mode,
                        GetNodePath(trunkNode, transaction),
                        lockModeBelow);
                }
            }

            return TError();
        }

        if (transaction) {
            // Check if any of parent transactions has taken a snapshot lock.
            auto* currentTransaction = transaction->GetParent();
            while (currentTransaction) {
                if (lockingState.HasSnapshotLock(currentTransaction)) {
                    return TError(
                        NCypressClient::EErrorCode::SameTransactionLockConflict,
                        "Cannot take %Qlv lock for node %v since %Qlv lock is already taken by parent transaction %v",
                        request.Mode,
                        GetNodePath(trunkNode, transaction),
                        ELockMode::Snapshot,
                        currentTransaction->GetId());
                }
                currentTransaction = currentTransaction->GetParent();
            }

            // Validate lock count.
            const auto& config = GetDynamicConfig();
            auto lockCountLimit = config->MaxLocksPerTransactionSubtree;
            currentTransaction = transaction;
            while (currentTransaction) {
                auto recursiveLockCount = currentTransaction->GetRecursiveLockCount();
                if (recursiveLockCount >= lockCountLimit) {
                    return TError(
                        NCypressClient::EErrorCode::TooManyLocksOnTransaction,
                        "Cannot create %Qlv lock for node %v since transaction %v and its descendants already have %v locks associated with them",
                        request.Mode,
                        GetNodePath(trunkNode, transaction),
                        currentTransaction->GetId(),
                        recursiveLockCount);
                }
                currentTransaction = currentTransaction->GetParent();
            }
        }

        auto checkExistingLock = [&] (const TLock* existingLock) {
            auto* existingTransaction = existingLock->GetTransaction();
            if (!IsConcurrentTransaction(transaction, existingTransaction)) {
                return TError();
            }
            switch (request.Key.Kind) {
                case ELockKeyKind::None:
                    return TError(
                        NCypressClient::EErrorCode::ConcurrentTransactionLockConflict,
                        "Cannot take %Qlv lock for node %v since %Qlv lock is taken by concurrent transaction %v",
                        request.Mode,
                        GetNodePath(trunkNode, transaction),
                        existingLock->Request().Mode,
                        existingTransaction->GetId())
                        << TErrorAttribute("winner_transaction", existingTransaction->GetErrorDescription());

                case ELockKeyKind::Child:
                    return TError(
                        NCypressClient::EErrorCode::ConcurrentTransactionLockConflict,
                        "Cannot take lock for child %Qv of node %v since this child is locked by concurrent transaction %v",
                        request.Key.Name,
                        GetNodePath(trunkNode, transaction),
                        existingTransaction->GetId())
                        << TErrorAttribute("winner_transaction", existingTransaction->GetErrorDescription());

                case ELockKeyKind::Attribute:
                    return TError(
                        NCypressClient::EErrorCode::ConcurrentTransactionLockConflict,
                        "Cannot take lock for attribute %Qv of node %v since this attribute is locked by concurrent transaction %v",
                        request.Key.Name,
                        GetNodePath(trunkNode, transaction),
                        existingTransaction->GetId())
                        << TErrorAttribute("winner_transaction", existingTransaction->GetErrorDescription());

                default:
                    YT_ABORT();
            }
        };

        for (auto [transaction, existingLock] : transactionToExclusiveLocks) {
            auto error = checkExistingLock(existingLock);
            if (!error.IsOK()) {
                return error;
            }
        }

        switch (request.Mode) {
            case ELockMode::Exclusive:
                for (const auto& [transaction, lockKey, lock] : transactionAndKeyToSharedLocks) {
                    auto error = checkExistingLock(lock);
                    if (!error.IsOK()) {
                        return error;
                    }
                }
                break;

            case ELockMode::Shared:
                if (request.Key.Kind != ELockKeyKind::None) {
                    auto range = keyToSharedLocks.equal_range(request.Key);
                    for (auto it = range.first; it != range.second; ++it) {
                        auto error = checkExistingLock(it->second);
                        if (!error.IsOK()) {
                            return error;
                        }
                    }
                }
                break;

            default:
                YT_ABORT();
        }

        return TError();
    }

    TError CheckUnlock(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        bool recursive,
        bool explicitOnly)
    {
        auto doCheck = [&] (TCypressNode* trunkNode) {
            return DoCheckUnlock(trunkNode, transaction, explicitOnly);
        };

        // Check that unlocking nodes won't drop any data.
        return CheckSubtreeTrunkNodes(trunkNode, transaction, recursive, doCheck);
    }

    TError DoCheckUnlock(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        bool explicitOnly)
    {
        YT_ASSERT(trunkNode->IsTrunk());
        YT_VERIFY(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();

        if (IsUnlockRedundant(trunkNode, transaction, explicitOnly)) {
            return TError();
        }

        auto* branchedNode = cypressManager->FindNode(trunkNode, transaction);
        if (!branchedNode) {
            // Pending locks don't imply branched yet should still be unlockable.
            return TError();
        }

        if (branchedNode->GetLockMode() == ELockMode::Snapshot) {
            return TError();
        }

        auto* originatorNode = branchedNode->GetOriginator();
        const auto& handler = cypressManager->GetHandler(trunkNode);
        if (handler->HasBranchedChanges(originatorNode, branchedNode)) {
            return TError("Node #%v is modified by transaction %Qv and cannot be unlocked",
                branchedNode->GetId(),
                transaction->GetId());
        }

        return TError();
    }

    static bool IsLockRedundant(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        const TLock* lockToIgnore = nullptr)
    {
        YT_ASSERT(trunkNode->IsTrunk());
        YT_ASSERT(request.Mode != ELockMode::None);

        if (!transaction) {
            return true;
        }

        const auto& lockingState = trunkNode->LockingState();
        switch (request.Mode) {
            case ELockMode::Exclusive: {
                auto range = lockingState.TransactionToExclusiveLocks.equal_range(transaction);
                for (auto it = range.first; it != range.second; ++it) {
                    auto* existingLock = it->second;
                    if (existingLock != lockToIgnore &&
                        existingLock->Request().Key == request.Key)
                    {
                        return true;
                    }
                }
                break;
            }

            case ELockMode::Shared: {
                auto range = lockingState.TransactionAndKeyToSharedLocks.equal_range(std::pair(transaction, request.Key));
                for (auto it = range.first; it != range.second; ++it) {
                    const auto* existingLock = get<TLock*>(*it);
                    if (existingLock != lockToIgnore) {
                        return true;
                    }
                }
                break;
            }

            case ELockMode::Snapshot: {
                auto range = lockingState.TransactionToSnapshotLocks.equal_range(transaction);
                for (auto it = range.first; it != range.second; ++it) {
                    const auto* existingLock = it->second;
                    if (existingLock != lockToIgnore) {
                        return true;
                    }
                }
                break;
            }

            default:
                YT_ABORT();
        }

        return false;
    }

    bool IsUnlockRedundant(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        bool explicitOnly)
    {
        YT_ASSERT(transaction);

        auto isLockRelevant = [&] (TLock* lock) {
            return (lock->GetTransaction() == transaction) && (!explicitOnly || !lock->GetImplicit());
        };

        const auto& lockingState = trunkNode->LockingState();
        const auto& pendingLocks = lockingState.PendingLocks;
        if (std::find_if(pendingLocks.begin(), pendingLocks.end(), isLockRelevant) != pendingLocks.end()) {
            return false;
        }
        const auto& acquiredLocks = lockingState.AcquiredLocks;
        if (std::find_if(acquiredLocks.begin(), acquiredLocks.end(), isLockRelevant) != acquiredLocks.end()) {
            return false;
        }

        return true;
    }

    static bool IsParentTransaction(
        TTransaction* transaction,
        TTransaction* parent)
    {
        auto* currentTransaction = transaction;
        while (currentTransaction) {
            if (currentTransaction == parent) {
                return true;
            }
            currentTransaction = currentTransaction->GetParent();
        }
        return false;
    }

    static bool IsConcurrentTransaction(
        TTransaction* requestingTransaction,
        TTransaction* existingTransaction)
    {
        return
            !requestingTransaction ||
            !IsParentTransaction(requestingTransaction, existingTransaction);
    }

    TCypressNode* DoAcquireLock(
        TLock* lock,
        bool dontLockForeign = false)
    {
        auto* trunkNode = lock->GetTrunkNode();
        auto* transaction = lock->GetTransaction();
        const auto& request = lock->Request();

        YT_LOG_DEBUG("Lock acquired (LockId: %v)",
            lock->GetId());

        YT_VERIFY(lock->GetState() == ELockState::Pending);
        lock->SetState(ELockState::Acquired);

        const auto* hydraContext = GetCurrentHydraContext();
        lock->SetAcquisitionTime(hydraContext->GetTimestamp());

        auto* lockingState = trunkNode->MutableLockingState();
        lockingState->PendingLocks.erase(lock->GetLockListIterator());
        lockingState->AcquiredLocks.push_back(lock);
        lock->SetLockListIterator(--lockingState->AcquiredLocks.end());

        RegisterLock(lock);

        if (transaction->LockedNodes().insert(trunkNode).second) {
            YT_LOG_DEBUG("Node locked (NodeId: %v, TransactionId: %v, DontLockForeign: %v)",
                trunkNode->GetId(),
                transaction->GetId(),
                dontLockForeign);
        }

        if (trunkNode->IsExternal() && trunkNode->IsNative() && !dontLockForeign) {
            PostLockForeignNodeRequest(lock);
        }

        // Branch node, if needed.
        auto* branchedNode = FindNode(trunkNode, transaction);
        if (branchedNode) {
            if (branchedNode->GetLockMode() < request.Mode) {
                branchedNode->SetLockMode(request.Mode);
            }
            return branchedNode;
        }

        TCypressNode* originatingNode;
        std::vector<TTransaction*> intermediateTransactions;
        // Walk up to the root, find originatingNode, construct the list of
        // intermediate transactions.
        auto* currentTransaction = transaction;
        while (true) {
            originatingNode = FindNode(trunkNode, currentTransaction);
            if (originatingNode) {
                break;
            }
            if (!currentTransaction) {
                break;
            }
            intermediateTransactions.push_back(currentTransaction);
            currentTransaction = currentTransaction->GetParent();
        }

        YT_VERIFY(originatingNode);
        YT_VERIFY(!intermediateTransactions.empty());

        if (request.Mode == ELockMode::Snapshot) {
            // Branch at requested transaction only.
            return BranchNode(originatingNode, transaction, request);
        } else {
            // Branch at all intermediate transactions.
            std::reverse(intermediateTransactions.begin(), intermediateTransactions.end());
            auto* currentNode = originatingNode;
            for (auto* transactionToBranch : intermediateTransactions) {
                currentNode = BranchNode(currentNode, transactionToBranch, request);
            }
            return currentNode;
        }
    }

    static void RegisterLock(TLock* lock)
    {
        auto* transaction = lock->GetTransaction();
        auto* lockingState = lock->GetTrunkNode()->MutableLockingState();

        switch (lock->Request().Mode) {
            case ELockMode::Snapshot:
                if (auto [it, inserted] = lockingState->TransactionToSnapshotLocks.emplace(transaction, lock);
                    inserted)
                {
                    lock->SetTransactionToSnapshotLocksIterator(it);
                } else {
                    AlertDuplicateLock(lock);
                }
                break;

            case ELockMode::Shared:
                if (auto [it, inserted] = lockingState->TransactionAndKeyToSharedLocks.emplace(transaction, lock->Request().Key, lock);
                    inserted)
                {
                    lock->SetTransactionAndKeyToSharedLocksIterator(it);
                    if (lock->Request().Key.Kind != ELockKeyKind::None) {
                        lock->SetKeyToSharedLocksIterator(
                            lockingState->KeyToSharedLocks.emplace(lock->Request().Key, lock));
                    }
                } else {
                    AlertDuplicateLock(lock);
                }
                break;

            case ELockMode::Exclusive:
                if (auto [it, inserted] = lockingState->TransactionToExclusiveLocks.emplace(transaction, lock);
                    inserted)
                {
                    lock->SetTransactionToExclusiveLocksIterator(it);
                } else {
                    AlertDuplicateLock(lock);
                }
                break;

            default:
                YT_ABORT();
        }
    }

    static void AlertDuplicateLock(TLock* lock)
    {
        YT_LOG_ALERT("Duplicate lock detected "
            "(NodeId: %v, TransactionId: %v, LockId: %v, LockMode: %v, LockKey: %v)",
            lock->GetTrunkNode()->GetId(),
            lock->GetTransaction()->GetId(),
            lock->GetId(),
            lock->Request().Mode,
            lock->Request().Key);
    }

    TLock* DoCreateLock(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        bool implicit)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Lock);
        auto lockHolder = TPoolAllocator::New<TLock>(id);
        auto* lock = LockMap_.Insert(id, std::move(lockHolder));

        lock->SetImplicit(implicit);
        lock->SetState(ELockState::Pending);
        lock->SetTrunkNode(trunkNode);
        lock->Request() = request;

        const auto* hydraContext = GetCurrentHydraContext();
        lock->SetCreationTime(hydraContext->GetTimestamp());

        auto* lockingState = trunkNode->MutableLockingState();
        lockingState->PendingLocks.push_back(lock);
        lock->SetLockListIterator(--lockingState->PendingLocks.end());

        transaction->AttachLock(lock, objectManager);

        YT_LOG_DEBUG(
            "Lock created (LockId: %v, Mode: %v, Key: %v, NodeId: %v, Implicit: %v, TransactionRecursiveLockCount: %v)",
            id,
            request.Mode,
            request.Key,
            TVersionedNodeId(trunkNode->GetId(), transaction->GetId()),
            implicit,
            transaction->GetRecursiveLockCount() + 1);

        return lock;
    }

    void DoReleaseLocks(
        TTransaction* transaction,
        bool promote,
        TRange<TLock*> locks,
        TRange<TCypressNode*> lockedNodes)
    {
        auto* parentTransaction = transaction->GetParent();

        for (auto* lock : locks) {
            auto* trunkNode = lock->GetTrunkNode();
            // Decide if the lock must be promoted.
            if (promote &&
                lock->Request().Mode != ELockMode::Snapshot &&
                (!lock->GetImplicit() || !IsLockRedundant(trunkNode, parentTransaction, lock->Request(), lock)))
            {
                DoPromoteLock(lock);
            } else {
                DoRemoveLock(lock, true /*resetEmptyLockingState*/);
            }
        }

        for (auto* trunkNode : lockedNodes) {
            YT_LOG_DEBUG("Node unlocked (NodeId: %v, TransactionId: %v)",
                trunkNode->GetId(),
                transaction->GetId());
        }

        for (auto* trunkNode : lockedNodes) {
            CheckPendingLocks(trunkNode);
        }

        for (auto* trunkNode : lockedNodes) {
            if (trunkNode->TryGetExpirationTimeout() &&
                !trunkNode->GetTouchTime() &&
                trunkNode->IsNative()) // COMPAT(shakurov): remove this part.
            {
                YT_LOG_ALERT(
                    "Touching a node with an expiration timeout but without a touch time (NodeId: %v)",
                    trunkNode->GetId());
                auto* context = GetCurrentHydraContext();
                trunkNode->SetTouchTime(context->GetTimestamp());
            }
            ExpirationTracker_->OnNodeTouched(trunkNode);
        }
    }

    void ReleaseLocks(TTransaction* transaction, bool promote)
    {
        TCompactVector<TLock*, 16> locks(transaction->Locks().begin(), transaction->Locks().end());
        Sort(locks, TObjectIdComparer());

        TCompactVector<TCypressNode*, 16> lockedNodes(transaction->LockedNodes().begin(), transaction->LockedNodes().end());
        transaction->LockedNodes().clear();
        Sort(lockedNodes, TCypressNodeIdComparer());

        DoReleaseLocks(transaction, promote, locks, lockedNodes);
    }

    void ForceRemoveAllLocksForNode(TTransaction* transaction, TCypressNode* trunkNode)
    {
        YT_ASSERT(trunkNode->IsTrunk());

        TCompactVector<TCypressNode*, 1> lockedNodes{trunkNode};
        TCompactVector<TLock*, 16> locks;
        locks.reserve(transaction->Locks().size());
        for (auto* lock : transaction->Locks()) {
            if (lock->GetTrunkNode() == trunkNode) {
                locks.push_back(lock);
            }
        }

        DoReleaseLocks(transaction, /*promote*/ false, locks, lockedNodes);
    }

    void DoPromoteLock(TLock* lock)
    {
        auto* transaction = lock->GetTransaction();
        auto* parentTransaction = transaction->GetParent();
        YT_VERIFY(parentTransaction);
        auto* trunkNode = lock->GetTrunkNode();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        parentTransaction->AttachLock(lock, objectManager);
        transaction->DetachLock(lock, objectManager, /*resetLockTransaction*/ false);

        if (trunkNode && lock->GetState() == ELockState::Acquired) {
            auto* lockingState = trunkNode->MutableLockingState();
            switch (lock->Request().Mode) {
                case ELockMode::Exclusive:
                    lockingState->TransactionToExclusiveLocks.erase(lock->GetTransactionToExclusiveLocksIterator());
                    if (auto [it, inserted] = lockingState->TransactionToExclusiveLocks.emplace(parentTransaction, lock);
                        inserted)
                    {
                        lock->SetTransactionToExclusiveLocksIterator(it);
                    } else {
                        AlertDuplicateLock(lock);
                    }
                    break;

                case ELockMode::Shared:
                    lockingState->TransactionAndKeyToSharedLocks.erase(lock->GetTransactionAndKeyToSharedLocksIterator());
                    if (auto [it, inserted] = lockingState->TransactionAndKeyToSharedLocks.emplace(parentTransaction, lock->Request().Key, lock);
                        inserted)
                    {
                        lock->SetTransactionAndKeyToSharedLocksIterator(it);
                    } else {
                        AlertDuplicateLock(lock);
                    }
                    break;

                default:
                    YT_ABORT();
            }

            // NB: Node could be locked more than once.
            parentTransaction->LockedNodes().insert(trunkNode);
        }
        YT_LOG_DEBUG("Lock promoted (LockId: %v, TransactionId: %v -> %v)",
            lock->GetId(),
            transaction->GetId(),
            parentTransaction->GetId());
    }

    void DoRemoveLock(TLock* lock, bool resetEmptyLockingState)
    {
        auto* transaction = lock->GetTransaction();
        auto* trunkNode = lock->GetTrunkNode();
        if (trunkNode) {
            auto* lockingState = trunkNode->MutableLockingState();
            switch (lock->GetState()) {
                case ELockState::Acquired: {
                    lockingState->AcquiredLocks.erase(lock->GetLockListIterator());
                    const auto& request = lock->Request();
                    switch (request.Mode) {
                        case ELockMode::Exclusive:
                            lockingState->TransactionToExclusiveLocks.erase(lock->GetTransactionToExclusiveLocksIterator());
                            break;

                        case ELockMode::Shared:
                            lockingState->TransactionAndKeyToSharedLocks.erase(lock->GetTransactionAndKeyToSharedLocksIterator());
                            if (lock->Request().Key.Kind != ELockKeyKind::None) {
                                lockingState->KeyToSharedLocks.erase(lock->GetKeyToSharedLocksIterator());
                            }
                            break;

                        case ELockMode::Snapshot:
                            lockingState->TransactionToSnapshotLocks.erase(lock->GetTransactionToSnapshotLocksIterator());
                            break;

                        default:
                            YT_ABORT();
                    }
                    break;
                }

                case ELockState::Pending:
                    lockingState->PendingLocks.erase(lock->GetLockListIterator());
                    break;

                default:
                    YT_ABORT();
            }

            if (resetEmptyLockingState) {
                trunkNode->ResetLockingStateIfEmpty();
            }
            lock->SetTrunkNode(nullptr);
        }
        const auto& objectManager = Bootstrap_->GetObjectManager();
        transaction->DetachLock(lock, objectManager);

        YT_LOG_DEBUG("Lock released (LockId: %v, TransactionId: %v)",
            lock->GetId(),
            transaction->GetId());
    }

    void CheckPendingLocks(TCypressNode* trunkNode)
    {
        // NB: IsOrphaned below lies on object ref-counters. This flush is essential to detect
        // when trunkNode becomes orphaned.
        FlushObjectUnrefs();

        // Ignore orphaned nodes. Eventually the node will get destroyed and the lock will become
        // orphaned.
        if (IsOrphaned(trunkNode)) {
            return;
        }

        // Make as many acquisitions as possible.
        const auto& lockingState = trunkNode->LockingState();
        auto it = lockingState.PendingLocks.begin();
        // Be prepared for locking state to vanish.
        while (trunkNode->HasLockingState() && it != lockingState.PendingLocks.end()) {
            // Be prepared to possible iterator invalidation.
            auto* lock = *it++;
            auto error = CheckLock(
                trunkNode,
                lock->GetTransaction(),
                lock->Request(),
                false);
            if (error.IsOK()) {
                DoAcquireLock(lock);
            }
        }
    }

    void PostLockForeignNodeRequest(const TLock* lock)
    {
        const auto* trunkNode = lock->GetTrunkNode();
        auto externalCellTag = trunkNode->GetExternalCellTag();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto externalizedTransactionId = transactionManager->ExternalizeTransaction(lock->GetTransaction(), {externalCellTag});

        NProto::TReqLockForeignNode request;
        ToProto(request.mutable_transaction_id(), externalizedTransactionId);
        ToProto(request.mutable_node_id(), trunkNode->GetId());
        request.set_mode(static_cast<int>(lock->Request().Mode));
        switch (lock->Request().Key.Kind) {
            case ELockKeyKind::None:
                break;
            case ELockKeyKind::Child:
                request.set_child_key(lock->Request().Key.Name);
                break;
            case ELockKeyKind::Attribute:
                request.set_attribute_key(lock->Request().Key.Name);
                break;
            default:
                break;
        }
        request.set_timestamp(lock->Request().Timestamp);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(request, externalCellTag);
    }

    void PostUnlockForeignNodeRequest(TCypressNode* trunkNode, TTransaction* transaction, bool explicitOnly)
    {
        auto externalCellTag = trunkNode->GetExternalCellTag();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto externalizedTransactionId = transactionManager->ExternalizeTransaction(transaction, {externalCellTag});

        NProto::TReqUnlockForeignNode request;
        ToProto(request.mutable_transaction_id(), externalizedTransactionId);
        ToProto(request.mutable_node_id(), trunkNode->GetId());
        request.set_explicit_only(explicitOnly);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(request, externalCellTag);
    }

    void ListSubtreeNodes(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        bool includeRoot,
        TSubtreeNodes* subtreeNodes)
    {
        YT_ASSERT(trunkNode->IsTrunk());

        if (includeRoot) {
            subtreeNodes->push_back(trunkNode);
        }

        switch (trunkNode->GetNodeType()) {
            case ENodeType::Map: {
                auto originators = GetNodeReverseOriginators(transaction, trunkNode);
                THashMap<TString, TCypressNode*> children;
                for (const auto* node : originators) {
                    const auto* mapNode = node->As<TCypressMapNode>();
                    for (const auto& [key, child] : mapNode->KeyToChild()) {
                        if (child) {
                            children[key] = child;
                        } else {
                            // NB: erase may fail.
                            children.erase(key);
                        }
                    }
                }

                for (const auto& [key, child] : children) {
                    ListSubtreeNodes(child, transaction, true, subtreeNodes);
                }

                break;
            }

            case ENodeType::List: {
                auto* node = GetVersionedNode(trunkNode, transaction);
                auto* listRoot = node->As<TListNode>();
                for (auto* trunkChild : listRoot->IndexToChild()) {
                    ListSubtreeNodes(trunkChild, transaction, true, subtreeNodes);
                }
                break;
            }

            default:
                break;
        }
    }


    TCypressNode* BranchNode(
        TCypressNode* originatingNode,
        TTransaction* transaction,
        const TLockRequest& request)
    {
        YT_VERIFY(originatingNode);
        YT_VERIFY(transaction);
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto id = originatingNode->GetId();

        // Create a branched node and initialize its state.
        const auto& handler = GetHandler(originatingNode);
        auto branchedNodeHolder = handler->Branch(originatingNode, transaction, request);

        TVersionedNodeId versionedId(id, transaction->GetId());
        auto* branchedNode = NodeMap_.Insert(versionedId, std::move(branchedNodeHolder));

        YT_VERIFY(branchedNode->GetLockMode() == request.Mode);

        // Register the branched node with the transaction.
        transaction->BranchedNodes().push_back(branchedNode);

        // The branched node holds an implicit reference to its trunk.
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(originatingNode->GetTrunkNode());

        return branchedNode;
    }

    // Returns originating node (and `nullptr` for snapshot branches).
    // NB: This function does not modify `transaction->BranchedNodes()`.
    TCypressNode* DoMergeNode(
        TTransaction* transaction,
        TCypressNode* branchedNode)
    {
        YT_ASSERT(branchedNode->GetTransaction() == transaction);

        const auto& objectManager = Bootstrap_->GetObjectManager();

        const auto& handler = GetHandler(branchedNode);

        auto* trunkNode = branchedNode->GetTrunkNode();
        auto branchedNodeId = branchedNode->GetVersionedId();
        bool isSnapshotBranch = branchedNode->GetLockMode() == ELockMode::Snapshot;

        TCypressNode* originatingNode = nullptr;

        if (!isSnapshotBranch) {
            // Merge changes back.
            originatingNode = branchedNode->GetOriginator();
            handler->Merge(originatingNode, branchedNode);

            // The root needs a special handling.
            // When Cypress gets cleared, the root is created and is assigned zero creation time.
            // (We don't have any mutation context at hand to provide a synchronized timestamp.)
            // Later on, Cypress is initialized and filled with nodes.
            // At this point we set the root's creation time.
            if (trunkNode == RootNode_ && !transaction->GetParent() && trunkNode->GetCreationTime() == TInstant::Zero()) {
                originatingNode->SetCreationTime(originatingNode->GetModificationTime());
            }
        } else {
            // Destroy the branched copy.
            handler->Zombify(branchedNode);
            handler->Destroy(branchedNode);

            YT_LOG_DEBUG("Node snapshot destroyed (NodeId: %v)", branchedNodeId);
        }

        // Drop the implicit reference to the trunk.
        objectManager->UnrefObject(trunkNode);

        // Remove the branched copy.
        NodeMap_.Remove(branchedNodeId);

        YT_LOG_DEBUG("Branched node removed (NodeId: %v)", branchedNodeId);

        return originatingNode;
    }

    //! Returns originating node.
    TCypressNode* MergeParticularBranchedNode(TTransaction* transaction, TCypressNode* node)
    {
        auto it = Find(transaction->BranchedNodes(), node);
        YT_VERIFY(it != transaction->BranchedNodes().end());

        auto* originator = DoMergeNode(transaction, node);

        transaction->BranchedNodes().erase(it);

        return originator;
    }

    void MergeBranchedNodes(TTransaction* transaction)
    {
        for (auto* node : transaction->BranchedNodes()) {
            DoMergeNode(transaction, node);
        }
        transaction->BranchedNodes().clear();
    }

    //! Unbranches all nodes branched by #transaction and updates their version trees.
    void RemoveBranchedNodes(TTransaction* transaction)
    {
        if (transaction->BranchedNodes().size() != transaction->LockedNodes().size()) {
            YT_LOG_ALERT("Transaction branched node count differs from its locked node count (TransactionId: %v, BranchedNodeCount: %v, LockedNodeCount: %v)",
                transaction->GetId(),
                transaction->BranchedNodes().size(),
                transaction->LockedNodes().size());
        }

        auto& branchedNodes = transaction->BranchedNodes();
        // The reverse order is for efficient removal.
        while (!branchedNodes.empty()) {
            auto* branchedNode = branchedNodes.back();
            RemoveOrUpdateNodesOnLockModeChange(
                branchedNode->GetTrunkNode(),
                transaction,
                branchedNode->GetLockMode(),
                ELockMode::None);
        }
        YT_VERIFY(branchedNodes.empty());
    }


    TCypressNode* DoCloneNode(
        TCypressNode* sourceNode,
        ICypressNodeFactory* factory,
        TNodeId hintId,
        ENodeCloneMode mode)
    {
        // Prepare account.
        auto* account = factory->GetClonedNodeAccount(sourceNode->Account().Get());

        const auto& handler = GetHandler(sourceNode);
        auto* clonedTrunkNode = handler->Clone(
            sourceNode,
            factory,
            hintId,
            mode,
            account);

        auto* sourceTrunkNode = sourceNode->GetTrunkNode();

        // Set owner.
        if (factory->ShouldPreserveOwner()) {
            clonedTrunkNode->Acd().SetOwner(sourceTrunkNode->Acd().GetOwner());
        } else {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto* user = securityManager->GetAuthenticatedUser();
            clonedTrunkNode->Acd().SetOwner(user);
        }

        // Copy creation time.
        if (factory->ShouldPreserveCreationTime()) {
            clonedTrunkNode->SetCreationTime(sourceTrunkNode->GetCreationTime());
        }

        // Copy modification time.
        if (factory->ShouldPreserveModificationTime()) {
            clonedTrunkNode->SetModificationTime(sourceTrunkNode->GetModificationTime());
        }

        // Copy expiration time.
        auto expirationTime = sourceNode->TryGetExpirationTime();
        if (factory->ShouldPreserveExpirationTime() && expirationTime) {
            SetExpirationTime(clonedTrunkNode, *expirationTime);
        }

        // Copy expiration timeout.
        auto expirationTimeout = sourceNode->TryGetExpirationTimeout();
        if (factory->ShouldPreserveExpirationTimeout() && expirationTimeout) {
            SetExpirationTimeout(clonedTrunkNode, *expirationTimeout);
        }

        NodeCreated_.Fire(clonedTrunkNode);

        return clonedTrunkNode;
    }


    void ValidateCreatedNodeTypePermission(EObjectType type)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* schema = objectManager->GetSchema(type);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(schema, EPermission::Create);
    }

    void HydraUpdateAccessStatistics(NProto::TReqUpdateAccessStatistics* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (const auto& update : request->updates()) {
            auto nodeId = FromProto<TNodeId>(update.node_id());
            auto* node = FindNode(TVersionedNodeId(nodeId));
            if (!IsObjectAlive(node)) {
                continue;
            }

            // Update access time.
            auto accessTime = FromProto<TInstant>(update.access_time());
            if (accessTime > node->GetAccessTime()) {
                node->SetAccessTime(accessTime);
            }

            // Update access counter.
            i64 accessCounter = node->GetAccessCounter() + update.access_counter_delta();
            node->SetAccessCounter(accessCounter);
        }
    }

    void HydraTouchNodes(NProto::TReqTouchNodes* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (const auto& update : request->updates()) {
            auto nodeId = FromProto<TNodeId>(update.node_id());
            auto* trunkNode = FindNode(TVersionedNodeId(nodeId));
            if (!IsObjectAlive(trunkNode)) {
                continue;
            }

            if (trunkNode->TryGetExpirationTimeout()) {
                auto touchTime = FromProto<TInstant>(update.touch_time());
                if (touchTime > trunkNode->GetTouchTime()) {
                    trunkNode->SetTouchTime(touchTime);
                }
                ExpirationTracker_->OnNodeTouched(trunkNode);
            }
        }
    }

    void HydraCreateForeignNode(NProto::TReqCreateForeignNode* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        auto nodeId = FromProto<TObjectId>(request->node_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto accountId = FromProto<TAccountId>(request->account_id());
        auto type = EObjectType(request->type());
        auto nativeContentRevision = request->native_content_revision();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionId
            ? transactionManager->GetTransactionOrThrow(transactionId)
            : nullptr;

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->GetAccount(accountId);

        auto explicitAttributes = FromProto(request->explicit_node_attributes());
        auto inheritedAttributes = FromProto(request->inherited_node_attributes());

        auto versionedNodeId = TVersionedNodeId(nodeId, transactionId);

        const auto& handler = GetHandler(type);
        auto* trunkNode = CreateNode(
            handler,
            nodeId,
            TCreateNodeContext{
                .ExternalCellTag = NotReplicatedCellTagSentinel,
                .Transaction = transaction,
                .InheritedAttributes = inheritedAttributes.Get(),
                .ExplicitAttributes = explicitAttributes.Get(),
                .Account = account,
                .NativeContentRevision = nativeContentRevision
            });

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(trunkNode);

        handler->FillAttributes(trunkNode, inheritedAttributes.Get(), explicitAttributes.Get());

        LockNode(trunkNode, transaction, ELockMode::Exclusive);
    }

    void HydraCloneForeignNode(NProto::TReqCloneForeignNode* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        auto sourceNodeId = FromProto<TNodeId>(request->source_node_id());
        auto sourceTransactionId = FromProto<TTransactionId>(request->source_transaction_id());
        auto clonedNodeId = FromProto<TNodeId>(request->cloned_node_id());
        auto clonedTransactionId = FromProto<TTransactionId>(request->cloned_transaction_id());
        auto mode = CheckedEnumCast<ENodeCloneMode>(request->mode());
        auto accountId = FromProto<TAccountId>(request->account_id());
        auto nativeContentRevision = request->native_content_revision();
        auto schemaId = FromProto<TMasterTableSchemaId>(request->schema_id_hint());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* sourceTransaction = sourceTransactionId
            ? transactionManager->FindTransaction(sourceTransactionId)
            : nullptr;
        if (sourceTransactionId && !IsObjectAlive(sourceTransaction)) {
            YT_LOG_ERROR("Source transaction is not alive (SourceNodeId: %v, ClonedNodeId: %v, SourceTransactionId: %v, ClonedTransactionId: %v, Mode: %v)",
                sourceNodeId,
                clonedNodeId,
                sourceTransactionId,
                clonedTransactionId,
                mode);
            return;
        }

        auto* clonedTransaction = clonedTransactionId
            ? transactionManager->FindTransaction(clonedTransactionId)
            : nullptr;
        if (clonedTransactionId && !IsObjectAlive(clonedTransaction)) {
            YT_LOG_ALERT("Cloned transaction is not alive (SourceNodeId: %v, ClonedNodeId: %v, SourceTransactionId: %v, ClonedTransactionId: %v, Mode: %v)",
                sourceNodeId,
                clonedNodeId,
                sourceTransactionId,
                clonedTransactionId,
                mode);
            return;
        }

        auto* sourceTrunkNode = FindNode(TVersionedObjectId(sourceNodeId));
        if (!IsObjectAlive(sourceTrunkNode)) {
            YT_LOG_DEBUG("Attempted to clone a non-alive foreign node (SourceNodeId: %v, ClonedNodeId: %v, SourceTransactionId: %v, ClonedTransactionId: %v, Mode: %v)",
                sourceNodeId,
                clonedNodeId,
                sourceTransactionId,
                clonedTransactionId,
                mode);
            return;
        }

        auto* sourceNode = GetVersionedNode(sourceTrunkNode, sourceTransaction);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->GetAccount(accountId);

        auto factory = CreateNodeFactory(
            nullptr,
            clonedTransaction,
            account,
            TNodeFactoryOptions(),
            sourceTrunkNode,
            NYPath::TYPath());

        auto* clonedTrunkNode = DoCloneNode(
            sourceNode,
            factory.get(),
            clonedNodeId,
            mode);

        // NB: Schema needs to be set here because cloned table's native cell is different from original's.
        if (schemaId) {
            YT_VERIFY(IsTableType(clonedTrunkNode->GetType()));

            const auto& tableManager = Bootstrap_->GetTableManager();
            auto* table = clonedTrunkNode->As<TTableNode>();

            tableManager->SetTableSchemaOrCrash(table, schemaId);
        }

        // TODO(danilalexeev): Refactor this.
        if (IsTableType(sourceNode->GetType())) {
            YT_VERIFY(IsTableType(clonedTrunkNode->GetType()));
            const auto& tableManager = Bootstrap_->GetTableManager();
            auto* sourceTable = sourceNode->As<TTableNode>();
            auto* clonedTable = clonedTrunkNode->As<TTableNode>();
            tableManager->OnTableCopied(sourceTable, clonedTable);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(clonedTrunkNode);

        clonedTrunkNode->SetNativeContentRevision(nativeContentRevision);

        LockNode(clonedTrunkNode, clonedTransaction, ELockMode::Exclusive);

        factory->Commit();

        YT_LOG_DEBUG("Foreign node cloned (SourceNodeId: %v, ClonedNodeId: %v, Account: %v)",
            TVersionedNodeId(sourceNodeId, sourceTransactionId),
            TVersionedNodeId(clonedNodeId, clonedTransactionId),
            account->GetName());
    }

    void HydraRemoveExpiredNodes(NProto::TReqRemoveExpiredNodes* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (const auto& protoId : request->node_ids()) {
            auto nodeId = FromProto<TNodeId>(protoId);

            auto* trunkNode = NodeMap_.Find(TVersionedNodeId(nodeId, NullTransactionId));
            if (!trunkNode) {
                continue;
            }

            // NB: IsOrphaned below lies on object ref-counters. This flush is seemingly redundant but makes the code more robust.
            FlushObjectUnrefs();

            if (IsOrphaned(trunkNode)) {
                continue;
            }

            const auto& cypressManager = Bootstrap_->GetCypressManager();
            auto path = cypressManager->GetNodePath(trunkNode, nullptr);
            try {
                YT_LOG_DEBUG("Removing expired node (NodeId: %v, Path: %v)",
                    nodeId,
                    path);
                auto nodeProxy = GetNodeProxy(trunkNode, nullptr);
                auto parentProxy = nodeProxy->GetParent();
                parentProxy->RemoveChild(nodeProxy);

                YT_LOG_ACCESS(
                    nodeId,
                    path,
                    nullptr,
                    "TtlRemove");
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Cannot remove an expired node; backing off and retrying (NodeId: %v, Path: %v)",
                    nodeId,
                    path);
                ExpirationTracker_->OnNodeRemovalFailed(trunkNode);
            }
        }
    }

    void HydraLockForeignNode(NProto::TReqLockForeignNode* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto nodeId = FromProto<TNodeId>(request->node_id());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            YT_LOG_ALERT("Lock transaction is missing (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        auto* trunkNode = FindNode(TVersionedObjectId(nodeId));
        if (!IsObjectAlive(trunkNode)) {
            YT_LOG_ALERT("Lock node is missing (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        TLockRequest lockRequest;
        if (request->has_child_key()) {
            lockRequest = TLockRequest::MakeSharedChild(request->child_key());
        } else if (request->has_attribute_key()) {
            lockRequest = TLockRequest::MakeSharedAttribute(request->attribute_key());
        } else {
            lockRequest = TLockRequest(static_cast<ELockMode>(request->mode()));
        }
        lockRequest.Timestamp = static_cast<TTimestamp>(request->timestamp());

        auto error = CheckLock(
            trunkNode,
            transaction,
            lockRequest,
            false);
        if (!error.IsOK()) {
            YT_LOG_ALERT(error, "Cannot lock foreign node (NodeId: %v, TransactionId: %v, Mode: %v, Key: %v)",
                nodeId,
                transactionId,
                lockRequest.Mode,
                lockRequest.Key);
            return;
        }

        auto* lock = DoCreateLock(trunkNode, transaction, lockRequest, false);
        DoAcquireLock(lock);
    }

    void HydraUnlockForeignNode(NProto::TReqUnlockForeignNode* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto explicitOnly = request->explicit_only();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            YT_LOG_ALERT("Unlock transaction is missing (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        auto* trunkNode = FindNode(TVersionedObjectId(nodeId));
        if (!IsObjectAlive(trunkNode)) {
            YT_LOG_ALERT("Unlock node is missing (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        auto error = CheckUnlock(trunkNode, transaction, false, explicitOnly);
        if (!error.IsOK()) {
            YT_LOG_ALERT(error, "Cannot unlock foreign node (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        if (IsUnlockRedundant(trunkNode, transaction, explicitOnly)) {
            return;
        }

        DoUnlockNode(trunkNode, transaction, explicitOnly);
    }

    // COMPAT(h0pless): RefactorSchemaExport. This is leftovers from EnsureSchemaExported methods.
    void HydraImportTableSchema(NProto::TReqExportTableSchema* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        auto schemaId = FromProto<TMasterTableSchemaId>(request->schema_id());
        auto schema = FromProto<TTableSchemaPtr>(request->schema());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(transactionId);
        YT_VERIFY(IsObjectAlive(transaction));

        YT_VERIFY(schema);
        YT_VERIFY(schemaId);

        const auto& tableManager = Bootstrap_->GetTableManager();
        tableManager->CreateImportedTemporaryMasterTableSchema(*schema, transaction, schemaId);
    }

    // COMPAT(h0pless): Remove this after schema migration is complete.
    void HydraSetTableSchemas(NProto::TReqSetTableSchemas* request)
    {
        YT_LOG_DEBUG("Received HydraSetTableSchemas request (RequestSize: %v)",
            request->subrequests_size());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& tableManager = Bootstrap_->GetTableManager();
        auto* emptySchema = tableManager->GetEmptyMasterTableSchema();

        for (const auto& subrequest : request->subrequests()) {
            auto nodeId = FromProto<TNodeId>(subrequest.table_node_id());
            auto transactionId = FromProto<TTransactionId>(subrequest.transaction_id());
            auto schemaId = FromProto<TMasterTableSchemaId>(subrequest.schema_id());

            auto externalizingCellTag = CellTagFromId(nodeId);
            auto effectiveTransactionId = transactionId;
            if (externalizingCellTag != CellTagFromId(transactionId)) {
                effectiveTransactionId = NTransactionClient::MakeExternalizedTransactionId(transactionId, externalizingCellTag);
            }

            auto* node = FindNode(TVersionedNodeId{nodeId, effectiveTransactionId});
            if (!node) {
                // This can happen when a node is still a zombie on the native cell, but was already destroyed on the external.
                YT_LOG_ALERT("Received HydraSetTableSchemas request for a non-existing node (NodeId: %v, SchemaId: %v)",
                    TVersionedNodeId(nodeId, effectiveTransactionId),
                    schemaId);
                continue;
            }

            YT_VERIFY(IsTableType(node->GetType()));
            auto* table = node->As<TTableNode>();

            auto* oldSchema = table->GetSchema();
            auto* existingSchema = tableManager->FindMasterTableSchema(schemaId);
            YT_VERIFY(IsObjectAlive(existingSchema));

            // Just a sanity check.
            if (*oldSchema->AsTableSchema() != *existingSchema->AsTableSchema()) {
                YT_LOG_ALERT(
                    "Schemas between native and external cells differ "
                    "(NativeSchemaId: %v, ExternalSchemaId: %v, NativeSchema: %v, ExternalSchema: %v)",
                    schemaId,
                    oldSchema->GetId(),
                    *existingSchema->AsTableSchema(),
                    oldSchema->AsTableSchema());
                YT_VERIFY(*oldSchema->AsTableSchema() == *emptySchema->AsTableSchema());
            }

            tableManager->SetTableSchema(table, existingSchema);
        }
    }

    void HydraPrepareSetAttributeOnTransactionCommit(
        TTransaction* /*transaction*/,
        NApi::NNative::NProto::TReqSetAttributeOnTransactionCommit* request,
        const TTransactionPrepareOptions& /*prepareOptions*/)
    {
        auto nodeId = FromProto<TObjectId>(request->node_id());
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (CellTagFromId(nodeId) == multicellManager->GetCellTag()) {
            GetNodeOrThrow(TVersionedNodeId(nodeId));
        }

        // NB: We cannot run this transaction action on native cell because this
        // leads to distributed transaction commit which can not be done with
        // preprequisite transactions. So just check in those rare cases where
        // transaction coordinator is also a native cell for this map node.
    }

    void SetAttributeOnTransactionCommit(
        TTransactionId transactionId,
        TNodeId nodeId,
        const TString& attribute,
        const TYsonString& value)
    {
        YT_VERIFY(HasMutationContext());

        auto* node = FindNode(TVersionedNodeId(nodeId));
        if (!IsObjectAlive(node)) {
            YT_LOG_ALERT(
                "Failed to set attribute on transaction commit: no such node "
                "(TransactionId: %v, NodeId: %v, Attribute: %v)",
                transactionId,
                nodeId,
                attribute);
            return;
        }

        try {
            node->GetMutableAttributes()->Set(attribute, value);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Failed to set attribute on transaction commit "
                "(TransactionId: %v, NodeId: %v, Attribute: %v)",
                transactionId,
                nodeId,
                attribute);
        }
    }

    void HydraCommitSetAttributeOnTransactionCommit(
        TTransaction* transaction,
        NApi::NNative::NProto::TReqSetAttributeOnTransactionCommit* request,
        const TTransactionCommitOptions& /*commitOptions*/)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cellTag = CellTagFromId(nodeId);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        if (cellTag == multicellManager->GetCellTag()) {
            SetAttributeOnTransactionCommit(
                transaction->GetId(),
                nodeId,
                request->attribute(),
                TYsonString(request->value()));
            return;
        }

        NProto::TReqSetAttributeOnTransactionCommit message;
        ToProto(message.mutable_transaction_id(),  transaction->GetId());
        ToProto(message.mutable_node_id(), nodeId);
        message.set_attribute(request->attribute());
        message.set_value(request->value());

        multicellManager->PostToMaster(message, cellTag);
    }

    void HydraSetAttributeOnTransactionCommit(
        NProto::TReqSetAttributeOnTransactionCommit* request)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (CellTagFromId(nodeId) != multicellManager->GetCellTag()) {
            YT_LOG_ALERT(
                "Failed to set attribute on transaction commit: not a native cell "
                "(TransactionId: %v, NodeId: %v, Attribute: %v)",
                transactionId,
                nodeId,
                request->attribute());
            return;
        }

        SetAttributeOnTransactionCommit(
            transactionId,
            nodeId,
            request->attribute(),
            TYsonString(request->value()));
    }

    void HydraCommitMergeToTrunkAndUnlockNode(
        TTransaction* /*transaction*/,
        NApi::NNative::NProto::TReqMergeToTrunkAndUnlockNode* request,
        const TTransactionCommitOptions& /*commitOptions*/)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto versionedId = TVersionedNodeId(nodeId, transactionId);

        auto* currentNode = FindNode(versionedId);
        if (!currentNode) {
            YT_LOG_DEBUG("Manual node unbranching failed; no such node %Qv", versionedId);
            return;
        }
        auto* trunkNode = currentNode->GetTrunkNode();

        while (currentNode != trunkNode) {
            auto* currentTransaction = currentNode->GetTransaction();
            currentNode = MergeParticularBranchedNode(currentTransaction, currentNode);
            ForceRemoveAllLocksForNode(currentTransaction, currentNode->GetTrunkNode());
        }
    }

    TFuture<TYsonString> DoComputeRecursiveResourceUsage(TVersionedNodeId versionedNodeId)
    {
        auto trunkNodeId = versionedNodeId.ObjectId;
        auto transactionId = versionedNodeId.TransactionId;
        auto* trunkNode = GetNodeOrThrow(TVersionedNodeId{trunkNodeId});
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionId
            ? transactionManager->GetTransactionOrThrow(transactionId)
            : nullptr;
        auto visitor = New<TResourceUsageVisitor>(Bootstrap_, trunkNode, transaction);
        return visitor->Run();
    }


    const TDynamicCypressManagerConfigPtr& GetDynamicConfig()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->CypressManager;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        RecursiveResourceUsageCache_->SetExpirationTimeout(
            GetDynamicConfig()->RecursiveResourceUsageCacheExpirationTimeout);
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager, Node, TCypressNode, NodeMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager, Lock, TLock, LockMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager, Shard, TCypressShard, ShardMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager, AccessControlObject, TAccessControlObject, AccessControlObjectMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager, AccessControlObjectNamespace, TAccessControlObjectNamespace, AccessControlObjectNamespaceMap_);

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TNodeMapTraits::TNodeMapTraits(TCypressManager* owner)
    : Owner_(owner)
{ }

std::unique_ptr<TCypressNode> TCypressManager::TNodeMapTraits::Create(TVersionedNodeId id) const
{
    auto type = TypeFromId(id.ObjectId);
    const auto& handler = Owner_->GetHandler(type);
    // This cell tag is fake and will be overwritten on load
    // (unless this is a pre-multicell snapshot, in which case NotReplicatedCellTagSentinel is just what we want).
    return handler->Instantiate(id, NotReplicatedCellTagSentinel);
}

////////////////////////////////////////////////////////////////////////////////

TNodeTypeHandler::TNodeTypeHandler(
    TCypressManager* owner,
    INodeTypeHandlerPtr underlyingHandler)
    : TObjectTypeHandlerBase(owner->Bootstrap_)
    , Owner_(owner)
    , UnderlyingHandler_(underlyingHandler)
{ }

void TNodeTypeHandler::DoZombifyObject(TCypressNode* node) noexcept
{
    Owner_->ZombifyNode(node);
}

void TNodeTypeHandler::DoDestroyObject(TCypressNode* node) noexcept
{
    Owner_->DestroyNode(node);
}

void TNodeTypeHandler::DoRecreateObjectAsGhost(TCypressNode* node) noexcept
{
    Owner_->RecreateNodeAsGhost(node);
}

TString TNodeTypeHandler::DoGetName(const TCypressNode* node)
{
    return Format("node %v", DoGetPath(node));
}

TString TNodeTypeHandler::DoGetPath(const TCypressNode* node)
{
    auto path = Owner_->GetNodePath(node->GetTrunkNode(), node->GetTransaction());
    return path;
}

////////////////////////////////////////////////////////////////////////////////

TLockTypeHandler::TLockTypeHandler(TCypressManager* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->LockMap_)
{ }

////////////////////////////////////////////////////////////////////////////////

ICypressManagerPtr CreateCypressManager(TBootstrap* bootstrap)
{
    return New<TCypressManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
