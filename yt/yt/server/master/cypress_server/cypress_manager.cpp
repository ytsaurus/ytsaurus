#include "cypress_manager.h"
#include "private.h"
#include "access_tracker.h"
#include "expiration_tracker.h"
#include "config.h"
#include "lock_proxy.h"
#include "node_detail.h"
#include "node_proxy_detail.h"
#include "portal_manager.h"
#include "portal_entrance_node.h"
#include "portal_exit_node.h"
#include "shard.h"
#include "portal_entrance_type_handler.h"
#include "portal_exit_type_handler.h"
#include "portal_node_map_type_handler.h"
#include "shard_type_handler.h"
#include "shard_map_type_handler.h"
#include "resolve_cache.h"
#include "link_node_type_handler.h"
#include "document_node_type_handler.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/multicell_manager.h>

#include <yt/server/master/object_server/object_detail.h>
#include <yt/server/master/object_server/type_handler_detail.h>

#include <yt/server/master/security_server/account.h>
#include <yt/server/master/security_server/group.h>
#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/user.h>

#include <yt/server/master/table_server/shared_table_schema.h>

#include <yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>
#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/client/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/misc/singleton.h>
#include <yt/core/misc/small_set.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/ypath_detail.h>

#include <yt/core/ypath/token.h>

namespace NYT::NCypressServer {

using namespace NBus;
using namespace NCellMaster;
using namespace NCypressClient::NProto;
using namespace NHydra;
using namespace NObjectClient::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;
static const INodeTypeHandlerPtr NullTypeHandler;

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TNodeFactory
    : public TTransactionalNodeFactoryBase
    , public ICypressNodeFactory
{
public:
    TNodeFactory(
        NCellMaster::TBootstrap* bootstrap,
        TCypressShard* shard,
        TTransaction* transaction,
        TAccount* account,
        const TNodeFactoryOptions& options)
        : Bootstrap_(bootstrap)
        , Shard_(shard)
        , Transaction_(transaction)
        , Account_(account)
        , Options_(options)
    {
        YT_VERIFY(Bootstrap_);
        YT_VERIFY(Account_);
    }

    virtual ~TNodeFactory() override
    {
        RollbackIfNeeded();
    }

    virtual void Commit() noexcept override
    {
        TTransactionalNodeFactoryBase::Commit();

        if (Transaction_) {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            for (auto* node : CreatedNodes_) {
                transactionManager->StageNode(Transaction_, node);
            }
        }

        const auto& portalManager = Bootstrap_->GetPortalManager();
        for (const auto& entrance : CreatedPortalEntrances_) {
            portalManager->RegisterEntranceNode(
                entrance.Node,
                *entrance.InheritedAttributes,
                *entrance.ExplicitAttributes);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        for (const auto& clone : ClonedExternalNodes_) {
            NProto::TReqCloneForeignNode protoRequest;
            ToProto(protoRequest.mutable_source_node_id(), clone.SourceNodeId.ObjectId);
            if (clone.SourceNodeId.TransactionId) {
                ToProto(protoRequest.mutable_source_transaction_id(), clone.SourceNodeId.TransactionId);
            }
            ToProto(protoRequest.mutable_cloned_node_id(), clone.ClonedNodeId.ObjectId);
            if (clone.ClonedNodeId.TransactionId) {
                ToProto(protoRequest.mutable_cloned_transaction_id(), clone.ClonedNodeId.TransactionId);
            }
            protoRequest.set_mode(static_cast<int>(clone.Mode));
            ToProto(protoRequest.mutable_account_id(), clone.CloneAccountId);
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
            multicellManager->PostToMaster(protoRequest, externalNode.ExternalCellTag);
        }

        ReleaseStagedObjects();
    }

    virtual void Rollback() noexcept override
    {
        TTransactionalNodeFactoryBase::Rollback();

        ReleaseStagedObjects();
    }

    virtual IStringNodePtr CreateString() override
    {
        return CreateNode(EObjectType::StringNode)->AsString();
    }

    virtual IInt64NodePtr CreateInt64() override
    {
        return CreateNode(EObjectType::Int64Node)->AsInt64();
    }

    virtual IUint64NodePtr CreateUint64() override
    {
        return CreateNode(EObjectType::Uint64Node)->AsUint64();
    }

    virtual IDoubleNodePtr CreateDouble() override
    {
        return CreateNode(EObjectType::DoubleNode)->AsDouble();
    }

    virtual IBooleanNodePtr CreateBoolean() override
    {
        return CreateNode(EObjectType::BooleanNode)->AsBoolean();
    }

    virtual IMapNodePtr CreateMap() override
    {
        return CreateNode(EObjectType::MapNode)->AsMap();
    }

    virtual IListNodePtr CreateList() override
    {
        return CreateNode(EObjectType::ListNode)->AsList();
    }

    virtual IEntityNodePtr CreateEntity() override
    {
        THROW_ERROR_EXCEPTION("Entity nodes cannot be created inside Cypress");
    }

    virtual NTransactionServer::TTransaction* GetTransaction() const override
    {
        return Transaction_;
    }

    virtual bool ShouldPreserveCreationTime() const override
    {
        return Options_.PreserveCreationTime;
    }

    virtual bool ShouldPreserveModificationTime() const override
    {
        return Options_.PreserveModificationTime;
    }

    virtual bool ShouldPreserveExpirationTime() const override
    {
        return Options_.PreserveExpirationTime;
    }

    virtual bool ShouldPreserveOwner() const override
    {
        return Options_.PreserveOwner;
    }

    virtual bool ShouldPreserveAcl() const override
    {
        return Options_.PreserveAcl;
    }

    virtual TAccount* GetNewNodeAccount() const override
    {
        return Account_;
    }

    virtual TAccount* GetClonedNodeAccount(TAccount* sourceAccount) const override
    {
        return Options_.PreserveAccount ? sourceAccount : Account_;
    }

    virtual void ValidateClonedAccount(
        ENodeCloneMode mode,
        TAccount* sourceAccount,
        TClusterResources sourceResourceUsage,
        TAccount* clonedAccount) override
    {
        clonedAccount->ValidateActiveLifeStage();

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
                    .SetMasterMemory(1);
            }
            securityManager->ValidateResourceUsageIncrease(clonedAccount, resourceUsageIncrease);
        }
    }

    virtual ICypressNodeProxyPtr CreateNode(
        EObjectType type,
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

        std::unique_ptr<IAttributeDictionary> explicitAttributeHolder;
        if (!explicitAttributes) {
            explicitAttributeHolder = CreateEphemeralAttributes();
            explicitAttributes = explicitAttributeHolder.get();
        }
        std::unique_ptr<IAttributeDictionary> inheritedAttributeHolder;
        if (!inheritedAttributes) {
            inheritedAttributeHolder = CreateEphemeralAttributes();
            inheritedAttributes = inheritedAttributeHolder.get();
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = GetNewNodeAccount();
        securityManager->ValidatePermission(account, EPermission::Use);
        auto deltaResources = TClusterResources()
            .SetNodeCount(1)
            .SetMasterMemory(1);
        securityManager->ValidateResourceUsageIncrease(account, deltaResources);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        bool defaultExternal =
            Any(handler->GetFlags() & ETypeFlags::Externalizable) &&
            (multicellManager->IsPrimaryMaster() || Shard_ && Shard_->GetRoot() && Shard_->GetRoot()->GetType() == EObjectType::PortalExit) &&
            !multicellManager->GetRegisteredMasterCellTags().empty();
        ValidateCreateNonExternalNode(explicitAttributes);
        bool external = explicitAttributes->GetAndRemove<bool>("external", defaultExternal);

        double externalCellBias = explicitAttributes->GetAndRemove<double>("external_cell_bias", 1.0);
        if (externalCellBias < 0.0 || externalCellBias > 1.0) {
            THROW_ERROR_EXCEPTION("\"external_cell_bias\" must be in range [0, 1]");
        }

        auto optionalExternalCellTag = explicitAttributes->FindAndRemove<TCellTag>("external_cell_tag");

        auto externalCellTag = NotReplicatedCellTag;
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
                    THROW_ERROR_EXCEPTION("No secondary masters registered");
                }
            }
        }

        if (externalCellTag == multicellManager->GetCellTag()) {
            external = false;
            externalCellTag = NotReplicatedCellTag;
        }

        if (externalCellTag == multicellManager->GetPrimaryCellTag()) {
            THROW_ERROR_EXCEPTION("Cannot place externalizable nodes at primary cell");
        }

        if (externalCellTag != NotReplicatedCellTag) {
            if (!multicellManager->IsRegisteredMasterCell(externalCellTag)) {
                THROW_ERROR_EXCEPTION("Unknown cell tag %v", externalCellTag);
            }
            if (None(multicellManager->GetMasterCellRoles(externalCellTag) & EMasterCellRoles::ChunkHost)) {
                THROW_ERROR_EXCEPTION("Cell with tag %v cannot host chunks", externalCellTag);
            }
        }

        // INodeTypeHandler::Create and ::FillAttributes may modify the attributes.
        std::unique_ptr<IAttributeDictionary> replicationExplicitAttributes;
        std::unique_ptr<IAttributeDictionary> replicationInheritedAttributes;
        if (external) {
            replicationExplicitAttributes = explicitAttributes->Clone();
            replicationInheritedAttributes = inheritedAttributes->Clone();
        }

        auto* trunkNode = cypressManager->CreateNode(
            handler,
            NullObjectId,
            TCreateNodeContext{
                .ExternalCellTag = externalCellTag,
                .Transaction = Transaction_,
                .InheritedAttributes = inheritedAttributes,
                .ExplicitAttributes = explicitAttributes,
                .Account = account,
                .Shard = Shard_
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
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto externalCellTag = node->GetExternalCellTag();
            auto externalizedTransactionId = transactionManager->ExternalizeTransaction(node->GetTransaction(), externalCellTag);
            CreatedExternalNodes_.push_back(TCreatedExternalNode{
                .NodeType = trunkNode->GetType(),
                .NodeId = TVersionedNodeId(trunkNode->GetId(), externalizedTransactionId),
                .AccountId = account->GetId(),
                .ReplicationExplicitAttributes = std::move(replicationExplicitAttributes),
                .ReplicationInheritedAttributes = std::move(replicationInheritedAttributes),
                .ExternalCellTag = externalCellTag
            });
        }

        if (type == EObjectType::PortalEntrance)  {
            CreatedPortalEntrances_.push_back({
                StageNode(node->As<TPortalEntranceNode>()),
                inheritedAttributes->Clone(),
                explicitAttributes->Clone()
            });
        }

        securityManager->UpdateMasterMemoryUsage(trunkNode);

        return cypressManager->GetNodeProxy(trunkNode, Transaction_);
    }

    virtual TCypressNode* InstantiateNode(
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

    virtual TCypressNode* CloneNode(
        TCypressNode* sourceNode,
        ENodeCloneMode mode) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* clonedTrunkNode = cypressManager->CloneNode(sourceNode, this, mode);
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
            auto externalizedSourceTransactionId = transactionManager->ExternalizeTransaction(sourceNode->GetTransaction(), externalCellTag);
            auto externalizedClonedTransactionId = transactionManager->ExternalizeTransaction(clonedNode->GetTransaction(), externalCellTag);
            ClonedExternalNodes_.push_back(TClonedExternalNode{
                .Mode = mode,
                .SourceNodeId = TVersionedObjectId(sourceNode->GetId(), externalizedSourceTransactionId),
                .ClonedNodeId = TVersionedObjectId(clonedNode->GetId(), externalizedClonedTransactionId),
                .CloneAccountId = clonedNode->GetAccount()->GetId(),
                .ExternalCellTag = externalCellTag
            });
        }

        return clonedNode;
    }

    virtual TCypressNode* EndCopyNode(TEndCopyContext* context) override
    {
        // See BeginCopyCore.
        using NYT::Load;
        auto sourceNodeId = Load<TNodeId>(*context);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* clonedTrunkNode = cypressManager->EndCopyNode(context, this, sourceNodeId);
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
            auto externalizedTransactionId = transactionManager->ExternalizeTransaction(Transaction_, externalCellTag);
            ClonedExternalNodes_.push_back(TClonedExternalNode{
                .Mode = context->GetMode(),
                .SourceNodeId = TVersionedObjectId(sourceNodeId, externalizedTransactionId),
                .ClonedNodeId = TVersionedObjectId(clonedNode->GetId(), externalizedTransactionId),
                .CloneAccountId = clonedNode->GetAccount()->GetId(),
                .ExternalCellTag = externalCellTag
            });
        }

        return clonedNode;
    }

    virtual TCypressNode* EndCopyNodeInplace(
        TCypressNode* trunkNode,
        TEndCopyContext* context) override
    {
        // See BeginCopyCore.
        using NYT::Load;
        auto sourceNodeId = Load<TNodeId>(*context);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->EndCopyNodeInplace(trunkNode, context, this, sourceNodeId);

        return cypressManager->LockNode(
            trunkNode,
            Transaction_,
            ELockMode::Exclusive,
            false,
            true);
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    TCypressShard* const Shard_;
    TTransaction* const Transaction_;
    TAccount* const Account_;
    const TNodeFactoryOptions Options_;

    std::vector<TCypressNode*> CreatedNodes_;
    std::vector<TObject*> StagedObjects_;

    struct TCreatedPortalEntrance
    {
        TPortalEntranceNode* Node;
        std::unique_ptr<IAttributeDictionary> InheritedAttributes;
        std::unique_ptr<IAttributeDictionary> ExplicitAttributes;
    };
    std::vector<TCreatedPortalEntrance> CreatedPortalEntrances_;

    struct TClonedExternalNode
    {
        ENodeCloneMode Mode;
        TVersionedNodeId SourceNodeId;
        TVersionedNodeId ClonedNodeId;
        TAccountId CloneAccountId;
        TCellTag ExternalCellTag;
    };
    std::vector<TClonedExternalNode> ClonedExternalNodes_;

    struct TCreatedExternalNode
    {
        EObjectType NodeType;
        TVersionedNodeId NodeId;
        TAccountId AccountId;
        std::unique_ptr<IAttributeDictionary> ReplicationExplicitAttributes;
        std::unique_ptr<IAttributeDictionary> ReplicationInheritedAttributes;
        TCellTag ExternalCellTag;
    };
    std::vector<TCreatedExternalNode> CreatedExternalNodes_;


    void RegisterCreatedNode(TCypressNode* trunkNode)
    {
        YT_ASSERT(trunkNode->IsTrunk());
        StageNode(trunkNode);
        CreatedNodes_.push_back(trunkNode);
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

        auto* attribute  = user->FindAttribute("allow_external_false");
        if (attribute && ConvertTo<bool>(*attribute)) {
            return;
        }

        THROW_ERROR_EXCEPTION(
            "User %Qv is not allowed to create explicitly non-external nodes. "
            "Check for `external=%%false' in object attributes and do not specify "
            "this attribute",
            user->GetName());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TNodeTypeHandler
    : public TObjectTypeHandlerBase<TCypressNode>
{
public:
    TNodeTypeHandler(
        TImpl* owner,
        INodeTypeHandlerPtr underlyingHandler);

    virtual ETypeFlags GetFlags() const override
    {
        return UnderlyingHandler_->GetFlags();
    }

    virtual EObjectType GetType() const override
    {
        return UnderlyingHandler_->GetObjectType();
    }

    virtual TObject* FindObject(TObjectId id) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->FindNode(TVersionedNodeId(id));
    }

    virtual TObject* CreateObject(
        TObjectId /*hintId*/,
        IAttributeDictionary* /*attributes*/) override
    {
        THROW_ERROR_EXCEPTION("Cypress nodes cannot be created via this call");
    }

private:
    TImpl* const Owner_;
    const INodeTypeHandlerPtr UnderlyingHandler_;


    virtual TCellTagList DoGetReplicationCellTags(const TCypressNode* node) override
    {
        auto externalCellTag = node->GetExternalCellTag();
        return externalCellTag == NotReplicatedCellTag ? TCellTagList() : TCellTagList{externalCellTag};
    }

    virtual TString DoGetName(const TCypressNode* node) override;

    virtual IObjectProxyPtr DoGetProxy(
        TCypressNode* node,
        TTransaction* transaction) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->GetNodeProxy(node, transaction);
    }

    virtual TAccessControlDescriptor* DoFindAcd(TCypressNode* node) override
    {
        return &node->GetTrunkNode()->Acd();
    }

    virtual TObject* DoGetParent(TCypressNode* node) override
    {
        return node->GetParent();
    }

    virtual void DoDestroyObject(TCypressNode* node) noexcept;

};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TLockTypeHandler
    : public TObjectTypeHandlerWithMapBase<TLock>
{
public:
    explicit TLockTypeHandler(TImpl* owner);

    virtual EObjectType GetType() const override
    {
        return EObjectType::Lock;
    }

private:
    virtual IObjectProxyPtr DoGetProxy(
        TLock* lock,
        TTransaction* /*transaction*/) override
    {
        return CreateLockProxy(Bootstrap_, &Metadata_, lock);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TImpl
    : public NCellMaster::TMasterAutomatonPart
{
public:
    explicit TImpl(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::CypressManager)
        , SharedTableSchemaRegistry_(New<NTableServer::TSharedTableSchemaRegistry>())
        , AccessTracker_(New<TAccessTracker>(bootstrap))
        , ExpirationTracker_(New<TExpirationTracker>(bootstrap))
        , NodeMap_(TNodeMapTraits(this))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);

        RootNodeId_ = MakeWellKnownId(EObjectType::MapNode, Bootstrap_->GetMulticellManager()->GetCellTag());
        RootShardId_ = MakeCypressShardId(RootNodeId_);
        ResolveCache_ = New<TResolveCache>(RootNodeId_);

        RegisterHandler(New<TStringNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TInt64NodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TUint64NodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TDoubleNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TBooleanNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TMapNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TListNodeTypeHandler>(Bootstrap_));
        RegisterHandler(CreateLinkNodeTypeHandler(Bootstrap_));
        RegisterHandler(CreateDocumentNodeTypeHandler(Bootstrap_));
        RegisterHandler(CreateShardMapTypeHandler(Bootstrap_));
        RegisterHandler(CreatePortalEntranceTypeHandler(Bootstrap_));
        RegisterHandler(CreatePortalExitTypeHandler(Bootstrap_));
        RegisterHandler(CreatePortalEntranceMapTypeHandler(Bootstrap_));
        RegisterHandler(CreatePortalExitMapTypeHandler(Bootstrap_));

        RegisterLoader(
            "CypressManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "CypressManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "CypressManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "CypressManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        RegisterMethod(BIND(&TImpl::HydraUpdateAccessStatistics, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraCreateForeignNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraCloneForeignNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRemoveExpiredNodes, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraLockForeignNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnlockForeignNode, Unretained(this)));
    }

    void Initialize()
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND(
            &TImpl::OnTransactionCommitted,
            MakeStrong(this)));
        transactionManager->SubscribeTransactionAborted(BIND(
            &TImpl::OnTransactionAborted,
            MakeStrong(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TLockTypeHandler>(this));
        objectManager->RegisterHandler(CreateShardTypeHandler(Bootstrap_, &ShardMap_));
    }


    void RegisterHandler(INodeTypeHandlerPtr handler)
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

    const INodeTypeHandlerPtr& FindHandler(EObjectType type)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (type < TEnumTraits<EObjectType>::GetMinValue() || type > TEnumTraits<EObjectType>::GetMaxValue()) {
            return NullTypeHandler;
        }

        return TypeToHandler_[type];
    }

    const INodeTypeHandlerPtr& GetHandler(EObjectType type)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& handler = FindHandler(type);
        YT_VERIFY(handler);
        return handler;
    }

    const INodeTypeHandlerPtr& GetHandler(const TCypressNode* node)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetHandler(node->GetType());
    }


    TCypressShard* CreateShard(TCypressShardId shardId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto shardHolder = std::make_unique<TCypressShard>(shardId);
        auto* shard = shardHolder.get();
        ShardMap_.Insert(shardId, std::move(shardHolder));
        return shard;
    }

    void SetShard(TCypressNode* node, TCypressShard* shard)
    {
        YT_ASSERT(node->IsTrunk());
        YT_ASSERT(!node->GetShard());

        node->SetShard(shard);

        auto* account = node->GetAccount();
        if (account) {
            UpdateShardNodeCount(shard, account, +1);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(shard);
    }

    void ResetShard(TCypressNode* node)
    {
        YT_ASSERT(node->IsTrunk());

        auto* shard = node->GetShard();
        if (!shard) {
            return;
        }

        node->SetShard(nullptr);

        auto* account = node->GetAccount();
        if (account) {
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
        int delta)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto it = shard->AccountStatistics().find(account);
        if (it == shard->AccountStatistics().end()) {
            it = shard->AccountStatistics().emplace(account, TCypressShardAccountStatistics()).first;
            objectManager->RefObject(account);
        }
        auto& statistics = it->second;
        statistics.NodeCount += delta;
        if (statistics.IsZero()) {
            shard->AccountStatistics().erase(it);
            objectManager->UnrefObject(account);
        }
    }


    std::unique_ptr<ICypressNodeFactory> CreateNodeFactory(
        TCypressShard* shard,
        TTransaction* transaction,
        TAccount* account,
        const TNodeFactoryOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return std::make_unique<TNodeFactory>(
            Bootstrap_,
            shard,
            transaction,
            account,
            options);
    }

    TCypressNode* CreateNode(
        const INodeTypeHandlerPtr& handler,
        TNodeId hintId,
        const TCreateNodeContext& context)
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
        TCellTag externalCellTag)
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
        ENodeCloneMode mode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(sourceNode);
        YT_VERIFY(factory);

        ValidateCreatedNodeTypePermission(sourceNode->GetType());

        auto* clonedAccount = factory->GetClonedNodeAccount(sourceNode->GetAccount());
        factory->ValidateClonedAccount(
            mode,
            sourceNode->GetAccount(),
            sourceNode->GetTotalResourceUsage(),
            clonedAccount);

        return DoCloneNode(
            sourceNode,
            factory,
            NullObjectId,
            mode);
    }

    TCypressNode* EndCopyNode(
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId)
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
        TNodeId sourceNodeId)
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


    TMapNode* GetRootNode() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return RootNode_;
    }

    TCypressNode* GetNodeOrThrow(const TVersionedNodeId& id)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* node = FindNode(id);
        if (!IsObjectAlive(node)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such node %v",
                id);
        }

        return node;
    }

    TYPath GetNodePath(TCypressNode* trunkNode, TTransaction* transaction)
    {
        auto fallbackToId = [&] {
            return FromObjectId(trunkNode->GetId());
        };

        using TToken = std::variant<TStringBuf, int>;
        SmallVector<TToken, 64> tokens;

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
                    auto key = FindMapNodeChildKey(currentParentNode->As<TMapNode>(), currentTrunkNode);
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
        } else if (currentNode->GetType() == EObjectType::PortalExit) {
            const auto* portalExit = currentNode->GetTrunkNode()->As<TPortalExitNode>();
            builder.AppendString(portalExit->GetPath());
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

    TYPath GetNodePath(const ICypressNodeProxy* nodeProxy)
    {
        return GetNodePath(nodeProxy->GetTrunkNode(), nodeProxy->GetTransaction());
    }

    TCypressNode* ResolvePathToTrunkNode(const TYPath& path, TTransaction* transaction)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* object = objectManager->ResolvePathToObject(
            path,
            transaction,
            TObjectManager::TResolvePathOptions{});
        if (!IsVersionedType(object->GetType())) {
            THROW_ERROR_EXCEPTION("Path %v points to a nonversioned %Qlv object instead of a node",
                path,
                object->GetType());
        }
        return object->As<TCypressNode>();
    }

    ICypressNodeProxyPtr ResolvePathToNodeProxy(const TYPath& path, TTransaction* transaction)
    {
        auto* trunkNode = ResolvePathToTrunkNode(path, transaction);
        return GetNodeProxy(trunkNode, transaction);
    }


    TCypressNode* FindNode(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
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
        TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
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
        TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        const auto& handler = GetHandler(trunkNode);
        return handler->GetProxy(trunkNode, transaction);
    }


    TCypressNode* LockNode(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        bool recursive = false,
        bool dontLockForeign = false)
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
        YT_ASSERT(transaction->IsTrunk());

        auto error = CheckUnlock(trunkNode, transaction, recursive, explicitOnly);
        error.ThrowOnError();

        ForEachSubtreeNode(trunkNode, transaction, recursive, [&] (TCypressNode* trunkChild) {
            if (IsUnlockRedundant(trunkChild, transaction, explicitOnly)) {
                return;
            }

            DoUnlockNode(trunkChild, transaction, explicitOnly);
        });
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Node explicitly unlocked (NodeId: %v, TransactionId: %v)",
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

        SmallSet<TCypressNode*, 8> unbranchedNodes;
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
        SmallVector<TTransaction*, 64> queue;

        size_t frontIndex = 0;
        queue.push_back(rootTransaction);

        while (frontIndex < queue.size()) {
            auto* transaction = queue[frontIndex++];

            for (auto* t : transaction->NestedTransactions()) {
                queue.push_back(t);
            }

            processTransaction(transaction);
        }
    }

    //! Destroys a single branched node.
    void DestroyBranchedNode(
        TTransaction* transaction,
        TCypressNode* branchedNode)
    {
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
        YT_ASSERT(branchedNode->GetTransaction() == transaction);
        handler->Destroy(branchedNode);
        NodeMap_.Remove(branchedNodeId);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Branched node removed (NodeId: %v)", branchedNodeId);
    }

    TLock* CreateLock(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool waitable)
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
            DoAcquireLock(lock);
            return lock;
        }

        // Should we wait?
        if (!waitable) {
            THROW_ERROR error;
        }

        // Will wait.
        return DoCreateLock(trunkNode, transaction, request, false);
    }

    void SetModified(
        TCypressNode* node,
        EModificationType modificationType)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AccessTracker_->SetModified(node, modificationType);
    }

    void SetAccessed(TCypressNode* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        if (HydraManager_->IsLeader() || HydraManager_->IsFollower() && !HasMutationContext()) {
            AccessTracker_->SetAccessed(trunkNode);
        }
    }

    void SetExpirationTime(TCypressNode* node, std::optional<TInstant> time)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (time) {
            node->SetExpirationTime(*time);
        } else if (node->IsTrunk()) {
            node->ResetExpirationTime();
        } else {
            node->RemoveExpirationTime();
        }

        if (node->IsTrunk()) {
            ExpirationTracker_->OnNodeExpirationTimeUpdated(node);
        } // Otherwise the tracker will be notified when and if the node is merged in.
    }


    TSubtreeNodes ListSubtreeNodes(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        bool includeRoot)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        TSubtreeNodes result;
        ListSubtreeNodes(trunkNode, transaction, includeRoot, &result);
        return result;
    }

    void AbortSubtreeTransactions(
        TCypressNode* trunkNode,
        TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        SmallVector<TTransaction*, 16> transactions;

        auto addLock = [&] (const TLock* lock) {
            // Get the top-most transaction.
            auto* transaction = lock->GetTransaction();
            while (transaction->GetParent()) {
                transaction = transaction->GetParent();
            }
            transactions.push_back(transaction);
        };

        auto addNode = [&] (const TCypressNode* node) {
            const auto& lockingState = node->LockingState();
            for (auto* lock : lockingState.AcquiredLocks) {
                addLock(lock);
            }
            for (auto* lock : lockingState.PendingLocks) {
                addLock(lock);
            }
        };

        ForEachSubtreeNode(
            trunkNode,
            transaction,
            true /* recursive */,
            addNode);

        std::sort(transactions.begin(), transactions.end(), TObjectRefComparer::Compare);
        transactions.erase(
            std::unique(transactions.begin(), transactions.end()),
            transactions.end());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        for (auto* transaction : transactions) {
            transactionManager->AbortTransaction(transaction, true);
        }
    }

    void AbortSubtreeTransactions(INodePtr node)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* cypressNode = ICypressNodeProxy::FromNode(node.Get());
        AbortSubtreeTransactions(cypressNode->GetTrunkNode(), cypressNode->GetTransaction());
    }


    bool IsOrphaned(TCypressNode* trunkNode)
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
        TCypressNode* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
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
        TCypressNode* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        auto result = GetNodeOriginators(transaction, trunkNode);
        std::reverse(result.begin(), result.end());
        return result;
    }

    DEFINE_BYREF_RO_PROPERTY(NTableServer::TSharedTableSchemaRegistryPtr, SharedTableSchemaRegistry);
    DEFINE_BYREF_RO_PROPERTY(TResolveCachePtr, ResolveCache);

    DECLARE_ENTITY_MAP_ACCESSORS(Node, TCypressNode);
    DECLARE_ENTITY_MAP_ACCESSORS(Lock, TLock);
    DECLARE_ENTITY_MAP_ACCESSORS(Shard, TCypressShard);

    DEFINE_SIGNAL(void(TCypressNode*), NodeCreated);

private:
    friend class TNodeTypeHandler;
    friend class TLockTypeHandler;

    class TNodeMapTraits
    {
    public:
        explicit TNodeMapTraits(TImpl* owner);

        std::unique_ptr<TCypressNode> Create(const TVersionedNodeId& id) const;

    private:
        TImpl* const Owner_;

    };

    const TAccessTrackerPtr AccessTracker_;
    const TExpirationTrackerPtr ExpirationTracker_;

    NHydra::TEntityMap<TCypressNode, TNodeMapTraits> NodeMap_;
    NHydra::TEntityMap<TLock> LockMap_;
    NHydra::TEntityMap<TCypressShard> ShardMap_;

    TEnumIndexedVector<NObjectClient::EObjectType, INodeTypeHandlerPtr> TypeToHandler_;

    TNodeId RootNodeId_;
    TMapNode* RootNode_ = nullptr;

    TCypressShardId RootShardId_;
    TCypressShard* RootShard_ = nullptr;

    // COMPAT(babenko)
    bool NeedBindNodesToRootShard_ = false;
    // COMPAT(babenko)
    bool NeedBindNodesToAncestorShard_ = false;
    // COMPAT(babenko)
    bool NeedSuggestShardNames_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveKeys(context);
        LockMap_.SaveKeys(context);
        ShardMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveValues(context);
        LockMap_.SaveValues(context);
        ShardMap_.SaveValues(context);
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NodeMap_.LoadKeys(context);
        LockMap_.LoadKeys(context);
        // COMPAT(babenko)
        if (context.GetVersion() >= EMasterReign::CypressShards) {
            ShardMap_.LoadKeys(context);
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NodeMap_.LoadValues(context);
        LockMap_.LoadValues(context);
        // COMPAT(babenko)
        if (context.GetVersion() >= EMasterReign::CypressShards) {
            ShardMap_.LoadValues(context);
        }

        // COMPAT(babenko)
        NeedBindNodesToRootShard_ = context.GetVersion() < EMasterReign::CypressShards;
        // COMPAT(babenko)
        NeedBindNodesToAncestorShard_ = context.GetVersion() < EMasterReign::FixSetShardInClone;
        // COMPAT(babenko)
        NeedSuggestShardNames_ = context.GetVersion() < EMasterReign::CypressShardName;
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        ExpirationTracker_->Clear();

        NodeMap_.Clear();
        LockMap_.Clear();
        ShardMap_.Clear();
        SharedTableSchemaRegistry_->Clear();

        RootNode_ = nullptr;
        RootShard_ = nullptr;
    }

    virtual void SetZeroState() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::SetZeroState();

        InitBuiltins();
    }

    virtual void OnBeforeSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnBeforeSnapshotLoaded();

        NeedBindNodesToRootShard_ = false;
        NeedBindNodesToAncestorShard_ = false;
        NeedSuggestShardNames_ = false;
    }

    virtual void OnAfterSnapshotLoaded() override
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
                auto* transaction = lock->GetTransaction();
                auto* lockingState = lock->GetTrunkNode()->MutableLockingState();
                switch (lock->Request().Mode) {
                    case ELockMode::Snapshot:
                        lock->SetTransactionToSnapshotLocksIterator(lockingState->TransactionToSnapshotLocks.emplace(
                            transaction,
                            lock));
                        break;

                    case ELockMode::Shared:
                        lock->SetTransactionAndKeyToSharedLocksIterator(lockingState->TransactionAndKeyToSharedLocks.emplace(
                            std::make_pair(transaction, lock->Request().Key),
                            lock));
                        if (lock->Request().Key.Kind != ELockKeyKind::None) {
                            lock->SetKeyToSharedLocksIterator(lockingState->KeyToSharedLocks.emplace(
                                lock->Request().Key,
                                lock));
                        }
                        break;

                    case ELockMode::Exclusive:
                        lock->SetTransactionToExclusiveLocksIterator(lockingState->TransactionToExclusiveLocks.emplace(
                            transaction,
                            lock));
                        break;

                    default:
                        YT_ABORT();
                }
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
        }
        YT_LOG_INFO("Finished initializing nodes");

        InitBuiltins();

        // COMPAT(babenko)
        if (NeedBindNodesToRootShard_) {
            for (auto [nodeId, node] : NodeMap_) {
                if (node->IsTrunk() && node->IsNative() && !node->GetShard()) {
                    SetShard(node, RootShard_);
                }
            }
        }

        // COMPAT(babenko)
        if (NeedBindNodesToAncestorShard_) {
            for (auto [nodeId, node] : NodeMap_) {
                if (!node->IsTrunk()) {
                    continue;
                }

                if (node->GetShard()) {
                    continue;
                }

                auto* ancestorNode = node;
                while (ancestorNode->GetParent() && !ancestorNode->GetShard()) {
                    ancestorNode = ancestorNode->GetParent();
                }

                if (auto* shard = ancestorNode->GetShard()) {
                    SetShard(node, shard);
                }
            }
        }
    }


    void InitBuiltins()
    {
        if (auto* untypedRootNode = FindNode(TVersionedNodeId(RootNodeId_))) {
            // Root node already exists.
            RootNode_ = untypedRootNode->As<TMapNode>();
        } else {
            // Create the root node.
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto rootNodeHolder = std::make_unique<TMapNode>(TVersionedNodeId(RootNodeId_));
            rootNodeHolder->SetTrunkNode(rootNodeHolder.get());
            rootNodeHolder->SetAccount(securityManager->GetSysAccount());
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
            auto rootShardHolder = std::make_unique<TCypressShard>(RootShardId_);
            rootShardHolder->SetRoot(RootNode_);

            RootShard_ = rootShardHolder.get();
            RootShard_->SetName(SuggestCypressShardName(RootShard_));
            ShardMap_.Insert(RootShardId_, std::move(rootShardHolder));

            SetShard(RootNode_, RootShard_);
        }

        // COMPAT(babenko)
        if (NeedSuggestShardNames_) {
            for (auto [shardId, shard] : ShardMap_) {
                shard->SetName(SuggestCypressShardName(shard));
            }
        }
    }


    virtual void OnRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnRecoveryComplete();

        AccessTracker_->Start();
    }

    virtual void OnLeaderRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderRecoveryComplete();

        ExpirationTracker_->Start();
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        ExpirationTracker_->Stop();
        OnStopEpoch();
    }

    virtual void OnStopFollowing() override
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

        const auto& nodeId = trunkNodeHolder->GetId();
        auto* node = NodeMap_.Insert(TVersionedNodeId(nodeId), std::move(trunkNodeHolder));

        const auto* mutationContext = GetCurrentMutationContext();
        node->SetCreationTime(mutationContext->GetTimestamp());
        node->SetModificationTime(mutationContext->GetTimestamp());
        node->SetAccessTime(mutationContext->GetTimestamp());
        node->SetAttributesRevision(mutationContext->GetVersion().ToRevision());
        node->SetContentRevision(mutationContext->GetVersion().ToRevision());

        if (node->IsExternal()) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "External node registered (NodeId: %v, Type: %v, ExternalCellTag: %v)",
                node->GetId(),
                node->GetType(),
                node->GetExternalCellTag());
        } else {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "%v node registered (NodeId: %v, Type: %v)",
                node->IsForeign() ? "Foreign" : "Local",
                node->GetId(),
                node->GetType());
        }

        return node;
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
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Lock orphaned due to node removal (LockId: %v, NodeId: %v)",
                lock->GetId(),
                trunkNode->GetId());
            lock->SetTrunkNode(nullptr);
            auto* transaction = lock->GetTransaction();
            YT_VERIFY(transaction->Locks().erase(lock) == 1);
            lock->SetTransaction(nullptr);
            objectManager->UnrefObject(lock);
        }

        trunkNode->ResetLockingState();

        ExpirationTracker_->OnNodeDestroyed(trunkNode);

        const auto& handler = GetHandler(trunkNode);
        YT_ASSERT(!trunkNode->GetTransaction());
        handler->Destroy(trunkNode);

        // Remove the object from the map but keep it alive.
        NodeMap_.Release(trunkNode->GetVersionedId()).release();
    }


    void OnTransactionCommitted(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        MergeNodes(transaction);
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

        std::sort(nodes.begin(), nodes.end(), TCypressNodeRefComparer::Compare);

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
            std::sort(nodes.begin(), nodes.end(), TCypressNodeRefComparer::Compare);

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
        const auto& transactionToSnapshotLocks = lockingState.TransactionToSnapshotLocks;
        const auto& transactionAndKeyToSharedLocks = lockingState.TransactionAndKeyToSharedLocks;
        const auto& keyToSharedLocks = lockingState.KeyToSharedLocks;
        const auto& transactionToExclusiveLocks = lockingState.TransactionToExclusiveLocks;

        // Handle snapshot locks.
        if (transaction && transactionToSnapshotLocks.find(transaction) != transactionToSnapshotLocks.end()) {
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
                if (transactionToExclusiveLocks.find(transaction) != transactionToExclusiveLocks.end()) {
                    return TError(
                        NCypressClient::EErrorCode::SameTransactionLockConflict,
                        "Cannot take %Qlv lock for node %v since %Qlv lock is already taken by same transaction %v",
                        request.Mode,
                        GetNodePath(trunkNode, transaction),
                        ELockMode::Exclusive,
                        transaction->GetId());
                }
                for (const auto& pair : transactionAndKeyToSharedLocks) {
                    const auto& lockTransaction = pair.first.first;
                    if (transaction == lockTransaction) {
                        return TError(
                            NCypressClient::EErrorCode::SameTransactionLockConflict,
                            "Cannot take %Qlv lock for node %v since %Qlv lock is already taken by same transaction %v",
                            request.Mode,
                            GetNodePath(trunkNode, transaction),
                            ELockMode::Shared,
                            transaction->GetId());
                    }
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

        // Check if any of parent transactions has taken a snapshot lock.
        if (transaction) {
            auto* currentTransaction = transaction->GetParent();
            while (currentTransaction) {
                if (transactionToSnapshotLocks.find(currentTransaction) != transactionToSnapshotLocks.end()) {
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

        for (const auto& pair : transactionToExclusiveLocks) {
            const auto* existingLock = pair.second;
            auto error = checkExistingLock(existingLock);
            if (!error.IsOK()) {
                return error;
            }
        }

        switch (request.Mode) {
            case ELockMode::Exclusive:
                for (const auto& pair : transactionAndKeyToSharedLocks) {
                    auto error = checkExistingLock(pair.second);
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
                auto range = lockingState.TransactionAndKeyToSharedLocks.equal_range(std::make_pair(transaction, request.Key));
                for (auto it = range.first; it != range.second; ++it) {
                    const auto* existingLock = it->second;
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Lock acquired (LockId: %v)",
            lock->GetId());

        YT_VERIFY(lock->GetState() == ELockState::Pending);
        lock->SetState(ELockState::Acquired);

        auto* lockingState = trunkNode->MutableLockingState();
        lockingState->PendingLocks.erase(lock->GetLockListIterator());
        lockingState->AcquiredLocks.push_back(lock);
        lock->SetLockListIterator(--lockingState->AcquiredLocks.end());

        switch (request.Mode) {
            case ELockMode::Exclusive:
                lock->SetTransactionToExclusiveLocksIterator(lockingState->TransactionToExclusiveLocks.emplace(
                    transaction,
                    lock));
                break;

            case ELockMode::Shared:
                lock->SetTransactionAndKeyToSharedLocksIterator(lockingState->TransactionAndKeyToSharedLocks.emplace(
                    std::make_pair(transaction, request.Key),
                    lock));
                if (request.Key.Kind != ELockKeyKind::None) {
                    lock->SetKeyToSharedLocksIterator(lockingState->KeyToSharedLocks.emplace(
                        request.Key,
                        lock));
                }
                break;

            case ELockMode::Snapshot:
                lock->SetTransactionToSnapshotLocksIterator(lockingState->TransactionToSnapshotLocks.emplace(
                    transaction,
                    lock));
                break;

            default:
                YT_ABORT();
        }

        if (transaction->LockedNodes().insert(trunkNode).second) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Node locked (NodeId: %v, TransactionId: %v)",
                trunkNode->GetId(),
                transaction->GetId());
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

    TLock* DoCreateLock(
        TCypressNode* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        bool implicit)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Lock, NullObjectId);
        auto lockHolder = std::make_unique<TLock>(id);
        auto* lock = LockMap_.Insert(id, std::move(lockHolder));

        lock->SetImplicit(implicit);
        lock->SetState(ELockState::Pending);
        lock->SetTrunkNode(trunkNode);
        lock->SetTransaction(transaction);
        lock->Request() = request;

        auto* lockingState = trunkNode->MutableLockingState();
        lockingState->PendingLocks.push_back(lock);
        lock->SetLockListIterator(--lockingState->PendingLocks.end());

        YT_VERIFY(transaction->Locks().insert(lock).second);
        objectManager->RefObject(lock);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Lock created (LockId: %v, Mode: %v, Key: %v, NodeId: %v, Implicit: %v)",
            id,
            request.Mode,
            request.Key,
            TVersionedNodeId(trunkNode->GetId(), transaction->GetId()),
            implicit);

        return lock;
    }

    void ReleaseLocks(TTransaction* transaction, bool promote)
    {
        auto* parentTransaction = transaction->GetParent();

        SmallVector<TLock*, 16> locks(transaction->Locks().begin(), transaction->Locks().end());
        transaction->Locks().clear();
        std::sort(locks.begin(), locks.end(), TObjectRefComparer::Compare);

        SmallVector<TCypressNode*, 16> lockedNodes(transaction->LockedNodes().begin(), transaction->LockedNodes().end());
        transaction->LockedNodes().clear();
        std::sort(lockedNodes.begin(), lockedNodes.end(), TCypressNodeRefComparer::Compare);

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
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Node unlocked (NodeId: %v, TransactionId: %v)",
                trunkNode->GetId(),
                transaction->GetId());
        }

        for (auto* trunkNode : lockedNodes) {
            CheckPendingLocks(trunkNode);
        }
    }

    void DoPromoteLock(TLock* lock)
    {
        auto* transaction = lock->GetTransaction();
        auto* parentTransaction = transaction->GetParent();
        YT_VERIFY(parentTransaction);
        auto* trunkNode = lock->GetTrunkNode();

        lock->SetTransaction(parentTransaction);
        if (trunkNode && lock->GetState() == ELockState::Acquired) {
            auto* lockingState = trunkNode->MutableLockingState();
            switch (lock->Request().Mode) {
                case ELockMode::Exclusive:
                    lockingState->TransactionToExclusiveLocks.erase(lock->GetTransactionToExclusiveLocksIterator());
                    lock->SetTransactionToExclusiveLocksIterator(lockingState->TransactionToExclusiveLocks.emplace(
                        parentTransaction,
                        lock));
                    break;

                case ELockMode::Shared:
                    lockingState->TransactionAndKeyToSharedLocks.erase(lock->GetTransactionAndKeyToSharedLocksIterator());
                    lock->SetTransactionAndKeyToSharedLocksIterator(lockingState->TransactionAndKeyToSharedLocks.emplace(
                        std::make_pair(parentTransaction, lock->Request().Key),
                        lock));
                    break;

                default:
                    YT_ABORT();
            }

            // NB: Node could be locked more than once.
            parentTransaction->LockedNodes().insert(trunkNode);
        }
        YT_VERIFY(parentTransaction->Locks().insert(lock).second);
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Lock promoted (LockId: %v, TransactionId: %v -> %v)",
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
        lock->SetTransaction(nullptr);
        transaction->Locks().erase(lock);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(lock);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Lock released (LockId: %v, TransactionId: %v)",
            lock->GetId(),
            transaction->GetId());
    }

    void CheckPendingLocks(TCypressNode* trunkNode)
    {
        // Ignore orphaned nodes.
        // Eventually the node will get destroyed and the lock will become
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
        auto externalizedTransactionId = transactionManager->ExternalizeTransaction(lock->GetTransaction(), externalCellTag);

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
        auto externalizedTransactionId = transactionManager->ExternalizeTransaction(transaction, externalCellTag);

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
                    const auto* mapNode = node->As<TMapNode>();
                    for (const auto& pair : mapNode->KeyToChild()) {
                        if (pair.second) {
                            children[pair.first] = pair.second;
                        } else {
                            // NB: erase may fail.
                            children.erase(pair.first);
                        }
                    }
                }

                for (const auto& pair : children) {
                    ListSubtreeNodes(pair.second, transaction, true, subtreeNodes);
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

    void MergeNode(
        TTransaction* transaction,
        TCypressNode* branchedNode)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        const auto& handler = GetHandler(branchedNode);

        auto* trunkNode = branchedNode->GetTrunkNode();
        auto branchedNodeId = branchedNode->GetVersionedId();

        if (branchedNode->GetLockMode() != ELockMode::Snapshot) {
            auto* originatingNode = branchedNode->GetOriginator();

            // Merge changes back.
            YT_ASSERT(branchedNode->GetTransaction() == transaction);
            handler->Merge(originatingNode, branchedNode);

            // The root needs a special handling.
            // When Cypress gets cleared, the root is created and is assigned zero creation time.
            // (We don't have any mutation context at hand to provide a synchronized timestamp.)
            // Later on, Cypress is initialized and filled with nodes.
            // At this point we set the root's creation time.
            if (trunkNode == RootNode_ && !transaction->GetParent()) {
                originatingNode->SetCreationTime(originatingNode->GetModificationTime());
            }
        } else {
            // Destroy the branched copy.
            YT_ASSERT(branchedNode->GetTransaction() == transaction);
            handler->Destroy(branchedNode);

            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Node snapshot destroyed (NodeId: %v)", branchedNodeId);
        }

        // Drop the implicit reference to the trunk.
        objectManager->UnrefObject(trunkNode);

        // Remove the branched copy.
        NodeMap_.Remove(branchedNodeId);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Branched node removed (NodeId: %v)", branchedNodeId);
    }

    void MergeNodes(TTransaction* transaction)
    {
        for (auto* node : transaction->BranchedNodes()) {
            MergeNode(transaction, node);
        }
        transaction->BranchedNodes().clear();
    }

    //! Unbranches all nodes branched by #transaction and updates their version trees.
    void RemoveBranchedNodes(TTransaction* transaction)
    {
        if (transaction->BranchedNodes().size() != transaction->LockedNodes().size()) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Transaction branched node count differs from its locked node count (TransactionId: %v, BranchedNodeCount: %v, LockedNodeCount: %v)",
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
        auto* account = factory->GetClonedNodeAccount(sourceNode->GetAccount());

        const auto& handler = GetHandler(sourceNode);
        auto* clonedTrunkNode = handler->Clone(
            sourceNode,
            factory,
            hintId,
            mode,
            account);

        // Set owner.
        if (factory->ShouldPreserveOwner()) {
            clonedTrunkNode->Acd().SetOwner(sourceNode->Acd().GetOwner());
        } else {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto* user = securityManager->GetAuthenticatedUser();
            clonedTrunkNode->Acd().SetOwner(user);
        }

        // Copy creation time.
        if (factory->ShouldPreserveCreationTime()) {
            clonedTrunkNode->SetCreationTime(sourceNode->GetTrunkNode()->GetCreationTime());
        }

        // Copy modification time.
        if (factory->ShouldPreserveModificationTime()) {
            clonedTrunkNode->SetModificationTime(sourceNode->GetTrunkNode()->GetModificationTime());
        }

        // Copy expiration time.
        auto expirationTime = sourceNode->TryGetExpirationTime();
        if (factory->ShouldPreserveExpirationTime() && expirationTime) {
            SetExpirationTime(clonedTrunkNode, *expirationTime);
        }

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
            if (!IsObjectAlive(node))
                continue;

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

    void HydraCreateForeignNode(NProto::TReqCreateForeignNode* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        auto nodeId = FromProto<TObjectId>(request->node_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto accountId = FromProto<TAccountId>(request->account_id());
        auto type = EObjectType(request->type());

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
                .ExternalCellTag = NotReplicatedCellTag,
                .Transaction = transaction,
                .InheritedAttributes = inheritedAttributes.get(),
                .ExplicitAttributes = explicitAttributes.get(),
                .Account = account
            });

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(trunkNode);

        handler->FillAttributes(trunkNode, inheritedAttributes.get(), explicitAttributes.get());

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
        auto mode = ENodeCloneMode(request->mode());
        auto accountId = FromProto<TAccountId>(request->account_id());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* sourceTransaction = sourceTransactionId
            ? transactionManager->GetTransactionOrThrow(sourceTransactionId)
            : nullptr;
        auto* clonedTransaction = clonedTransactionId
            ? transactionManager->GetTransactionOrThrow(clonedTransactionId)
            : nullptr;

        auto* sourceTrunkNode = FindNode(TVersionedObjectId(sourceNodeId));
        if (!IsObjectAlive(sourceTrunkNode)) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Attempted to clone non-existing foreign node (SourceNodeId: %v, ClonedNodeId: %v, SourceTransactionId: %v, ClonedTransactionId: %v, Mode: %v)",
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
            TNodeFactoryOptions());

        auto* clonedTrunkNode = DoCloneNode(
            sourceNode,
            factory.get(),
            clonedNodeId,
            mode);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(clonedTrunkNode);

        LockNode(clonedTrunkNode, clonedTransaction, ELockMode::Exclusive);

        factory->Commit();

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Foreign node cloned (SourceNodeId: %v, ClonedNodeId: %v, Account: %v)",
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

            if (IsOrphaned(trunkNode)) {
                continue;
            }

            const auto& cypressManager = Bootstrap_->GetCypressManager();
            try {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Removing expired node (NodeId: %v, Path: %v)",
                    nodeId,
                    cypressManager->GetNodePath(trunkNode, nullptr));
                auto nodeProxy = GetNodeProxy(trunkNode, nullptr);
                auto parentProxy = nodeProxy->GetParent();
                parentProxy->RemoveChild(nodeProxy);
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), ex, "Cannot remove an expired node; backing off and retrying (NodeId: %v, Path: %v)",
                    nodeId,
                    cypressManager->GetNodePath(trunkNode, nullptr));
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
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Lock transaction is missing (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        auto* trunkNode = FindNode(TVersionedObjectId(nodeId));
        if (!IsObjectAlive(trunkNode)) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Lock node is missing (NodeId: %v, TransactionId: %v)",
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
            YT_LOG_ALERT_UNLESS(IsRecovery(), error, "Cannot lock foreign node (NodeId: %v, TransactionId: %v, Mode: %v, Key: %v)",
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
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Unlock transaction is missing (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        auto* trunkNode = FindNode(TVersionedObjectId(nodeId));
        if (!IsObjectAlive(trunkNode)) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Unlock node is missing (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        auto error = CheckUnlock(trunkNode, transaction, false, explicitOnly);
        if (!error.IsOK()) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), error, "Cannot unlock foreign node (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        if (IsUnlockRedundant(trunkNode, transaction, explicitOnly)) {
            return;
        }

        DoUnlockNode(trunkNode, transaction, explicitOnly);
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager::TImpl, Node, TCypressNode, NodeMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager::TImpl, Lock, TLock, LockMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager::TImpl, Shard, TCypressShard, ShardMap_)

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TImpl::TNodeMapTraits::TNodeMapTraits(TImpl* owner)
    : Owner_(owner)
{ }

std::unique_ptr<TCypressNode> TCypressManager::TImpl::TNodeMapTraits::Create(const TVersionedNodeId& id) const
{
    auto type = TypeFromId(id.ObjectId);
    const auto& handler = Owner_->GetHandler(type);
    // This cell tag is fake and will be overwritten on load
    // (unless this is a pre-multicell snapshot, in which case NotReplicatedCellTag is just what we want).
    return handler->Instantiate(id, NotReplicatedCellTag);
}

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TNodeTypeHandler::TNodeTypeHandler(
    TImpl* owner,
    INodeTypeHandlerPtr underlyingHandler)
    : TObjectTypeHandlerBase(owner->Bootstrap_)
    , Owner_(owner)
    , UnderlyingHandler_(underlyingHandler)
{ }

void TCypressManager::TNodeTypeHandler::DoDestroyObject(TCypressNode* node) noexcept
{
    Owner_->DestroyNode(node);
}

TString TCypressManager::TNodeTypeHandler::DoGetName(const TCypressNode* node)
{
    auto path = Owner_->GetNodePath(node->GetTrunkNode(), node->GetTransaction());
    return Format("node %v", path);
}

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TLockTypeHandler::TLockTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->LockMap_)
{ }

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TCypressManager(TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TCypressManager::~TCypressManager()
{ }

void TCypressManager::Initialize()
{
    Impl_->Initialize();
}

void TCypressManager::RegisterHandler(INodeTypeHandlerPtr handler)
{
    Impl_->RegisterHandler(std::move(handler));
}

const INodeTypeHandlerPtr& TCypressManager::FindHandler(EObjectType type)
{
    return Impl_->FindHandler(type);
}

const INodeTypeHandlerPtr& TCypressManager::GetHandler(EObjectType type)
{
    return Impl_->GetHandler(type);
}

const INodeTypeHandlerPtr& TCypressManager::GetHandler(const TCypressNode* node)
{
    return Impl_->GetHandler(node);
}

TCypressShard* TCypressManager::CreateShard(TCypressShardId shardId)
{
    return Impl_->CreateShard(shardId);
}

void TCypressManager::SetShard(TCypressNode* node, TCypressShard* shard)
{
    Impl_->SetShard(node, shard);
}

void TCypressManager::ResetShard(TCypressNode* node)
{
    Impl_->ResetShard(node);
}

void TCypressManager::UpdateShardNodeCount(
    TCypressShard* shard,
    TAccount* account,
    int delta)
{
    Impl_->UpdateShardNodeCount(shard, account, delta);
}

std::unique_ptr<ICypressNodeFactory> TCypressManager::CreateNodeFactory(
    TCypressShard* shard,
    TTransaction* transaction,
    TAccount* account,
    const TNodeFactoryOptions& options)
{
    return Impl_->CreateNodeFactory(
        shard,
        transaction,
        account,
        options);
}

TCypressNode* TCypressManager::CreateNode(
    const INodeTypeHandlerPtr& handler,
    TNodeId hintId,
    const TCreateNodeContext& context)
{
    return Impl_->CreateNode(
        handler,
        hintId,
        context);
}

TCypressNode* TCypressManager::InstantiateNode(
    TNodeId id,
    TCellTag externalCellTag)
{
    return Impl_->InstantiateNode(id, externalCellTag);
}

TCypressNode* TCypressManager::CloneNode(
    TCypressNode* sourceNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode)
{
    return Impl_->CloneNode(sourceNode, factory, mode);
}

TCypressNode* TCypressManager::EndCopyNode(
    TEndCopyContext* context,
    ICypressNodeFactory* factory,
    TNodeId sourceNodeId)
{
    return Impl_->EndCopyNode(context, factory, sourceNodeId);
}

void TCypressManager::EndCopyNodeInplace(
    TCypressNode* trunkNode,
    TEndCopyContext* context,
    ICypressNodeFactory* factory,
    TNodeId sourceNodeId)
{
    return Impl_->EndCopyNodeInplace(trunkNode, context, factory, sourceNodeId);
}

TMapNode* TCypressManager::GetRootNode() const
{
    return Impl_->GetRootNode();
}

TCypressNode* TCypressManager::GetNodeOrThrow(const TVersionedNodeId& id)
{
    return Impl_->GetNodeOrThrow(id);
}

TYPath TCypressManager::GetNodePath(TCypressNode* trunkNode, TTransaction* transaction)
{
    return Impl_->GetNodePath(trunkNode, transaction);
}

TYPath TCypressManager::GetNodePath(const ICypressNodeProxy* nodeProxy)
{
    return Impl_->GetNodePath(nodeProxy);
}

TCypressNode* TCypressManager::ResolvePathToTrunkNode(const TYPath& path, TTransaction* transaction)
{
    return Impl_->ResolvePathToTrunkNode(path, transaction);
}

ICypressNodeProxyPtr TCypressManager::ResolvePathToNodeProxy(const TYPath& path, TTransaction* transaction)
{
    return Impl_->ResolvePathToNodeProxy(path, transaction);
}

TCypressNode* TCypressManager::FindNode(
    TCypressNode* trunkNode,
    TTransaction* transaction)
{
    return Impl_->FindNode(trunkNode, transaction);
}

TCypressNode* TCypressManager::GetVersionedNode(
    TCypressNode* trunkNode,
    TTransaction* transaction)
{
    return Impl_->GetVersionedNode(trunkNode, transaction);
}

ICypressNodeProxyPtr TCypressManager::GetNodeProxy(
    TCypressNode* trunkNode,
    TTransaction* transaction)
{
    return Impl_->GetNodeProxy(trunkNode, transaction);
}

TCypressNode* TCypressManager::LockNode(
    TCypressNode* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request,
    bool recursive,
    bool dontLockForeign)
{
    return Impl_->LockNode(trunkNode, transaction, request, recursive, dontLockForeign);
}

TLock* TCypressManager::CreateLock(
    TCypressNode* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request,
    bool waitable)
{
    return Impl_->CreateLock(trunkNode, transaction, request, waitable);
}

void TCypressManager::UnlockNode(
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction)
{
    return Impl_->UnlockNode(trunkNode, transaction, false /*recursive*/, true /*explicitOnly*/);
}

void TCypressManager::SetModified(
    TCypressNode* node,
    EModificationType modificationType)
{
    Impl_->SetModified(node, modificationType);
}

void TCypressManager::SetAccessed(TCypressNode* trunkNode)
{
    Impl_->SetAccessed(trunkNode);
}

void TCypressManager::SetExpirationTime(TCypressNode* trunkNode, std::optional<TInstant> time)
{
    Impl_->SetExpirationTime(trunkNode, time);
}

TCypressManager::TSubtreeNodes TCypressManager::ListSubtreeNodes(
    TCypressNode* trunkNode,
    TTransaction* transaction,
    bool includeRoot)
{
    return Impl_->ListSubtreeNodes(trunkNode, transaction, includeRoot);
}

void TCypressManager::AbortSubtreeTransactions(
    TCypressNode* trunkNode,
    TTransaction* transaction)
{
    Impl_->AbortSubtreeTransactions(trunkNode, transaction);
}

void TCypressManager::AbortSubtreeTransactions(INodePtr node)
{
    Impl_->AbortSubtreeTransactions(std::move(node));
}

bool TCypressManager::IsOrphaned(TCypressNode* trunkNode)
{
    return Impl_->IsOrphaned(trunkNode);
}

TCypressNodeList TCypressManager::GetNodeOriginators(
    TTransaction* transaction,
    TCypressNode* trunkNode)
{
    return Impl_->GetNodeOriginators(transaction, trunkNode);
}

TCypressNodeList TCypressManager::GetNodeReverseOriginators(
    TTransaction* transaction,
    TCypressNode* trunkNode)
{
    return Impl_->GetNodeReverseOriginators(transaction, trunkNode);
}

const NTableServer::TSharedTableSchemaRegistryPtr& TCypressManager::GetSharedTableSchemaRegistry() const
{
    return Impl_->SharedTableSchemaRegistry();
}

const TResolveCachePtr& TCypressManager::GetResolveCache()
{
    return Impl_->ResolveCache();
}

DELEGATE_ENTITY_MAP_ACCESSORS(TCypressManager, Node, TCypressNode, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TCypressManager, Lock, TLock, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TCypressManager, Shard, TCypressShard, *Impl_)

DELEGATE_SIGNAL(TCypressManager, void(TCypressNode*), NodeCreated, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
