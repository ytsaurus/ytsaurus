#include "cypress_manager.h"
#include "private.h"
#include "access_tracker.h"
#include "expiration_tracker.h"
#include "config.h"
#include "lock_proxy.h"
#include "node_detail.h"
#include "node_proxy_detail.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config_manager.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>

#include <yt/server/object_server/object_detail.h>
#include <yt/server/object_server/type_handler_detail.h>

#include <yt/server/security_server/account.h>
#include <yt/server/security_server/group.h>
#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/user.h>

#include <yt/server/table_server/shared_table_schema.h>

#include <yt/ytlib/cypress_client/cypress_ypath.pb.h>
#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/client/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/misc/singleton.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/ypath_detail.h>

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
        TCypressManagerConfigPtr config,
        TTransaction* transaction,
        TAccount* account,
        const TNodeFactoryOptions& options)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , Transaction_(transaction)
        , Account_(account)
        , Options_(options)
    {
        YCHECK(Bootstrap_);
        YCHECK(Account_);
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

        ReleaseCreatedNodes();
    }

    virtual void Rollback() noexcept override
    {
        TTransactionalNodeFactoryBase::Rollback();

        ReleaseCreatedNodes();
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

    virtual bool ShouldPreserveExpirationTime() const override
    {
        return Options_.PreserveExpirationTime;
    }

    virtual bool ShouldPreserveCreationTime() const override
    {
        return Options_.PreserveCreationTime;
    }

    virtual TAccount* GetNewNodeAccount() const override
    {
        return Account_;
    }

    virtual TAccount* GetClonedNodeAccount(TCypressNodeBase* sourceNode) const override
    {
        return Options_.PreserveAccount ? sourceNode->GetAccount() : Account_;
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

        ValidateCreatedNodeTypePermission(type);

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
        auto optionalAccount = explicitAttributes->FindAndRemove<TString>("account");
        if (optionalAccount) {
            account = securityManager->GetAccountByNameOrThrow(*optionalAccount);
        }
        securityManager->ValidatePermission(account, EPermission::Use);
        securityManager->ValidateResourceUsageIncrease(account, TClusterResources().SetNodeCount(1));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        bool isExternalDefault =
            Bootstrap_->IsPrimaryMaster() &&
            !multicellManager->GetRegisteredMasterCellTags().empty() &&
            handler->IsExternalizable();
        bool isExternal = explicitAttributes->GetAndRemove<bool>("external", isExternalDefault);

        double externalCellBias = explicitAttributes->GetAndRemove<double>("external_cell_bias", 1.0);
        if (externalCellBias < 0.0 || externalCellBias > 1.0) {
            THROW_ERROR_EXCEPTION("\"external_cell_bias\" must be in range [0, 1]");
        }

        auto cellTag = NotReplicatedCellTag;
        if (isExternal) {
            if (!Bootstrap_->IsPrimaryMaster()) {
                THROW_ERROR_EXCEPTION("External nodes are only created at primary masters");
            }

            if (!handler->IsExternalizable()) {
                THROW_ERROR_EXCEPTION("Type %Qlv is not externalizable",
                    handler->GetObjectType());
            }

            auto optionalExternalCellTag = explicitAttributes->FindAndRemove<TCellTag>("external_cell_tag");
            if (optionalExternalCellTag) {
                cellTag = *optionalExternalCellTag;
                if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
                    THROW_ERROR_EXCEPTION("Unknown cell tag %v", cellTag);
                }
            } else {
                cellTag = multicellManager->PickSecondaryMasterCell(externalCellBias);
                if (cellTag == InvalidCellTag) {
                    THROW_ERROR_EXCEPTION("No secondary masters registered");
                }
            }
        }

        // INodeTypeHandler::Create and ::FillAttributes may modify the attributes.
        std::unique_ptr<IAttributeDictionary> replicationExplicitAttributes;
        std::unique_ptr<IAttributeDictionary> replicationInheritedAttributes;
        if (isExternal) {
            replicationExplicitAttributes = explicitAttributes->Clone();
            replicationInheritedAttributes = inheritedAttributes->Clone();
        }

        auto* trunkNode = cypressManager->CreateNode(
            NullObjectId,
            cellTag,
            handler,
            account,
            Transaction_,
            inheritedAttributes,
            explicitAttributes);

        RegisterCreatedNode(trunkNode);

        handler->FillAttributes(trunkNode, inheritedAttributes, explicitAttributes);

        cypressManager->LockNode(
            trunkNode,
            Transaction_,
            ELockMode::Exclusive,
            false,
            true);

        if (isExternal) {
            NProto::TReqCreateForeignNode replicationRequest;
            ToProto(replicationRequest.mutable_node_id(), trunkNode->GetId());
            if (Transaction_) {
                ToProto(replicationRequest.mutable_transaction_id(), Transaction_->GetId());
            }
            replicationRequest.set_type(static_cast<int>(type));
            ToProto(replicationRequest.mutable_explicit_node_attributes(), *replicationExplicitAttributes);
            ToProto(replicationRequest.mutable_inherited_node_attributes(), *replicationInheritedAttributes);
            ToProto(replicationRequest.mutable_account_id(), account->GetId());
            multicellManager->PostToMaster(replicationRequest, cellTag);
        }

        return cypressManager->GetNodeProxy(trunkNode, Transaction_);
    }

    virtual TCypressNodeBase* InstantiateNode(
        TNodeId id,
        TCellTag externalCellTag) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->InstantiateNode(id, externalCellTag);

        RegisterCreatedNode(node);

        return node;
    }

    virtual TCypressNodeBase* CloneNode(
        TCypressNodeBase* sourceNode,
        ENodeCloneMode mode) override
    {
        ValidateCreatedNodeTypePermission(sourceNode->GetType());

        auto* clonedAccount = GetClonedNodeAccount(sourceNode);
        // Resource limit check must be suppressed when moving nodes
        // without altering the account.
        if (mode != ENodeCloneMode::Move || clonedAccount != sourceNode->GetAccount()) {
            // NB: Ignore disk space increase since in multicell mode the primary cell
            // might not be aware of the actual resource usage.
            // This should be safe since chunk lists are shared anyway.
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidateResourceUsageIncrease(clonedAccount, TClusterResources().SetNodeCount(1));
        }

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
        if (sourceNode->IsExternal()) {
            NProto::TReqCloneForeignNode protoRequest;
            ToProto(protoRequest.mutable_source_node_id(), sourceNode->GetId());
            if (sourceNode->GetTransaction()) {
                ToProto(protoRequest.mutable_source_transaction_id(), sourceNode->GetTransaction()->GetId());
            }
            ToProto(protoRequest.mutable_cloned_node_id(), clonedNode->GetId());
            if (clonedNode->GetTransaction()) {
                ToProto(protoRequest.mutable_cloned_transaction_id(), clonedNode->GetTransaction()->GetId());
            }
            protoRequest.set_mode(static_cast<int>(mode));
            ToProto(protoRequest.mutable_account_id(), clonedNode->GetAccount()->GetId());

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(protoRequest, sourceNode->GetExternalCellTag());
        }

        return clonedTrunkNode;
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TCypressManagerConfigPtr Config_;
    TTransaction* const Transaction_;
    TAccount* const Account_;
    const TNodeFactoryOptions Options_;

    std::vector<TCypressNodeBase*> CreatedNodes_;


    void ValidateCreatedNodeTypePermission(EObjectType type)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* schema = objectManager->GetSchema(type);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(schema, EPermission::Create);
    }

    void RegisterCreatedNode(TCypressNodeBase* trunkNode)
    {
        Y_ASSERT(trunkNode->IsTrunk());
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(trunkNode);
        CreatedNodes_.push_back(trunkNode);
    }

    void ReleaseCreatedNodes()
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* node : CreatedNodes_) {
            objectManager->UnrefObject(node);
        }
        CreatedNodes_.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TNodeTypeHandler
    : public TObjectTypeHandlerBase<TCypressNodeBase>
{
public:
    TNodeTypeHandler(
        TImpl* owner,
        INodeTypeHandlerPtr underlyingHandler);

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::Creatable;
    }

    virtual EObjectType GetType() const override
    {
        return UnderlyingHandler_->GetObjectType();
    }

    virtual TObjectBase* FindObject(TObjectId id) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->FindNode(TVersionedNodeId(id));
    }

    virtual TObjectBase* CreateObject(
        TObjectId /*hintId*/,
        IAttributeDictionary* /*attributes*/) override
    {
        THROW_ERROR_EXCEPTION("Cypress nodes cannot be created via this call");
    }

    virtual void DestroyObject(TObjectBase* object) noexcept;

private:
    TImpl* const Owner_;
    const INodeTypeHandlerPtr UnderlyingHandler_;


    virtual TCellTagList DoGetReplicationCellTags(const TCypressNodeBase* node) override
    {
        auto externalCellTag = node->GetExternalCellTag();
        return externalCellTag == NotReplicatedCellTag ? TCellTagList() : TCellTagList{externalCellTag};
    }

    virtual TString DoGetName(const TCypressNodeBase* node);

    virtual IObjectProxyPtr DoGetProxy(
        TCypressNodeBase* node,
        TTransaction* transaction) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->GetNodeProxy(node, transaction);
    }

    virtual TAccessControlDescriptor* DoFindAcd(TCypressNodeBase* node) override
    {
        return &node->GetTrunkNode()->Acd();
    }

    virtual TObjectBase* DoGetParent(TCypressNodeBase* node) override
    {
        return node->GetParent();
    }
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
    virtual TString DoGetName(const TLock* lock) override
    {
        return Format("lock %v", lock->GetId());
    }

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
    TImpl(
        TCypressManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::CypressManager)
        , SharedTableSchemaRegistry_(New<NTableServer::TSharedTableSchemaRegistry>())
        , Config_(config)
        , AccessTracker_(New<TAccessTracker>(config, bootstrap))
        , ExpirationTracker_(New<TExpirationTracker>(config, bootstrap))
        , NodeMap_(TNodeMapTraits(this))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);

        RootNodeId_ = MakeWellKnownId(EObjectType::MapNode, Bootstrap_->GetCellTag());

        RegisterHandler(New<TStringNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TInt64NodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TUint64NodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TDoubleNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TBooleanNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TMapNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TListNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TLinkNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TDocumentNodeTypeHandler>(Bootstrap_));

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
    }


    void RegisterHandler(INodeTypeHandlerPtr handler)
    {
        // No thread affinity is given here.
        // This will be called during init-time only.
        YCHECK(handler);

        auto type = handler->GetObjectType();
        YCHECK(!TypeToHandler_[type]);
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
        YCHECK(handler);
        return handler;
    }

    const INodeTypeHandlerPtr& GetHandler(const TCypressNodeBase* node)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetHandler(node->GetType());
    }


    std::unique_ptr<ICypressNodeFactory> CreateNodeFactory(
        TTransaction* transaction,
        TAccount* account,
        const TNodeFactoryOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return std::unique_ptr<ICypressNodeFactory>(new TNodeFactory(
            Bootstrap_,
            Config_,
            transaction,
            account,
            options));
    }

    TCypressNodeBase* CreateNode(
        TNodeId hintId,
        TCellTag externalCellTag,
        INodeTypeHandlerPtr handler,
        TAccount* account,
        TTransaction* transaction,
        IAttributeDictionary* inheritedAttributes,
        IAttributeDictionary* explicitAttributes)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(handler);
        YCHECK(account);
        YCHECK(inheritedAttributes);
        YCHECK(explicitAttributes);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        securityManager->ValidatePermission(account, user, NSecurityServer::EPermission::Use);

        auto nodeHolder = handler->Create(
            hintId,
            externalCellTag,
            transaction,
            inheritedAttributes,
            explicitAttributes,
            account);
        auto* node = RegisterNode(std::move(nodeHolder));

        // Set owner.
        auto* acd = securityManager->GetAcd(node);
        acd->SetOwner(user);

        NodeCreated_.Fire(node);

        return node;
    }


    TCypressNodeBase* InstantiateNode(
        TNodeId id,
        TCellTag externalCellTag)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto type = TypeFromId(id);
        const auto& handler = GetHandler(type);
        auto nodeHolder = handler->Instantiate(TVersionedNodeId(id), externalCellTag);
        return RegisterNode(std::move(nodeHolder));
    }

    TCypressNodeBase* CloneNode(
        TCypressNodeBase* sourceNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(sourceNode);
        YCHECK(factory);

        // Validate account access _before_ creating the actual copy.
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* clonedAccount = factory->GetClonedNodeAccount(sourceNode);
        securityManager->ValidatePermission(clonedAccount, EPermission::Use);

        return DoCloneNode(
            sourceNode,
            factory,
            NullObjectId,
            mode);
    }


    TMapNode* GetRootNode() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return RootNode_;
    }

    TCypressNodeBase* GetNodeOrThrow(const TVersionedNodeId& id)
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

    TYPath GetNodePath(TCypressNodeBase* trunkNode, TTransaction* transaction)
    {
        auto fallbackToId = [&] {
            return FromObjectId(trunkNode->GetId());
        };

        using TToken = TVariant<TStringBuf, int>;
        SmallVector<TToken, 32> tokens;

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
                    Y_UNREACHABLE();
            }
            currentNode = currentParentNode;
        }

        if (currentNode->GetTrunkNode() != RootNode_) {
            return fallbackToId();
        }

        TStringBuilder builder;
        builder.AppendChar('/');
        for (auto it = tokens.rbegin(); it != tokens.rend(); ++it) {
            auto token = *it;
            builder.AppendChar('/');
            switch (token.Tag()) {
                case TToken::TagOf<TStringBuf>():
                    builder.AppendString(token.As<TStringBuf>());
                    break;
                case TToken::TagOf<int>():
                    builder.AppendFormat("%v", token.As<int>());
                    break;
                default:
                    Y_UNREACHABLE();
            }
        }

        return builder.Flush();
    }

    TYPath GetNodePath(const ICypressNodeProxy* nodeProxy)
    {
        return GetNodePath(nodeProxy->GetTrunkNode(), nodeProxy->GetTransaction());
    }

    TCypressNodeBase* ResolvePathToTrunkNode(const TYPath& path, TTransaction* transaction)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* object = objectManager->ResolvePathToObject(path, transaction);
        if (!IsVersionedType(object->GetType())) {
            THROW_ERROR_EXCEPTION("Path %v points to a nonversioned %Qlv object instead of a node",
                path,
                object->GetType());
        }
        return object->As<TCypressNodeBase>();
    }

    ICypressNodeProxyPtr ResolvePathToNodeProxy(const TYPath& path, TTransaction* transaction)
    {
        auto* trunkNode = ResolvePathToTrunkNode(path, transaction);
        return GetNodeProxy(trunkNode, transaction);
    }


    TCypressNodeBase* FindNode(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        // Fast path -- no transaction.
        if (!transaction) {
            return trunkNode;
        }

        TVersionedNodeId versionedId(trunkNode->GetId(), GetObjectId(transaction));
        return FindNode(versionedId);
    }

    TCypressNodeBase* GetVersionedNode(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

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
        TCypressNodeBase* trunkNode,
        TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        const auto& handler = GetHandler(trunkNode);
        return handler->GetProxy(trunkNode, transaction);
    }


    TCypressNodeBase* LockNode(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        bool recursive = false,
        bool dontLockForeign = false)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());
        YCHECK(request.Mode != ELockMode::None && request.Mode != ELockMode::Snapshot);
        YCHECK(!recursive || request.Key.Kind == ELockKeyKind::None);

        auto error = CheckLock(
            trunkNode,
            transaction,
            request,
            recursive);
        error.ThrowOnError();

        if (IsLockRedundant(trunkNode, transaction, request)) {
            return GetVersionedNode(trunkNode, transaction);
        }

        TCypressNodeBase* lockedNode = nullptr;
        ForEachSubtreeNode(trunkNode, transaction, recursive, [&] (TCypressNodeBase* child) {
            auto* lock = DoCreateLock(child, transaction, request, true);
            auto* lockedChild = DoAcquireLock(lock, dontLockForeign);
            if (child == trunkNode) {
                lockedNode = lockedChild;
            }
        });

        YCHECK(lockedNode);
        return lockedNode;
    }

    void UnlockNode(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        bool recursive,
        bool explicitOnly)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(transaction->IsTrunk());

        auto error = CheckUnlock(trunkNode, transaction, recursive, explicitOnly);
        error.ThrowOnError();

        ForEachSubtreeNode(trunkNode, transaction, recursive, [&] (TCypressNodeBase* trunkChild) {
            if (IsUnlockRedundant(trunkChild, transaction, explicitOnly)) {
                return;
            }

            DoUnlockNode(trunkChild, transaction, explicitOnly);
        });
    }

    void DoUnlockNode(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        bool explicitOnly)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(transaction);
        YCHECK(!transaction->Locks().empty());

        auto getStrongestLockMode = [&] () {
            auto result = ELockMode::None;
            for (auto* lock : trunkNode->LockingState().AcquiredLocks) {
                if (lock->GetTransaction() != transaction) {
                    continue;
                }
                if (lock->Request().Mode > result) {
                    result = lock->Request().Mode;
                    if (result == ELockMode::Exclusive) {
                        break; // as strong as it gets
                    }
                }
            }
            return result;
        };

        auto strongestLockModeBefore = getStrongestLockMode();
        RemoveTransactionNodeLocks(trunkNode, transaction, explicitOnly);
        auto strongestLockModeAfter = getStrongestLockMode();

        if (trunkNode->IsExternal() && Bootstrap_->IsPrimaryMaster()) {
            PostUnlockForeignNodeRequest(trunkNode, transaction, explicitOnly);
        }

        if (strongestLockModeBefore != strongestLockModeAfter) {
            UnbranchOrUpdateNodesAfterUnlock(trunkNode, transaction, strongestLockModeBefore, strongestLockModeAfter);
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Node explicitly unlocked (NodeId: %v, TransactionId: %v)",
            trunkNode->GetId(),
            transaction->GetId());

        CheckPendingLocks(trunkNode);
    }

    void RemoveTransactionNodeLocks(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        bool explicitOnly)
    {
        auto isLockRelevant = [&] (TLock* l) { return l->GetTransaction() == transaction; };

        auto maybeRemoveLock = [&] (TLock* lock) {
            Y_ASSERT(lock->GetTrunkNode() == trunkNode);
            if (isLockRelevant(lock) && (!explicitOnly || !lock->GetImplicit())) {
                DoRemoveLock(lock);
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

    void UnbranchOrUpdateNodesAfterUnlock(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode strongestLockModeBefore,
        ELockMode strongestLockModeAfter)
    {
        auto mustUnbranchThisNode = strongestLockModeAfter <= ELockMode::None;
        auto mustUpdateThisNode = !mustUnbranchThisNode;
        auto mustUnbranchAboveNodes = strongestLockModeAfter <= ELockMode::Snapshot && strongestLockModeBefore > ELockMode::Snapshot;
        auto mustUpdateAboveNodes = strongestLockModeAfter > ELockMode::Snapshot;

        // The mode of the strongest lock among all of the descendants
        // (branches, subbranches, subsubbranches...) of the node being unlocked.
        // NB: ELockMode::None and ELockMode::Snapshot are indistinguishable
        // because snapshot locks may lie arbitrarily deep without affecting ancestors.
        auto strongestLockModeBelow = ELockMode::None;
        for (auto* nestedTransaction : transaction->NestedTransactions()) {
            auto* versionedNode = FindNode(TVersionedNodeId{trunkNode->GetId(), nestedTransaction->GetId()});
            if (!versionedNode) {
                continue;
            }

            auto lockMode = versionedNode->GetLockMode();
            YCHECK(lockMode != ELockMode::None);

            if (strongestLockModeBelow < lockMode) {
                strongestLockModeBelow = lockMode;

                if (strongestLockModeBelow == ELockMode::Exclusive) {
                    break; // as strong as it gets
                }
            }
        }

        auto* branchedNode = GetNode(TVersionedNodeId{trunkNode->GetId(), transaction->GetId()});
        YCHECK(branchedNode->GetLockMode() != ELockMode::None);

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

        auto unbranchNode = [&] (TCypressNodeBase* node) {
            auto* transaction = node->GetTransaction();

            RemoveBranchedNode(transaction, node);

            auto& branchedNodes = transaction->BranchedNodes();
            auto it = std::remove(branchedNodes.begin(), branchedNodes.end(), node);
            Y_ASSERT(std::distance(it, branchedNodes.end()) == 1);
            branchedNodes.erase(it, branchedNodes.end());
        };

        auto updateNode = [&] (TCypressNodeBase* node) {
            node->SetLockMode(newLockMode);
        };

        YCHECK(!mustUnbranchThisNode || !mustUpdateThisNode);

        if (mustUnbranchThisNode) {
            // Nested arbitrarily deeply under the node being unbranched, there
            // may lie a snapshot-locked branched node referencing the node we're
            // about to unbranch as its originator. We must update these
            // references to avoid dangling pointers.
            // NB: the same needn't be done for the nodes to be unbrached above
            // as they're not locked and thus couldn't possibly have been
            // chosen to be originators of snapshot locks.
            TCypressNodeBase* newOriginator = nullptr;
            for (auto* current = transaction->GetParent(); current; current = current->GetParent()) {
                if (current->LockedNodes().has(trunkNode)) {
                    newOriginator = GetNode(TVersionedNodeId{trunkNode->GetId(), current->GetId()});
                    break;
                }
            }
            if (!newOriginator) {
                newOriginator = trunkNode;
            }

            for (auto* lock : trunkNode->LockingState().AcquiredLocks) {
                if (lock->Request().Mode != ELockMode::Snapshot) {
                    continue;
                }
                auto* lockTransaction = lock->GetTransaction();
                if (lockTransaction->IsDescendantOf(transaction)) {
                    auto* node = GetNode(TVersionedNodeId{trunkNode->GetId(), lockTransaction->GetId()});
                    auto* nodeOriginator = node->GetOriginator();
                    if (nodeOriginator == branchedNode) {
                        node->SetOriginator(newOriginator);
                    }
                }
            }

            unbranchNode(branchedNode);
        }
        if (mustUpdateThisNode) {
            YCHECK(!mustUnbranchThisNode);
            updateNode(branchedNode);
        }

        YCHECK(!mustUnbranchAboveNodes || !mustUpdateAboveNodes);
        if (mustUnbranchAboveNodes || mustUpdateAboveNodes) {
            // Process nodes above until another lock is met.
            for (auto* t = transaction->GetParent(); t; t = t->GetParent()) {
                if (t->LockedNodes().has(trunkNode)) {
                    break;
                }

                auto* node = GetNode(TVersionedNodeId{trunkNode->GetId(), t->GetId()});
                Y_ASSERT(t == node->GetTransaction());

                if (mustUnbranchAboveNodes) {
                    unbranchNode(node);
                }
                if (mustUpdateAboveNodes) {
                    updateNode(node);
                }
            }
        }
    }

    TLock* CreateLock(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool waitable)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());
        YCHECK(transaction);
        YCHECK(request.Mode != ELockMode::None);

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
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        EModificationType modificationType)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        AccessTracker_->SetModified(trunkNode, transaction, modificationType);
    }

    void SetAccessed(TCypressNodeBase* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        if (HydraManager_->IsLeader() || HydraManager_->IsFollower() && !HasMutationContext()) {
            AccessTracker_->SetAccessed(trunkNode);
        }
    }

    void SetExpirationTime(TCypressNodeBase* trunkNode, std::optional<TInstant> time)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        trunkNode->SetExpirationTime(time);
        ExpirationTracker_->OnNodeExpirationTimeUpdated(trunkNode);
    }


    TSubtreeNodes ListSubtreeNodes(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        bool includeRoot)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        TSubtreeNodes result;
        ListSubtreeNodes(trunkNode, transaction, includeRoot, &result);
        return result;
    }

    void AbortSubtreeTransactions(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        SmallVector<TTransaction*, 16> transactions;

        auto addLock = [&] (const TLock* lock) {
            // Get the top-most transaction.
            auto* transaction = lock->GetTransaction();
            while (transaction->GetParent()) {
                transaction = transaction->GetParent();
            }
            transactions.push_back(transaction);
        };

        auto addNode = [&] (const TCypressNodeBase* node) {
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


    bool IsOrphaned(TCypressNodeBase* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        auto* currentNode = trunkNode;
        while (true) {
            if (!IsObjectAlive(currentNode)) {
                return true;
            }
            if (currentNode == RootNode_) {
                return false;
            }
            currentNode = currentNode->GetParent();
        }
    }


    TCypressNodeList GetNodeOriginators(
        TTransaction* transaction,
        TCypressNodeBase* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

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
        TCypressNodeBase* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        auto result = GetNodeOriginators(transaction, trunkNode);
        std::reverse(result.begin(), result.end());
        return result;
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Node, TCypressNodeBase);
    DECLARE_ENTITY_MAP_ACCESSORS(Lock, TLock);

    DEFINE_BYREF_RO_PROPERTY(NTableServer::TSharedTableSchemaRegistryPtr, SharedTableSchemaRegistry);

    DEFINE_SIGNAL(void(TCypressNodeBase*), NodeCreated);

private:
    friend class TNodeTypeHandler;
    friend class TLockTypeHandler;

    class TNodeMapTraits
    {
    public:
        explicit TNodeMapTraits(TImpl* owner);

        std::unique_ptr<TCypressNodeBase> Create(const TVersionedNodeId& id) const;

    private:
        TImpl* const Owner_;

    };

    const TCypressManagerConfigPtr Config_;

    const TAccessTrackerPtr AccessTracker_;
    const TExpirationTrackerPtr ExpirationTracker_;

    NHydra::TEntityMap<TCypressNodeBase, TNodeMapTraits> NodeMap_;
    NHydra::TEntityMap<TLock> LockMap_;

    TEnumIndexedVector<INodeTypeHandlerPtr, NObjectClient::EObjectType> TypeToHandler_;

    TNodeId RootNodeId_;
    TMapNode* RootNode_ = nullptr;

    // COMPAT(babenko)
    bool FixLinkPaths_ = false;
    // COMPAT(savrus)
    bool ClearSysAttributes_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveKeys(context);
        LockMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveValues(context);
        LockMap_.SaveValues(context);
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NodeMap_.LoadKeys(context);
        LockMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NodeMap_.LoadValues(context);
        LockMap_.LoadValues(context);

        // COMPAT(babenko)
        FixLinkPaths_ = context.GetVersion() < 403;
        // COMPAT(savrus)
        ClearSysAttributes_ = context.GetVersion() < 620;
    }


    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        ExpirationTracker_->Clear();

        NodeMap_.Clear();
        LockMap_.Clear();
        SharedTableSchemaRegistry_->Clear();

        RootNode_ = nullptr;
    }

    virtual void SetZeroState() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::SetZeroState();

        InitBuiltins();
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        YT_LOG_INFO("Started initializing locks");
        for (const auto& pair : LockMap_) {
            auto* lock = pair.second;
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
                        Y_UNREACHABLE();
                }
            }
        }
        YT_LOG_INFO("Finished initializing locks");

        YT_LOG_INFO("Started initializing nodes");
        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;

            // Reconstruct immediate ancestor sets.
            auto* parent = node->GetParent();
            if (parent) {
                YCHECK(parent->ImmediateDescendants().insert(node).second);
            }

            // Reconstruct TrunkNode and Transaction.
            auto transactionId = node->GetVersionedId().TransactionId;
            if (transactionId) {
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

            if (node->IsTrunk() && node->GetExpirationTime()) {
                ExpirationTracker_->OnNodeExpirationTimeUpdated(node);
            }
        }
        YT_LOG_INFO("Finished initializing nodes");

        InitBuiltins();

        // COMPAT(babenko)
        if (FixLinkPaths_) {
            for (const auto& pair : NodeMap_) {
                auto* node = pair.second;
                if (node->GetType() == EObjectType::Link) {
                    auto* linkNode = node->As<TLinkNode>();
                    const auto& targetPath = linkNode->GetTargetPath();
                    if (targetPath.StartsWith(ObjectIdPathPrefix)) {
                        TObjectId objectId;
                        TStringBuf objectIdString(targetPath.begin() + ObjectIdPathPrefix.length(), targetPath.end());
                        if (TObjectId::FromString(objectIdString, &objectId)) {
                            auto* targetNode = FindNode(TVersionedObjectId(objectId));
                            if (IsObjectAlive(targetNode)) {
                                auto fixedPath = GetNodePath(targetNode, nullptr);
                                YT_LOG_DEBUG("Fixed link target: %v -> %v", targetPath, fixedPath);
                                linkNode->SetTargetPath(fixedPath);
                            }
                        }
                    }
                }
            }
        }

        if (ClearSysAttributes_) {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            auto* sysNode = cypressManager->ResolvePathToTrunkNode("//sys");
            auto& attributes = sysNode->GetMutableAttributes()->Attributes();
            auto processAttribute = [&] (const TString& attributeName)
            {
                auto it = attributes.find(attributeName);
                if (it != attributes.end()) {
                    YT_LOG_DEBUG("Remove //sys attribute (AttributeName: %Qv, AttributeValue: %v)",
                        attributeName,
                        ConvertToYsonString(it->second, EYsonFormat::Text));
                    attributes.erase(it);
                }
            };
            static const TString enableTabletBalancerAttributeName("enable_tablet_balancer");
            static const TString disableChunkReplicatorAttributeName("disable_chunk_replicator");
            processAttribute(enableTabletBalancerAttributeName);
            processAttribute(disableChunkReplicatorAttributeName);
            if (attributes.empty()) {
                sysNode->ClearAttributes();
            }
        }
    }


    void InitBuiltins()
    {
        auto* untypedRootNode = FindNode(TVersionedNodeId(RootNodeId_));
        if (untypedRootNode) {
            // Root already exists.
            RootNode_ = untypedRootNode->As<TMapNode>();
        } else {
            // Create the root.
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
            YCHECK(RootNode_->RefObject() == 1);
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

        if (Bootstrap_->IsPrimaryMaster()) {
            ExpirationTracker_->Start();
        }
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        AccessTracker_->Stop();

        if (Bootstrap_->IsPrimaryMaster()) {
            ExpirationTracker_->Stop();
        }
    }

    virtual void OnStopFollowing() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopFollowing();

        AccessTracker_->Stop();
    }


    TCypressNodeBase* RegisterNode(std::unique_ptr<TCypressNodeBase> trunkNodeHolder)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(trunkNodeHolder->IsTrunk());

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

    void DestroyNode(TCypressNodeBase* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        const auto& lockingState = trunkNode->LockingState();

        for (auto* lock : lockingState.AcquiredLocks) {
            lock->SetTrunkNode(nullptr);
            // NB: Transaction may have more than one lock for a given node.
            lock->GetTransaction()->LockedNodes().erase(trunkNode);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* lock : lockingState.PendingLocks) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Lock orphaned (LockId: %v)",
                lock->GetId());
            lock->SetTrunkNode(nullptr);
            auto* transaction = lock->GetTransaction();
            YCHECK(transaction->Locks().erase(lock) == 1);
            lock->SetTransaction(nullptr);
            objectManager->UnrefObject(lock);
        }

        trunkNode->ResetLockingState();

        ExpirationTracker_->OnNodeDestroyed(trunkNode);

        const auto& handler = GetHandler(trunkNode);
        Y_ASSERT(!trunkNode->GetTransaction());
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

        RemoveBranchedNodes(transaction);
        ReleaseLocks(transaction, false);
    }


    template <typename F>
    TError CheckSubtreeTrunkNodes(
        TCypressNodeBase* trunkNode,
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
        TCypressNodeBase* trunkNode,
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
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        bool recursive)
    {
        auto doCheck = [&](TCypressNodeBase* trunkNode) {
            return DoCheckLock(trunkNode, transaction, request);
        };


        // Validate all potential locks to see if we need to take at least one of them.
        // This throws an exception in case the validation fails.
        return CheckSubtreeTrunkNodes(trunkNode, transaction, recursive, doCheck);
    }

    TError DoCheckLock(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request)
    {
        Y_ASSERT(trunkNode->IsTrunk());
        YCHECK(transaction || request.Mode != ELockMode::Snapshot);

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
                    Y_UNREACHABLE();
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
                Y_UNREACHABLE();
        }

        return TError();
    }

    TError CheckUnlock(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        bool recursive,
        bool explicitOnly)
    {
        auto doCheck = [&] (TCypressNodeBase* trunkNode) {
            return DoCheckUnlock(trunkNode, transaction, explicitOnly);
        };

        // Check that unlocking nodes won't drop any data.
        return CheckSubtreeTrunkNodes(trunkNode, transaction, recursive, doCheck);
    }

    TError DoCheckUnlock(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        bool explicitOnly)
    {
        Y_ASSERT(trunkNode->IsTrunk());
        YCHECK(transaction);

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

    bool IsLockRedundant(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        const TLock* lockToIgnore = nullptr)
    {
        Y_ASSERT(trunkNode->IsTrunk());
        Y_ASSERT(request.Mode != ELockMode::None && request.Mode != ELockMode::Snapshot);

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

            default:
                Y_UNREACHABLE();
        }

        return false;
    }

    bool IsUnlockRedundant(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        bool explicitOnly)
    {
        Y_ASSERT(transaction);

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

    TCypressNodeBase* DoAcquireLock(
        TLock* lock,
        bool dontLockForeign = false)
    {
        auto* trunkNode = lock->GetTrunkNode();
        auto* transaction = lock->GetTransaction();
        const auto& request = lock->Request();

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Lock acquired (LockId: %v)",
            lock->GetId());

        YCHECK(lock->GetState() == ELockState::Pending);
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
                Y_UNREACHABLE();
        }

        if (transaction->LockedNodes().insert(trunkNode).second) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Node locked (NodeId: %v, TransactionId: %v)",
                trunkNode->GetId(),
                transaction->GetId());
        }

        if (trunkNode->IsExternal() && Bootstrap_->IsPrimaryMaster() && !dontLockForeign) {
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

        TCypressNodeBase* originatingNode;
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

        YCHECK(originatingNode);
        YCHECK(!intermediateTransactions.empty());

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
        TCypressNodeBase* trunkNode,
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

        YCHECK(transaction->Locks().insert(lock).second);
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

        SmallVector<TCypressNodeBase*, 16> lockedNodes(transaction->LockedNodes().begin(), transaction->LockedNodes().end());
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
                DoRemoveLock(lock);
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
        YCHECK(parentTransaction);
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
                    Y_UNREACHABLE();
            }
        }
        YCHECK(parentTransaction->Locks().insert(lock).second);
        // NB: Node could be locked more than once.
        parentTransaction->LockedNodes().insert(trunkNode);
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Lock promoted (LockId: %v, TransactionId: %v -> %v)",
            lock->GetId(),
            transaction->GetId(),
            parentTransaction->GetId());
    }

    void DoRemoveLock(TLock* lock)
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
                            Y_UNREACHABLE();
                    }
                    break;
                }

                case ELockState::Pending:
                    lockingState->PendingLocks.erase(lock->GetLockListIterator());
                    break;

                default:
                    Y_UNREACHABLE();
            }

            trunkNode->ResetLockingStateIfEmpty();
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

    void CheckPendingLocks(TCypressNodeBase* trunkNode)
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
        const auto* node = lock->GetTrunkNode();

        NProto::TReqLockForeignNode request;
        ToProto(request.mutable_transaction_id(), lock->GetTransaction()->GetId());
        ToProto(request.mutable_node_id(), node->GetId());
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
        }
        request.set_timestamp(lock->Request().Timestamp);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(request, node->GetExternalCellTag());
    }

    void PostUnlockForeignNodeRequest(TCypressNodeBase* trunkNode, TTransaction* transaction, bool explicitOnly)
    {
        NProto::TReqUnlockForeignNode request;
        ToProto(request.mutable_transaction_id(), transaction->GetId());
        ToProto(request.mutable_node_id(), trunkNode->GetId());
        request.set_explicit_only(explicitOnly);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(request, trunkNode->GetExternalCellTag());
    }

    void ListSubtreeNodes(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        bool includeRoot,
        TSubtreeNodes* subtreeNodes)
    {
        Y_ASSERT(trunkNode->IsTrunk());

        if (includeRoot) {
            subtreeNodes->push_back(trunkNode);
        }

        switch (trunkNode->GetNodeType()) {
            case ENodeType::Map: {
                auto originators = GetNodeReverseOriginators(transaction, trunkNode);
                THashMap<TString, TCypressNodeBase*> children;
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


    TCypressNodeBase* BranchNode(
        TCypressNodeBase* originatingNode,
        TTransaction* transaction,
        const TLockRequest& request)
    {
        YCHECK(originatingNode);
        YCHECK(transaction);
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& objectManager = Bootstrap_->GetObjectManager();

        const auto& id = originatingNode->GetId();

        // Create a branched node and initialize its state.
        const auto& handler = GetHandler(originatingNode);
        auto branchedNodeHolder = handler->Branch(originatingNode, transaction, request);

        TVersionedNodeId versionedId(id, transaction->GetId());
        auto* branchedNode = NodeMap_.Insert(versionedId, std::move(branchedNodeHolder));

        YCHECK(branchedNode->GetLockMode() == request.Mode);

        // Register the branched node with the transaction.
        transaction->BranchedNodes().push_back(branchedNode);

        // The branched node holds an implicit reference to its trunk.
        objectManager->RefObject(originatingNode->GetTrunkNode());

        return branchedNode;
    }

    void MergeNode(
        TTransaction* transaction,
        TCypressNodeBase* branchedNode)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        const auto& handler = GetHandler(branchedNode);

        auto* trunkNode = branchedNode->GetTrunkNode();
        auto branchedNodeId = branchedNode->GetVersionedId();

        if (branchedNode->GetLockMode() != ELockMode::Snapshot) {
            auto* originatingNode = branchedNode->GetOriginator();

            // Merge changes back.
            Y_ASSERT(branchedNode->GetTransaction() == transaction);
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
            Y_ASSERT(branchedNode->GetTransaction() == transaction);
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

    void RemoveBranchedNode(
        TTransaction* transaction,
        TCypressNodeBase* branchedNode)
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
        Y_ASSERT(branchedNode->GetTransaction() == transaction);
        handler->Destroy(branchedNode);
        NodeMap_.Remove(branchedNodeId);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Branched node removed (NodeId: %v)", branchedNodeId);
    }

    void RemoveBranchedNodes(TTransaction* transaction)
    {
        for (auto* branchedNode : transaction->BranchedNodes()) {
            RemoveBranchedNode(transaction, branchedNode);
        }
        transaction->BranchedNodes().clear();
    }


    TCypressNodeBase* DoCloneNode(
        TCypressNodeBase* sourceNode,
        ICypressNodeFactory* factory,
        TNodeId hintId,
        ENodeCloneMode mode)
    {
        // Prepare account.
        auto* account = factory->GetClonedNodeAccount(sourceNode);

        const auto& handler = GetHandler(sourceNode);
        auto* clonedNode = handler->Clone(
            sourceNode,
            factory,
            hintId,
            mode,
            account);

        // Set owner.
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        auto* acd = securityManager->GetAcd(clonedNode);
        acd->SetOwner(user);

        // Copy expiration time.
        auto expirationTime = sourceNode->GetTrunkNode()->GetExpirationTime();
        if (factory->ShouldPreserveExpirationTime() && expirationTime) {
            SetExpirationTime(clonedNode, *expirationTime);
        }

        // Copy creation time.
        if (factory->ShouldPreserveCreationTime()) {
            clonedNode->SetCreationTime(sourceNode->GetTrunkNode()->GetCreationTime());
        }

        return clonedNode;
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
        YCHECK(Bootstrap_->IsSecondaryMaster());

        auto nodeId = FromProto<TObjectId>(request->node_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto accountId = FromProto<TAccountId>(request->account_id());
        auto type = EObjectType(request->type());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionId
            ? transactionManager->GetTransaction(transactionId)
            : nullptr;

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = accountId
            ? securityManager->GetAccount(accountId)
            : nullptr;

        auto explicitAttributes = request->has_explicit_node_attributes()
            ? FromProto(request->explicit_node_attributes())
            : CreateEphemeralAttributes();
        auto inheritedAttributes = request->has_inherited_node_attributes()
            ? FromProto(request->inherited_node_attributes())
            : CreateEphemeralAttributes();

        auto versionedNodeId = TVersionedNodeId(nodeId, transactionId);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Creating foreign node (NodeId: %v, Type: %v, Account: %v)",
            versionedNodeId,
            type,
            account ? std::make_optional(account->GetName()) : std::nullopt);

        const auto& handler = GetHandler(type);

        auto* trunkNode = CreateNode(
            nodeId,
            NotReplicatedCellTag,
            handler,
            account,
            transaction,
            inheritedAttributes.get(),
            explicitAttributes.get());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(trunkNode);

        handler->FillAttributes(trunkNode, inheritedAttributes.get(), explicitAttributes.get());

        LockNode(trunkNode, transaction, ELockMode::Exclusive);
    }

    void HydraCloneForeignNode(NProto::TReqCloneForeignNode* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(Bootstrap_->IsSecondaryMaster());

        auto sourceNodeId = FromProto<TNodeId>(request->source_node_id());
        auto sourceTransactionId = FromProto<TTransactionId>(request->source_transaction_id());
        auto clonedNodeId = FromProto<TNodeId>(request->cloned_node_id());
        auto clonedTransactionId = FromProto<TTransactionId>(request->cloned_transaction_id());
        auto mode = ENodeCloneMode(request->mode());
        auto accountId = FromProto<TAccountId>(request->account_id());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* sourceTransaction = sourceTransactionId
            ? transactionManager->GetTransaction(sourceTransactionId)
            : nullptr;
        auto* clonedTransaction = clonedTransactionId
            ? transactionManager->GetTransaction(clonedTransactionId)
            : nullptr;

        auto* sourceTrunkNode = GetNode(TVersionedObjectId(sourceNodeId));
        auto* sourceNode = GetVersionedNode(sourceTrunkNode, sourceTransaction);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->GetAccount(accountId);

        auto factory = CreateNodeFactory(
            clonedTransaction,
            account,
            TNodeFactoryOptions());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Cloning foreign node (SourceNodeId: %v, ClonedNodeId: %v, Account: %v)",
            TVersionedNodeId(sourceNodeId, sourceTransactionId),
            TVersionedNodeId(clonedNodeId, clonedTransactionId),
            account->GetName());

        auto* clonedTrunkNode = DoCloneNode(
            sourceNode,
            factory.get(),
            clonedNodeId,
            mode);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(clonedTrunkNode);

        LockNode(clonedTrunkNode, clonedTransaction, ELockMode::Exclusive);

        factory->Commit();
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
        YCHECK(Bootstrap_->IsSecondaryMaster());

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto nodeId = FromProto<TNodeId>(request->node_id());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            YT_LOG_ERROR("Unexpected error: lock transaction is missing (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        auto* trunkNode = FindNode(TVersionedObjectId(nodeId));
        if (!IsObjectAlive(trunkNode)) {
            YT_LOG_ERROR("Unexpected error: lock node is missing (NodeId: %v, TransactionId: %v)",
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
            YT_LOG_ERROR(error, "Unexpected error: cannot lock foreign node (NodeId: %v, TransactionId: %v, Mode: %v, Key: %v)",
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
        YCHECK(Bootstrap_->IsSecondaryMaster());

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto explicitOnly = request->explicit_only();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            YT_LOG_ERROR("Unexpected error: unlock transaction is missing (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        auto* trunkNode = FindNode(TVersionedObjectId(nodeId));
        if (!IsObjectAlive(trunkNode)) {
            YT_LOG_ERROR("Unexpected error: unlock node is missing (NodeId: %v, TransactionId: %v)",
                nodeId,
                transactionId);
            return;
        }

        auto error = CheckUnlock(trunkNode, transaction, false, explicitOnly);
        if (!error.IsOK()) {
            YT_LOG_ERROR(error, "Unexpected error: cannot unlock foreign node (NodeId: %v, TransactionId: %v)",
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

DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager::TImpl, Node, TCypressNodeBase, NodeMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager::TImpl, Lock, TLock, LockMap_)

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TImpl::TNodeMapTraits::TNodeMapTraits(TImpl* owner)
    : Owner_(owner)
{ }

std::unique_ptr<TCypressNodeBase> TCypressManager::TImpl::TNodeMapTraits::Create(const TVersionedNodeId& id) const
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

void TCypressManager::TNodeTypeHandler::DestroyObject(TObjectBase* object) noexcept
{
    Owner_->DestroyNode(object->As<TCypressNodeBase>());
}

TString TCypressManager::TNodeTypeHandler::DoGetName(const TCypressNodeBase* node)
{
    auto path = Owner_->GetNodePath(node->GetTrunkNode(), node->GetTransaction());
    return Format("node %v", path);
}

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TLockTypeHandler::TLockTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->LockMap_)
{ }

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TCypressManager(
    TCypressManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
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

const INodeTypeHandlerPtr& TCypressManager::GetHandler(const TCypressNodeBase* node)
{
    return Impl_->GetHandler(node);
}

std::unique_ptr<ICypressNodeFactory> TCypressManager::CreateNodeFactory(
    TTransaction* transaction,
    TAccount* account,
    const TNodeFactoryOptions& options)
{
    return Impl_->CreateNodeFactory(
        transaction,
        account,
        options);
}

TCypressNodeBase* TCypressManager::CreateNode(
    TNodeId hintId,
    TCellTag externalCellTag,
    INodeTypeHandlerPtr handler,
    TAccount* account,
    TTransaction* transaction,
    IAttributeDictionary* inheritedAttributes,
    IAttributeDictionary* explicitAttributes)
{
    return Impl_->CreateNode(
        hintId,
        externalCellTag,
        std::move(handler),
        account,
        transaction,
        inheritedAttributes,
        explicitAttributes);
}

TCypressNodeBase* TCypressManager::InstantiateNode(
    TNodeId id,
    TCellTag externalCellTag)
{
    return Impl_->InstantiateNode(id, externalCellTag);
}

TCypressNodeBase* TCypressManager::CloneNode(
    TCypressNodeBase* sourceNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode)
{
    return Impl_->CloneNode(sourceNode, factory, mode);
}

TMapNode* TCypressManager::GetRootNode() const
{
    return Impl_->GetRootNode();
}

TCypressNodeBase* TCypressManager::GetNodeOrThrow(const TVersionedNodeId& id)
{
    return Impl_->GetNodeOrThrow(id);
}

TYPath TCypressManager::GetNodePath(TCypressNodeBase* trunkNode, TTransaction* transaction)
{
    return Impl_->GetNodePath(trunkNode, transaction);
}

TYPath TCypressManager::GetNodePath(const ICypressNodeProxy* nodeProxy)
{
    return Impl_->GetNodePath(nodeProxy);
}

TCypressNodeBase* TCypressManager::ResolvePathToTrunkNode(const TYPath& path, TTransaction* transaction)
{
    return Impl_->ResolvePathToTrunkNode(path, transaction);
}

ICypressNodeProxyPtr TCypressManager::ResolvePathToNodeProxy(const TYPath& path, TTransaction* transaction)
{
    return Impl_->ResolvePathToNodeProxy(path, transaction);
}

TCypressNodeBase* TCypressManager::FindNode(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    return Impl_->FindNode(trunkNode, transaction);
}

TCypressNodeBase* TCypressManager::GetVersionedNode(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    return Impl_->GetVersionedNode(trunkNode, transaction);
}

ICypressNodeProxyPtr TCypressManager::GetNodeProxy(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    return Impl_->GetNodeProxy(trunkNode, transaction);
}

TCypressNodeBase* TCypressManager::LockNode(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request,
    bool recursive,
    bool dontLockForeign)
{
    return Impl_->LockNode(trunkNode, transaction, request, recursive, dontLockForeign);
}

TLock* TCypressManager::CreateLock(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request,
    bool waitable)
{
    return Impl_->CreateLock(trunkNode, transaction, request, waitable);
}

void TCypressManager::UnlockNode(
    TCypressNodeBase* trunkNode,
    NTransactionServer::TTransaction* transaction)
{
    return Impl_->UnlockNode(trunkNode, transaction, false /*recursive*/, true /*explicitOnly*/);
}

void TCypressManager::SetModified(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    EModificationType modificationType)
{
    Impl_->SetModified(trunkNode, transaction, modificationType);
}

void TCypressManager::SetAccessed(TCypressNodeBase* trunkNode)
{
    Impl_->SetAccessed(trunkNode);
}

void TCypressManager::SetExpirationTime(TCypressNodeBase* trunkNode, std::optional<TInstant> time)
{
    Impl_->SetExpirationTime(trunkNode, time);
}

TCypressManager::TSubtreeNodes TCypressManager::ListSubtreeNodes(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    bool includeRoot)
{
    return Impl_->ListSubtreeNodes(trunkNode, transaction, includeRoot);
}

void TCypressManager::AbortSubtreeTransactions(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    Impl_->AbortSubtreeTransactions(trunkNode, transaction);
}

void TCypressManager::AbortSubtreeTransactions(INodePtr node)
{
    Impl_->AbortSubtreeTransactions(std::move(node));
}

bool TCypressManager::IsOrphaned(TCypressNodeBase* trunkNode)
{
    return Impl_->IsOrphaned(trunkNode);
}

TCypressNodeList TCypressManager::GetNodeOriginators(
    TTransaction* transaction,
    TCypressNodeBase* trunkNode)
{
    return Impl_->GetNodeOriginators(transaction, trunkNode);
}

TCypressNodeList TCypressManager::GetNodeReverseOriginators(
    TTransaction* transaction,
    TCypressNodeBase* trunkNode)
{
    return Impl_->GetNodeReverseOriginators(transaction, trunkNode);
}

const NTableServer::TSharedTableSchemaRegistryPtr& TCypressManager::GetSharedTableSchemaRegistry() const
{
    return Impl_->SharedTableSchemaRegistry();
}

DELEGATE_ENTITY_MAP_ACCESSORS(TCypressManager, Node, TCypressNodeBase, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TCypressManager, Lock, TLock, *Impl_)

DELEGATE_SIGNAL(TCypressManager, void(TCypressNodeBase*), NodeCreated, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
