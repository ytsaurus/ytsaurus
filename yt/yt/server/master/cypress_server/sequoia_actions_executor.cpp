#include "sequoia_actions_executor.h"

#include "cypress_manager.h"
#include "node_detail.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/helpers.h>

#include <yt/yt/server/lib/sequoia/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NCellMaster;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSequoiaServer;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaActionsExecutor
    : public ISequoiaActionsExecutor
{
public:
    explicit TSequoiaActionsExecutor(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers<TReqCreateNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareCreateNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitCreateNode, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraAbortCreateNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqAttachChild>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAttachChild, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitAttachChild, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqRemoveNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareRemoveNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitRemoveNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqDetachChild>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareDetachChild, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitDetachChild, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqSetNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitSetNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqMultisetAttributes>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitMultisetAttributes, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqCloneNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareCloneNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitCloneNode, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraAbortCloneNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqLockNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitLockNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqUnlockNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitUnlockNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqRemoveNodeAttribute>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPerpareAndCommitRemoveNodeAttribute, Unretained(this)),
        });
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    static void VerifySequoiaNode(TCypressNode* node)
    {
        if (node->GetType() != EObjectType::Rootstock) {
            node->VerifySequoia();
        }
    }

    static bool CanCreateSequoiaType(EObjectType type)
    {
        return
            IsSequoiaNode(type) ||
            IsScalarType(type) ||
            IsChunkOwnerType(type);
    }

    static void PrepareSequoiaNodeCreation(
        TCypressNode* node,
        TNodeId parentId,
        TStringBuf key,
        TStringBuf rawPath) noexcept
    {
        using TImmutableProperties = TCypressNode::TImmutableSequoiaProperties;
        using TMutableProperties = TCypressNode::TMutableSequoiaProperties;

        auto path = TYPath(rawPath);
        while (true) {
            node->ImmutableSequoiaProperties() = std::make_unique<TImmutableProperties>(TImmutableProperties(
                std::string(key),
                path,
                parentId));
            node->MutableSequoiaProperties() = std::make_unique<TMutableProperties>(TMutableProperties{
                .BeingCreated = true,
            });

            node->VerifySequoia();

            if (node->IsTrunk()) {
                break;
            }

            node = node->GetOriginator();
        }
    }

    static void CommitSequoiaNodeCreation(TCypressNode* node)
    {
        while (true) {
            VerifySequoiaNode(node);

            YT_VERIFY(node->MutableSequoiaProperties()->BeingCreated);
            node->MutableSequoiaProperties()->BeingCreated = false;

            if (node->IsTrunk()) {
                break;
            }

            node = node->GetOriginator();
        }
    }

    // NB: Sequoia node has to be created in prepare phase since we cannot
    // guarantee that object id will not be used between prepare and commit.
    // It's ok because this node cannot be reached from any other node until
    // Sequoia tx is committed: AttachChild for created subtree's root is
    // executed via "late prepare" after prepares on all participants are
    // succeeded.
    void HydraPrepareCreateNode(
        TTransaction* /*transaction*/,
        NProto::TReqCreateNode* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto roles = multicellManager->GetMasterCellRoles(multicellManager->GetCellTag());
        if (None(roles & EMasterCellRoles::SequoiaNodeHost) && IsSequoiaId(nodeId)) {
            THROW_ERROR_EXCEPTION("This cell cannot host Sequoia nodes");
        }

        auto type = FromProto<EObjectType>(request->type());
        if (!CanCreateSequoiaType(type)) {
            THROW_ERROR_EXCEPTION("Type %Qlv is not supported in Sequoia", type);
        }

        IAttributeDictionaryPtr explicitAttributes;
        if (request->has_node_attributes()) {
            explicitAttributes = NYTree::FromProto(request->node_attributes());
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        auto nodeFactory = cypressManager->CreateNodeFactory(
            cypressManager->GetRootCypressShard(),
            cypressTransaction,
            securityManager->GetSysAccount(),
            /*options*/ {});

        auto* trunkNode = nodeFactory->CreateNode(
            type,
            nodeId,
            /*inheritedAttributes*/ nullptr,
            explicitAttributes.Get())
            ->GetTrunkNode();

        auto* node = cypressManager->GetNode({nodeId, cypressTransactionId});

        PrepareSequoiaNodeCreation(
            node,
            parentId,
            /*key*/ NYPath::DirNameAndBaseName(request->path()).second,
            /*path*/ request->path());

        trunkNode->RefObject();

        nodeFactory->Commit();
    }

    void HydraCommitCreateNode(
        TTransaction* /*transaction*/,
        NProto::TReqCreateNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());

        auto* node = cypressManager->GetNode(TVersionedNodeId{nodeId, cypressTransactionId});
        CommitSequoiaNodeCreation(node);
    }

    void HydraAbortCreateNode(
        TTransaction* /*transaction*/,
        NProto::TReqCreateNode* request,
        const TTransactionAbortOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->node_id());

        if (auto* node = Bootstrap_->GetCypressManager()->FindNode(TVersionedNodeId(nodeId))) {
            Bootstrap_->GetObjectManager()->UnrefObject(node);
        }

        // TODO(kvk1920): implement proper UnstageNode() and use it here.
        // See YT-14219.
    }

    void HydraPrepareAttachChild(
        TTransaction* /*transaction*/,
        NProto::TReqAttachChild* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto parentId = FromProto<TNodeId>(request->parent_id());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* parent = cypressManager->GetNodeOrThrow(TVersionedNodeId(parentId));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        TTransaction* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        VerifySequoiaNode(parent);
        auto beingCreated = parent->MutableSequoiaProperties()->BeingCreated;
        THROW_ERROR_EXCEPTION_IF(
            !beingCreated && !options.LatePrepare,
            "Operation is not atomic for user");

        if (options.LatePrepare) {
            cypressManager->LockNode(
                parent,
                cypressTransaction,
                TLockRequest::MakeSharedChild(request->key()));
        } else {
            YT_VERIFY(parent->MutableSequoiaProperties()->BeingCreated);
        }
    }

    void HydraCommitAttachChild(
        TTransaction* /*transaction*/,
        NProto::TReqAttachChild* request,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto parentId = FromProto<TNodeId>(request->parent_id());
        YT_VERIFY(
            TypeFromId(parentId) == EObjectType::SequoiaMapNode ||
            TypeFromId(parentId) == EObjectType::Scion);

        auto childId = FromProto<TNodeId>(request->child_id());
        auto key = request->key();
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransaction(cypressTransactionId)
            : nullptr;

        auto* trunkParent = cypressManager->GetNode(TVersionedNodeId(parentId))->As<TSequoiaMapNode>();
        if (!IsObjectAlive(trunkParent)) {
            YT_LOG_ALERT("An attempt to attach a child to a zombie Sequoia node was made "
                "(ParentId: %v, ChildId: %v, ChildKey: %v)",
                parentId,
                childId,
                key);
        }

        // NB: No-op in case of late prepare.
        auto* parent = cypressManager->LockNode(
            trunkParent,
            cypressTransaction,
            TLockRequest::MakeSharedChild(request->key()));

        VerifySequoiaNode(parent);
        AttachChildToSequoiaNodeOrThrow(parent, key, childId);

        MaybeTouchNode(
            cypressManager,
            parent,
            EModificationType::Content,
            request->access_tracking_options());
    }

    void HydraPrepareDetachChild(
        TTransaction* /*transaction*/,
        NProto::TReqDetachChild* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& key = request->key();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(parentId));
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        auto* node = cypressManager
            ->GetVersionedNode(trunkNode, cypressTransaction)
            ->As<TSequoiaMapNode>();
        while (!node->IsTrunk() && !node->KeyToChild().contains(key)) {
            node = node->GetOriginator()->As<TSequoiaMapNode>();
        }

        if (!GetOrDefault(node->KeyToChild(), key, NullObjectId)) {
            THROW_ERROR_EXCEPTION("Node %v does not has a child with key %Qv",
                parentId,
                key);
        }

        cypressManager->CheckLock(
            trunkNode,
            cypressTransaction,
            TLockRequest::MakeSharedChild(key))
            .ThrowOnError();

        // NB: Nobody can acquire the shared child lock for this node between
        // prepare and commit due to Sequoia table lock. DetachChild acquires
        // exclusive lock on (nodeId, topmostTx, key) in "child_node" Sequoia
        // table.
    }

    void HydraCommitDetachChild(
        TTransaction* /*transaction*/,
        NProto::TReqDetachChild* request,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        auto key = request->key();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        auto* trunkParent = cypressManager->GetNode(TVersionedNodeId(parentId))->As<TSequoiaMapNode>();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransaction(cypressTransactionId)
            : nullptr;

        if (!FindMapNodeChild(cypressManager, trunkParent, cypressTransaction, key)) {
            YT_LOG_ALERT("Sequoia map node has no such child (ParentId: %v, Key: %v)",
                parentId,
                key);
            return;
        }

        auto* parent = cypressManager->LockNode(
            trunkParent,
            cypressTransaction,
            TLockRequest::MakeSharedChild(key))
            ->As<TSequoiaMapNode>();

        auto& children = parent->MutableChildren();
        if (parent->IsTrunk()) {
            auto childIt = children.KeyToChild().find(request->key());
            children.Remove(request->key(), childIt->second);
        } else {
            children.Set(key, NullObjectId);
        }

        MaybeTouchNode(
            cypressManager,
            parent,
            EModificationType::Content,
            request->access_tracking_options());
    }

    void HydraPrepareRemoveNode(
        TTransaction* /*transaction*/,
        NProto::TReqRemoveNode* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
        VerifySequoiaNode(trunkNode);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        TTransaction* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        if (TypeFromId(nodeId) == EObjectType::Rootstock && cypressTransaction) {
            THROW_ERROR_EXCEPTION("Rootstock cannot be removed under transaction");
        }

        cypressManager
            ->CheckLock(trunkNode, cypressTransaction, ELockMode::Exclusive)
            .ThrowOnError();
    }

    void HydraCommitRemoveNode(
        TTransaction* /*transaction*/,
        NProto::TReqRemoveNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNode(TVersionedObjectId(nodeId));

        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransaction(cypressTransactionId)
            : nullptr;

        if (!IsObjectAlive(trunkNode)) {
            YT_LOG_ALERT(
                "Attempted to remove unexisting Sequoia node; ignored "
                "(NodeId: %v)",
                nodeId);
            return;
        }

        VerifySequoiaNode(trunkNode);

        if (trunkNode->GetType() == EObjectType::Rootstock) {
            auto parentNode = trunkNode->GetParent()->As<TCypressMapNode>();
            if (!parentNode) {
                YT_LOG_ALERT(
                    "Attempted to remove rootstock that is already detached from a parent, ignored "
                    "(RootstockNodeId: %v)",
                    nodeId);
                return;
            }
            auto parentProxy = cypressManager->GetNodeProxy(parentNode);
            parentProxy->AsMap()->RemoveChild(cypressManager->GetNodeProxy(trunkNode));
            return;
        }

        // NB: lock is already checked in prepare. Nobody cannot lock this node
        // between prepare and commit of Sequoia tx due to:
        //   - exlusive lock in "node_id_to_path" Sequoia table;
        //   - barrier for Sequoia tx
        if (cypressTransaction) {
            auto* branchedNode = cypressManager->LockNode(trunkNode, cypressTransaction, ELockMode::Exclusive);
            branchedNode->MutableSequoiaProperties()->Tombstone = true;
        } else {
            Bootstrap_->GetObjectManager()->UnrefObject(trunkNode);
        }
    }

    void HydraPrepareAndCommitSetNode(
        TTransaction* /*transaction*/,
        NProto::TReqSetNode* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedObjectId(nodeId));
        VerifySequoiaNode(trunkNode);

        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        auto innerRequest = TCypressYPathProxy::Set(request->path());
        innerRequest->set_value(request->value());
        innerRequest->set_force(request->force());
        SetAccessTrackingOptions(innerRequest, request->access_tracking_options());

        SyncExecuteVerb(
            cypressManager->GetNodeProxy(trunkNode, cypressTransaction),
            innerRequest);
    }

    void HydraPrepareAndCommitMultisetAttributes(
        TTransaction* /*transaction*/,
        NProto::TReqMultisetAttributes* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedObjectId(nodeId));
        VerifySequoiaNode(trunkNode);

        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        auto innerRequest = TYPathProxy::MultisetAttributes(request->path());
        *innerRequest->mutable_subrequests() = request->subrequests();
        innerRequest->set_force(request->force());
        SetAccessTrackingOptions(innerRequest, request->access_tracking_options());

        SyncExecuteVerb(
            cypressManager->GetNodeProxy(trunkNode, cypressTransaction),
            innerRequest);
    }

    void HydraPerpareAndCommitRemoveNodeAttribute(
        TTransaction* /*transaction*/,
        NProto::TReqRemoveNodeAttribute* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);
        YT_VERIFY(options.LatePrepare);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedObjectId(nodeId));
        VerifySequoiaNode(trunkNode);

        if (!IsObjectAlive(trunkNode)) {
            THROW_ERROR_EXCEPTION("Cypress node %v is not alive", nodeId);
        }

        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransaction(cypressTransactionId)
            : nullptr;

        auto innerRequest = TCypressYPathProxy::Remove(request->path());
        innerRequest->set_force(request->force());

        SyncExecuteVerb(
            cypressManager->GetNodeProxy(trunkNode, cypressTransaction),
            innerRequest);
    }

    // NB: See PrepareCreateNode.
    void HydraPrepareCloneNode(
        TTransaction* /*transaction*/,
        TReqCloneNode* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto sourceNodeId = FromProto<TNodeId>(request->src_id());
        auto destinationNodeId = FromProto<TNodeId>(request->dst_id());
        auto destinationParentId = FromProto<TNodeId>(request->dst_parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());

        // TODO(h0pless): Think about throwing an error if this cell is not sequoia_node_host anymore.
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* sourceNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(sourceNodeId));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        // Maybe this is excessive.
        auto type = sourceNode->GetType();
        if (!CanCreateSequoiaType(type)) {
            THROW_ERROR_EXCEPTION("Type %Qlv is not supported in Sequoia", type);
        }

        auto* requestOptions = request->mutable_options();
        auto mode = FromProto<ENodeCloneMode>(requestOptions->mode());
        TNodeFactoryOptions factoryOptions{
            .PreserveAccount = requestOptions->preserve_account(),
            .PreserveCreationTime = requestOptions->preserve_creation_time(),
            .PreserveModificationTime = requestOptions->preserve_modification_time(),
            .PreserveExpirationTime = requestOptions->preserve_expiration_time(),
            .PreserveExpirationTimeout = requestOptions->preserve_expiration_timeout(),
            .PreserveOwner = requestOptions->preserve_owner(),
            .PreserveAcl = requestOptions->preserve_acl(),
            .PessimisticQuotaCheck = requestOptions->pessimistic_quota_check(),
            .AllowSecondaryIndexAbandonment = requestOptions->allow_secondary_index_abandonment(),
        };

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto nodeFactory = cypressManager->CreateNodeFactory(
            cypressManager->GetRootCypressShard(),
            cypressTransaction,
            securityManager->GetSysAccount(),
            factoryOptions);

        // TODO(cherepashka): after inherited attributes are supported, implement copyable-inherited attributes.
        auto emptyInheritedAttributes = CreateEphemeralAttributes();
        auto* clonedNode = nodeFactory->CloneNode(sourceNode, mode, emptyInheritedAttributes.Get(), destinationNodeId);

        PrepareSequoiaNodeCreation(
            clonedNode,
            destinationParentId,
            NYPath::DirNameAndBaseName(request->dst_path()).second,
            request->dst_path());

        clonedNode->GetTrunkNode()->RefObject();
        nodeFactory->Commit();

        Bootstrap_->GetObjectManager()->WeakRefObject(sourceNode);
    }

    void HydraCommitCloneNode(
        TTransaction* /*transaction*/,
        NProto::TReqCloneNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto sourceNodeId = FromProto<TNodeId>(request->src_id());
        auto destinationNodeId = FromProto<TNodeId>(request->dst_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto* trunkSourceNode = cypressManager->GetNode({sourceNodeId, NullTransactionId});

        // TODO(danilalexeev): Sequoia transaction has to lock |sourceNodeId| to conflict
        // with an expired nodes removal.
        if (IsObjectAlive(trunkSourceNode)) {
            MaybeTouchNode(cypressManager, trunkSourceNode);
        }

        objectManager->WeakUnrefObject(trunkSourceNode);

        auto* destinationNode = cypressManager->GetNode(TVersionedNodeId(destinationNodeId, cypressTransactionId));

        if (!IsObjectAlive(destinationNode->GetTrunkNode())) {
            YT_LOG_ALERT("Cloned Sequoia node was zombified before its cloning is committed (NodeId: %v)",
                destinationNodeId);
            return;
        }

        CommitSequoiaNodeCreation(destinationNode);
    }

    void HydraAbortCloneNode(
        TTransaction* /*transaction*/,
        NProto::TReqCloneNode* request,
        const TTransactionAbortOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto sourceNodeId = FromProto<TNodeId>(request->src_id());
        auto destinationNodeId = FromProto<TNodeId>(request->dst_id());

        if (auto* sourceNode = Bootstrap_->GetCypressManager()->FindNode(TVersionedNodeId(sourceNodeId))) {
            Bootstrap_->GetObjectManager()->WeakUnrefObject(sourceNode);
        }

        if (auto* destinationNode = Bootstrap_->GetCypressManager()->FindNode(TVersionedNodeId(destinationNodeId))) {
            Bootstrap_->GetObjectManager()->UnrefObject(destinationNode);
        }
    }

    void HydraPrepareAndCommitLockNode(
        TTransaction* /*transaction*/,
        NProto::TReqLockNode* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.LatePrepare);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId{nodeId});
        VerifySequoiaNode(trunkNode);

        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());

        // Shoud be validated in Cypress proxy.
        YT_VERIFY(cypressTransactionId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransactionOrThrow(cypressTransactionId);

        auto mode = FromProto<ELockMode>(request->mode());
        auto childKey = YT_PROTO_OPTIONAL(*request, child_key);
        auto attributeKey = YT_PROTO_OPTIONAL(*request, attribute_key);
        auto timestamp = request->timestamp();
        bool waitable = request->waitable();

        // It was checked on Cypress proxy.
        YT_VERIFY(CheckLockRequest(mode, childKey, attributeKey).IsOK());

        auto proxy = cypressManager->GetNodeProxy(trunkNode, cypressTransaction);
        auto lockId = FromProto<TLockId>(request->lock_id());
        auto lockRequest = CreateLockRequest(mode, childKey, attributeKey, timestamp);
        proxy->Lock(lockRequest, waitable, lockId);

        MaybeTouchNode(cypressManager, trunkNode);
    }

    void HydraPrepareAndCommitUnlockNode(
        TTransaction* /*transaction*/,
        NProto::TReqUnlockNode* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.LatePrepare);

        const auto& cypressManager = Bootstrap_->GetCypressManager();

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
        VerifySequoiaNode(trunkNode);

        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        // Shoud be validated in Cypress proxy.
        YT_VERIFY(cypressTransactionId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransaction(cypressTransactionId);

        auto proxy = cypressManager->GetNodeProxy(trunkNode, cypressTransaction);
        proxy->Unlock();

        MaybeTouchNode(cypressManager, trunkNode);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(TBootstrap* bootstrap)
{
    return New<TSequoiaActionsExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
