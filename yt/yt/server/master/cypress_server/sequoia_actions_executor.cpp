#include "sequoia_actions_executor.h"

#include "cypress_manager.h"
#include "node_detail.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/lib/sequoia/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NCellMaster;
using namespace NCypressClient;
using namespace NObjectClient;
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
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto parentId = FromProto<TNodeId>(request->parent_id());

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
            /*transaction*/ nullptr,
            securityManager->GetSysAccount(),
            /*options*/ {});

        auto* node = nodeFactory->CreateNode(
            type,
            nodeId,
            /*inheritedAttributes*/ nullptr,
            explicitAttributes.Get())
            ->GetTrunkNode();

        TCypressNode::TImmutableSequoiaProperties immutableProperties(
            /*key*/ NYPath::DirNameAndBaseName(request->path()).second,
            /*path*/ request->path(),
            parentId);

        node->ImmutableSequoiaProperties() = std::make_unique<TCypressNode::TImmutableSequoiaProperties>(std::move(immutableProperties));
        node->MutableSequoiaProperties() = std::make_unique<TCypressNode::TMutableSequoiaProperties>();
        *node->MutableSequoiaProperties() = { .BeingCreated = true };

        node->VerifySequoia();
        node->RefObject();

        nodeFactory->Commit();
    }

    void HydraCommitCreateNode(
        TTransaction* /*transaction*/,
        NProto::TReqCreateNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->GetNode(TVersionedNodeId(nodeId));

        VerifySequoiaNode(node);

        auto beingCreated = std::exchange(node->MutableSequoiaProperties()->BeingCreated, false);
        YT_VERIFY(beingCreated);
    }

    void HydraAbortCreateNode(
        TTransaction* /*transaction*/,
        NProto::TReqCreateNode* request,
        const TTransactionAbortOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        // TODO(h0pless): Add cypress transaction id here.
        auto versionedNodeId = TVersionedObjectId(nodeId, NullObjectId);

        if (auto* node = Bootstrap_->GetCypressManager()->FindNode(versionedNodeId)) {
            Bootstrap_->GetObjectManager()->UnrefObject(node);
        }
    }

    void HydraPrepareAttachChild(
        TTransaction* /*transaction*/,
        NProto::TReqAttachChild* request,
        const TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto parentId = FromProto<TNodeId>(request->parent_id());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* parent = cypressManager->GetNodeOrThrow(TVersionedNodeId(parentId));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto cypressTransactionId = YT_PROTO_OPTIONAL(*request, transaction_id, TTransactionId)
            .value_or(NullTransactionId);
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
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // XXX(kvk1920): support transactions.

        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto childId = FromProto<TNodeId>(request->child_id());
        auto key = request->key();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* parent = cypressManager->GetNode(TVersionedNodeId(parentId));

        if (!IsObjectAlive(parent)) {
            YT_LOG_ALERT("An attempt to attach a child to a zombie Sequoia node was made "
                "(ParentId: %v, ChildId: %v, ChildKey: %v)",
                parentId,
                childId,
                key);
        }

        VerifySequoiaNode(parent);
        AttachChildToSequoiaNodeOrThrow(parent, key, childId);

        auto proxy = cypressManager->GetNodeProxy(parent);
        MaybeTouchNode(proxy, request->access_tracking_options());
    }

    void HydraCommitDetachChild(
        TTransaction* /*transaction*/,
        NProto::TReqDetachChild* request,
        const TTransactionCommitOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto* parent = cypressManager->GetNode(TVersionedNodeId(parentId));

        auto& children = parent->As<TSequoiaMapNode>()->MutableChildren();
        auto childIt = children.KeyToChild().find(request->key());
        // This is temporary logic.
        // TODO(cherepashka): In future DetachChild should remove child from parent node proxy.
        if (childIt == children.KeyToChild().end()) {
            YT_LOG_ALERT("Sequoia map node has no such child (ParentId: %v, Key: %v)",
                parentId,
                request->key());
            return;
        }

        children.Remove(request->key(), childIt->second);

        auto proxy = cypressManager->GetNodeProxy(parent);
        MaybeTouchNode(proxy, request->access_tracking_options());
    }

    void HydraPrepareRemoveNode(
        TTransaction* /*transaction*/,
        NProto::TReqRemoveNode* request,
        const TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
        VerifySequoiaNode(trunkNode);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto cypressTransactionId = YT_PROTO_OPTIONAL(*request, transaction_id, TTransactionId)
            .value_or(NullTransactionId);
        TTransaction* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        if (TypeFromId(nodeId) == EObjectType::Rootstock && cypressTransaction) {
            THROW_ERROR_EXCEPTION("Rootstock cannot be removed under transaction");
        }

        cypressManager
            ->CheckExclusiveLock(trunkNode, cypressTransaction)
            .ThrowOnError();
    }

    void HydraCommitRemoveNode(
        TTransaction* /*transaction*/,
        NProto::TReqRemoveNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->GetNode(TVersionedObjectId(nodeId));

        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        if (cypressTransactionId) {
            // NB: removal in transaction is not supported but lock conflict
            // detection is already implemented. We want to test it.

            // XXX(kvk1920): support it.
            YT_LOG_WARNING("Sequoia node removal in transaction is not supported yet");
            return;
        }

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransaction(cypressTransactionId)
            : nullptr;

        if (!IsObjectAlive(node)) {
            YT_LOG_ALERT(
                "Attempted to remove unexisting Sequoia node; ignored "
                "(NodeId: %v)",
                nodeId);
            return;
        }

        VerifySequoiaNode(node);

        if (node->GetType() == EObjectType::Rootstock) {
            auto parentNode = node->GetParent()->As<TCypressMapNode>();
            if (!parentNode) {
                YT_LOG_ALERT(
                    "Attempted to remove rootstock that is already detached from a parent, ignored "
                    "(RootstockNodeId: %v)",
                    nodeId);
                return;
            }
            auto parentProxy = cypressManager->GetNodeProxy(parentNode);
            parentProxy->AsMap()->RemoveChild(cypressManager->GetNodeProxy(node));
        } else if (cypressTransaction) {
            // XXX(kvk1920): tests for chunk owners removal.
            auto* branchedNode = cypressManager->LockNode(node, cypressTransaction, ELockMode::Exclusive);
            branchedNode->MutableSequoiaProperties()->Tombstone = true;
        } else {
            // Lock is already checked in prepare. Nobody cannot lock this node
            // between prepare and commit of Sequoia tx due to
            //   - exlusive lock in "node_id_to_path" Sequoia table;
            //   - barrier for Sequoia tx
            Bootstrap_->GetObjectManager()->UnrefObject(node);
        }
    }

    void HydraPrepareAndCommitSetNode(
        TTransaction* /*transaction*/,
        NProto::TReqSetNode* request,
        const TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
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

        const auto& accessTrackingOptions = request->access_tracking_options();
        SetSuppressAccessTracking(innerRequest, accessTrackingOptions.suppress_access_tracking());
        SetSuppressExpirationTimeoutRenewal(innerRequest, accessTrackingOptions.suppress_expiration_timeout_renewal());

        SyncExecuteVerb(
            cypressManager->GetNodeProxy(trunkNode, cypressTransaction),
            innerRequest);
    }

    void HydraPrepareAndCommitMultisetAttributes(
        TTransaction* /*transaction*/,
        NProto::TReqMultisetAttributes* request,
        const TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
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

        SyncExecuteVerb(
            cypressManager->GetNodeProxy(trunkNode, cypressTransaction),
            innerRequest);
    }

    void HydraPerpareAndCommitRemoveNodeAttribute(
        TTransaction* /*transaction*/,
        NProto::TReqRemoveNodeAttribute* request,
        const TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
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
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto sourceNodeId = FromProto<TNodeId>(request->src_id());
        auto destinationNodeId = FromProto<TNodeId>(request->dst_id());
        auto destinationParentId = FromProto<TNodeId>(request->dst_parent_id());

        // TODO(h0pless): Think about throwing an error if this cell is not sequoia_node_host anymore.
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* sourceNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(sourceNodeId));

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
        };

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto nodeFactory = cypressManager->CreateNodeFactory(
            cypressManager->GetRootCypressShard(),
            /*transaction*/ nullptr,
            securityManager->GetSysAccount(),
            factoryOptions);

        // TODO(cherepashka): after inherited attributes are supported, implement copyable-inherited attributes.
        auto emptyInheritedAttributes = CreateEphemeralAttributes();
        auto* clonedNode = nodeFactory->CloneNode(sourceNode, mode, emptyInheritedAttributes.Get(), destinationNodeId);

        TCypressNode::TImmutableSequoiaProperties immutableProperties(
            /*key*/ NYPath::DirNameAndBaseName(request->dst_path()).second,
            /*path*/ request->dst_path(),
            destinationParentId);

        clonedNode->ImmutableSequoiaProperties() = std::make_unique<TCypressNode::TImmutableSequoiaProperties>(std::move(immutableProperties));
        clonedNode->MutableSequoiaProperties() = std::make_unique<TCypressNode::TMutableSequoiaProperties>();
        *clonedNode->MutableSequoiaProperties() = {.BeingCreated = true};

        clonedNode->VerifySequoia();
        clonedNode->RefObject();
        nodeFactory->Commit();

        Bootstrap_->GetObjectManager()->WeakRefObject(sourceNode);
    }

    void HydraCommitCloneNode(
        TTransaction* /*transaction*/,
        NProto::TReqCloneNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto sourceNodeId = FromProto<TNodeId>(request->src_id());
        auto destinationNodeId = FromProto<TNodeId>(request->dst_id());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* sourceNode = cypressManager->GetNode(TVersionedNodeId(sourceNodeId));
        auto* destinationNode = cypressManager->GetNode(TVersionedNodeId(destinationNodeId));

        if (!IsObjectAlive(destinationNode)) {
            YT_LOG_ALERT("An attempt to clone a zombie Sequoia node was made (NodeId: %v)",
                destinationNodeId);
            return;
        }

        auto beingCreated = std::exchange(destinationNode->MutableSequoiaProperties()->BeingCreated, false);
        YT_VERIFY(beingCreated);

        auto finally = Finally([&] {
            if (sourceNode) {
                Bootstrap_->GetObjectManager()->WeakUnrefObject(sourceNode);
            }
        });

        if (!IsObjectAlive(sourceNode)) {
            YT_LOG_ALERT("Source node is no longer alive (NodeId: %v)",
                sourceNodeId);
            return;
        }

        // TODO(danilalexeev): Sequoia transaction has to lock |sourceNodeId| to conflict
        // with an expired nodes removal.
        auto proxy = cypressManager->GetNodeProxy(sourceNode);
        MaybeTouchNode(proxy);
    }

    void HydraAbortCloneNode(
        TTransaction* /*transaction*/,
        NProto::TReqCloneNode* request,
        const TTransactionAbortOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto sourceNodeId = FromProto<TNodeId>(request->src_id());
        auto destinationNodeId = FromProto<TNodeId>(request->dst_id());
        // XXX(kvk1920): Add cypress transaction id here.

        if (auto* destinationNode = Bootstrap_->GetCypressManager()->FindNode(TVersionedObjectId{destinationNodeId})) {
            Bootstrap_->GetObjectManager()->UnrefObject(destinationNode);
        }

        if (auto* sourceNode = Bootstrap_->GetCypressManager()->FindNode(TVersionedObjectId{sourceNodeId})) {
            Bootstrap_->GetObjectManager()->WeakUnrefObject(sourceNode);
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

        MaybeTouchNode(proxy);
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

        MaybeTouchNode(proxy);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(TBootstrap* bootstrap)
{
    return New<TSequoiaActionsExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
