#include "sequoia_actions_executor.h"

#include "cypress_manager.h"
#include "node_detail.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cypress_server/helpers.h>

#include <yt/yt/server/lib/sequoia/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NSequoiaServer;
using namespace NTableClient;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;
using namespace NYson;
using namespace NYTree;

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressServerLogger;

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
        transactionManager->RegisterTransactionActionHandlers<TReqCreateNode, TCreateNodeActionState>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareCreateNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitCreateNode, Unretained(this)),
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
        transactionManager->RegisterTransactionActionHandlers<TReqCloneNode, TCloneNodeActionState>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareCloneNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitCloneNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqExplicitlyLockNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitExplicitlyLockNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqImplicitlyLockNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareImplicitlyLockNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitImplicitlyLockNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqUnlockNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitUnlockNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqRemoveNodeAttribute>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPerpareAndCommitRemoveNodeAttribute, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqMaterializeNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitMaterializeNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqFinishNodeMaterialization>({
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitFinishNodeMaterialization, Unretained(this)),
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
            type == EObjectType::Document ||
            type == EObjectType::Orchid ||
            IsSequoiaNode(type) ||
            IsScalarType(type) ||
            IsChunkOwnerType(type);
    }

    template <CInvocable<void(TCypressNode*)> F>
    static void ForEachOriginator(
        TCypressNode* node,
        F callback) noexcept
    {
        while (true) {
            callback(node);

            VerifySequoiaNode(node);

            if (node->IsTrunk()) {
                break;
            }

            node = node->GetOriginator();
        }
    }

    static void PrepareSequoiaNodeCreation(
        TCypressNode* node,
        TNodeId parentId,
        TStringBuf key,
        TStringBuf rawPath) noexcept
    {
        auto path = TYPath(rawPath);
        ForEachOriginator(
            node,
            [&] (TCypressNode* node) {
                using TImmutableProperties = TCypressNode::TImmutableSequoiaProperties;
                using TMutableProperties = TCypressNode::TMutableSequoiaProperties;

                node->ImmutableSequoiaProperties() = std::make_unique<TImmutableProperties>(TImmutableProperties(
                    std::string(key),
                    path,
                    parentId));
                node->MutableSequoiaProperties() = std::make_unique<TMutableProperties>(TMutableProperties{
                    .BeingCreated = true,
                });
            });
    }

    void CommitSequoiaNodeCreation(const TCypressNodePtr& node)
    {
        auto* currentNode = node.Get();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& handler = cypressManager->GetHandler(currentNode);
        handler->SetReachable(currentNode);

        ForEachOriginator(
            currentNode,
            [] (TCypressNode* node) {
                YT_VERIFY(std::exchange(node->MutableSequoiaProperties()->BeingCreated, false));
            });
    }

    struct TCreateNodeActionState
    {
        TCypressNodePtr Node;

        void Persist(const NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Node);
        }
    };

    static void CreateSequoiaPropertiesForMaterializedNode(TCypressNode* node) noexcept
    {
        ForEachOriginator(
            node,
            [] (TCypressNode* node) {
                node->MutableSequoiaProperties() = std::make_unique<TCypressNode::TMutableSequoiaProperties>(
                    TCypressNode::TMutableSequoiaProperties{
                        .BeingCreated = true,
                    });
            });
    }

    void FinishSequoiaNodeMaterialization(TCypressNode* node, TNodeId parentId, const TString& path)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& handler = cypressManager->GetHandler(node);
        handler->SetReachable(node);

        ForEachOriginator(
            node,
            [&] (TCypressNode* node) {
                node->ImmutableSequoiaProperties() = std::make_unique<TCypressNode::TImmutableSequoiaProperties>(
                    TCypressNode::TImmutableSequoiaProperties(
                        NYPath::DirNameAndBaseName(path).second,
                        path,
                        parentId));

                node->MutableSequoiaProperties()->BeingCreated = false;
            });
    }

    // NB: Sequoia node has to be created in prepare phase since we cannot
    // guarantee that object id will not be used between prepare and commit.
    // It's ok because this node cannot be reached from any other node until
    // Sequoia tx is committed: AttachChild for created subtree's root is
    // executed via "late prepare" after prepares on all participants are
    // succeeded.
    // TODO(kvk1920): implement proper UnstageNode() and use it here.
    // See YT-14219.
    void HydraPrepareCreateNode(
        TTransaction* /*transaction*/,
        NProto::TReqCreateNode* request,
        TCreateNodeActionState* state,
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
            explicitAttributes = FromProto(request->node_attributes());
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        auto nodeFactory = cypressManager->CreateNodeFactory(
            cypressManager->GetRootCypressShard(),
            cypressTransaction,
            securityManager->GetSysAccount(),
            /*options*/ {});

        Y_UNUSED(nodeFactory->CreateNode(
            type,
            nodeId,
            /*inheritedAttributes*/ nullptr,
            explicitAttributes.Get()));
        auto* node = cypressManager->GetNode({nodeId, cypressTransactionId});
        // This takes a strong reference for the newly-created node.
        state->Node.Assign(node);

        PrepareSequoiaNodeCreation(
            node,
            parentId,
            /*key*/ NYPath::DirNameAndBaseName(request->path()).second,
            /*path*/ request->path());

        nodeFactory->Commit();
    }

    void HydraCommitCreateNode(
        TTransaction* /*transaction*/,
        NProto::TReqCreateNode* /*request*/,
        TCreateNodeActionState* state,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        CommitSequoiaNodeCreation(state->Node);
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
            YT_LOG_ALERT_AND_THROW_UNLESS(
                parent->MutableSequoiaProperties()->BeingCreated,
                "An attempt to attach a child in non late prepare mode was made, despite the fact that "
                "parent node is not marked as BeingCreated (ParentId: %v, TransactionId: %v, ChildKey: %v)",
                parentId,
                cypressTransactionId,
                request->key());
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

        --parent->ChildCountDelta();

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


        auto* node = trunkNode;
        // NB: lock is already checked in prepare. Nobody cannot lock this node
        // between prepare and commit of Sequoia tx due to:
        //   - exlusive lock in "node_id_to_path" Sequoia table;
        //   - barrier for Sequoia tx.
        if (cypressTransaction) {
            auto* branchedNode = cypressManager->LockNode(trunkNode, cypressTransaction, ELockMode::Exclusive);
            branchedNode->MutableSequoiaProperties()->Tombstone = true;
            node = branchedNode;
        }

        const auto& handler = cypressManager->GetHandler(trunkNode);
        handler->SetUnreachable(node);
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

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_LOG_ALERT_AND_THROW_UNLESS(
            options.LatePrepare,
            "An attempt to remove node attribute with non late prepare mode was made (NodeId: %v, TransactionId: %v)",
            nodeId,
            cypressTransactionId);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedObjectId(nodeId));
        VerifySequoiaNode(trunkNode);

        if (!IsObjectAlive(trunkNode)) {
            THROW_ERROR_EXCEPTION("Cypress node %v is not alive", nodeId);
        }

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

    struct TCloneNodeActionState
    {
        TWeakObjectPtr<TCypressNode> TrunkSourceNode;
        TCypressNodePtr DestinationNode;

        void Persist(const NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, TrunkSourceNode);
            Persist(context, DestinationNode);
        }
    };

    // NB: See PrepareCreateNode.
    void HydraPrepareCloneNode(
        TTransaction* /*transaction*/,
        TReqCloneNode* request,
        TCloneNodeActionState* state,
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
        auto* trunkSourceNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(sourceNodeId));
        state->TrunkSourceNode.Assign(trunkSourceNode);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        // Maybe this is excessive.
        auto type = trunkSourceNode->GetType();
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
        auto* sourceNode = cypressManager->GetVersionedNode(trunkSourceNode, cypressTransaction);
        auto* destinationNode = nodeFactory->CloneNode(sourceNode, mode, emptyInheritedAttributes.Get(), destinationNodeId);
        state->DestinationNode.Assign(destinationNode);

        PrepareSequoiaNodeCreation(
            destinationNode,
            destinationParentId,
            NYPath::DirNameAndBaseName(request->dst_path()).second,
            request->dst_path());

        nodeFactory->Commit();
    }

    void HydraCommitCloneNode(
        TTransaction* /*transaction*/,
        NProto::TReqCloneNode* /*request*/,
        TCloneNodeActionState* state,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        // TODO(danilalexeev): Sequoia transaction has to lock |sourceNodeId| to conflict
        // with an expired nodes removal.
        if (auto* trunkSourceNode = state->TrunkSourceNode.Get(); IsObjectAlive(trunkSourceNode)) {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            MaybeTouchNode(cypressManager, trunkSourceNode);
        }

        CommitSequoiaNodeCreation(state->DestinationNode);
    }

    void HydraPrepareAndCommitExplicitlyLockNode(
        TTransaction* /*transaction*/,
        NProto::TReqExplicitlyLockNode* request,
        const TTransactionPrepareOptions& options)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_LOG_ALERT_AND_THROW_UNLESS(
            options.LatePrepare,
            "An attempt to explicitly lock node with non late prepare mode was made (NodeId: %v, TransactionId: %v)",
            nodeId,
            cypressTransactionId);

        YT_LOG_ALERT_AND_THROW_UNLESS(
            cypressTransactionId,
            "Received a request to explicitly lock node with unspecified Cypress transaction (NodeId: %v)",
            nodeId);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
        VerifySequoiaNode(trunkNode);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransactionOrThrow(cypressTransactionId);

        auto mode = FromProto<ELockMode>(request->mode());
        auto childKey = YT_OPTIONAL_FROM_PROTO(*request, child_key);
        auto attributeKey = YT_OPTIONAL_FROM_PROTO(*request, attribute_key);
        auto timestamp = request->timestamp();
        bool waitable = request->waitable();

        YT_LOG_ALERT_AND_THROW_UNLESS(
            CheckLockRequest(mode, childKey, attributeKey).IsOK(),
            "Explicit lock request is malformed "
            "(NodeId: %v, TransactionId: %v, Mode: %v, ChildKey: %v, AttributeKey: %v)",
            nodeId,
            cypressTransactionId,
            mode,
            childKey,
            attributeKey);

        auto proxy = cypressManager->GetNodeProxy(trunkNode, cypressTransaction);
        auto lockId = FromProto<TLockId>(request->lock_id());
        auto lockRequest = CreateLockRequest(mode, childKey, attributeKey, timestamp);
        proxy->Lock(lockRequest, waitable, lockId);

        MaybeTouchNode(cypressManager, trunkNode);
    }

    void HydraPrepareImplicitlyLockNode(
        TTransaction* /*transaction*/,
        NProto::TReqImplicitlyLockNode* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
        VerifySequoiaNode(trunkNode);

        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_LOG_ALERT_AND_THROW_UNLESS(
            cypressTransactionId,
            "Received a request to implicitly lock node with unspecified Cypress transaction (NodeId: %v)",
            nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(cypressTransactionId);

        auto mode = FromProto<ELockMode>(request->mode());
        auto childKey = YT_OPTIONAL_FROM_PROTO(*request, child_key);
        auto attributeKey = YT_OPTIONAL_FROM_PROTO(*request, attribute_key);

        auto lockRequest = CreateLockRequest(
            mode,
            childKey,
            attributeKey,
            /*timestamp*/ 0);
        cypressManager->CheckLock(trunkNode, transaction, lockRequest).ThrowOnError();
    }

    void HydraCommitImplicitlyLockNode(
        TTransaction* /*transaction*/,
        NProto::TReqImplicitlyLockNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNode(TVersionedNodeId(nodeId));

        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransaction(cypressTransactionId);

        auto mode = FromProto<ELockMode>(request->mode());
        auto childKey = YT_OPTIONAL_FROM_PROTO(*request, child_key);
        auto attributeKey = YT_OPTIONAL_FROM_PROTO(*request, attribute_key);

        auto lockRequest = CreateLockRequest(
            mode,
            childKey,
            attributeKey,
            /*timestamp*/ 0);
        cypressManager->LockNode(trunkNode, cypressTransaction, lockRequest);
    }

    void HydraPrepareAndCommitUnlockNode(
        TTransaction* /*transaction*/,
        NProto::TReqUnlockNode* request,
        const TTransactionPrepareOptions& options)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_LOG_ALERT_AND_THROW_UNLESS(
            options.LatePrepare,
            "An attempt to unlock node with non late prepare mode was made (NodeId: %v, TransactionId: %v)",
            nodeId,
            cypressTransactionId);

        YT_LOG_ALERT_AND_THROW_UNLESS(
            cypressTransactionId,
            "Received a request to unlock node with unspecified Cypress transaction (NodeId: %v)",
            nodeId);

        const auto& cypressManager = Bootstrap_->GetCypressManager();

        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
        VerifySequoiaNode(trunkNode);


        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransaction(cypressTransactionId);

        auto proxy = cypressManager->GetNodeProxy(trunkNode, cypressTransaction);
        proxy->Unlock();

        MaybeTouchNode(cypressManager, trunkNode);
    }

    void HydraPrepareAndCommitMaterializeNode(
        TTransaction* /*transaction*/,
        NProto::TReqMaterializeNode* request,
        const TTransactionPrepareOptions& options)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_LOG_ALERT_AND_THROW_UNLESS(
            options.LatePrepare,
            "An attempt to materialize node with non late prepare mode was made (NodeId: %v, TransactionId: %v)",
            nodeId,
            cypressTransactionId);

        YT_LOG_ALERT_AND_THROW_UNLESS(
            cypressTransactionId,
            "Received a request to materialize node with unspecified Cypress transaction (NodeId: %v)",
            nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransactionOrThrow(cypressTransactionId);

        auto version = request->version();
        if (version != NCellMaster::GetCurrentReign()) {
            THROW_ERROR_EXCEPTION("Invalid node metadata format version: expected %v, actual %v",
                NCellMaster::GetCurrentReign(),
                version);
        }

        // Presence of a new account means that user does not want to preserve an old one.
        auto newAccountId = request->has_new_account_id()
            ? std::make_optional(FromProto<TAccountId>(request->new_account_id()))
            : std::nullopt;
        TAccount* account = nullptr;
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        if (newAccountId) {
            account = securityManager->GetAccountOrThrow(*newAccountId, /*activeLifeStageOnly*/ true);
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->ValidateObjectLifeStage(account);
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto factory = cypressManager->CreateNodeFactory(
            /*shard*/ nullptr, // Shard will be set when assembling the tree.
            cypressTransaction,
            account,
            TNodeFactoryOptions{
                .PreserveAccount = !newAccountId.has_value(),
                .PreserveCreationTime = request->preserve_creation_time(),
                .PreserveModificationTime = true, // Modification time will be changed when assembling subtree, if needed.
                .PreserveExpirationTime = request->preserve_expiration_time(),
                .PreserveExpirationTimeout = request->preserve_expiration_timeout(),
                .PreserveOwner = request->preserve_owner(),
                .PreserveAcl = true, // Same as modification time.
                .PessimisticQuotaCheck = false // This is checked on the cypress proxy.
            },
            /*serviceTrunkNode*/ nullptr,
            /*unresolvedSuffix*/ {}); // Unused during copy, relevant only for "create".

        const auto& serializedNode = request->serialized_node();
        TMaterializeNodeContext copyContext(
            Bootstrap_,
            FromProto<ENodeCloneMode>(request->mode()),
            TRef::FromString(serializedNode.data()),
            FromProto<TNodeId>(request->node_id()),
            /*materializeAsSequoiaNode*/ true,
            FromProto<TMasterTableSchemaId>(serializedNode.schema_id()),
            FromProto<TNodeId>(request->existing_node_id()));

        auto inheritedAttributes = request->has_inherited_attributes_override()
            ? FromProto(request->inherited_attributes_override())
            : New<TInheritedAttributeDictionary>(Bootstrap_);

        auto* node = factory->MaterializeNode(inheritedAttributes.Get(), &copyContext);

        // It's important to respect the invariant that each Sequoia node has mutable properties initialized.
        CreateSequoiaPropertiesForMaterializedNode(node);

        // TODO(h0pless): When expanding list of inherited attributes re-calculated during copy, some trivial
        // setter code should be written somewhere here.
        // Since only chunk_merger_mode is supported right now it's fine to leave it as-is.
        factory->Commit();
    }

    /*
     * This function changes BeingCreated without late prepare option.
     * This can go bad because of two things:
     * 1. User could read this mostly-but-not-quite created node before this function.
     *    This is not possible because Cypress to Sequoia copy requests are always executed
     *    under a transaction. Said transaction is internal, and client does not know its ID.
     *    Additionally, the copy process involves creation of new nodes under aforementioned
     *    transaction, which makes it impossible for client to access before commit.
     *
     * 2. Currently Sequoia session replies to user when all participants are prepared,
     *    but before they commit. This is also not possible.
     *    See TTransactionSupervisor::WaitUntilPreparedTransactionsFinished().
     */
    void HydraPrepareAndCommitFinishNodeMaterialization(
        TTransaction* /*transaction*/,
        NProto::TReqFinishNodeMaterialization* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_LOG_ALERT_AND_THROW_UNLESS(
            cypressTransactionId,
            "Received a request to finish node materialization with unspecified Cypress transaction (NodeId: %v)",
            nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransactionOrThrow(cypressTransactionId);

        auto preserveAcl = request->preserve_acl();
        auto preserveModificationTime = request->preserve_modification_time();
        auto path = request->path();
        auto parentId = FromProto<TNodeId>(request->parent_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNode(TVersionedNodeId(nodeId));
        auto* node = cypressManager->GetVersionedNode(trunkNode, cypressTransaction);
        if (!preserveAcl) {
            // Acls are always preserved during materialization.
            trunkNode->Acd().ClearEntries();
        }

        if (!preserveModificationTime) {
            // Using this to avoid writing "Revised" to the access log.
            // It's actually probably fine to just use cypressManager->SetModified.
            node->SetModified(EModificationType::Content);
        }

        FinishSequoiaNodeMaterialization(node, parentId, path);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(TBootstrap* bootstrap)
{
    return New<TSequoiaActionsExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
