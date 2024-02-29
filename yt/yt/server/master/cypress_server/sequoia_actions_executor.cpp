#include "sequoia_actions_executor.h"

#include "cypress_manager.h"
#include "node_proxy.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

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
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareSetNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitSetNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqCloneNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareCloneNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitCloneNode, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraAbortCloneNode, Unretained(this)),
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
            type == EObjectType::SequoiaMapNode ||
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

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto roles = multicellManager->GetMasterCellRoles(multicellManager->GetCellTag());
        if (None(roles & EMasterCellRoles::SequoiaNodeHost) && IsSequoiaId(nodeId)) {
            THROW_ERROR_EXCEPTION("This cell cannot host Sequoia nodes");
        }

        auto type = CheckedEnumCast<EObjectType>(request->type());
        if (!CanCreateSequoiaType(type)) {
            THROW_ERROR_EXCEPTION("Type %Qlv is not supported in Sequoia", type);
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto nodeFactory = cypressManager->CreateNodeFactory(
            cypressManager->GetRootCypressShard(),
            /*transaction*/ nullptr,
            securityManager->GetSysAccount(),
            /*options*/ {});
        // TODO(kvk1920): Remove const cast.
        auto* attributes = &NYTree::EmptyAttributes();
        auto* node = nodeFactory->CreateNode(
            type,
            nodeId,
            const_cast<NYTree::IAttributeDictionary*>(attributes),
            const_cast<NYTree::IAttributeDictionary*>(attributes))->GetTrunkNode();

        TCypressNode::TImmutableSequoiaProperties immutableProperties(
            /*key*/ NYPath::DirNameAndBaseName(request->path()).second,
            /*path*/ request->path());

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

        auto node = Bootstrap_->GetCypressManager()->GetNode(versionedNodeId);
        Bootstrap_->GetObjectManager()->UnrefObject(node);
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

        VerifySequoiaNode(parent);
        auto beingCreated = parent->MutableSequoiaProperties()->BeingCreated;
        THROW_ERROR_EXCEPTION_IF(
            !beingCreated && !options.LatePrepare,
            "Operation is not atomic for user");
    }

    void HydraCommitAttachChild(
        TTransaction* /*transaction*/,
        NProto::TReqAttachChild* request,
        const TTransactionCommitOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

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
            YT_LOG_FATAL("Sequoia map node has no such child (ParentId: %v, Key: %v)",
                parentId,
                request->key());
            return;
        }

        children.Remove(request->key(), childIt->second);
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
        cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
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
        } else {
            Bootstrap_->GetObjectManager()->UnrefObject(node);
        }
    }

    void HydraPrepareSetNode(
        TTransaction* /*transaction*/,
        NProto::TReqSetNode* request,
        const TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));

        auto type = node->GetType();
        if (!IsScalarType(type)) {
            THROW_ERROR_EXCEPTION("Invalid set request for node %v", nodeId);
        }
    }

    void HydraCommitSetNode(
        TTransaction* /*transaction*/,
        NProto::TReqSetNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->GetNode(TVersionedObjectId(nodeId));

        if (!IsObjectAlive(node)) {
            YT_LOG_ALERT(
                "Attempted to set unexisting Sequoia node; ignored "
                "(NodeId: %v)",
                nodeId);
            return;
        }

        YT_VERIFY(IsScalarType(node->GetType()));
        VerifySequoiaNode(node);

        SetNodeFromProducer(
            cypressManager->GetNodeProxy(node),
            ConvertToProducer(TYsonString(request->value())),
            /*builder*/ nullptr);
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

        // TODO(h0pless): Think about trowing an error if this cell is not sequoia_node_host anymore.
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* sourceNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(sourceNodeId));

        // Maybe this is excessive.
        auto type = sourceNode->GetType();
        if (!CanCreateSequoiaType(type)) {
            THROW_ERROR_EXCEPTION("Type %Qlv is not supported in Sequoia", type);
        }

        auto requestOptions = request->mutable_options();
        auto mode = CheckedEnumCast<ENodeCloneMode>(requestOptions->mode());
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

        auto* clonedNode = nodeFactory->CloneNode(sourceNode, mode, destinationNodeId);

        TCypressNode::TImmutableSequoiaProperties immutableProperties(
            /*key*/ NYPath::DirNameAndBaseName(request->dst_path()).second,
            /*path*/ request->dst_path());

        clonedNode->ImmutableSequoiaProperties() = std::make_unique<TCypressNode::TImmutableSequoiaProperties>(std::move(immutableProperties));
        clonedNode->MutableSequoiaProperties() = std::make_unique<TCypressNode::TMutableSequoiaProperties>();
        *clonedNode->MutableSequoiaProperties() = {.BeingCreated = true};

        clonedNode->VerifySequoia();
        clonedNode->RefObject();
        nodeFactory->Commit();
    }

    void HydraCommitCloneNode(
        TTransaction* /*transaction*/,
        NProto::TReqCloneNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->dst_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->GetNode(TVersionedNodeId(nodeId));

        if (!IsObjectAlive(node)) {
            YT_LOG_ALERT("An attempt to clone a zombie Sequoia node was made (NodeId: %v)",
                nodeId);
        }

        VerifySequoiaNode(node);

        auto beingCreated = std::exchange(node->MutableSequoiaProperties()->BeingCreated, false);
        YT_VERIFY(beingCreated);
    }

    void HydraAbortCloneNode(
        TTransaction* /*transaction*/,
        NProto::TReqCloneNode* request,
        const TTransactionAbortOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->dst_id());
        // TODO(h0pless): Add cypress transaction id here.
        auto versionedNodeId = TVersionedObjectId(nodeId, NullObjectId);

        auto node = Bootstrap_->GetCypressManager()->GetNode(versionedNodeId);
        Bootstrap_->GetObjectManager()->UnrefObject(node);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(TBootstrap* bootstrap)
{
    return New<TSequoiaActionsExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
