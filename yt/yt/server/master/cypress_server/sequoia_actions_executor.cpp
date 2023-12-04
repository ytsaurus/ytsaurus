#include "sequoia_actions_executor.h"
#include "cypress_manager.h"
#include "node_proxy.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/lib/transaction_supervisor/helpers.h>

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
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareCreateNode, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqCreateNode, const TTransactionCommitOptions&>()),
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraAbortCreateNode, Unretained(this))));
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAttachChild, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitAttachChild, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqAttachChild, const TTransactionAbortOptions&>()));
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareRemoveNode, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitRemoveNode, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqRemoveNode, const TTransactionAbortOptions&>()));
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqDetachChild, const TTransactionPrepareOptions&>()),
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitDetachChild, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqDetachChild, const TTransactionAbortOptions&>()));
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareSetNode, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitSetNode, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqSetNode, const TTransactionAbortOptions&>()));
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

    // NB: Sequoia node has to be created in prepare phase since we cannot
    // guarantee that object id will not be used between prepare and commit.
    // It's ok because this node cannot be reached from any other node until
    // sequoia tx is committed: AttachChild for created subtree's root is
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
        if (type != EObjectType::SequoiaMapNode && !IsScalarType(type)) {
            THROW_ERROR_EXCEPTION("Type %Qv is not supported in Sequoia", type);
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

        node->SequoiaProperties() = std::make_unique<TCypressNode::TSequoiaProperties>();
        *node->SequoiaProperties() = {
            .Key = NYPath::DirNameAndBaseName(request->path()).second,
            .Path = request->path(),
            .BeingCreated = true,
        };

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

        auto& beingCreated = node->SequoiaProperties()->BeingCreated;
        YT_VERIFY(beingCreated);
        beingCreated = false;
    }

    void HydraAbortCreateNode(
        TTransaction* /*transaction*/,
        NProto::TReqCreateNode* request,
        const TTransactionAbortOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->node_id());

        const auto& nodes = Bootstrap_->GetCypressManager()->Nodes();

        // TODO(kvk1920): Don't execute tx abort if prepare has not happen.
        if (auto* node = nodes.Find(TVersionedNodeId(nodeId))) {
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

        VerifySequoiaNode(parent);
        auto beingCreated = parent->SequoiaProperties()->BeingCreated;
        THROW_ERROR_EXCEPTION_IF(
            !beingCreated && !options.LatePrepare,
            "Operation is not atomic for user");
    }

    void HydraCommitAttachChild(
        TTransaction* /*transaction*/,
        NProto::TReqAttachChild* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto childId = FromProto<TNodeId>(request->child_id());
        auto key = request->key();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* parent = cypressManager->GetNode(TVersionedNodeId(parentId));

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

        // This is temporary logic.
        // TODO(cherepashka): In future DetachChild should remove child from parent node proxy.
        auto& children = parent->As<TSequoiaMapNode>()->MutableChildren();
        if (!children.Contains(request->key())) {
            YT_LOG_FATAL("Missing sequoia map node has no such child (Key: %v)", request->key());
        }
        auto childIt = children.KeyToChild().find(request->key());
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
                "Attempted to remove unexisting sequoia node; ignored "
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
                "Attempted to set unexisting sequoia node; ignored "
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
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(TBootstrap* bootstrap)
{
    return New<TSequoiaActionsExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
