#include "sequoia_actions_executor.h"

#include "cypress_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/lib/transaction_supervisor/helpers.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/core/ypath/helpers.h>

namespace NYT::NCypressServer {

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
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitAttachChild, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqAttachChild, const TTransactionCommitOptions&>()),
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

        auto type = CheckedEnumCast<EObjectType>(request->type());
        if (type != EObjectType::SequoiaMapNode) {
            THROW_ERROR_EXCEPTION("Type %Qv is not supported in Sequoia", type);
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto nodeFactory = cypressManager->CreateNodeFactory(
            cypressManager->GetRootCypressShard(),
            /*transaction*/ nullptr,
            securityManager->GetSysAccount(),
            /*options*/ {});
        auto nodeId = FromProto<TNodeId>(request->node_id());
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
        };

        node->VerifySequoia();
        node->RefObject();

        nodeFactory->Commit();
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

    void HydraPrepareAndCommitAttachChild(
        TTransaction* /*transaction*/,
        NProto::TReqAttachChild* request,
        const TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);
        YT_VERIFY(options.LatePrepare);

        // AttachChild should be _atomic_ for user. To achive this we use late
        // prepare and choose parent's native cell as tx coordinator.

        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto childId = FromProto<TNodeId>(request->child_id());
        auto key = request->key();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* parent = cypressManager->GetNode(TVersionedNodeId(parentId));

        VerifySequoiaNode(parent);
        YT_VERIFY(
            parent->GetType() == EObjectType::SequoiaMapNode ||
            parent->GetType() == EObjectType::Scion);

        auto& children = parent->As<TSequoiaMapNode>()->MutableChildren();
        if (children.Contains(key)) {
            THROW_ERROR_EXCEPTION("Sequoia map node already has such child: %Qv", key);
        }

        children.Insert(key, childId);
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
        auto parentProxy = cypressManager->GetNodeProxy(parent)->AsMap();
        parentProxy->RemoveChild(request->key());
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
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(TBootstrap* bootstrap)
{
    return New<TSequoiaActionsExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
