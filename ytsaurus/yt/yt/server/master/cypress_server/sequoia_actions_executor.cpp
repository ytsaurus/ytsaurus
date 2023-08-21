#include "sequoia_actions_executor.h"

#include "cypress_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/lib/transaction_supervisor/helpers.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;

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
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqCreateNode, const NTransactionSupervisor::TTransactionCommitOptions&>()),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqCreateNode, const NTransactionSupervisor::TTransactionAbortOptions&>()));
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAttachChild, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqAttachChild, const NTransactionSupervisor::TTransactionCommitOptions&>()),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, NProto::TReqAttachChild, const NTransactionSupervisor::TTransactionAbortOptions&>()));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void HydraPrepareCreateNode(
        TTransaction* /*transaction*/,
        NProto::TReqCreateNode* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);
        YT_VERIFY(options.LatePrepare);
        YT_VERIFY(Bootstrap_->IsPrimaryMaster());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto nodeFactory = cypressManager->CreateNodeFactory(
            cypressManager->GetRootCypressShard(),
            /*transaction*/ nullptr,
            securityManager->GetSysAccount(),
            /*options*/ {});
        auto type = CheckedEnumCast<EObjectType>(request->type());
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto* attributes = &NYTree::EmptyAttributes();
        nodeFactory->CreateNode(
            type,
            nodeId,
            const_cast<NYTree::IAttributeDictionary*>(attributes),
            const_cast<NYTree::IAttributeDictionary*>(attributes));
        nodeFactory->Commit();
    }

    void HydraPrepareAttachChild(
        TTransaction* /*transaction*/,
        NProto::TReqAttachChild* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);
        YT_VERIFY(options.LatePrepare);
        YT_VERIFY(Bootstrap_->IsPrimaryMaster());

        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto childId = FromProto<TNodeId>(request->child_id());
        auto key = request->key();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* parent = cypressManager->GetNode(TVersionedNodeId(parentId));
        auto parentProxy = cypressManager->GetNodeProxy(parent);
        auto* child = cypressManager->GetNode(TVersionedNodeId(childId));
        auto childProxy = cypressManager->GetNodeProxy(child);

        parentProxy->SetChildNode(
            /*nodeFactory*/ nullptr,
            "/" + key,
            childProxy,
            /*recursive*/ false);
        child->SetParent(parent);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(TBootstrap* bootstrap)
{
    return New<TSequoiaActionsExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
