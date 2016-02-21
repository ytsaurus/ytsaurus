#include "cypress_traversing.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/cypress_server/cypress_manager.h>
#include <yt/server/cypress_server/node_proxy.h>

#include <yt/server/transaction_server/transaction.h>
#include <yt/server/transaction_server/transaction_manager.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const int MaxNodesPerStep = 1000;

////////////////////////////////////////////////////////////////////////////////

class TNodeTraverser
    : public TRefCounted
{
public:
    TNodeTraverser(
        TBootstrap* bootstrap,
        ICypressNodeVisitorPtr visitor,
        ICypressNodeProxyPtr rootNode)
        : Bootstrap_(bootstrap)
        , Visitor_(std::move(visitor))
        , RootNode_(std::move(rootNode))
        , Invoker_(Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::CypressTraverser))
        , TransactionId_(GetObjectId(RootNode_->GetTransaction()))
    { }

    void Run()
    {
        VisitNode(RootNode_);
        DoTraverse();
    }

private:
    TBootstrap* const Bootstrap_;
    const ICypressNodeVisitorPtr Visitor_;
    const ICypressNodeProxyPtr RootNode_;

    const IInvokerPtr Invoker_;
    const TTransactionId TransactionId_;

    struct TStackEntry
    {
        TNodeId Id;
        int ChildIndex = 0;
        std::vector<TNodeId> Children;

        explicit TStackEntry(const TNodeId& nodeId)
            : Id(nodeId)
        { }
    };

    std::vector<TStackEntry> Stack_;

    // Used to determine cyclic references.
    yhash_set<TNodeId> VisitedNodes_;


    void VisitNode(ICypressNodeProxyPtr nodeProxy)
    {
        Visitor_->OnNode(nodeProxy);
        const auto& id = nodeProxy->GetId();

        YCHECK(VisitedNodes_.insert(id).second);
        Stack_.push_back(TStackEntry(id));
        auto& entry = Stack_.back();

        switch (nodeProxy->GetType()) {
            case ENodeType::Map: {
                auto map = nodeProxy->AsMap();
                auto children = map->GetChildren();

                // Dump children node ids and thereby determine order on children.
                // After that use ChildIndex to maintain traversal order.
                for (auto& pair : children) {
                    auto* childProxy = ICypressNodeProxy::FromNode(pair.second.Get());
                    entry.Children.push_back(childProxy->GetId());
                }
                break;
            }

            case ENodeType::List: {
                auto list = nodeProxy->AsList();
                auto children = list->GetChildren();

                // Dump children node ids and thereby determine order on children.
                // After that use ChildIndex to maintain traversal order.
                for (auto& node : children) {
                    auto* childProxy = ICypressNodeProxy::FromNode(node.Get());
                    entry.Children.push_back(childProxy->GetId());
                }
                break;
            }

            default:
                // Do nothing.
                break;
        };
    }

    void DoTraverse()
    {
        try {
            auto transactionManager = Bootstrap_->GetTransactionManager();
            auto cypressManager = Bootstrap_->GetCypressManager();

            auto* transaction = TransactionId_
                ? transactionManager->GetTransactionOrThrow(TransactionId_)
                : nullptr;

            int currentNodeCount = 0;
            while (currentNodeCount < MaxNodesPerStep) {
                YASSERT(!Stack_.empty());
                auto& entry = Stack_.back();
                auto childIndex = entry.ChildIndex++;
                if (childIndex >= entry.Children.size()) {
                    Stack_.pop_back();
                    if (Stack_.empty()) {
                        Visitor_->OnCompleted();
                        return;
                    }
                } else {
                    const auto& nodeId = entry.Children[childIndex];
                    auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
                    auto nodeProxy = cypressManager->GetNodeProxy(trunkNode, transaction);
                    VisitNode(nodeProxy);
                    ++currentNodeCount;
                }
            }

            // Schedule continuation.
            Invoker_->Invoke(BIND(&TNodeTraverser::DoTraverse, MakeStrong(this)));
        } catch (const std::exception& ex) {
            Visitor_->OnError(ex);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void TraverseCypress(
    NCellMaster::TBootstrap* bootstrap,
    ICypressNodeProxyPtr rootNode,
    ICypressNodeVisitorPtr visitor)
{
    New<TNodeTraverser>(bootstrap, visitor, rootNode)->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
