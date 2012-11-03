#include "stdafx.h"
#include "cypress_traversing.h"

#include <ytlib/ytree/public.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/transaction_server/transaction_manager.h>
#include <server/transaction_server/transaction.h>

#include <server/cypress_server/node_proxy.h>
#include <server/cypress_server/cypress_manager.h>

namespace NYT {
namespace NCypressServer {

using NCellMaster::TBootstrap;
using namespace NYTree;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

namespace {

static const int MaxNodesPerAction = 1000;

class TNodeTraverser 
    : public TRefCounted
{
private:
    struct TStackEntry
    {
        TNodeId Id;
        int ChildIndex;
        std::vector<TNodeId> Children;

        TStackEntry(const TNodeId& nodeId)
            : Id(nodeId)
            , ChildIndex(0)
        { }
    };

    void VisitNode(ICypressNodeProxyPtr nodeProxy)
    {
        Visitor->OnNode(nodeProxy);
        const auto& id = nodeProxy->GetId();

        YCHECK(VisitedNodes.insert(id).second);
        Stack.push_back(TStackEntry(id));
        auto &entry = Stack.back();

        switch (nodeProxy->GetType()) {
            case ENodeType::Map: {
                auto map = nodeProxy->AsMap();
                auto children = map->GetChildren();

                // Dump children node ids and thereby determine order on children.
                // After that use ChildIndex to maintain traversal order.
                FOREACH (auto& pair, children) {
                    auto* childProxy = dynamic_cast<ICypressNodeProxy*>(~pair.second);
                    YCHECK(childProxy);
                    entry.Children.push_back(childProxy->GetId());
                }
                break;
            }

            case ENodeType::List: {
                auto list = nodeProxy->AsList();
                auto children = list->GetChildren();

                // Dump children node ids and thereby determine order on children.
                // After that use ChildIndex to maintain traversal order.
                FOREACH (auto& node, children) {
                    auto* childProxy = dynamic_cast<ICypressNodeProxy*>(~node);
                    YCHECK(childProxy);
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
        auto transactionManager = Bootstrap->GetTransactionManager();
        auto cypressManager = Bootstrap->GetCypressManager();

        TTransaction* transaction = NULL;

        if (TransactionId != NullTransactionId) {
            transaction = transactionManager->FindTransaction(TransactionId);
            if (!transaction) {
                Visitor->OnError(TError("No such transaction: %s", 
                    ~TransactionId.ToString()));
                return;
            }
        }

         int currentNodeCount = 0;
         while (currentNodeCount < MaxNodesPerAction) {
            YASSERT(!Stack.empty());
            auto& entry = Stack.back();
            auto childIndex = entry.ChildIndex++;

            if (childIndex >= entry.Children.size()) {
                Stack.pop_back();
                if (Stack.empty()) {
                    Visitor->OnCompleted();
                    return;
                }
                continue;
            } else {
                const auto& nodeId = entry.Children[childIndex];
                auto nodeProxy = cypressManager->FindVersionedNodeProxy(nodeId, transaction);

                VisitNode(nodeProxy);
                ++currentNodeCount;
            }
         }

         // Schedule continuation.
         {
             auto invoker = Bootstrap->GetMetaStateFacade()->GetGuardedInvoker();
             auto result = invoker->Invoke(BIND(&TNodeTraverser::DoTraverse, MakeStrong(this)));
             if (!result) {
                 Visitor->OnError(TError("Yield error"));
             }
         }
    }

    TBootstrap* Bootstrap;
    ICypressNodeVisitorPtr Visitor;

    TTransactionId TransactionId;

    std::vector<TStackEntry> Stack;

    // Used to determine cyclic references.
    yhash_set<TNodeId> VisitedNodes;


public:
    TNodeTraverser(TBootstrap* bootstrap, ICypressNodeVisitorPtr visitor)
        : Bootstrap(bootstrap)
        , TransactionId(NullTransactionId)
        , Visitor(visitor)
    {  }

    void Run(ICypressNodeProxyPtr rootNode) 
    {
        auto* transaction = rootNode->GetTransaction();
        TransactionId = transaction ? transaction->GetId() : NullTransactionId;
        VisitNode(rootNode);
        DoTraverse();
    };

};

}

////////////////////////////////////////////////////////////////////////////////

void TraverseCypress(
    NCellMaster::TBootstrap* bootstrap, 
    ICypressNodeProxyPtr rootNode, 
    ICypressNodeVisitorPtr visitor)
{
    auto traverser = New<TNodeTraverser>(bootstrap, visitor);
    traverser->Run(rootNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT