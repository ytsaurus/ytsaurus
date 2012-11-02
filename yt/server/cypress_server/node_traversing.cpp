#include "stdafx.h"
#include "node_traversing.h"

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

// Extract to private.
static NLog::TLogger Logger("Cypress");

////////////////////////////////////////////////////////////////////////////////

namespace {

static const int MaxNodeCountPerAction = 1000;

class TNodeTraverser 
    : public TRefCounted
{
private:
    struct TStackEntry {
        TNodeId Id;
        int ChildIndex;
        std::vector<TNodeId> Children;

        TStackEntry(const TNodeId& nodeId)
        : Id(nodeId)
        , ChildIndex(0)
        { }

        void NextChild()
        {
            ++ChildIndex;
        }
    };

    void VisitNode(ICypressNodeProxyPtr nodeProxy)
    {
        Visitor->OnNode(nodeProxy);
        auto id = nodeProxy->GetId();

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
            // do nothing
            break;
        };
    }

    void DoTraverse()
    {
        TTransaction* transaction = NULL;

        if (TransactionId != NullTransactionId) {
            transaction = Bootstrap->GetTransactionManager()->GetTransaction(TransactionId);
            if (!transaction) {
                Visitor->OnError(TError(
                    "Transaction does not exist (TransactionId: %s)", 
                    ~TransactionId.ToString()));
                return;
            }
        }

         int currentNodeCount = 0;
         while (currentNodeCount < MaxNodeCountPerAction) {
            YASSERT(!Stack.empty());
            auto& entry = Stack.back();
            auto childIndex = entry.ChildIndex;
            entry.NextChild();

            if (childIndex >= entry.Children.size()) {
                Stack.pop_back();
                if (Stack.empty()) {
                    Visitor->OnCompleted();
                    return;
                }
                continue;
            } else {
                auto& nodeId = entry.Children[childIndex];
                auto nodeProxy = Bootstrap->GetCypressManager()->FindVersionedNodeProxy(nodeId, transaction);

                VisitNode(nodeProxy);
                ++currentNodeCount;
            }
         }

         auto res = Bootstrap->GetMetaStateFacade()->GetGuardedInvoker()->Invoke(BIND(
            &TNodeTraverser::DoTraverse,
            MakeStrong(this)));

         if (!res) {
            Visitor->OnError(TError("Yeild error."));
         }
    }

    TBootstrap* Bootstrap;
    TTransactionId TransactionId;

    std::vector<TStackEntry> Stack;

    // Used to determine cyclic references.
    yhash_set<TNodeId> VisitedNodes;

    ICypressNodeVisitorPtr Visitor;

public:
    TNodeTraverser(TBootstrap* bootstrap, ICypressNodeVisitorPtr visitor)
        : Bootstrap(bootstrap)
        , TransactionId(NullTransactionId)
        , Visitor(visitor)
    {  }

    void Run(ICypressNodeProxyPtr nodeProxy) 
    {
        auto* tx = nodeProxy->GetTransaction();
        if (tx) {
            TransactionId = tx->GetId();
        }

        LOG_DEBUG("Run cypress node traverser (RootNodeId: %s, TransactionId: %s)",
            ~nodeProxy->GetId().ToString(),
            ~TransactionId.ToString());

        VisitNode(nodeProxy);
        DoTraverse();
    };

};

}

////////////////////////////////////////////////////////////////////////////////

void TraverseSubtree(
    NCellMaster::TBootstrap* bootstrap, 
    ICypressNodeProxyPtr nodeProxy, 
    ICypressNodeVisitorPtr visitor)
{
    auto traverser = New<TNodeTraverser>(bootstrap, visitor);
    traverser->Run(nodeProxy);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT