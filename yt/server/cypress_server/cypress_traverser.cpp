#include "cypress_traverser.h"
#include "node_detail.h"

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/transaction_server/transaction.h>
#include <yt/server/transaction_server/transaction_manager.h>

#include <yt/server/object_server/object_manager.h>

#include <yt/core/ytree/public.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const int MaxNodesPerIteration = 1000;

////////////////////////////////////////////////////////////////////////////////

class TCypressTraverser
    : public TRefCounted
{
public:
    TCypressTraverser(
        TCypressManagerPtr cypressManager,
        TTransactionManagerPtr transactionManager,
        TObjectManagerPtr objectManager,
        IInvokerPtr invoker,
        ICypressNodeVisitorPtr visitor,
        TCypressNodeBase* trunkRootNode,
        TTransaction* transaction)
        : CypressManager_(std::move(cypressManager))
        , TransactionManager_(std::move(transactionManager))
        , ObjectManager_(std::move(objectManager))
        , Invoker_(std::move(invoker))
        , Visitor_(std::move(visitor))
        , Transaction_(transaction)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);
        VERIFY_THREAD_AFFINITY(Automaton);

        if (Transaction_) {
            ObjectManager_->WeakRefObject(Transaction_);
        }
        PushEntry(trunkRootNode);
    }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Automaton);

        DoTraverse();
    }

private:
    const TCypressManagerPtr CypressManager_;
    const TTransactionManagerPtr TransactionManager_;
    const TObjectManagerPtr ObjectManager_;
    const IInvokerPtr Invoker_;
    const ICypressNodeVisitorPtr Visitor_;
    TTransaction* const Transaction_;

    DECLARE_THREAD_AFFINITY_SLOT(Automaton);


    struct TStackEntry
    {
        TCypressNodeBase* TrunkNode;
        int ChildIndex = -1; // -1 means the node itself
        std::vector<TCypressNodeBase*> TrunkChildren;

        explicit TStackEntry(TCypressNodeBase* trunkNode)
            : TrunkNode(trunkNode)
        { }
    };

    std::vector<TStackEntry> Stack_;


    void ReleaseEntry(const TStackEntry& entry)
    {
        ObjectManager_->WeakUnrefObject(entry.TrunkNode);
        for (auto* child : entry.TrunkChildren) {
            ObjectManager_->WeakUnrefObject(child);
        }
    }

    void AddEntryChild(TStackEntry* entry, TCypressNodeBase* trunkChild)
    {
        ObjectManager_->WeakRefObject(trunkChild);
        entry->TrunkChildren.push_back(trunkChild);
    }

    void PushEntry(TCypressNodeBase* trunkNode)
    {
        ObjectManager_->WeakRefObject(trunkNode);
        Stack_.push_back(TStackEntry(trunkNode));

        auto& entry = Stack_.back();

        switch (trunkNode->GetNodeType()) {
            case ENodeType::Map: {
                auto keyToChild = GetMapNodeChildren(CypressManager_, trunkNode, Transaction_);
                for (const auto& pair : keyToChild) {
                    AddEntryChild(&entry, pair.second);
                }
                break;
            }

            case ENodeType::List: {
                const auto& children = GetListNodeChildren(CypressManager_, trunkNode, Transaction_);
                for (auto* child : children) {
                    AddEntryChild(&entry, child);
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
        VERIFY_THREAD_AFFINITY(Automaton);

        try {
            if (Transaction_ && !IsObjectAlive(Transaction_)) {
                THROW_ERROR_EXCEPTION("Transaction %v no longer exists",
                    Transaction_->GetId());
            }

            int currentNodeCount = 0;
            while (currentNodeCount < MaxNodesPerIteration && !Stack_.empty()) {
                auto& entry = Stack_.back();
                auto childIndex = entry.ChildIndex++;
                if (childIndex < 0) {
                    if (IsObjectAlive(entry.TrunkNode)) {
                        Visitor_->OnNode(entry.TrunkNode, Transaction_);
                    }
                    ++currentNodeCount;
                } else if (childIndex < entry.TrunkChildren.size()) {
                    PushEntry(entry.TrunkChildren[childIndex]);
                    ++currentNodeCount;
                } else {
                    ReleaseEntry(entry);
                    Stack_.pop_back();
                }
            }

            if (Stack_.empty()) {
                Finalize();
                Visitor_->OnCompleted();
            } else {
                // Schedule continuation.
                Invoker_->Invoke(BIND(&TCypressTraverser::DoTraverse, MakeStrong(this)));
            }
        } catch (const std::exception& ex) {
            Finalize();
            Visitor_->OnError(ex);
        }
    }

    void Finalize()
    {
        VERIFY_THREAD_AFFINITY(Automaton);

        if (Transaction_) {
            ObjectManager_->WeakUnrefObject(Transaction_);
        }
        while (!Stack_.empty()) {
            ReleaseEntry(Stack_.back());
            Stack_.pop_back();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void TraverseCypress(
    TCypressManagerPtr cypressManager,
    TTransactionManagerPtr transactionManager,
    TObjectManagerPtr objectManager,
    IInvokerPtr invoker,
    TCypressNodeBase* trunkRootNode,
    TTransaction* transaction,
    ICypressNodeVisitorPtr visitor)
{
    YCHECK(trunkRootNode->IsTrunk());

    New<TCypressTraverser>(
        std::move(cypressManager),
        std::move(transactionManager),
        std::move(objectManager),
        std::move(invoker),
        std::move(visitor),
        trunkRootNode,
        transaction)
    ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
