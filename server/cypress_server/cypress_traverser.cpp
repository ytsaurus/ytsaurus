#include "cypress_traverser.h"
#include "node_detail.h"

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/transaction_server/transaction.h>
#include <yt/server/transaction_server/transaction_manager.h>

#include <yt/server/object_server/object_manager.h>

#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/user.h>

#include <yt/core/ytree/public.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NSecurityServer;
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
        TSecurityManagerPtr securityManager,
        IInvokerPtr invoker,
        ICypressNodeVisitorPtr visitor,
        TCypressNodeBase* trunkRootNode,
        TTransaction* transaction)
        : CypressManager_(std::move(cypressManager))
        , TransactionManager_(std::move(transactionManager))
        , ObjectManager_(std::move(objectManager))
        , SecurityManager_(std::move(securityManager))
        , Invoker_(std::move(invoker))
        , Visitor_(std::move(visitor))
        , Transaction_(transaction)
        , UserName_(SecurityManager_->GetAuthenticatedUser()->GetName())
    {
        VERIFY_THREAD_AFFINITY(Automaton);

        if (Transaction_) {
            ObjectManager_->EphemeralRefObject(Transaction_);
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
    const TSecurityManagerPtr SecurityManager_;
    const IInvokerPtr Invoker_;
    const ICypressNodeVisitorPtr Visitor_;
    TTransaction* const Transaction_;
    const TString UserName_;

    TDuration TotalTime_;

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
        ObjectManager_->EphemeralUnrefObject(entry.TrunkNode);
        for (auto* child : entry.TrunkChildren) {
            ObjectManager_->EphemeralUnrefObject(child);
        }
    }

    void PushEntry(TCypressNodeBase* trunkNode)
    {
        ObjectManager_->EphemeralRefObject(trunkNode);
        Stack_.push_back(TStackEntry(trunkNode));

        auto addChildren = [&] (std::vector<TCypressNodeBase*> children) {
            auto& entry = Stack_.back();
            entry.TrunkChildren = std::move(children);
            for (auto* child : entry.TrunkChildren) {
                ObjectManager_->EphemeralRefObject(child);
            }
        };

        switch (trunkNode->GetNodeType()) {
            case ENodeType::Map:
                addChildren(GetMapNodeChildList(CypressManager_, trunkNode, Transaction_));
                break;

            case ENodeType::List:
                addChildren(GetListNodeChildList(CypressManager_, trunkNode, Transaction_));
                break;

            default:
                // Do nothing.
                break;
        }
    }

    void DoTraverse()
    {
        VERIFY_THREAD_AFFINITY(Automaton);

        try {
            if (Transaction_ && !IsObjectAlive(Transaction_)) {
                THROW_ERROR_EXCEPTION("Transaction %v no longer exists",
                    Transaction_->GetId());
            }

            {
                NProfiling::TWallTimingGuard timingGuard(&TotalTime_);
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
                        auto* child = entry.TrunkChildren[childIndex];
                        if (IsObjectAlive(child)) {
                            PushEntry(child);
                        }
                        ++currentNodeCount;
                    } else {
                        ReleaseEntry(entry);
                        Stack_.pop_back();
                    }
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
            ObjectManager_->EphemeralUnrefObject(Transaction_);
        }

        auto* user = SecurityManager_->FindUserByName(UserName_);
        if (IsObjectAlive(user)) {
            SecurityManager_->ChargeUserRead(user, 0, TotalTime_);
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
    TSecurityManagerPtr securityManager,
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
        std::move(securityManager),
        std::move(invoker),
        std::move(visitor),
        trunkRootNode,
        transaction)
    ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
