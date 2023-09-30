#include "cypress_traverser.h"
#include "node_detail.h"

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NCypressServer {

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
        ICypressManagerPtr cypressManager,
        ITransactionManagerPtr transactionManager,
        IObjectManagerPtr objectManager,
        ISecurityManagerPtr securityManager,
        IInvokerPtr invoker,
        ICypressNodeVisitorPtr visitor,
        TCypressNode* trunkRootNode,
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
        VERIFY_THREAD_AFFINITY_ANY();

        PushEntry(trunkRootNode);
    }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Automaton);

        DoTraverse();
    }

private:
    const ICypressManagerPtr CypressManager_;
    const ITransactionManagerPtr TransactionManager_;
    const IObjectManagerPtr ObjectManager_;
    const ISecurityManagerPtr SecurityManager_;
    const IInvokerPtr Invoker_;
    const ICypressNodeVisitorPtr Visitor_;
    const TEphemeralObjectPtr<TTransaction> Transaction_;
    const TString UserName_;

    TDuration TotalTime_;

    DECLARE_THREAD_AFFINITY_SLOT(Automaton);


    struct TStackEntry
    {
        TEphemeralObjectPtr<TCypressNode> TrunkNode;
        int ChildIndex = -1; // -1 means the node itself
        std::vector<TEphemeralObjectPtr<TCypressNode>> TrunkChildren;

        explicit TStackEntry(TCypressNode* trunkNode)
            : TrunkNode(trunkNode)
        { }
    };

    std::vector<TStackEntry> Stack_;


    void PushEntry(TCypressNode* trunkNode)
    {
        auto& entry = Stack_.emplace_back(trunkNode);
        switch (trunkNode->GetNodeType()) {
            case ENodeType::Map: {
                THashMap<TString, TCypressNode*> childMapStorage;
                const auto& childMap = GetMapNodeChildMap(
                    CypressManager_,
                    trunkNode->As<TCypressMapNode>(),
                    Transaction_.Get(),
                    &childMapStorage);
                entry.TrunkChildren.reserve(childMap.size());
                for (const auto& [key, child] : childMap) {
                    entry.TrunkChildren.emplace_back(child);
                }
                break;
            }

            case ENodeType::List: {
                const auto& children = GetListNodeChildList(
                    CypressManager_,
                    trunkNode->As<TListNode>(),
                    Transaction_.Get());
                entry.TrunkChildren.reserve(children.size());
                for (auto* child : children) {
                    entry.TrunkChildren.emplace_back(child);
                }
                break;
            }

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
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::NoSuchTransaction,
                    "Transaction %v no longer exists",
                    Transaction_->GetId());
            }

            {
                NProfiling::TValueIncrementingTimingGuard<NProfiling::TWallTimer> timingGuard(&TotalTime_);
                int currentNodeCount = 0;
                while (currentNodeCount < MaxNodesPerIteration && !Stack_.empty()) {
                    auto& entry = Stack_.back();
                    auto childIndex = entry.ChildIndex++;
                    if (childIndex < 0) {
                        if (IsObjectAlive(entry.TrunkNode)) {
                            Visitor_->OnNode(entry.TrunkNode.Get(), Transaction_.Get());
                        }
                        ++currentNodeCount;
                    } else if (childIndex < std::ssize(entry.TrunkChildren)) {
                        const auto& child = entry.TrunkChildren[childIndex];
                        if (IsObjectAlive(child)) {
                            PushEntry(child.Get());
                        }
                        ++currentNodeCount;
                    } else {
                        Stack_.pop_back();
                    }
                }
            }

            if (Stack_.empty()) {
                Finalize();
                Visitor_->OnCompleted();
                return;
            }

            // Schedule continuation.
            Invoker_->Invoke(BIND(&TCypressTraverser::DoTraverse, MakeStrong(this)));
        } catch (const std::exception& ex) {
            Finalize();
            Visitor_->OnError(ex);
        }
    }

    void Finalize()
    {
        VERIFY_THREAD_AFFINITY(Automaton);

        auto* user = SecurityManager_->FindUserByName(UserName_, true /*activeLifeStageOnly*/);
        SecurityManager_->ChargeUser(user, {EUserWorkloadType::Read, 0, TotalTime_});
    }
};

////////////////////////////////////////////////////////////////////////////////

void TraverseCypress(
    ICypressManagerPtr cypressManager,
    ITransactionManagerPtr transactionManager,
    IObjectManagerPtr objectManager,
    ISecurityManagerPtr securityManager,
    IInvokerPtr invoker,
    TCypressNode* trunkRootNode,
    TTransaction* transaction,
    ICypressNodeVisitorPtr visitor)
{
    YT_VERIFY(trunkRootNode->IsTrunk());

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

} // namespace NYT::NCypressServer
