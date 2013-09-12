#include "stdafx.h"
#include "helpers.h"
#include "node_detail.h"

#include <server/cell_master/bootstrap.h>

#include <server/transaction_server/transaction_manager.h>

#include <server/cypress_server/cypress_manager.h>

namespace NYT {
namespace NCypressServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

yhash_map<Stroka, TCypressNodeBase*> GetMapNodeChildren(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    yhash_map<Stroka, TCypressNodeBase*> result;

    auto cypressManager = bootstrap->GetCypressManager();
    auto transactionManager = bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(transaction);
    std::reverse(transactions.begin(), transactions.end());

    FOREACH (const auto* currentTransaction, transactions) {
        TVersionedObjectId versionedId(trunkNode->GetId(), GetObjectId(currentTransaction));
        const auto* node = cypressManager->FindNode(versionedId);
        if (node) {
            const auto* mapNode = static_cast<const TMapNode*>(node);
            FOREACH (const auto& pair, mapNode->KeyToChild()) {
                if (!pair.second) {
                    // NB: key may be absent.
                    result.erase(pair.first);
                } else {
                    result[pair.first] = pair.second;
                }
            }
        }
    }

    return result;
}

TCypressNodeBase* FindMapNodeChild(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const Stroka& key)
{
    auto transactionManager = bootstrap->GetTransactionManager();
    auto cypressManager = bootstrap->GetCypressManager();

    auto transactions = transactionManager->GetTransactionPath(transaction);

    FOREACH (const auto* currentTransaction, transactions) {
        TVersionedObjectId versionedId(trunkNode->GetId(), GetObjectId(currentTransaction));
        const auto* node = cypressManager->FindNode(versionedId);
        if (node) {
            const auto* mapNode = static_cast<const TMapNode*>(node);
            auto it = mapNode->KeyToChild().find(key);
            if (it != mapNode->KeyToChild().end()) {
                return it->second;
            }
        }
    }

    return nullptr;
}

yhash_map<Stroka, NYTree::TYsonString> GetNodeAttributes(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    yhash_map<Stroka, TYsonString> result;

    auto objectManager = bootstrap->GetObjectManager();
    auto transactionManager = bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(transaction);
    std::reverse(transactions.begin(), transactions.end());

    FOREACH (const auto* currentTransaction, transactions) {
        TVersionedObjectId versionedId(trunkNode->GetId(), GetObjectId(currentTransaction));
        const auto* userAttributes = objectManager->FindAttributes(versionedId);
        if (userAttributes) {
            FOREACH (const auto& pair, userAttributes->Attributes()) {
                if (pair.second) {
                    result[pair.first] = pair.second.Get();
                } else {
                    YCHECK(result.erase(pair.first) == 1);
                }
            }
        }
    }

    return result;
}

yhash_set<Stroka> ListNodeAttributes(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    yhash_set<Stroka> result;

    auto objectManager = bootstrap->GetObjectManager();
    auto transactionManager = bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(transaction);
    std::reverse(transactions.begin(), transactions.end());

    FOREACH (const auto* currentTransaction, transactions) {
        TVersionedObjectId versionedId(trunkNode->GetId(), GetObjectId(currentTransaction));
        const auto* userAttributes = objectManager->FindAttributes(versionedId);
        if (userAttributes) {
            FOREACH (const auto& pair, userAttributes->Attributes()) {
                if (pair.second) {
                    result.insert(pair.first);
                } else {
                    YCHECK(result.erase(pair.first) == 1);
                }
            }
        }
    }

    return result;
}

void AttachChild(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkParent,
    TCypressNodeBase* child)
{
    YCHECK(trunkParent->IsTrunk());

    child->SetParent(trunkParent);

    // Walk upwards along the transaction path and set missing parents
    // This ensures that when a new node is created within a transaction
    // and then attached somewhere, its originators have valid parent links.
    auto* trunkChild = child->GetTrunkNode();
    if (trunkChild != child) {
        auto cypressManager = bootstrap->GetCypressManager();
        auto* transaction = child->GetTransaction()->GetParent();
        while (true) {
            TVersionedNodeId versionedId(child->GetId(), GetObjectId(transaction));
            auto* childOriginator = cypressManager->GetNode(versionedId);
            if (childOriginator->GetParent()) {
                break;
            }
            childOriginator->SetParent(trunkParent);
            if (!transaction) {
                break;
            }
            transaction = transaction->GetParent();
        }
    }

    auto objectManager = bootstrap->GetObjectManager();
    objectManager->RefObject(trunkChild);
}

void DetachChild(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkParent,
    TCypressNodeBase* child,
    bool unref)
{
    UNUSED(trunkParent);

    child->SetParent(nullptr);

    if (unref) {
        auto objectManager = bootstrap->GetObjectManager();
        objectManager->UnrefObject(child->GetTrunkNode());
    }
}

bool NodeHasKey(
    NCellMaster::TBootstrap* bootstrap,
    const TCypressNodeBase* node)
{
    auto* parent = node->GetParent();
    if (!parent) {
        return false;
    }
    auto cypressManager = bootstrap->GetCypressManager();
    auto handler = cypressManager->GetHandler(parent);
    return handler->GetNodeType() == ENodeType::Map;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

