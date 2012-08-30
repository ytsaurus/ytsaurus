#include "stdafx.h"
#include "helpers.h"
#include "node_detail.h"

#include <server/cell_master/bootstrap.h>

#include <server/transaction_server/transaction_manager.h>

#include <server/cypress_server/cypress_manager.h>

namespace NYT {
namespace NCypressServer {

using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

yhash_map<Stroka, TNodeId> GetMapNodeChildren(
    NCellMaster::TBootstrap* bootstrap,
    const TNodeId& nodeId,
    NTransactionServer::TTransaction* transaction)
{
    yhash_map<Stroka, TNodeId> result;

    auto cypressManager = bootstrap->GetCypressManager();
    auto transactionManager = bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(transaction);
    std::reverse(transactions.begin(), transactions.end());

    FOREACH (const auto* currentTransaction, transactions) {
        const auto* node = cypressManager->GetVersionedNode(nodeId, currentTransaction);
        const auto* mapNode = static_cast<const TMapNode*>(node);
        FOREACH (const auto& pair, mapNode->KeyToChild()) {
            if (pair.second == NullObjectId) {
                YCHECK(result.erase(pair.first) == 1);
            } else {
                result[pair.first] = pair.second;
            }
        }
    }

    return result;
}

TVersionedNodeId FindMapNodeChild(
    NCellMaster::TBootstrap* bootstrap,
    const TNodeId& nodeId,
    NTransactionServer::TTransaction* transaction,
    const Stroka& key)
{
    auto transactionManager = bootstrap->GetTransactionManager();
    auto cypressManager = bootstrap->GetCypressManager();

    auto transactions = transactionManager->GetTransactionPath(transaction);

    FOREACH (const auto* currentTransaction, transactions) {
        const auto* node = cypressManager->GetVersionedNode(nodeId, currentTransaction);
        const auto& map = static_cast<const TMapNode*>(node)->KeyToChild();
        auto it = map.find(key);
        if (it != map.end()) {
            return TVersionedNodeId(it->second, GetObjectId(transaction));
        }
    }

    return TVersionedNodeId(NullObjectId, NullTransactionId);
}

yhash_map<Stroka, NYTree::TYsonString> GetNodeAttributes(
    NCellMaster::TBootstrap* bootstrap,
    const TNodeId& nodeId,
    NTransactionServer::TTransaction* transaction)
{
    yhash_map<Stroka, TYsonString> result;

    auto objectManager = bootstrap->GetObjectManager();
    auto transactionManager = bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(transaction);
    std::reverse(transactions.begin(), transactions.end());

    FOREACH (const auto* currentTransaction, transactions) {
        NObjectServer::TVersionedObjectId versionedId(nodeId, NObjectServer::GetObjectId(currentTransaction));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

