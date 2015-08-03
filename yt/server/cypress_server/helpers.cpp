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
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

yhash_map<Stroka, TCypressNodeBase*> GetMapNodeChildren(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    auto cypressManager = bootstrap->GetCypressManager();
    auto originators = cypressManager->GetNodeReverseOriginators(transaction, trunkNode);

    yhash_map<Stroka, TCypressNodeBase*> result;
    for (const auto* node : originators) {
        const auto* mapNode = static_cast<const TMapNode*>(node);
        for (const auto& pair : mapNode->KeyToChild()) {
            if (!pair.second) {
                // NB: key may be absent.
                result.erase(pair.first);
            } else {
                result[pair.first] = pair.second;
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
    auto cypressManager = bootstrap->GetCypressManager();
    auto originators = cypressManager->GetNodeOriginators(transaction, trunkNode);

    for (const auto* node : originators) {
        const auto* mapNode = static_cast<const TMapNode*>(node);
        auto it = mapNode->KeyToChild().find(key);
        if (it != mapNode->KeyToChild().end()) {
            return it->second;
        }
    }

    return nullptr;
}

yhash_map<Stroka, NYson::TYsonString> GetNodeAttributes(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    auto cypressManager = bootstrap->GetCypressManager();
    auto originators = cypressManager->GetNodeReverseOriginators(transaction, trunkNode);

    yhash_map<Stroka, TYsonString> result;
    for (const auto* node : originators) {
        const auto* userAttributes = node->GetAttributes();
        if (userAttributes) {
            for (const auto& pair : userAttributes->Attributes()) {
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
    auto cypressManager = bootstrap->GetCypressManager();
    auto originators = cypressManager->GetNodeReverseOriginators(transaction, trunkNode);

    yhash_set<Stroka> result;
    for (const auto* node : originators) {
        const auto* userAttributes = node->GetAttributes();
        if (userAttributes) {
            for (const auto& pair : userAttributes->Attributes()) {
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

    // Walk upwards along the originator links and set missing parents
    // This ensures that when a new node is created within a transaction
    // and then attached somewhere, its originators have valid parent links.
    auto* trunkChild = child->GetTrunkNode();
    if (trunkChild != child) {
        auto* currentChild = child->GetOriginator();
        while (currentChild && !currentChild->GetParent()) {
            currentChild->SetParent(trunkParent);
            currentChild = currentChild->GetOriginator();
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

bool IsParentOf(
    const TCypressNodeBase* parent,
    const TCypressNodeBase* descendant)
{
    auto* current = descendant;
    while (current) {
        if (current == parent) {
            return true;
        }
        current = current->GetParent();
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

