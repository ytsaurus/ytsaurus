#include "helpers.h"
#include "node_detail.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/transaction_server/transaction_manager.h>

namespace NYT {
namespace NCypressServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

yhash_map<Stroka, TCypressNodeBase*> GetMapNodeChildMap(
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    Y_ASSERT(trunkNode->GetNodeType() == ENodeType::Map);

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

std::vector<TCypressNodeBase*> GetMapNodeChildList(
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    Y_ASSERT(trunkNode->GetNodeType() == ENodeType::Map);

    if (transaction) {
        return GetValues(GetMapNodeChildMap(cypressManager, trunkNode, transaction));
    } else {
        const auto* mapNode = static_cast<const TMapNode*>(trunkNode);
        return GetValues(mapNode->KeyToChild());
    }
}

const std::vector<TCypressNodeBase*>& GetListNodeChildList(
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    NTransactionServer::TTransaction* transaction)
{
    Y_ASSERT(trunkNode->GetNodeType() == ENodeType::List);

    auto* node = cypressManager->GetVersionedNode(trunkNode, transaction);
    auto* listNode = static_cast<TListNode*>(node);
    return listNode->IndexToChild();
}

std::vector<std::pair<Stroka, TCypressNodeBase*>> SortKeyToChild(
    const yhash_map<Stroka, TCypressNodeBase*>& keyToChildMap)
{
    std::vector<std::pair<Stroka, TCypressNodeBase*>> keyToChildList;
    keyToChildList.reserve(keyToChildMap.size());
    for (const auto& pair : keyToChildMap) {
        keyToChildList.emplace_back(pair.first, pair.second);
    }
    std::sort(keyToChildList.begin(), keyToChildList.end(),
        [] (const std::pair<Stroka, TCypressNodeBase*>& lhs, std::pair<Stroka, TCypressNodeBase*>& rhs) {
            return lhs.first < rhs.first;
        });
    return keyToChildList;
}

TCypressNodeBase* FindMapNodeChild(
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const Stroka& key)
{
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
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
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
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
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
    const TObjectManagerPtr& objectManager,
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

    objectManager->RefObject(trunkChild);
}

void DetachChild(
    const TObjectManagerPtr& objectManager,
    TCypressNodeBase* /*trunkParent*/,
    TCypressNodeBase* child,
    bool unref)
{
    child->SetParent(nullptr);

    if (unref) {
        objectManager->UnrefObject(child->GetTrunkNode());
    }
}

bool NodeHasKey(
    const TCypressManagerPtr& cypressManager,
    const TCypressNodeBase* node)
{
    auto* parent = node->GetParent();
    if (!parent) {
        return false;
    }
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

