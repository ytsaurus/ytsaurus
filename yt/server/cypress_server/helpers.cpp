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

const THashMap<TString, TCypressNodeBase*>& GetMapNodeChildMap(
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    THashMap<TString, TCypressNodeBase*>* storage)
{
    Y_ASSERT(trunkNode->GetNodeType() == ENodeType::Map);

    if (!transaction) {
        // Fast path.
        return trunkNode->As<TMapNode>()->KeyToChild();
    }

    // Slow path.
    storage->clear();
    auto originators = cypressManager->GetNodeReverseOriginators(transaction, trunkNode);
    for (const auto* node : originators) {
        const auto* mapNode = node->As<TMapNode>();
        const auto& keyToChild = mapNode->KeyToChild();
        if (mapNode == trunkNode) {
            storage->reserve(keyToChild.size());
        }
        for (const auto& pair : keyToChild) {
            if (!pair.second) {
                // NB: key may be absent.
                storage->erase(pair.first);
            } else {
                (*storage)[pair.first] = pair.second;
            }
        }
    }

    return *storage;
}

std::vector<TCypressNodeBase*> GetMapNodeChildList(
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    Y_ASSERT(trunkNode->GetNodeType() == ENodeType::Map);

    THashMap<TString, TCypressNodeBase*> keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        trunkNode,
        transaction,
        &keyToChildMapStorage);
    return GetValues(keyToChildMap);
}

const std::vector<TCypressNodeBase*>& GetListNodeChildList(
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    NTransactionServer::TTransaction* transaction)
{
    Y_ASSERT(trunkNode->GetNodeType() == ENodeType::List);

    auto* node = cypressManager->GetVersionedNode(trunkNode, transaction);
    auto* listNode = node->As<TListNode>();
    return listNode->IndexToChild();
}

std::vector<std::pair<TString, TCypressNodeBase*>> SortKeyToChild(
    const THashMap<TString, TCypressNodeBase*>& keyToChildMap)
{
    std::vector<std::pair<TString, TCypressNodeBase*>> keyToChildList;
    keyToChildList.reserve(keyToChildMap.size());
    for (const auto& pair : keyToChildMap) {
        keyToChildList.emplace_back(pair.first, pair.second);
    }
    std::sort(keyToChildList.begin(), keyToChildList.end(),
        [] (const std::pair<TString, TCypressNodeBase*>& lhs, const std::pair<TString, TCypressNodeBase*>& rhs) {
            return lhs.first < rhs.first;
        });
    return keyToChildList;
}

TCypressNodeBase* FindMapNodeChild(
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const TString& key)
{
    auto originators = cypressManager->GetNodeOriginators(transaction, trunkNode);

    for (const auto* node : originators) {
        const auto* mapNode = node->As<TMapNode>();
        auto it = mapNode->KeyToChild().find(key);
        if (it != mapNode->KeyToChild().end()) {
            return it->second;
        }
    }

    return nullptr;
}

THashMap<TString, NYson::TYsonString> GetNodeAttributes(
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    auto originators = cypressManager->GetNodeReverseOriginators(transaction, trunkNode);

    THashMap<TString, TYsonString> result;
    for (const auto* node : originators) {
        const auto* userAttributes = node->GetAttributes();
        if (userAttributes) {
            for (const auto& pair : userAttributes->Attributes()) {
                if (pair.second) {
                    result[pair.first] = pair.second;
                } else {
                    YCHECK(result.erase(pair.first) == 1);
                }
            }
        }
    }

    return result;
}

THashSet<TString> ListNodeAttributes(
    const TCypressManagerPtr& cypressManager,
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    auto originators = cypressManager->GetNodeReverseOriginators(transaction, trunkNode);

    THashSet<TString> result;
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

bool NodeHasKey(const TCypressNodeBase* node)
{
    auto* parent = node->GetParent();
    if (!parent) {
        return false;
    }
    return parent->GetNodeType() == ENodeType::Map;
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

