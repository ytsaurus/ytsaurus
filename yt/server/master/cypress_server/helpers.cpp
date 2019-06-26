#include "helpers.h"
#include "node_detail.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/master/transaction_server/transaction_manager.h>

namespace NYT::NCypressServer {

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
    YT_ASSERT(trunkNode->GetNodeType() == ENodeType::Map);

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

        if (mapNode->GetLockMode() == ELockMode::None ||
            mapNode->GetLockMode() == ELockMode::Snapshot)
        {
            YT_VERIFY(mapNode == trunkNode || mapNode->GetLockMode() == ELockMode::Snapshot);
            *storage = keyToChild;
        } else {
            YT_ASSERT(mapNode != trunkNode);

            for (const auto& [childId, childNode] : keyToChild) {
                if (!childNode) {
                    // NB: key may be absent.
                    storage->erase(childId);
                } else {
                    (*storage)[childId] = childNode;
                }
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
    YT_ASSERT(trunkNode->GetNodeType() == ENodeType::Map);

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
    YT_ASSERT(trunkNode->GetNodeType() == ENodeType::List);

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
    TStringBuf key)
{
    auto originators = cypressManager->GetNodeOriginators(transaction, trunkNode);

    for (const auto* node : originators) {
        const auto* mapNode = node->As<TMapNode>();
        auto it = mapNode->KeyToChild().find(key);
        if (it != mapNode->KeyToChild().end()) {
            return it->second;
        }

        if (mapNode->GetLockMode() == ELockMode::Snapshot) {
            break;
        }
    }

    return nullptr;
}

TStringBuf FindMapNodeChildKey(
    TMapNode* parentNode,
    TCypressNodeBase* trunkChildNode)
{
    YT_ASSERT(trunkChildNode->IsTrunk());

    TStringBuf key;

    for (const auto* currentParentNode = parentNode; currentParentNode;) {
        auto it = currentParentNode->ChildToKey().find(trunkChildNode);
        if (it != currentParentNode->ChildToKey().end()) {
            key = it->second;
            break;
        }

        if (currentParentNode->GetLockMode() == ELockMode::Snapshot) {
            break;
        }

        auto* originator = currentParentNode->GetOriginator();
        if (!originator) {
            break;
        }
        currentParentNode = originator->As<TMapNode>();
    }

    if (!key.data()) {
        return TStringBuf();
    }

    for (const auto* currentParentNode = parentNode; currentParentNode;) {
        auto it = currentParentNode->KeyToChild().find(key);
        if (it != currentParentNode->KeyToChild().end() && !it->second) {
            return TStringBuf();
        }

        if (currentParentNode->GetLockMode() == ELockMode::Snapshot) {
            break;
        }

        auto* originator = currentParentNode->GetOriginator();
        if (!originator) {
            break;
        }
        currentParentNode = originator->As<TMapNode>();
    }

    return key;
}

int FindListNodeChildIndex(
    TListNode* parentNode,
    TCypressNodeBase* trunkChildNode)
{
    YT_ASSERT(trunkChildNode->IsTrunk());

    while (true) {
        auto it = parentNode->ChildToIndex().find(trunkChildNode);
        if (it != parentNode->ChildToIndex().end()) {
            return it->second;
        }
        auto* originator = parentNode->GetOriginator();
        if (!originator) {
            break;
        }
        parentNode = originator->As<TListNode>();
    }

    return -1;
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
                    // NB: key may be absent.
                    result.erase(pair.first);
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
                    // NB: key may be absent.
                    result.erase(pair.first);
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
    YT_VERIFY(trunkParent->IsTrunk());

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

bool IsAncestorOf(
    const TCypressNodeBase* trunkAncestor,
    const TCypressNodeBase* trunkDescendant)
{
    YT_ASSERT(trunkAncestor->IsTrunk());
    YT_ASSERT(trunkDescendant->IsTrunk());
    auto* current = trunkDescendant;
    while (current) {
        if (current == trunkAncestor) {
            return true;
        }
        current = current->GetParent();
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

