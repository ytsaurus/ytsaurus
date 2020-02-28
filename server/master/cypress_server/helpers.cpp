#include "helpers.h"
#include "node_detail.h"
#include "portal_exit_node.h"
#include "shard.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/cypress_server/cypress_manager.h>

namespace NYT::NCypressServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

const THashMap<TString, TCypressNode*>& GetMapNodeChildMap(
    const TCypressManagerPtr& cypressManager,
    TMapNode* trunkNode,
    TTransaction* transaction,
    THashMap<TString, TCypressNode*>* storage)
{
    YT_ASSERT(trunkNode->IsTrunk());

    if (!transaction) {
        // Fast path.
        return trunkNode->KeyToChild();
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

std::vector<TCypressNode*> GetMapNodeChildList(
    const TCypressManagerPtr& cypressManager,
    TMapNode* trunkNode,
    TTransaction* transaction)
{
    YT_ASSERT(trunkNode->IsTrunk());

    THashMap<TString, TCypressNode*> keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        trunkNode,
        transaction,
        &keyToChildMapStorage);
    return GetValues(keyToChildMap);
}

const std::vector<TCypressNode*>& GetListNodeChildList(
    const TCypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    NTransactionServer::TTransaction* transaction)
{
    YT_ASSERT(trunkNode->IsTrunk());

    auto* node = cypressManager->GetVersionedNode(trunkNode, transaction);
    auto* listNode = node->As<TListNode>();
    return listNode->IndexToChild();
}

TCypressNode* FindMapNodeChild(
    const TCypressManagerPtr& cypressManager,
    TMapNode* trunkNode,
    TTransaction* transaction,
    TStringBuf key)
{
    YT_ASSERT(trunkNode->IsTrunk());

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

TCypressNode* GetMapNodeChildOrThrow(
    const TCypressManagerPtr& cypressManager,
    TMapNode* trunkNode,
    TTransaction* transaction,
    TStringBuf key)
{
    YT_ASSERT(trunkNode->IsTrunk());

    auto* child = FindMapNodeChild(cypressManager, trunkNode, transaction, key);
    if (!child) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "%v has no child with key %Qv",
            cypressManager->GetNodePath(trunkNode, transaction),
            ToYPathLiteral(key));
    }
    return child;
}

TStringBuf FindMapNodeChildKey(
    TMapNode* parentNode,
    TCypressNode* trunkChildNode)
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

TCypressNode* FindListNodeChild(
    const TCypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    TTransaction* /*transaction*/,
    TStringBuf key)
{
    YT_ASSERT(trunkNode->IsTrunk());

    const auto& indexToChild = trunkNode->IndexToChild();
    int index = ParseListIndex(key);
    auto adjustedIndex = TryAdjustChildIndex(index, static_cast<int>(indexToChild.size()));
    if (!adjustedIndex) {
        return nullptr;
    }
    return indexToChild[*adjustedIndex];
}

TCypressNode* GetListNodeChildOrThrow(
    const TCypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    TTransaction* transaction,
    TStringBuf key)
{
    YT_ASSERT(trunkNode->IsTrunk());

    const auto& indexToChild = trunkNode->IndexToChild();
    int index = ParseListIndex(key);
    auto adjustedIndex = TryAdjustChildIndex(index, static_cast<int>(indexToChild.size()));
    if (!adjustedIndex) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "%v has no child with index %v",
            cypressManager->GetNodePath(trunkNode, transaction),
            index);
    }
    return indexToChild[*adjustedIndex];
}

int FindListNodeChildIndex(
    TListNode* parentNode,
    TCypressNode* trunkChildNode)
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
    TCypressNode* trunkNode,
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
    TCypressNode* trunkNode,
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
    TCypressNode* trunkParent,
    TCypressNode* child)
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
}

void DetachChild(
    TCypressNode* /*trunkParent*/,
    TCypressNode* child)
{
    child->SetParent(nullptr);
}

bool NodeHasKey(const TCypressNode* node)
{
    if (node->GetType() == EObjectType::PortalExit) {
        return node->As<TPortalExitNode>()->GetKey().has_value();
    } else {
        auto* parent = node->GetParent();
        return parent && parent->GetNodeType() == ENodeType::Map;
    }
}

std::optional<TString> FindNodeKey(
    const TCypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    TTransaction* transaction)
{
    YT_ASSERT(trunkNode->IsTrunk());
    if (trunkNode->GetType() == EObjectType::PortalExit) {
        return trunkNode->As<TPortalExitNode>()->GetKey();
    } else {
        auto* parent = trunkNode->GetParent();
        if (!parent || parent->GetNodeType() != ENodeType::Map) {
            return {};
        }

        auto originators = cypressManager->GetNodeOriginators(transaction, parent);
        for (const auto* originator : originators) {
            const auto* mapOriginator = originator->As<TMapNode>();
            auto it = mapOriginator->ChildToKey().find(trunkNode);
            if (it != mapOriginator->ChildToKey().end()) {
                return it->second;
            }
        }
        return std::nullopt;
    }
}

bool NodeHasParentId(const TCypressNode* node)
{
    if (node->GetType() == EObjectType::PortalExit) {
        return true;
    } else {
        return node->GetParent() != nullptr;
    }
}

TNodeId GetNodeParentId(const TCypressNode* node)
{
    if (node->GetType() == EObjectType::PortalExit) {
        return node->As<TPortalExitNode>()->GetParentId();
    } else {
        return node->GetParent()->GetId();
    }
}

bool IsAncestorOf(
    const TCypressNode* trunkAncestor,
    const TCypressNode* trunkDescendant)
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

TNodeId MakePortalExitNodeId(
    TNodeId entranceNodeId,
    TCellTag exitCellTag)
{
    return ReplaceCellTagInId(ReplaceTypeInId(entranceNodeId, EObjectType::PortalExit), exitCellTag);
}

TNodeId MakePortalEntranceNodeId(
    TNodeId exitNodeId,
    TCellTag entranceCellTag)
{
    return ReplaceCellTagInId(ReplaceTypeInId(exitNodeId, EObjectType::PortalEntrance), entranceCellTag);
}

TCypressShardId MakeCypressShardId(
    TNodeId rootNodeId)
{
    return ReplaceTypeInId(rootNodeId, EObjectType::CypressShard);
}

TString SuggestCypressShardName(TCypressShard* shard)
{
    const auto* root = shard->GetRoot();
    switch (root->GetType()) {
        case EObjectType::MapNode:
            return Format("root:%v", root->GetNativeCellTag());

        case EObjectType::PortalExit:
            return Format("portal:%v", root->As<TPortalExitNode>()->GetPath());

        default:
            return "<invalid root type>";
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

