#include "helpers.h"

#include "config.h"
#include "node_detail.h"
#include "portal_exit_node.h"
#include "shard.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/helpers.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/sequoia/helpers.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NSequoiaServer;
using namespace NTableServer;
using namespace NTransactionServer;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TImpl, class TChild>
const NDetail::TKeyToCypressNodeImpl<TChild>& GetMapNodeChildMapImpl(
    const ICypressManagerPtr& cypressManager,
    TImpl* trunkNode,
    TTransaction* transaction,
    NDetail::TKeyToCypressNodeImpl<TChild>* storage)
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
        const auto* mapNode = node->template As<TImpl>();
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

} // namespace

////////////////////////////////////////////////////////////////////////////////

const TKeyToCypressNode& GetMapNodeChildMap(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
    TTransaction* transaction,
    TKeyToCypressNode* storage)
{
    return GetMapNodeChildMapImpl(cypressManager, trunkNode, transaction, storage);
}

const TKeyToCypressNodeId& GetMapNodeChildMap(
    const ICypressManagerPtr& cypressManager,
    TSequoiaMapNode* trunkNode,
    TTransaction* transaction,
    TKeyToCypressNodeId* storage)
{
    return GetMapNodeChildMapImpl(cypressManager, trunkNode, transaction, storage);
}

std::vector<TCypressNode*> GetMapNodeChildList(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
    TTransaction* transaction)
{
    YT_ASSERT(trunkNode->IsTrunk());

    TKeyToCypressNode keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        trunkNode,
        transaction,
        &keyToChildMapStorage);
    return GetValues(keyToChildMap);
}

const std::vector<TCypressNode*>& GetListNodeChildList(
    const ICypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    NTransactionServer::TTransaction* transaction)
{
    YT_ASSERT(trunkNode->IsTrunk());

    auto* node = cypressManager->GetVersionedNode(trunkNode, transaction);
    auto* listNode = node->As<TListNode>();
    return listNode->IndexToChild();
}

namespace {

template <class TMapNodeImpl>
auto FindMapNodeChildImpl(
    const ICypressManagerPtr& cypressManager,
    TMapNodeImpl* trunkNode,
    TTransaction* transaction,
    TStringBuf key)
    -> std::decay_t<decltype(trunkNode->KeyToChild())>::mapped_type
{
    YT_ASSERT(trunkNode->IsTrunk());

    auto originators = cypressManager->GetNodeOriginators(transaction, trunkNode);
    for (const auto* node : originators) {
        const auto* mapNode = node->template As<TMapNodeImpl>();
        auto it = mapNode->KeyToChild().find(key);
        if (it != mapNode->KeyToChild().end()) {
            return it->second;
        }

        if (mapNode->GetLockMode() == ELockMode::Snapshot) {
            break;
        }
    }
    return {};
}

} // namespace

TCypressNode* FindMapNodeChild(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
    TTransaction* transaction,
    TStringBuf key)
{
    return FindMapNodeChildImpl(cypressManager, trunkNode, transaction, key);
}

TNodeId FindMapNodeChild(
    const ICypressManagerPtr& cypressManager,
    TSequoiaMapNode* trunkNode,
    TTransaction* transaction,
    TStringBuf key)
{
    return FindMapNodeChildImpl(cypressManager, trunkNode, transaction, key);
}

TCypressNode* GetMapNodeChildOrThrow(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
    TTransaction* transaction,
    TStringBuf key)
{
    YT_ASSERT(trunkNode->IsTrunk());

    auto* child = FindMapNodeChild(cypressManager, trunkNode, transaction, key);
    if (!child) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "Node %v has no child with key %Qv",
            cypressManager->GetNodePath(trunkNode, transaction),
            ToYPathLiteral(key));
    }
    return child;
}

TStringBuf FindMapNodeChildKey(
    TCypressMapNode* parentNode,
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
        currentParentNode = originator->As<TCypressMapNode>();
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
        currentParentNode = originator->As<TCypressMapNode>();
    }

    return key;
}

TCypressNode* FindListNodeChild(
    const ICypressManagerPtr& /*cypressManager*/,
    TListNode* trunkNode,
    TTransaction* /*transaction*/,
    TStringBuf key)
{
    YT_ASSERT(trunkNode->IsTrunk());

    const auto& indexToChild = trunkNode->IndexToChild();
    int index = ParseListIndex(key);
    auto adjustedIndex = TryAdjustListIndex(index, static_cast<int>(indexToChild.size()));
    if (!adjustedIndex) {
        return nullptr;
    }
    return indexToChild[*adjustedIndex];
}

TCypressNode* GetListNodeChildOrThrow(
    const ICypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    TTransaction* transaction,
    TStringBuf key)
{
    YT_ASSERT(trunkNode->IsTrunk());

    const auto& indexToChild = trunkNode->IndexToChild();
    int index = ParseListIndex(key);
    auto adjustedIndex = TryAdjustListIndex(index, static_cast<int>(indexToChild.size()));
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
    const ICypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    TTransaction* transaction)
{
    auto originators = cypressManager->GetNodeReverseOriginators(transaction, trunkNode);

    THashMap<TString, TYsonString> result;
    for (const auto* node : originators) {
        const auto* userAttributes = node->GetAttributes();
        if (userAttributes) {
            for (const auto& [key, value] : userAttributes->Attributes()) {
                if (value) {
                    result[key] = value;
                } else {
                    // NB: key may be absent.
                    result.erase(key);
                }
            }
        }
    }

    return result;
}

THashSet<std::string> ListNodeAttributes(
    const ICypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    TTransaction* transaction)
{
    auto originators = cypressManager->GetNodeReverseOriginators(transaction, trunkNode);

    THashSet<std::string> result;
    for (const auto* node : originators) {
        const auto* userAttributes = node->GetAttributes();
        if (userAttributes) {
            for (const auto& [key, value] : userAttributes->Attributes()) {
                if (value) {
                    result.insert(key);
                } else {
                    // NB: key may be absent.
                    result.erase(key);
                }
            }
        }
    }

    return result;
}

void AttachChildToNode(
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

void DetachChildFromNode(
    TCypressNode* /*trunkParent*/,
    TCypressNode* child)
{
    child->SetParent(nullptr);
}

void AttachChildToSequoiaNodeOrThrow(
    TCypressNode* parent,
    const std::string& childKey,
    TNodeId childId)
{
    auto type = parent->GetType();
    YT_VERIFY(type == EObjectType::Scion || type == EObjectType::SequoiaMapNode);
    auto* sequoiaNode = parent->As<TSequoiaMapNode>();
    auto& children = sequoiaNode->MutableChildren();
    if (auto it = children.KeyToChild().find(childKey); it != children.KeyToChild().end()) {
        // NB: "ignore_existing" flag is validated in Cypress proxy.
        children.Remove(childKey, it->second);
    }
    children.Insert(childKey, childId);
}

void MaybeSetUnreachable(
    const INodeTypeHandlerPtr& handler,
    TCypressNode* node)
{
    if (!node->GetReachable()) {
        return;
    }
    handler->SetUnreachable(node);
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

std::optional<std::string> FindNodeKey(
    const ICypressManagerPtr& cypressManager,
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
            const auto* mapOriginator = originator->As<TCypressMapNode>();
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
    if (node->IsSequoia() && node->ImmutableSequoiaProperties()) {
        return static_cast<bool>(node->ImmutableSequoiaProperties()->ParentId);
    } else if (node->GetType() == EObjectType::PortalExit) {
        return true;
    } else {
        return node->GetParent() != nullptr;
    }
}

TNodeId GetNodeParentId(const TCypressNode* node)
{
    if (node->IsSequoia() && node->ImmutableSequoiaProperties()) {
        return node->ImmutableSequoiaProperties()->ParentId;
    } if (node->GetType() == EObjectType::PortalExit) {
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

std::string SuggestCypressShardName(TCypressShard* shard)
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

void ValidateCompressionCodec(
    const NYson::TYsonString& value,
    const std::optional<THashSet<NCompression::ECodec>>& configuredForbiddenCodecs,
    const std::optional<THashMap<std::string, std::string>>& configuredForbiddenCodecNameToAlias)
{
    if (NCellMaster::IsSubordinateMutation()) {
        return;
    }

    auto deprecatedCodecs = configuredForbiddenCodecs
        ? *configuredForbiddenCodecs
        : NCompression::GetForbiddenCodecs();
    auto codecId = ConvertTo<NCompression::ECodec>(value);
    if (deprecatedCodecs.find(codecId) != deprecatedCodecs.end()) {
        THROW_ERROR_EXCEPTION("Compression codec %Qv is forbidden", codecId);
    }

    auto deprecatedCodecNameToAlias = configuredForbiddenCodecNameToAlias
        ? *configuredForbiddenCodecNameToAlias
        : NCompression::GetForbiddenCodecNameToAlias();
    auto codecName = ConvertTo<std::string>(value);
    auto it = deprecatedCodecNameToAlias.find(codecName);
    if (deprecatedCodecNameToAlias.find(codecName) != deprecatedCodecNameToAlias.end()) {
        auto& [_, alias] = *it;
        THROW_ERROR_EXCEPTION("Compression codec name %Qv is forbidden, use %Qv instead", codecName, alias);
    }
}

void ValidateErasureCodec(
    const NYson::TYsonString& value,
    const THashSet<NErasure::ECodec>& forbiddenCodecs)
{
    auto codecId = ConvertTo<NErasure::ECodec>(value);
    ValidateErasureCodec(codecId, forbiddenCodecs);
}
void ValidateErasureCodec(
    NErasure::ECodec codecId,
    const THashSet<NErasure::ECodec>& forbiddenCodecs)
{
    if (!NCellMaster::IsSubordinateMutation() && forbiddenCodecs.contains(codecId)) {
        THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::ForbiddenErasureCodec, "Erasure codec %Qlv is forbidden", codecId);
    }
}

TRichClusterResources GetNodeResourceUsage(const TCypressNode* node)
{
    auto resourceUsage = node->GetTotalResourceUsage();
    resourceUsage.DetailedMasterMemory() = node->GetDetailedMasterMemoryUsage();
    auto tabletResourceUsage = node->GetTabletResourceUsage();
    return {resourceUsage, tabletResourceUsage};
}

TCypressNode* FindClosestAncestorWithAnnotation(TCypressNode* node)
{
    while (node && !node->TryGetAnnotation()) {
        node = node->GetParent();
    }
    return node;
}

std::optional<TString> GetEffectiveAnnotation(TCypressNode* node)
{
    auto* ancestor = FindClosestAncestorWithAnnotation(node);
    return ancestor ? ancestor->TryGetAnnotation() : std::nullopt;
}

void ValidateAccessControlObjectNamespaceName(const std::string& name)
{
    static const auto maxNameLength = 1024;

    if (name.empty()) {
        THROW_ERROR_EXCEPTION("Access control object namespace name cannot be an empty string");
    }

    if (ssize(name) >= maxNameLength) {
        THROW_ERROR_EXCEPTION("Access control object namespace name is too long")
            << TErrorAttribute("name_length", name.size())
            << TErrorAttribute("max_name_length", maxNameLength);
    }

    auto isAsciiText = [] (char c) {
        return IsAsciiAlnum(c) || IsAsciiPunct(c);
    };

    if (!::AllOf(name.begin(), name.end(), isAsciiText)) {
        THROW_ERROR_EXCEPTION("Only ASCII alphanumeric  and punctuation characters are allowed in access control object namespace names");
    }
}

void ValidateAccessControlObjectName(const std::string& name)
{
    static const auto maxNameLength = 1024;

    if (name.empty()) {
        THROW_ERROR_EXCEPTION("Access control object name cannot be an empty string");
    }

    if (ssize(name) >= maxNameLength) {
        THROW_ERROR_EXCEPTION("Access control object name is too long")
            << TErrorAttribute("name_length", name.size())
            << TErrorAttribute("max_name_length", maxNameLength);
    }

    auto isAsciiText = [] (char c) {
        return IsAsciiAlnum(c) || IsAsciiPunct(c);
    };

    if (!::AllOf(name.begin(), name.end(), isAsciiText)) {
        THROW_ERROR_EXCEPTION("Only ASCII alphanumeric  and punctuation characters are allowed in access control object namespace names");
    }
}

////////////////////////////////////////////////////////////////////////////////

void MaybeTouchNode(
    const ICypressNodeProxyPtr& nodeProxy,
    const NCypressServer::NProto::TAccessTrackingOptions& protoOptions)
{
    if (!protoOptions.suppress_access_tracking()) {
        nodeProxy->SetTouched();
    }
    if (!protoOptions.suppress_expiration_timeout_renewal()) {
        nodeProxy->SetAccessed();
    }
}

TLockRequest CreateLockRequest(
    ELockMode mode,
    const std::optional<TString>& childKey,
    const std::optional<TString>& attributeKey,
    TTimestamp timestamp)
{
    YT_ASSERT(CheckLockRequest(mode, childKey, attributeKey).IsOK());

    TLockRequest lockRequest;
    if (childKey) {
        lockRequest = TLockRequest::MakeSharedChild(*childKey);
    } else if (attributeKey) {
        lockRequest = TLockRequest::MakeSharedAttribute(*attributeKey);
    } else {
        lockRequest = TLockRequest(mode);
    }
    lockRequest.Timestamp = timestamp;

    return lockRequest;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
