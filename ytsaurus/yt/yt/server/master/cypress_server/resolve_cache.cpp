#include "resolve_cache.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "link_node.h"
#include "portal_entrance_node.h"
#include "private.h"

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NCypressServer {

using namespace NObjectClient;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

TResolveCacheNode::TResolveCacheNode(
    TCypressNode* node,
    const TYPath& path, TPayload payload)
    : TrunkNode(node)
    , Path(path)
    , Payload(std::move(payload))
{ }

TNodeId TResolveCacheNode::GetId() const
{
    return TrunkNode->GetId();
}

////////////////////////////////////////////////////////////////////////////////

TResolveCache::TResolveCache(
    TNodeId rootNodeId,
    bool primaryMaster)
    : RootNodeId_(rootNodeId)
    , PrimaryMaster_(primaryMaster)
{ }

// Cf. TPathResolver::Resolve.
std::optional<TResolveCache::TResolveResult> TResolveCache::TryResolve(const TYPath& path)
{
    TTokenizer tokenizer(path);

    static const auto EmptyYPath = TYPath();
    static const auto SlashYPath = TYPath("/");
    static const auto AmpersandYPath = TYPath("&");

    // Nullptr indicates that one must resolve the root.
    TResolveCacheNodePtr currentNode;

    auto resolveRoot = [&] {
        tokenizer.Advance();
        bool ampersandSkipped = tokenizer.Skip(NYPath::ETokenType::Ampersand);

        switch (tokenizer.GetType()) {
            case ETokenType::EndOfStream:
                if (ampersandSkipped) {
                    THROW_ERROR_EXCEPTION("YPath cannot end with ampersand");
                }
                return std::make_tuple(TNodeId(), false);

            case ETokenType::Slash:
                tokenizer.Advance();
                return std::make_tuple(RootNodeId_, ampersandSkipped);

            case ETokenType::Literal: {
                auto token = tokenizer.GetToken();
                if (!token.StartsWith(ObjectIdPathPrefix)) {
                    tokenizer.ThrowUnexpected();
                }

                TStringBuf objectIdString(token.begin() + ObjectIdPathPrefix.length(), token.end());
                TObjectId objectId;
                if (!TObjectId::FromString(objectIdString, &objectId)) {
                    THROW_ERROR_EXCEPTION(
                        NYTree::EErrorCode::ResolveError,
                        "Error parsing object id %v",
                        objectIdString);
                }
                tokenizer.Advance();

                return std::make_tuple(objectId, ampersandSkipped);
            }

            default:
                tokenizer.ThrowUnexpected();
                YT_ABORT();
        }
    };

    TYPath rewrittenPath;
    for (int resolveDepth = 0; ; ++resolveDepth) {
        ValidateYPathResolutionDepth(path, resolveDepth);

        if (!currentNode) {
            auto [rootId, ampersandSkipped] = resolveRoot();
            if (!rootId) {
                return std::nullopt;
            }
            if (!ampersandSkipped && PrimaryMaster_ && CellTagFromId(rootId) != CellTagFromId(RootNodeId_)) {
                return TResolveResult{
                    rootId,
                    TYPath(tokenizer.GetInput())
                };
            }
            currentNode = FindNode(rootId);
            if (!currentNode) {
                return std::nullopt;
            }
        }

        auto unresolvedPathSuffix = tokenizer.GetInput();
        bool ampersandSkipped = tokenizer.Skip(NYPath::ETokenType::Ampersand);
        bool slashSkipped = tokenizer.Skip(NYPath::ETokenType::Slash);

        auto guard = ReaderGuard(currentNode->Lock);
        if (const auto* mapPayload = std::get_if<TResolveCacheNode::TMapPayload>(&currentNode->Payload)) {
            if (!slashSkipped) {
                return std::nullopt;
            }

            if (tokenizer.GetType() != NYPath::ETokenType::Literal) {
                return std::nullopt;
            }

            auto key = tokenizer.GetToken();
            auto it = mapPayload->KeyToChild.find(key);
            if (it == mapPayload->KeyToChild.end()) {
                return std::nullopt;
            }

            currentNode = it->second;

            tokenizer.Advance();
        } else if (const auto* linkPayload = std::get_if<TResolveCacheNode::TLinkPayload>(&currentNode->Payload)) {
            if (ampersandSkipped) {
                return std::nullopt;
            }

            if (!slashSkipped) {
                return std::nullopt;
            }

            rewrittenPath =
                linkPayload->TargetPath +
                (slashSkipped ? SlashYPath : EmptyYPath) +
                tokenizer.GetInput();
            tokenizer.Reset(rewrittenPath);

            ++resolveDepth;

            // Reset currentNode to request root resolve at the beginning of the next iteration.
            currentNode.Reset();
        } else if (const auto* entrancePayload = std::get_if<TResolveCacheNode::TPortalEntrancePayload>(&currentNode->Payload)) {
            if (ampersandSkipped) {
                return std::nullopt;
            }

            return TResolveResult{
                entrancePayload->PortalExitId,
                TYPath(unresolvedPathSuffix)
            };
        } else  {
            return std::nullopt;
        }
    }
}

TResolveCacheNodePtr TResolveCache::FindNode(TNodeId nodeId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // TODO(babenko): fastpath for root
    auto* shard = GetShard(nodeId);
    auto guard = ReaderGuard(shard->Lock);
    auto it = shard->IdToNode.find(nodeId);
    return it == shard->IdToNode.end() ? nullptr : it->second;
}

TResolveCacheNodePtr TResolveCache::TryInsertNode(
    TCypressNode* trunkNode,
    const TYPath& path)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* shard = GetShard(trunkNode);
    auto guard = WriterGuard(shard->Lock);

    auto& idToNode = shard->IdToNode;

    auto nodeId = trunkNode->GetId();
    auto nodeIt = idToNode.find(nodeId);
    if (nodeIt == idToNode.end()) {
        auto payload = MakePayload(trunkNode);
        auto node = New<TResolveCacheNode>(trunkNode, path, std::move(payload));
        trunkNode->SetResolveCacheNode(node.Get());
        YT_VERIFY(idToNode.emplace(nodeId, node).second);

        YT_LOG_DEBUG("Resolve cache node added (NodeId: %v, Path: %v)",
            nodeId,
            path);

        return node;
    } else {
        return nodeIt->second;
    }
}

void TResolveCache::AddNodeChild(
    const TResolveCacheNodePtr& parentNode,
    const TResolveCacheNodePtr& childNode,
    const TString& key)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(parentNode->Lock);
    YT_ASSERT(std::holds_alternative<TResolveCacheNode::TMapPayload>(parentNode->Payload));
    auto& parentPayload = std::get<TResolveCacheNode::TMapPayload>(parentNode->Payload);
    auto it = parentPayload.KeyToChild.find(key);
    if (it != parentPayload.KeyToChild.end()) {
        YT_VERIFY(it->second == childNode);
        return;
    }

    YT_ASSERT(!childNode->Parent);
    childNode->ParentKeyToChildIt = parentPayload.KeyToChild.emplace(key, childNode).first;
    childNode->Parent = parentNode.Get();

    YT_LOG_DEBUG("Resolve cache child added (ParentId: %v, ChildId: %v, ParentPath: %v, Key: %v)",
        parentNode->GetId(),
        childNode->GetId(),
        parentNode->Path,
        key);
}

void TResolveCache::InvalidateNode(TCypressNode* node)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto cacheNode = node->GetResolveCacheNode();
    if (!cacheNode) {
        return;
    }

    std::vector<TCypressNode*> invalidatedNodes;
    std::function<void(TResolveCacheNode*)> traverse;
    traverse = [&] (TResolveCacheNode* currentCacheNode) {
        YT_LOG_DEBUG("Resolve cache descendant node invalidated (NodeId: %v, Path: %v)",
            currentCacheNode->GetId(),
            currentCacheNode->Path);
        invalidatedNodes.push_back(currentCacheNode->TrunkNode);
        if (const auto* mapPayload = std::get_if<TResolveCacheNode::TMapPayload>(&currentCacheNode->Payload)) {
            for (const auto& [key, child] : mapPayload->KeyToChild) {
                traverse(child.Get());
            }
        }
    };
    traverse(cacheNode.Get());

    auto* currentCacheNode = cacheNode.Get();
    while (auto* parentCacheNode = currentCacheNode->Parent) {
        auto guard = WriterGuard(parentCacheNode->Lock);
        YT_ASSERT(std::holds_alternative<TResolveCacheNode::TMapPayload>(parentCacheNode->Payload));
        auto& parentPayload = std::get<TResolveCacheNode::TMapPayload>(parentCacheNode->Payload);
        parentPayload.KeyToChild.erase(currentCacheNode->ParentKeyToChildIt);
        currentCacheNode->Parent = nullptr;
        if (!parentPayload.KeyToChild.empty()) {
            break;
        }
        YT_LOG_DEBUG("Resolve cache ancestor node trimmed (NodeId: %v, Path: %v)",
            parentCacheNode->GetId(),
            parentCacheNode->Path);
        invalidatedNodes.push_back(parentCacheNode->TrunkNode);
        currentCacheNode = parentCacheNode;
    }

    {
        for (auto* trunkNode : invalidatedNodes) {
            auto* shard = GetShard(trunkNode);
            auto guard = WriterGuard(shard->Lock);
            ResetNode(trunkNode);
            YT_VERIFY(shard->IdToNode.erase(trunkNode->GetId()) == 1);
        }

        for (auto* trunkNode : invalidatedNodes) {
            trunkNode->SetResolveCacheNode(nullptr);
        }
    }
}

TResolveCache::TShard* TResolveCache::GetShard(TNodeId nodeId)
{
    auto shardIndex = GetShardIndex<ShardCount>(nodeId);
    return &Shards_[shardIndex];
}

TResolveCache::TShard* TResolveCache::GetShard(TCypressNode* trunkNode)
{
    return GetShard(trunkNode->GetId());
}

void TResolveCache::ResetNode(TCypressNode* trunkNode)
{
    VERIFY_WRITER_SPINLOCK_AFFINITY(GetShard(trunkNode)->Lock);

    trunkNode->GetResolveCacheNode()->TrunkNode = nullptr;
    trunkNode->GetResolveCacheNode()->Parent = nullptr;
    trunkNode->SetResolveCacheNode(nullptr);
}

void TResolveCache::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    for (auto& shard : Shards_) {
        auto guard = WriterGuard(shard.Lock);
        for (auto [nodeId, cacheNode] : shard.IdToNode) {
            ResetNode(cacheNode->TrunkNode);
        }
        shard.IdToNode.clear();
    }

    YT_LOG_INFO("Resolve cache cleared");
}

TResolveCacheNode::TPayload TResolveCache::MakePayload(TCypressNode* trunkNode)
{
    if (trunkNode->GetType() == EObjectType::Link) {
        auto* linkNode = trunkNode->As<TLinkNode>();
        return TResolveCacheNode::TLinkPayload{linkNode->ComputeEffectiveTargetPath()};
    } else if (trunkNode->GetType() == EObjectType::PortalEntrance) {
        auto* entranceNode = trunkNode->As<TPortalEntranceNode>();
        auto portalExitId = MakePortalExitNodeId(entranceNode->GetId(), entranceNode->GetExitCellTag());
        return TResolveCacheNode::TPortalEntrancePayload{portalExitId};
    } else if (trunkNode->GetNodeType() == NYTree::ENodeType::Map) {
        return TResolveCacheNode::TMapPayload{};
    } else {
        YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
