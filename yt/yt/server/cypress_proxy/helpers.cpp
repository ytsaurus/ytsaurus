#include "private.h"
#include "helpers.h"
#include "action_helpers.h"

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NCypressProxy {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NYPath;

using TYPath = NYPath::TYPath;
using TYPathBuf = NYPath::TYPathBuf;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

std::vector<TYPathBuf> TokenizeUnresolvedSuffix(const TYPath& unresolvedSuffix)
{
    constexpr auto TypicalPathTokenCount = 3;
    std::vector<TYPathBuf> pathTokens;
    pathTokens.reserve(TypicalPathTokenCount);

    TTokenizer tokenizer(unresolvedSuffix);
    tokenizer.Advance();

    while (tokenizer.GetType() != ETokenType::EndOfStream) {
        tokenizer.Expect(ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);
        pathTokens.push_back(tokenizer.GetToken());
        tokenizer.Advance();
    };

    return pathTokens;
}

TYPath JoinNestedNodesToPath(
    const TYPath& parentPath,
    const std::vector<TYPathBuf>& childKeys)
{
    TStringBuilder builder;

    auto nestedLength = std::accumulate(
        childKeys.begin(),
        childKeys.end(),
        size_t(0),
        [] (size_t result, TYPathBuf childKey) {
            return result + childKey.size() + 1;
        });
    builder.Reserve(parentPath.size() + nestedLength);

    builder.AppendString(parentPath);
    for (auto childKey : childKeys) {
        builder.AppendChar('/');
        builder.AppendString(childKey);
    }
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedSequoiaType(EObjectType type)
{
    return IsSequoiaCompositeNodeType(type) || IsScalarType(type) || IsChunkOwnerType(type);
}

bool IsSequoiaCompositeNodeType(EObjectType type)
{
    return type == EObjectType::SequoiaMapNode || type == EObjectType::Scion;
}

void ValidateSupportedSequoiaType(EObjectType type)
{
    if (!IsSupportedSequoiaType(type)) {
        THROW_ERROR_EXCEPTION(
            "Object type %Qlv is not supported in Sequoia yet",
            type);
    }
}

void ThrowAlreadyExists(const TYPath& path)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::AlreadyExists,
        "Node %v already exists",
        path);
}

void ThrowNoSuchChild(const TYPath& existingPath, const TYPathBuf& missingPath)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Node %v has no child with key %Qv",
        existingPath,
        missingPath);
}

////////////////////////////////////////////////////////////////////////////////

// TODO(h0pless): Change this to TFuture<std::vector>.
std::vector<NRecords::TPathToNodeId> SelectSubtree(
    const TYPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    auto mangledPath = MangleSequoiaPath(path);
    return WaitFor(transaction->SelectRows<NRecords::TPathToNodeIdKey>({
        .Where = {
            Format("path >= %Qv", mangledPath),
            Format("path <= %Qv", MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(mangledPath))
        },
        .OrderBy = {"path"}
    }))
        .ValueOrThrow();
}

TNodeId LookupNodeId(
    const TYPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    NRecords::TPathToNodeIdKey nodeKey{
        .Path = MangleSequoiaPath(path),
    };
    auto rows = WaitFor(transaction->LookupRows<NRecords::TPathToNodeIdKey>({nodeKey}))
        .ValueOrThrow();
    if (rows.size() != 1) {
        YT_LOG_ALERT("Unexpected number of rows received while looking up a node by its path "
            "(Path: %v, RowCount: %v)",
            path,
            rows.size());
    } else if (!rows[0]) {
        YT_LOG_ALERT("Row with null value received while looking up a node by its path (Path: %v)",
            path);
    }

    return rows[0]->NodeId;
}

TNodeId CreateIntermediateNodes(
    const TYPath& parentPath,
    TNodeId parentId,
    const std::vector<TYPathBuf>& nodeKeys,
    const ISequoiaTransactionPtr& transaction)
{
    auto currentNodePath = parentPath;
    auto prevNodeId = parentId;
    for (auto key : nodeKeys) {
        // TODO(h0pless): Maybe use a different function here? This doesn't seem terribly efficient.
        // Replace this with something better once TYPath will be refactored.
        currentNodePath = JoinNestedNodesToPath(currentNodePath, {key});
        auto currentNodeId = transaction->GenerateObjectId(EObjectType::SequoiaMapNode);

        CreateNode(
            EObjectType::SequoiaMapNode,
            currentNodeId,
            currentNodePath,
            transaction);
        AttachChild(prevNodeId, currentNodeId, TYPath(key), transaction);
        prevNodeId = currentNodeId;
    }
    return prevNodeId;
}

TNodeId CopySubtree(
    const std::vector<NRecords::TPathToNodeId>& sourceNodes,
    const TYPath& sourceRootPath,
    const TYPath& destinationRootPath,
    const TCopyOptions& options,
    const ISequoiaTransactionPtr& transaction)
{
    THashMap<TYPath, std::vector<std::pair<TString, TNodeId>>> nodePathToChildren;
    TNodeId destinationNodeId;
    for (auto it = sourceNodes.rbegin(); it != sourceNodes.rend(); ++it) {
        auto destinationNodePath = DemangleSequoiaPath(it->Key.Path);
        destinationNodePath.replace(0, sourceRootPath.size(), destinationRootPath);
        destinationNodeId = CopyNode(
            it->NodeId,
            destinationNodePath,
            options,
            transaction);

        auto nodeIt = nodePathToChildren.find(destinationNodePath);
        if (nodeIt != nodePathToChildren.end()) {
            for (const auto& [childKey, childId] : nodeIt->second) {
                AttachChild(destinationNodeId, childId, childKey, transaction);
            }
            nodePathToChildren.erase(nodeIt);
        }

        auto [parentPath, childKey] = DirNameAndBaseName(destinationNodePath);
        nodePathToChildren[std::move(parentPath)].emplace_back(std::move(childKey), destinationNodeId);
    }

    YT_VERIFY(nodePathToChildren.size() == 1);
    return destinationNodeId;
}

void RemoveSelectedSubtree(
    const std::vector<NRecords::TPathToNodeId>& subtreeNodes,
    const ISequoiaTransactionPtr& transaction,
    bool removeRoot,
    TNodeId subtreeParentIdHint)
{
    YT_VERIFY(!subtreeNodes.empty());

    THashMap<TYPath, TNodeId> pathToNodeId;
    pathToNodeId.reserve(subtreeNodes.size());
    for (const auto& node : subtreeNodes) {
        pathToNodeId[DemangleSequoiaPath(node.Key.Path)] = node.NodeId;
    }

    for (auto nodeIt = subtreeNodes.begin() + (removeRoot ? 0 : 1); nodeIt < subtreeNodes.end(); ++nodeIt) {
        RemoveNode(nodeIt->NodeId, nodeIt->Key.Path, transaction);
    }

    for (auto it = subtreeNodes.rbegin(); it < subtreeNodes.rend(); ++it) {
        auto [parentPath, childKey] = DirNameAndBaseName(DemangleSequoiaPath(it->Key.Path));
        if (auto parentIt = pathToNodeId.find(parentPath)) {
            DetachChild(parentIt->second, childKey, transaction);
        }
    }

    auto rootType = TypeFromId(subtreeNodes.front().NodeId);
    if (!removeRoot || rootType == EObjectType::Scion) {
        return;
    }

    auto subtreeRootPath = DemangleSequoiaPath(subtreeNodes.front().Key.Path);
    auto [subtreeRootParentPath, subtreeRootKey] = DirNameAndBaseName(subtreeRootPath);
    if (!subtreeParentIdHint) {
        subtreeParentIdHint = LookupNodeId(subtreeRootParentPath, transaction);
    }
    DetachChild(subtreeParentIdHint, subtreeRootKey, transaction);
}

TFuture<void> RemoveSubtree(
    const TYPath& path,
    const ISequoiaTransactionPtr& transaction,
    bool removeRoot,
    TNodeId subtreeParentIdHint)
{
    if (!subtreeParentIdHint && removeRoot) {
        auto subtreeParentPath = removeRoot ? DirNameAndBaseName(path).first : path;
        subtreeParentIdHint = LookupNodeId(subtreeParentPath, transaction);
    }

    auto mangledPath = MangleSequoiaPath(path);
    return transaction->SelectRows<NRecords::TPathToNodeIdKey>({
        .Where = {
            Format("path >= %Qv", mangledPath),
            Format("path <= %Qv", MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(mangledPath))
        },
        .OrderBy = {"path"}
    }).Apply(
        BIND([transaction, removeRoot, subtreeParentIdHint] (const std::vector<NRecords::TPathToNodeId>& nodesToRemove) {
            RemoveSelectedSubtree(
                nodesToRemove,
                transaction,
                removeRoot,
                subtreeParentIdHint);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
