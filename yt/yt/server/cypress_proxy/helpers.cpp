#include "private.h"
#include "helpers.h"
#include "action_helpers.h"

#include <yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

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
    return IsSequoiaCompositeNodeType(type) || IsScalarType(type);
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

// TODO(h0pless): Change this to TFuture<std::vector>.
std::vector<NRecords::TPathToNodeId> SelectSubtree(
    const TYPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    auto mangledPath = MangleSequoiaPath(path);
    return WaitFor(transaction->SelectRows<NRecords::TPathToNodeIdKey>(
        {
            Format("path >= %Qv", mangledPath),
            Format("path <= %Qv", MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(mangledPath)),
        }))
        .ValueOrThrow();
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

TFuture<void> RemoveSubtree(
    const TYPath& path,
    const ISequoiaTransactionPtr& transaction,
    bool removeRoot)
{
    auto topmostRemainingNodePath = removeRoot ? DirNameAndBaseName(path).first : path;
    auto topmostRemainingNodeId = LookupNodeId(topmostRemainingNodePath, transaction);

    auto mangledPath = MangleSequoiaPath(path);
    TString rootComparator = removeRoot ? ">=" : ">";
    auto removeFuture = transaction->SelectRows<NRecords::TPathToNodeIdKey>({
        Format("path " + rootComparator + " %Qv", mangledPath),
        Format("path <= %Qv", MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(mangledPath)),
    }).Apply(
        BIND([transaction] (const std::vector<NRecords::TPathToNodeId>& nodesToRemove) {
            for (const auto& node : nodesToRemove) {
                RemoveNode(node.NodeId, node.Key.Path, transaction);
            }
        }));

    if (removeRoot) {
        DetachChild(topmostRemainingNodeId, DirNameAndBaseName(path).second, transaction);
        return removeFuture;
    } else {
        auto detachFuture = transaction->SelectRows<NRecords::TChildNodeKey>({
            Format("parent_path = %Qv", mangledPath),
        }).Apply(
            BIND([transaction, topmostRemainingNodeId] (const std::vector<NRecords::TChildNode>& childrenRows) {
                for (const auto& row : childrenRows) {
                    DetachChild(topmostRemainingNodeId, row.Key.ChildKey, transaction);
                }
            }));

        return AllSucceeded(std::vector{removeFuture, detachFuture});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
