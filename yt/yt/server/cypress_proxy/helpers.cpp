#include "helpers.h"

#include "private.h"

#include "action_helpers.h"
#include "path_resolver.h"
#include "sequoia_service.h"

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>
#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressClient::NProto;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NYPath;
using namespace NRpc;
using namespace NYTree;

using NYT::FromProto;

using TYPath = NSequoiaClient::TYPath;
using TYPathBuf = NSequoiaClient::TYPathBuf;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TSequoiaTransactionActionSequencer
    : public ISequoiaTransactionActionSequencer
{
    int GetActionPriority(TStringBuf actionType) const override
    {
        #define HANDLE_ACTION_TYPE(TAction, priority) \
            if (actionType == NCypressServer::NProto::TAction::GetDescriptor()->full_name()) { \
                return priority; \
            }

        HANDLE_ACTION_TYPE(TReqCloneNode, 100)
        HANDLE_ACTION_TYPE(TReqDetachChild, 200)
        HANDLE_ACTION_TYPE(TReqRemoveNode, 300)
        HANDLE_ACTION_TYPE(TReqCreateNode, 400)
        HANDLE_ACTION_TYPE(TReqAttachChild, 500)
        HANDLE_ACTION_TYPE(TReqSetNode, 600)

        #undef HANDLE_ACTION_TYPE

        YT_ABORT();
    }
};

static const TSequoiaTransactionActionSequencer TransactionActionSequencer;

static const TSequoiaTransactionSequencingOptions SequencingOptions = {
    .TransactionActionSequencer = &TransactionActionSequencer,
    .RequestPriorities = TSequoiaTransactionRequestPriorities{
        .DatalessLockRow = 100,
        .LockRow = 200,
        .WriteRow = 400,
        .DeleteRow = 300,
    },
};

} // namespace

TFuture<ISequoiaTransactionPtr> StartCypressProxyTransaction(
    const ISequoiaClientPtr& sequoiaClient,
    const TTransactionStartOptions& options)
{
    return sequoiaClient->StartTransaction(options, SequencingOptions);
}

////////////////////////////////////////////////////////////////////////////////

bool IsLinkType(NCypressClient::EObjectType type)
{
    return type == EObjectType::Link || type == EObjectType::SequoiaLink;
}

namespace {

TYPathBuf SkipAmpersand(TYPathBuf pathSuffix)
{
    TTokenizer tokenizer(pathSuffix.Underlying());
    tokenizer.Advance();
    tokenizer.Skip(ETokenType::Ampersand);
    return TYPathBuf(tokenizer.GetInput());
}

TAbsoluteYPath GetCanonicalYPath(const TResolveResult& resolveResult)
{
    return Visit(resolveResult,
        [] (const TCypressResolveResult& resolveResult) -> TAbsoluteYPath {
            // NB: Cypress resolve result doesn't contain unresolved symlinks.
            return TAbsoluteYPath(resolveResult.Path);
        },
        [] (const TSequoiaResolveResult& resolveResult) -> TAbsoluteYPath {
            // We don't want to distinguish "//tmp/a&/my-link" from
            // "//tmp/a/my-link".
            return resolveResult.Path + SkipAmpersand(resolveResult.UnresolvedSuffix);
        });
}

} // namespace

void ValidateLinkNodeCreation(
    const TSequoiaSessionPtr& session,
    TRawYPath targetPath,
    const TResolveResult& resolveResult)
{
    // TODO(danilalexeev): In case of a master-object designator the following
    // resolve will not produce a meaningful result. Such YPath has to be
    // resolved by master first.
    // TODO(kvk1920): probably works (since symlinks are stored in both resolve
    // tables now), but has to be tested.
    auto linkPath = GetCanonicalYPath(resolveResult);

    auto checkAcyclicity = [&] (
        TRawYPath pathToResolve,
        const TAbsoluteYPath& forbiddenPrefix)
    {
        std::vector<TSequoiaResolveIterationResult> history;
        auto resolveResult = ResolvePath(session, std::move(pathToResolve), /*method*/ {}, &history);

        for (const auto& [id, path] : history) {
            if (IsLinkType(TypeFromId(id)) && path == forbiddenPrefix) {
                return false;
            }
        }

        return GetCanonicalYPath(resolveResult) != forbiddenPrefix;
    };

    if (!checkAcyclicity(targetPath, linkPath)) {
        THROW_ERROR_EXCEPTION("Failed to create link: link is cyclic")
            << TErrorAttribute("target_path", targetPath.Underlying())
            << TErrorAttribute("path", linkPath);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> TokenizeUnresolvedSuffix(const TYPath& unresolvedSuffix)
{
    constexpr auto TypicalPathTokenCount = 3;
    std::vector<TString> pathTokens;
    pathTokens.reserve(TypicalPathTokenCount);

    TTokenizer tokenizer(unresolvedSuffix.Underlying());
    tokenizer.Advance();

    while (tokenizer.GetType() != ETokenType::EndOfStream) {
        tokenizer.Expect(ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);
        pathTokens.push_back(tokenizer.GetLiteralValue());
        tokenizer.Advance();
    };

    return pathTokens;
}

TAbsoluteYPath JoinNestedNodesToPath(
    const TAbsoluteYPath& parentPath,
    const std::vector<TString>& childKeys)
{
    TStringBuilder builder;

    auto nestedLength = 0;
    for (const auto& childKey : childKeys) {
        nestedLength += std::ssize(childKey) + 1;
    }
    builder.Reserve(std::ssize(parentPath.Underlying()) + nestedLength);

    builder.AppendString(parentPath.Underlying());
    for (auto childKey : childKeys) {
        builder.AppendChar('/');
        AppendYPathLiteral(&builder, childKey);
    }
    return TAbsoluteYPath(builder.Flush());
}

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedSequoiaType(EObjectType type)
{
    return IsSequoiaCompositeNodeType(type) ||
        IsScalarType(type) ||
        IsChunkOwnerType(type) ||
        type == EObjectType::SequoiaLink;
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

void ThrowAlreadyExists(const TAbsoluteYPath& path)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::AlreadyExists,
        "Node %v already exists",
        path);
}

void ThrowNoSuchChild(const TAbsoluteYPath& existingPath, TStringBuf missingPath)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Node %v has no child with key %Qv",
        existingPath,
        missingPath);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<std::vector<NRecords::TPathToNodeId>> SelectSubtree(
    const TAbsoluteYPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    auto mangledPath = path.ToMangledSequoiaPath();
    return transaction->SelectRows<NRecords::TPathToNodeIdKey>({
        .WhereConjuncts = {
            Format("path >= %Qv", mangledPath),
            Format("path <= %Qv", MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(mangledPath))
        },
        .OrderBy = {"path"}
    });
}

TNodeId LookupNodeId(
    TAbsoluteYPathBuf path,
    const ISequoiaTransactionPtr& transaction)
{
    NRecords::TPathToNodeIdKey nodeKey{
        .Path = path.ToMangledSequoiaPath(),
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
    const TAbsoluteYPath& parentPath,
    TNodeId parentId,
    TRange<TString> nodeKeys,
    const ISequoiaTransactionPtr& transaction)
{
    auto currentNodePath = parentPath;
    auto currentNodeId = parentId;
    for (const auto& key : nodeKeys) {
        currentNodePath.Append(key);
        auto newNodeId = transaction->GenerateObjectId(EObjectType::SequoiaMapNode);

        CreateNode(
            EObjectType::SequoiaMapNode,
            newNodeId,
            currentNodePath,
            /*explicitAttributes*/ nullptr,
            transaction);
        AttachChild(currentNodeId, newNodeId, key, transaction);
        currentNodeId = newNodeId;
    }
    return currentNodeId;
}

TNodeId CopySubtree(
    const std::vector<NRecords::TPathToNodeId>& sourceNodes,
    const TAbsoluteYPath& sourceRootPath,
    const TAbsoluteYPath& destinationRootPath,
    const TCopyOptions& options,
    const THashMap<TNodeId, NYPath::TYPath>& subtreeLinks,
    const ISequoiaTransactionPtr& transaction)
{
    THashMap<TAbsoluteYPath, std::vector<std::pair<TString, TNodeId>>> nodePathToChildren;
    nodePathToChildren.reserve(sourceNodes.size());
    TNodeId destinationNodeId;
    for (auto it = sourceNodes.rbegin(); it != sourceNodes.rend(); ++it) {
        TAbsoluteYPath destinationNodePath(it->Key.Path);
        destinationNodePath.UnsafeMutableUnderlying()->replace(
            0,
            sourceRootPath.Underlying().size(),
            destinationRootPath.Underlying());

        NRecords::TNodeIdToPath record{
            .Key = {.NodeId = it->NodeId},
            .Path = DemangleSequoiaPath(it->Key.Path),
        };

        if (IsLinkType(TypeFromId(it->NodeId))) {
            record.TargetPath = GetOrCrash(subtreeLinks, it->NodeId);
        }

        // NB: due to the reverse order of subtree traversing we naturally get
        // subtree root after the end of loop.
        destinationNodeId = CopyNode(
            record,
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

        TAbsoluteYPath parentPath(destinationNodePath.GetDirPath());
        auto childKey = destinationNodePath.GetBaseName();
        nodePathToChildren[std::move(parentPath)].emplace_back(std::move(childKey), destinationNodeId);
    }

    YT_VERIFY(nodePathToChildren.size() == 1);
    return destinationNodeId;
}

void RemoveSelectedSubtree(
    const std::vector<NRecords::TPathToNodeId>& subtreeNodes,
    const ISequoiaTransactionPtr& transaction,
    bool removeRoot,
    TNodeId subtreeParentId)
{
    YT_VERIFY(!subtreeNodes.empty());
    // For root removal we need to know its parent (excluding scion removal).
    YT_VERIFY(
        !removeRoot ||
        subtreeParentId ||
        TypeFromId(subtreeNodes.front().NodeId) == EObjectType::Scion);

    THashMap<TAbsoluteYPath, TNodeId> pathToNodeId;
    pathToNodeId.reserve(subtreeNodes.size());
    for (const auto& node : subtreeNodes) {
        pathToNodeId[TAbsoluteYPath(node.Key.Path)] = node.NodeId;
    }

    for (auto nodeIt = subtreeNodes.begin() + (removeRoot ? 0 : 1); nodeIt < subtreeNodes.end(); ++nodeIt) {
        RemoveNode(nodeIt->NodeId, nodeIt->Key.Path, transaction);
    }

    for (auto it = subtreeNodes.rbegin(); it < subtreeNodes.rend(); ++it) {
        TAbsoluteYPath path(it->Key.Path);
        if (auto parentIt = pathToNodeId.find(path.GetDirPath())) {
            DetachChild(parentIt->second, path.GetBaseName(), transaction);
        }
    }

    auto rootType = TypeFromId(subtreeNodes.front().NodeId);
    if (!removeRoot || rootType == EObjectType::Scion) {
        return;
    }

    TAbsoluteYPath subtreeRootPath(subtreeNodes.front().Key.Path);
    DetachChild(subtreeParentId, subtreeRootPath.GetBaseName(), transaction);
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TParsedReqCreate> TryParseReqCreate(ISequoiaServiceContextPtr context)
{
    YT_VERIFY(context->GetRequestHeader().method() == "Create");

    auto typedContext = New<TTypedSequoiaServiceContext<TReqCreate, TRspCreate>>(
        std::move(context),
        THandlerInvocationOptions{});

    // NB: this replies to underlying context on error.
    if (!typedContext->DeserializeRequest()) {
        return std::nullopt;
    }

    const auto& request = typedContext->Request();

    try {
        return TParsedReqCreate{
            .Type = CheckedEnumCast<EObjectType>(request.type()),
            .ExplicitAttributes = request.has_node_attributes()
                ? NYTree::FromProto(request.node_attributes())
                : CreateEphemeralAttributes(),
        };
    } catch (const std::exception& ex) {
        typedContext->Reply(ex);
        return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
