#include "private.h"
#include "helpers.h"
#include "action_helpers.h"
#include "path_resolver.h"
#include "sequoia_service_detail.h"

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>
#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>

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

using NYT::FromProto;

using TYPath = NSequoiaClient::TYPath;

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

TFuture<TSharedRefArray> ExecuteVerb(
    const ISequoiaServicePtr& service,
    const TSharedRefArray& requestMessage,
    const ISequoiaClientPtr& client,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
{
    TIntrusivePtr<TSequoiaServiceContext> context;
    try {
        auto transaction = WaitFor(StartCypressProxyTransaction(client))
            .ValueOrThrow();;

        context = CreateSequoiaContext(
            requestMessage,
            std::move(transaction),
            std::move(logger),
            logLevel);
        ResolvePath(context.Get());
    } catch (const std::exception& ex) {
        return MakeFuture(CreateErrorResponseMessage(ex));
    }

    const auto& resolveResult = context->GetResolveResultOrThrow();
    if (auto* payload = std::get_if<TSequoiaResolveResult>(&resolveResult)) {
        auto requestHeader = std::make_unique<NRpc::NProto::TRequestHeader>();
        if (!ParseRequestHeader(requestMessage, requestHeader.get())) {
            THROW_ERROR_EXCEPTION("Error parsing request header");
        }
        NYTree::SetRequestTargetYPath(requestHeader.get(), payload->UnresolvedSuffix.Underlying());
        context->SetRequestHeader(std::move(requestHeader));
    }

    auto asyncResponseMessage = context->GetAsyncResponseMessage();

    service->Invoke(context);

    return asyncResponseMessage;
}

////////////////////////////////////////////////////////////////////////////////

bool IsCreateRootstockRequest(const ISequoiaServiceContextPtr& context)
{
    if (context->GetMethod() != "Create") {
        return false;
    }

    THandlerInvocationOptions options;
    auto typedContext = New<TTypedSequoiaServiceContext<TReqCreate, TRspCreate>>(context, options);
    if (!typedContext->DeserializeRequest()) {
        THROW_ERROR_EXCEPTION("Error deserializing request");
    }

    return FromProto<EObjectType>(typedContext->Request().type()) == EObjectType::Rootstock;
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

// TODO(h0pless): Change this to TFuture<std::vector>.
std::vector<NRecords::TPathToNodeId> SelectSubtree(
    const TAbsoluteYPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    auto mangledPath = path.ToMangledSequoiaPath();
    return WaitFor(transaction->SelectRows<NRecords::TPathToNodeIdKey>({
        .WhereConjuncts = {
            Format("path >= %Qv", mangledPath),
            Format("path <= %Qv", MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(mangledPath))
        },
        .OrderBy = {"path"}
    }))
        .ValueOrThrow();
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
    const std::vector<TString>& nodeKeys,
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
        AttachChild(prevNodeId, currentNodeId, key, transaction);
        prevNodeId = currentNodeId;
    }
    return prevNodeId;
}

TNodeId CopySubtree(
    const std::vector<NRecords::TPathToNodeId>& sourceNodes,
    const TAbsoluteYPath& sourceRootPath,
    const TAbsoluteYPath& destinationRootPath,
    const TCopyOptions& options,
    const ISequoiaTransactionPtr& transaction)
{
    THashMap<TAbsoluteYPath, std::vector<std::pair<TString, TNodeId>>> nodePathToChildren;
    TNodeId destinationNodeId;
    for (auto it = sourceNodes.rbegin(); it != sourceNodes.rend(); ++it) {
        TAbsoluteYPath destinationNodePath(it->Key.Path);
        destinationNodePath.Underlying().replace(
            0,
            sourceRootPath.Underlying().size(),
            destinationRootPath.Underlying());
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
    TNodeId subtreeParentIdHint)
{
    YT_VERIFY(!subtreeNodes.empty());

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
    if (!subtreeParentIdHint) {
        subtreeParentIdHint = LookupNodeId(subtreeRootPath.GetDirPath(), transaction);
    }
    DetachChild(subtreeParentIdHint, subtreeRootPath.GetBaseName(), transaction);
}

TFuture<void> RemoveSubtree(
    const TAbsoluteYPath& path,
    const ISequoiaTransactionPtr& transaction,
    bool removeRoot,
    TNodeId subtreeParentIdHint)
{
    if (!subtreeParentIdHint && removeRoot) {
        auto subtreeParentPath = removeRoot ? TAbsoluteYPath(path.GetDirPath()) : path;
        subtreeParentIdHint = LookupNodeId(subtreeParentPath, transaction);
    }

    auto mangledPath = path.ToMangledSequoiaPath();
    return transaction->SelectRows<NRecords::TPathToNodeIdKey>({
        .WhereConjuncts = {
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
