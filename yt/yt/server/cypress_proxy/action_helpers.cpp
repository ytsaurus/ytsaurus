#include "action_helpers.h"

#include "private.h"

#include "actions.h"
#include "helpers.h"

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

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
        HANDLE_ACTION_TYPE(TReqMultisetAttributes, 700)

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

TFuture<std::vector<NRecords::TPathToNodeId>> SelectSubtree(
    const TAbsoluteYPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    auto mangledPath = path.ToMangledSequoiaPath();
    return transaction->SelectRows<NRecords::TPathToNodeId>({
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
    TRange<std::string> nodeKeys,
    const ISequoiaTransactionPtr& transaction)
{
    auto currentNodePath = parentPath;
    auto currentNodeId = parentId;
    for (const auto& key : nodeKeys) {
        currentNodePath.Append(key);
        auto newNodeId = transaction->GenerateObjectId(EObjectType::SequoiaMapNode);

        CreateNode(
            newNodeId,
            /*parentId*/ currentNodeId,
            currentNodePath,
            /*explicitAttributes*/ nullptr,
            transaction);
        AttachChild(currentNodeId, newNodeId, key, transaction);
        currentNodeId = newNodeId;
    }
    return currentNodeId;
}

TNodeId CopySubtree(
    const std::vector<TCypressNodeDescriptor>& sourceNodes,
    const TAbsoluteYPath& sourceRootPath,
    const TAbsoluteYPath& destinationRootPath,
    TNodeId destinationSubtreeParentId,
    const TCopyOptions& options,
    const THashMap<TNodeId, NSequoiaClient::TAbsoluteYPath>& subtreeLinks,
    const ISequoiaTransactionPtr& transaction)
{
    // Node must occur earlier then its children.
    YT_ASSERT(IsSortedBy(sourceNodes, [] (const auto& node) {
        return node.Path.ToMangledSequoiaPath();
    }));
    YT_VERIFY(!sourceNodes.empty());
    YT_VERIFY(sourceNodes.front().Path == sourceRootPath);

    THashMap<TAbsoluteYPath, TNodeId> createdNodePathToId;
    createdNodePathToId.reserve(sourceNodes.size());

    for (const auto& sourceNode : sourceNodes) {
        TAbsoluteYPath destinationPath(sourceNode.Path);
        destinationPath.UnsafeMutableUnderlying()->replace(
            0,
            sourceRootPath.Underlying().size(),
            destinationRootPath.Underlying());

        NRecords::TNodeIdToPath sourceRecord{
            .Key = {.NodeId = sourceNode.Id},
            .Path = sourceNode.Path.Underlying(),
        };

        if (IsLinkType(TypeFromId(sourceNode.Id))) {
            sourceRecord.TargetPath = GetOrCrash(subtreeLinks, sourceNode.Id).Underlying();
        }

        auto destinationParentId = sourceNode.Path == sourceRootPath
            ? destinationSubtreeParentId
            : GetOrCrash(createdNodePathToId, destinationPath.GetDirPath());

        auto clonedNodeId = CopyNode(
            sourceRecord,
            destinationPath,
            destinationParentId,
            options,
            transaction);
        createdNodePathToId.emplace(destinationPath, clonedNodeId);

        AttachChild(destinationParentId, clonedNodeId, destinationPath.GetBaseName(), transaction);
    }

    return GetOrCrash(createdNodePathToId, destinationRootPath);
}

void RemoveSelectedSubtree(
    const std::vector<TCypressNodeDescriptor>& subtreeNodes,
    const ISequoiaTransactionPtr& transaction,
    TTransactionId cypressTransactionId,
    bool removeRoot,
    TNodeId subtreeParentId)
{
    YT_VERIFY(!subtreeNodes.empty());
    // For root removal we need to know its parent (excluding scion removal).
    YT_VERIFY(
        !removeRoot ||
        subtreeParentId ||
        TypeFromId(subtreeNodes.front().Id) == EObjectType::Scion);

    THashMap<TAbsoluteYPath, TNodeId> pathToNodeId;
    pathToNodeId.reserve(subtreeNodes.size());
    for (const auto& node : subtreeNodes) {
        pathToNodeId[node.Path] = node.Id;
    }

    for (auto nodeIt = subtreeNodes.begin() + (removeRoot ? 0 : 1); nodeIt < subtreeNodes.end(); ++nodeIt) {
        RemoveNode({nodeIt->Id, cypressTransactionId}, MangleSequoiaPath(nodeIt->Path.Underlying()), transaction);
    }

    for (auto it = subtreeNodes.rbegin(); it < subtreeNodes.rend(); ++it) {
        if (auto parentIt = pathToNodeId.find(it->Path.GetDirPath())) {
            DetachChild(parentIt->second, it->Path.GetBaseName(), transaction);
        }
    }

    auto rootType = TypeFromId(subtreeNodes.front().Id);
    if (!removeRoot || rootType == EObjectType::Scion) {
        return;
    }

    TAbsoluteYPath subtreeRootPath(subtreeNodes.front().Path);
    DetachChild(subtreeParentId, subtreeRootPath.GetBaseName(), transaction);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
