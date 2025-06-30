#include "action_helpers.h"

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
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

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
        HANDLE_ACTION_TYPE(TReqFinishNodeMaterialization, 500)
        HANDLE_ACTION_TYPE(TReqAttachChild, 600)
        HANDLE_ACTION_TYPE(TReqSetNode, 700)
        HANDLE_ACTION_TYPE(TReqMultisetAttributes, 800)
        HANDLE_ACTION_TYPE(TReqImplicitlyLockNode, 900)

        #undef HANDLE_ACTION_TYPE

        YT_ABORT();
    }
};

static const TSequoiaTransactionActionSequencer TransactionActionSequencer;

static const TSequoiaTransactionOptions SequoiaTransactionOptionsTemplate = {
    .TransactionActionSequencer = &TransactionActionSequencer,
    .RequestPriorities = TSequoiaTransactionRequestPriorities{
        .DatalessLockRow = 100,
        .LockRow = 200,
        .WriteRow = 400,
        .DeleteRow = 300,
    },
    .SequenceTabletCommitSessions = true,
};

} // namespace

TFuture<ISequoiaTransactionPtr> StartCypressProxyTransaction(
    const ISequoiaClientPtr& sequoiaClient,
    ESequoiaTransactionType type,
    const std::vector<TTransactionId>& cypressPrerequisiteTransactionIds,
    const TTransactionStartOptions& options)
{
    auto sequoiaTransactionOptions = SequoiaTransactionOptionsTemplate;
    sequoiaTransactionOptions.CypressPrerequisiteTransactionIds = cypressPrerequisiteTransactionIds;
    return sequoiaClient->StartTransaction(type, options, sequoiaTransactionOptions);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<std::vector<NRecords::TPathToNodeId>> SelectSubtree(
    const ISequoiaTransactionPtr& transaction,
    const TAbsolutePath& path,
    TRange<TTransactionId> cypressTransactionIds)
{
    // NB: #cypressTransactionIds must contain at least 0-0-0-0 ("trunk")
    // transaction.
    YT_VERIFY(!cypressTransactionIds.Empty());

    auto mangledPath = path.ToMangledSequoiaPath();
    return transaction->SelectRows<NRecords::TPathToNodeId>({
        .WhereConjuncts = {
            Format("is_prefix(%Qv, path)", mangledPath),
            BuildMultipleTransactionSelectCondition(cypressTransactionIds),
        },
        .OrderBy = {"path"},
    });
}

TNodeId CreateIntermediateMapNodes(
    const TAbsolutePath& parentPath,
    TVersionedNodeId parentId,
    TRange<std::string> nodeKeys,
    const TSuppressableAccessTrackingOptions& options,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    auto currentNodePath = parentPath;
    auto currentNodeId = parentId.ObjectId;
    auto currentTransactionId = parentId.TransactionId;

    for (int index = 0; index < std::ssize(nodeKeys); ++index) {
        const auto& newNodeKey = nodeKeys[index];
        currentNodePath.Append(newNodeKey);
        auto newNodeId = sequoiaTransaction->GenerateObjectId(EObjectType::SequoiaMapNode);

        CreateNode(
            {newNodeId, currentTransactionId},
            currentNodeId,
            currentNodePath,
            /*explicitAttributes*/ nullptr,
            progenitorTransactionCache,
            sequoiaTransaction);
        AttachChild(
            {currentNodeId, currentTransactionId},
            newNodeId,
            newNodeKey,
            options,
            progenitorTransactionCache,
            sequoiaTransaction);
        currentNodeId = newNodeId;
    }

    return currentNodeId;
}

TNodeId CopySubtree(
    const std::vector<TCypressNodeDescriptor>& sourceNodes,
    const TAbsolutePath& sourceRootPath,
    const TAbsolutePath& destinationRootPath,
    TNodeId destinationSubtreeParentId,
    TTransactionId cypressTransactionId,
    const TCopyOptions& options,
    const THashMap<TNodeId, TYPath>& subtreeLinks,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const ISequoiaTransactionPtr& transaction)
{
    // Node must be placed ahead of its children.
    YT_ASSERT(IsSortedBy(sourceNodes, [] (const auto& node) {
        return node.Path.ToMangledSequoiaPath();
    }));
    YT_VERIFY(!sourceNodes.empty());
    YT_VERIFY(sourceNodes.front().Path == sourceRootPath);

    THashMap<TAbsolutePath, TNodeId> createdNodePathToId;
    createdNodePathToId.reserve(sourceNodes.size());

    for (const auto& sourceNode : sourceNodes) {
        TAbsolutePath destinationPath(sourceNode.Path);
        destinationPath.UnsafeMutableUnderlying()->replace(
            0,
            sourceRootPath.Underlying().size(),
            destinationRootPath.Underlying());

        NRecords::TNodeIdToPath sourceRecord{
            .Key = {.NodeId = sourceNode.Id},
            .Path = sourceNode.Path.ToRealPath().Underlying(),
        };

        if (IsLinkType(TypeFromId(sourceNode.Id))) {
            sourceRecord.TargetPath = GetOrCrash(subtreeLinks, sourceNode.Id);
        }

        auto destinationParentId = sourceNode.Path == sourceRootPath
            ? destinationSubtreeParentId
            : GetOrCrash(createdNodePathToId, destinationPath.GetDirPath());

        auto clonedNodeId = CopyNode(
            sourceRecord,
            destinationPath,
            destinationParentId,
            cypressTransactionId,
            options,
            progenitorTransactionCache,
            transaction);
        createdNodePathToId.emplace(destinationPath, clonedNodeId);

        AttachChild(
            {destinationParentId, cypressTransactionId},
            clonedNodeId,
            destinationPath.GetBaseName(),
            /*options*/ {},
            progenitorTransactionCache,
            transaction);
    }

    return GetOrCrash(createdNodePathToId, destinationRootPath);
}

void RemoveSelectedSubtree(
    const std::vector<TCypressNodeDescriptor>& subtreeNodes,
    const ISequoiaTransactionPtr& transaction,
    TTransactionId cypressTransactionId,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    bool removeRoot,
    TNodeId subtreeParentId,
    const TSuppressableAccessTrackingOptions& options)
{
    YT_VERIFY(!subtreeNodes.empty());
    // For root removal we need to know its parent (excluding scion removal).
    YT_VERIFY(
        !removeRoot ||
        subtreeParentId ||
        TypeFromId(subtreeNodes.front().Id) == EObjectType::Scion);

    THashMap<TAbsolutePath, TNodeId> pathToNodeId;
    pathToNodeId.reserve(subtreeNodes.size());
    for (const auto& node : subtreeNodes) {
        pathToNodeId[node.Path] = node.Id;
    }

    for (auto nodeIt = subtreeNodes.begin() + (removeRoot ? 0 : 1); nodeIt < subtreeNodes.end(); ++nodeIt) {
        RemoveNode(
            {nodeIt->Id, cypressTransactionId},
            nodeIt->Path,
            progenitorTransactionCache,
            transaction);
    }

    for (auto it = subtreeNodes.rbegin(); it < subtreeNodes.rend(); ++it) {
        if (auto parentIt = pathToNodeId.find(it->Path.GetDirPath())) {
            DetachChild(
                {parentIt->second, cypressTransactionId},
                it->Path.GetBaseName(),
                options,
                progenitorTransactionCache,
                transaction);
        }
    }

    auto rootType = TypeFromId(subtreeNodes.front().Id);
    if (!removeRoot || rootType == EObjectType::Scion) {
        return;
    }

    TAbsolutePath subtreeRootPath(subtreeNodes.front().Path);
    DetachChild(
        {subtreeParentId, cypressTransactionId},
        subtreeRootPath.GetBaseName(),
        options,
        progenitorTransactionCache,
        transaction);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
