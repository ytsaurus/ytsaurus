#include "sequoia_session.h"

#include "actions.h"
#include "action_helpers.h"
#include "helpers.h"

#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TSequoiaSession)

TSequoiaSessionPtr TSequoiaSession::Start(const ISequoiaClientPtr& sequoiaClient)
{
    auto sequoiaTransaction = WaitFor(StartCypressProxyTransaction(sequoiaClient))
        .ValueOrThrow();
    return New<TSequoiaSession>(std::move(sequoiaTransaction));
}

void TSequoiaSession::Commit(TCellId coordinatorCellId)
{
    if (!AdditionalFutures_.empty()) {
        WaitFor(AllSucceeded(std::move(AdditionalFutures_)))
            .ThrowOnError();
    }

    WaitFor(SequoiaTransaction_->Commit({
        .CoordinatorCellId = coordinatorCellId,
        .CoordinatorPrepareMode = coordinatorCellId
            ? ETransactionCoordinatorPrepareMode::Late
            : ETransactionCoordinatorPrepareMode::Early,
    }))
        .ThrowOnError();
}

TCellTag TSequoiaSession::RemoveRootstock(TNodeId rootstockId)
{
    NCypressServer::NProto::TReqRemoveNode reqRemoveRootstock;
    ToProto(reqRemoveRootstock.mutable_node_id(), rootstockId);
    SequoiaTransaction_->AddTransactionAction(
        CellTagFromId(rootstockId),
        MakeTransactionActionData(reqRemoveRootstock));
    return CellTagFromId(rootstockId);
}

void TSequoiaSession::LockNodeInSequoiaTable(TNodeId nodeId, ELockType lockType)
{
    if (!JustCreated(nodeId)) {
        LockRowInNodeIdToPathTable(nodeId, SequoiaTransaction_, lockType);
    }
}

bool TSequoiaSession::JustCreated(TObjectId id)
{
    return SequoiaTransaction_->CouldGenerateId(id);
}

void TSequoiaSession::SetNode(TNodeId nodeId, TYsonString value)
{
    LockNodeInSequoiaTable(nodeId, ELockType::SharedStrong);
    NYT::NCypressProxy::SetNode(nodeId, value, SequoiaTransaction_);
}

bool TSequoiaSession::IsMapNodeEmpty(TNodeId nodeId)
{
    YT_VERIFY(IsSequoiaCompositeNodeType(TypeFromId(nodeId)));

    return WaitFor(SequoiaTransaction_->SelectRows<NRecords::TChildNode>({
        .WhereConjuncts = {Format("parent_id = %Qv", nodeId)},
        .Limit = 1,
    }))
        .ValueOrThrow()
        .empty();
}

void TSequoiaSession::DetachAndRemoveSingleNode(
    TNodeId nodeId,
    TAbsoluteYPathBuf path,
    TNodeId parentId)
{
    RemoveNode(nodeId, path.ToMangledSequoiaPath(), SequoiaTransaction_);
    if (TypeFromId(nodeId) != EObjectType::Scion) {
        LockNodeInSequoiaTable(parentId, ELockType::SharedStrong);
        DetachChild(parentId, path.GetBaseName(), SequoiaTransaction_);
    }
}

TSequoiaSession::TSelectedSubtree TSequoiaSession::SelectSubtree(TAbsoluteYPathBuf path)
{
    auto records = WaitFor(NCypressProxy::SelectSubtree(path, SequoiaTransaction_))
        .ValueOrThrow();
    return {std::move(records)};
}

void TSequoiaSession::DetachAndRemoveSubtree(const TSelectedSubtree& subtree, TNodeId parentId)
{
    LockNodeInSequoiaTable(subtree.Records.front().NodeId, ELockType::SharedStrong);
    RemoveSelectedSubtree(subtree.Records, SequoiaTransaction_, /*removeRoot*/ true, parentId);
}

TNodeId TSequoiaSession::CopySubtree(
    const TSelectedSubtree& subtree,
    TAbsoluteYPathBuf sourceRoot,
    TAbsoluteYPathBuf destinationRoot,
    TNodeId destinationParentId,
    const TCopyOptions& options)
{
    // To copy simlinks properly we have to fetch their target paths.
    std::vector<NRecords::TNodeIdToPathKey> symlinkKeys;
    for (const auto& record : subtree.Records) {
        if (IsLinkType(TypeFromId(record.NodeId))) {
            symlinkKeys.push_back({.NodeId = record.NodeId});
        }
    }

    auto symlinks = WaitFor(SequoiaTransaction_->LookupRows(symlinkKeys))
        .ValueOrThrow();

    THashMap<TNodeId, NYPath::TYPath> targetPaths;
    targetPaths.reserve(symlinks.size());
    for (const auto& record : symlinks) {
        // Failure here means that Sequoia tables corrupted: node exists in
        // "path_to_node_id" table but hasn't corresponding record in
        // "node_id_to_path" table.
        YT_VERIFY(record);
        EmplaceOrCrash(targetPaths, record->Key.NodeId, record->TargetPath);
    }

    auto createdSubtreeRootId = NCypressProxy::CopySubtree(
        subtree.Records,
        sourceRoot,
        destinationRoot,
        options,
        targetPaths,
        SequoiaTransaction_);

    AttachChild(
        destinationParentId,
        createdSubtreeRootId,
        destinationRoot.GetBaseName(),
        SequoiaTransaction_);

    return createdSubtreeRootId;
}

void TSequoiaSession::ClearSubtree(const TSelectedSubtree& subtree)
{
    LockNodeInSequoiaTable(subtree.Records.front().NodeId, ELockType::SharedStrong);
    RemoveSelectedSubtree(subtree.Records, SequoiaTransaction_, /*removeRoot*/ false);
}

void TSequoiaSession::ClearSubtree(TAbsoluteYPathBuf path)
{
    auto future = NCypressProxy::SelectSubtree(path, SequoiaTransaction_)
        .Apply(BIND([this, strongThis = MakeStrong(this)] (
            const std::vector<NRecords::TPathToNodeId>& records
        ) {
            LockNodeInSequoiaTable(records.front().NodeId, ELockType::SharedStrong);
            RemoveSelectedSubtree(records, SequoiaTransaction_, /*removeRoot*/ false);
        }));

    AdditionalFutures_.emplace_back(std::move(future));
}

std::optional<NRecords::TNodeIdToPath> TSequoiaSession::FindNodeById(TNodeId id)
{
    auto rsp = WaitFor(SequoiaTransaction_->LookupRows<NRecords::TNodeIdToPathKey>({{.NodeId = id}}))
        .ValueOrThrow();
    YT_VERIFY(rsp.size() == 1);
    return std::move(rsp.front());
}

std::vector<std::optional<NRecords::TPathToNodeId>> TSequoiaSession::FindNodesByPath(
    const std::vector<NSequoiaClient::NRecords::TPathToNodeIdKey>& keys)
{
    return WaitFor(SequoiaTransaction_->LookupRows(keys))
        .ValueOrThrow();
}

TNodeId TSequoiaSession::CreateMapNodeChain(
    TAbsoluteYPathBuf startPath,
    TNodeId startId,
    TRange<TString> names)
{
    LockNodeInSequoiaTable(startId, ELockType::SharedStrong);
    return CreateIntermediateNodes(startPath, startId, names, SequoiaTransaction_);
}

TNodeId TSequoiaSession::CreateNode(
    EObjectType type,
    TAbsoluteYPathBuf path,
    const IAttributeDictionary* explicitAttributes,
    TNodeId parentId)
{
    auto createdNodeId = SequoiaTransaction_->GenerateObjectId(type);
    NCypressProxy::CreateNode(createdNodeId, path, explicitAttributes, SequoiaTransaction_);
    AttachChild(parentId, createdNodeId, path.GetBaseName(), SequoiaTransaction_);

    return createdNodeId;
}

TFuture<std::vector<NRecords::TChildNode>> TSequoiaSession::FetchChildren(TNodeId nodeId)
{
    YT_VERIFY(IsSequoiaCompositeNodeType(TypeFromId(nodeId)));

    return SequoiaTransaction_->SelectRows<NRecords::TChildNode>({
        .WhereConjuncts = {Format("parent_id = %Qv", nodeId)},
        .OrderBy = {"child_key"},
    });
}

TSequoiaSession::TSequoiaSession(ISequoiaTransactionPtr sequoiaTransaction)
    : SequoiaTransaction_(std::move(sequoiaTransaction))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
