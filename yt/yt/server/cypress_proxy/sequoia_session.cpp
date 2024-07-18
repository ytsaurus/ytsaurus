#include "sequoia_session.h"

#include "actions.h"
#include "action_helpers.h"
#include "helpers.h"

#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

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

namespace {

std::vector<TCypressNodeDescriptor> ParseSubtree(TRange<NRecords::TPathToNodeId> records)
{
    std::vector<TCypressNodeDescriptor> nodes;
    nodes.reserve(records.size());

    std::transform(
        records.begin(),
        records.end(),
        std::back_inserter(nodes),
        [] (const NRecords::TPathToNodeId& record) {
            return TCypressNodeDescriptor{
                .Id = record.NodeId,
                .Path = TAbsoluteYPath(DemangleSequoiaPath(record.Key.Path)),
            };
        });
    return nodes;
}

} // namespace

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

void TSequoiaSession::MaybeLockNodeInSequoiaTable(TNodeId nodeId, ELockType lockType)
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
    MaybeLockNodeInSequoiaTable(nodeId, ELockType::SharedStrong);
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
        MaybeLockNodeInSequoiaTable(parentId, ELockType::SharedStrong);
        DetachChild(parentId, path.GetBaseName(), SequoiaTransaction_);
    }
}

TSequoiaSession::TSubtree TSequoiaSession::FetchSubtree(TAbsoluteYPathBuf path)
{
    auto records = WaitFor(SelectSubtree(path, SequoiaTransaction_))
        .ValueOrThrow();
    return {ParseSubtree(records)};
}

void TSequoiaSession::DetachAndRemoveSubtree(const TSubtree& subtree, TNodeId parentId)
{
    MaybeLockNodeInSequoiaTable(subtree.Nodes.front().Id, ELockType::SharedStrong);
    RemoveSelectedSubtree(subtree.Nodes, SequoiaTransaction_, /*removeRoot*/ true, parentId);
}

TNodeId TSequoiaSession::CopySubtree(
    const TSubtree& subtree,
    TAbsoluteYPathBuf sourceRoot,
    TAbsoluteYPathBuf destinationRoot,
    TNodeId destinationParentId,
    const TCopyOptions& options)
{
    // To copy simlinks properly we have to fetch their target paths.

    std::vector<TNodeId> linkIds;
    for (const auto& node : subtree.Nodes) {
        if (IsLinkType(TypeFromId(node.Id))) {
            linkIds.push_back(node.Id);
        }
    }

    auto links = GetLinkTargetPaths(linkIds);

    auto createdSubtreeRootId = NCypressProxy::CopySubtree(
        subtree.Nodes,
        sourceRoot,
        destinationRoot,
        options,
        links,
        SequoiaTransaction_);

    AttachChild(
        destinationParentId,
        createdSubtreeRootId,
        destinationRoot.GetBaseName(),
        SequoiaTransaction_);

    return createdSubtreeRootId;
}

void TSequoiaSession::ClearSubtree(const TSubtree& subtree)
{
    MaybeLockNodeInSequoiaTable(subtree.Nodes.front().Id, ELockType::SharedStrong);
    RemoveSelectedSubtree(subtree.Nodes, SequoiaTransaction_, /*removeRoot*/ false);
}

void TSequoiaSession::ClearSubtree(TAbsoluteYPathBuf path)
{
    auto future = NCypressProxy::SelectSubtree(path, SequoiaTransaction_)
        .Apply(BIND([this, strongThis = MakeStrong(this)] (
            const std::vector<NRecords::TPathToNodeId>& records
        ) {
            MaybeLockNodeInSequoiaTable(records.front().NodeId, ELockType::SharedStrong);
            RemoveSelectedSubtree(ParseSubtree(records), SequoiaTransaction_, /*removeRoot*/ false);
        }));

    AdditionalFutures_.emplace_back(std::move(future));
}

std::optional<TAbsoluteYPath> TSequoiaSession::FindNodePath(TNodeId id)
{
    auto rsp = WaitFor(SequoiaTransaction_->LookupRows<NRecords::TNodeIdToPathKey>({{.NodeId = id}}))
        .ValueOrThrow();
    YT_VERIFY(rsp.size() == 1);

    if (!rsp.front()) {
        return std::nullopt;
    }

    if (IsLinkType(TypeFromId(id))) {
        auto guard = WriterGuard(CachedLinkTargetPathsLock_);
        CachedLinkTargetPaths_.emplace(id, TAbsoluteYPath(rsp.front()->TargetPath));
    }
    return TAbsoluteYPath(rsp.front()->Path);
}

THashMap<TNodeId, TAbsoluteYPath> TSequoiaSession::GetLinkTargetPaths(TRange<TNodeId> linkIds)
{
    THashMap<TNodeId, TAbsoluteYPath> result;
    result.reserve(linkIds.Size());

    std::vector<NRecords::TNodeIdToPathKey> linksToFetch;

    {
        auto guard = ReaderGuard(CachedLinkTargetPathsLock_);
        for (auto linkId : linkIds) {
            YT_VERIFY(IsLinkType(TypeFromId(linkId)));

            auto it = CachedLinkTargetPaths_.find(linkId);
            if (it != CachedLinkTargetPaths_.end()) {
                result.emplace(it->first, it->second);
            } else {
                linksToFetch.push_back({.NodeId = linkId});
            }
        }
    }

    if (!linksToFetch.empty()) {
        auto fetchedLinks = WaitFor(SequoiaTransaction_->LookupRows(linksToFetch))
            .ValueOrThrow();

        auto guard = WriterGuard(CachedLinkTargetPathsLock_);
        for (auto& record : fetchedLinks) {
            YT_VERIFY(record);
            auto nodeId = record->Key.NodeId;
            auto targetPath = TAbsoluteYPath(std::move(record->TargetPath));
            CachedLinkTargetPaths_.emplace(nodeId, targetPath);
            result.emplace(nodeId, std::move(targetPath));
        }
    }

    return result;
}

TAbsoluteYPath TSequoiaSession::GetLinkTargetPath(TNodeId linkId)
{
    return GetLinkTargetPaths(TRange(&linkId, 1)).at(linkId);
}

std::vector<TNodeId> TSequoiaSession::FindNodeIds(TRange<TAbsoluteYPathBuf> paths)
{
    std::vector<NRecords::TPathToNodeIdKey> keys(paths.size());
    std::transform(
        paths.begin(),
        paths.end(),
        keys.begin(),
        [] (TAbsoluteYPathBuf path) {
            return NRecords::TPathToNodeIdKey{MangleSequoiaPath(path.Underlying())};
        });

    auto rsps = WaitFor(SequoiaTransaction_->LookupRows(keys))
        .ValueOrThrow();

    std::vector<TNodeId> result(paths.size());
    std::transform(
        rsps.begin(),
        rsps.end(),
        result.begin(),
        [] (const std::optional<NRecords::TPathToNodeId>& record) {
            if (record) {
                return record->NodeId;
            } else {
                return TNodeId{};
            }
        });
    return result;
}

TNodeId TSequoiaSession::CreateMapNodeChain(
    TAbsoluteYPathBuf startPath,
    TNodeId startId,
    TRange<TString> names)
{
    MaybeLockNodeInSequoiaTable(startId, ELockType::SharedStrong);
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

TFuture<std::vector<TCypressChildDescriptor>> TSequoiaSession::FetchChildren(TNodeId nodeId)
{
    YT_VERIFY(IsSequoiaCompositeNodeType(TypeFromId(nodeId)));

    static auto parseRecords = BIND([] (std::vector<NRecords::TChildNode>&& records) {
        std::vector<TCypressChildDescriptor> result(records.size());
        std::transform(
            std::make_move_iterator(records.begin()),
            std::make_move_iterator(records.end()),
            result.begin(),
            [] (NRecords::TChildNode&& record) {
                return TCypressChildDescriptor{
                    .ParentId = record.Key.ParentId,
                    .ChildId = record.ChildId,
                    .ChildKey = std::move(record.Key.ChildKey),
                };
            });
        return result;
    });

    return SequoiaTransaction_->SelectRows<NRecords::TChildNode>({
        .WhereConjuncts = {Format("parent_id = %Qv", nodeId)},
        .OrderBy = {"child_key"},
    })
        .ApplyUnique(parseRecords);
}

TSequoiaSession::TSequoiaSession(ISequoiaTransactionPtr sequoiaTransaction)
    : SequoiaTransaction_(std::move(sequoiaTransaction))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
