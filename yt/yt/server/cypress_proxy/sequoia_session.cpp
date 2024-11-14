#include "sequoia_session.h"

#include "actions.h"
#include "action_helpers.h"
#include "bootstrap.h"
#include "helpers.h"

#include <yt/yt/server/lib/sequoia/cypress_transaction.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_replicas.record.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

using TYPathBuf = NSequoiaClient::TYPathBuf;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto& Logger = CypressProxyLogger;

const auto EmptyYPath = NSequoiaClient::TYPath("");

////////////////////////////////////////////////////////////////////////////////

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

TSequoiaSessionPtr TSequoiaSession::Start(
    IBootstrap* bootstrap,
    TTransactionId cypressTransactionId)
{
    auto sequoiaClient = bootstrap->GetSequoiaClient();
    auto sequoiaTransaction = WaitFor(StartCypressProxyTransaction(sequoiaClient))
        .ValueOrThrow();
    return New<TSequoiaSession>(bootstrap, std::move(sequoiaTransaction), cypressTransactionId);
}

void TSequoiaSession::LockAndReplicateCypressTransaction()
{
    if (!CypressTransactionId_) {
        return;
    }

    // To prevent concurrent finishing of this Cypress tx.
    SequoiaTransaction_->LockRow(
        NRecords::TTransactionKey{.TransactionId = CypressTransactionId_},
        ELockType::SharedStrong);

    auto affectedCellTags = SequoiaTransaction_->GetAffectedMasterCellTags();
    Erase(affectedCellTags, CellTagFromId(CypressTransactionId_));

    std::vector<NRecords::TTransactionReplicaKey> replicaKeys(affectedCellTags.size());
    std::transform(affectedCellTags.begin(), affectedCellTags.end(), replicaKeys.begin(), [&] (TCellTag cellTag) {
        return NRecords::TTransactionReplicaKey{
            .TransactionId = CypressTransactionId_,
            .CellTag = cellTag,
        };
    });

    auto replicas = WaitFor(SequoiaTransaction_->LookupRows(replicaKeys))
        .ValueOrThrow();

    TTransactionReplicationDestinationCellTagList dstCellTags;
    for (int i = 0; i < std::ssize(affectedCellTags); ++i) {
        if (!replicas[i].has_value()) {
            dstCellTags.push_back(affectedCellTags[i]);
        }
    }

    // Fast path.
    if (dstCellTags.empty()) {
        YT_LOG_DEBUG("Cypress transaction is already replicated to all participands");
        return;
    }

    YT_LOG_DEBUG("Need Cypress transaction replication (MasterCellTags: %v)",
        dstCellTags);

    auto coordinatorCellId = Bootstrap_
        ->GetNativeConnection()
        ->GetMasterCellId(CellTagFromId(CypressTransactionId_));

    // TODO(kvk1920): design a way to "stick" 2 Sequoia transactions together
    // to reduce latency. For now, there are 3 necessary waiting points in
    // Sequoia transaction:
    //   1. coordinator prepare;
    //   2. participants prepare;
    //   3. coordinator commit.
    // If Cypress transaction replication is needed we are waiting 6 times
    // because of additional Sequoia transaction for replication. If we could
    // execute main Sequoia transaction's prepare right in the same mutation as
    // additional Sequoia transaction's commit we could eliminate 2 of 3 waiting
    // points of main Sequoia transactions. The final cost of verb execution
    // with Cypress transaction replication may be 1 + 1/3 Sequoia transactions
    // instead of 2.

    AdditionalFutures_.push_back(ReplicateCypressTransactions(
        SequoiaTransaction_->GetClient(),
        {CypressTransactionId_},
        dstCellTags,
        coordinatorCellId,
        NRpc::TDispatcher::Get()->GetHeavyInvoker(),
        Logger()));
}

void TSequoiaSession::Commit(TCellId coordinatorCellId)
{
    YT_VERIFY(coordinatorCellId);

    Finished_ = true;

    LockAndReplicateCypressTransaction();

    if (!AdditionalFutures_.empty()) {
        WaitFor(AllSucceeded(std::move(AdditionalFutures_)))
            .ThrowOnError();
    }

    WaitFor(SequoiaTransaction_->Commit({
        .CoordinatorCellId = coordinatorCellId,
        .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
    }))
        .ThrowOnError();
}

TSequoiaSession::~TSequoiaSession()
{
    if (!Finished_) {
        Abort();
    }
}

void TSequoiaSession::Abort()
{
    Finished_ = true;

    if (!AdditionalFutures_.empty()) {
        for (auto& future : AdditionalFutures_) {
            future.Cancel(TError("Sequoia session aborted"));
        }
        AdditionalFutures_.clear();
    }
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

TLockId TSequoiaSession::LockNode(
    TNodeId nodeId,
    ELockMode lockMode,
    const std::optional<std::string>& childKey,
    const std::optional<std::string>& attributeKey,
    TTimestamp timestamp,
    bool waitable)
{
    YT_VERIFY(CypressTransactionId_);

    return LockNodeInMaster(
        {nodeId, CypressTransactionId_},
        lockMode,
        childKey,
        attributeKey,
        timestamp,
        waitable,
        SequoiaTransaction_);
}

void TSequoiaSession::UnlockNode(TNodeId nodeId)
{
    YT_VERIFY(CypressTransactionId_);

    return UnlockNodeInMaster({nodeId, CypressTransactionId_}, SequoiaTransaction_);
}

void TSequoiaSession::MaybeLockNodeInSequoiaTable(TNodeId nodeId, ELockType lockType)
{
    if (!JustCreated(nodeId)) {
        LockRowInNodeIdToPathTable(nodeId, SequoiaTransaction_, lockType);
    }
}

void TSequoiaSession::MaybeLockForRemovalInSequoiaTable(TNodeId nodeId)
{
    YT_VERIFY(!JustCreated(nodeId));

    // XXX(kvk1920): traverse all ancestor transactions.

    LockRowInNodeIdToPathTable(nodeId, SequoiaTransaction_, ELockType::Exclusive);
}

bool TSequoiaSession::JustCreated(TObjectId id)
{
    return SequoiaTransaction_->CouldGenerateId(id);
}

void TSequoiaSession::SetNode(TNodeId nodeId, TYsonString value)
{
    // NB: Force flag is irrelevant when setting node's value.
    DoSetNode(nodeId, EmptyYPath, value, /*force*/ false);
}

void TSequoiaSession::SetNodeAttribute(TNodeId nodeId, TYPathBuf path, TYsonString value, bool force)
{
    YT_VERIFY(path.Underlying().StartsWith("/@"));
    DoSetNode(nodeId, path, value, force);
}

void TSequoiaSession::MultisetNodeAttributes(
    TNodeId nodeId,
    TYPathBuf path,
    const std::vector<TMultisetAttributesSubrequest>& subrequests,
    bool force)
{
    MaybeLockNodeInSequoiaTable(nodeId, ELockType::SharedStrong);
    NYT::NCypressProxy::MultisetNodeAttributes(
        {nodeId, CypressTransactionId_},
        path,
        subrequests,
        force,
        SequoiaTransaction_);
}

void TSequoiaSession::RemoveNodeAttribute(TNodeId nodeId, TYPathBuf path, bool force)
{
    MaybeLockNodeInSequoiaTable(nodeId, ELockType::SharedStrong);
    NYT::NCypressProxy::RemoveNodeAttribute({nodeId, CypressTransactionId_}, path, force, SequoiaTransaction_);
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
    RemoveNode({nodeId, CypressTransactionId_}, path.ToMangledSequoiaPath(),  SequoiaTransaction_);
    if (TypeFromId(nodeId) != EObjectType::Scion) {
        MaybeLockNodeInSequoiaTable(parentId, ELockType::Exclusive);
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
    for (auto node : subtree.Nodes) {
        MaybeLockNodeInSequoiaTable(node.Id, ELockType::Exclusive);
    }

    RemoveSelectedSubtree(
        subtree.Nodes,
        SequoiaTransaction_,
        CypressTransactionId_,
        /*removeRoot*/ true,
        parentId);
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
        destinationParentId,
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
    RemoveSelectedSubtree(
        subtree.Nodes,
        SequoiaTransaction_,
        CypressTransactionId_,
        /*removeRoot*/ false);
}

void TSequoiaSession::ClearSubtree(TAbsoluteYPathBuf path)
{
    auto future = NCypressProxy::SelectSubtree(path, SequoiaTransaction_)
        .Apply(BIND([this, this_ = MakeStrong(this)] (
            const std::vector<NRecords::TPathToNodeId>& records
        ) {
            MaybeLockNodeInSequoiaTable(records.front().NodeId, ELockType::SharedStrong);
            RemoveSelectedSubtree(
                ParseSubtree(records),
                SequoiaTransaction_,
                CypressTransactionId_,
                /*removeRoot*/ false);
        }));

    AdditionalFutures_.push_back(std::move(future));
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
    TRange<std::string> names)
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
    NCypressProxy::CreateNode(createdNodeId, parentId, path, explicitAttributes, SequoiaTransaction_);
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

void TSequoiaSession::DoSetNode(TNodeId nodeId, TYPathBuf path, TYsonString value, bool force)
{
    MaybeLockNodeInSequoiaTable(nodeId, ELockType::SharedStrong);
    NYT::NCypressProxy::SetNode({nodeId, CypressTransactionId_}, path, value, force, SequoiaTransaction_);
}

TSequoiaSession::TSequoiaSession(
    IBootstrap* bootstrap,
    ISequoiaTransactionPtr sequoiaTransaction,
    TTransactionId cypressTransactionId)
    : SequoiaTransaction_(std::move(sequoiaTransaction))
    , CypressTransactionId_(cypressTransactionId)
    , Bootstrap_(bootstrap)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
