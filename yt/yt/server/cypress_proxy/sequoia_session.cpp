#include "sequoia_session.h"

#include "actions.h"
#include "action_helpers.h"
#include "bootstrap.h"
#include "helpers.h"

#include <yt/yt/server/lib/sequoia/cypress_transaction.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/sequoia_client/record_helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_replicas.record.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/iterator/zip.h>

#include <stack>

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

using TCypressTransactionAncestryView = TSequoiaSession::TCypressTransactionAncestryView;
using TCypressTransactionDepths = TSequoiaSession::TCypressTransactionDepths;

template <class T>
using TStack = std::stack<T, std::vector<T>>;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto& Logger = CypressProxyLogger;

const auto EmptyYPath = NSequoiaClient::TYPath("");

////////////////////////////////////////////////////////////////////////////////

void VerifyCypressTransactionAncestryInitialized(TCypressTransactionAncestryView ancestry)
{
    // Ancestry has to contain at least trunk (null transaction ID).
    YT_VERIFY(!ancestry.Empty());
    // First entry is always trunk.
    YT_VERIFY(!ancestry.Front());
}

TCypressTransactionDepths EnumerateCypressTransactionAncestry(TCypressTransactionAncestryView ancestry)
{
    VerifyCypressTransactionAncestryInitialized(ancestry);

    // NB: transactions have to be sorted by depth in ascending order.
    TCypressTransactionDepths depths;
    depths.reserve(ancestry.size());
    for (auto [index, cypressTransactionId] : Enumerate(ancestry)) {
        depths.emplace(cypressTransactionId, index);
    }
    return depths;
}

TStringBuf GetPrimarySortKeyForRecord(const NRecords::TChildNode& record)
{
    return record.Key.ChildKey;
}

TStringBuf GetPrimarySortKeyForRecord(const NRecords::TPathToNodeId& record)
{
    return record.Key.Path.Underlying();
}

template <class TRecord>
void SortRecordsByTransactionDepth(
    std::vector<TRecord>* record,
    const TCypressTransactionDepths& transactionDepths)
{
    // NB: we cannot sort as part of select query because transaction IDs
    // are stored as strings.
    SortBy(*record, [&] (const TRecord& record) {
        return std::pair(
            GetPrimarySortKeyForRecord(record),
            GetOrCrash(transactionDepths, record.Key.TransactionId));
    });
}

////////////////////////////////////////////////////////////////////////////////

std::optional<NRecords::TNodeIdToPath> LookupNodeById(
    const ISequoiaTransactionPtr& sequoiaTransaction,
    TNodeId nodeId,
    TCypressTransactionAncestryView ancestry)
{
    VerifyCypressTransactionAncestryInitialized(ancestry);

    std::vector<NRecords::TNodeIdToPathKey> keys(ancestry.size());
    std::ranges::copy(
        std::views::transform(ancestry, [=] (TTransactionId cypressTransactionId) {
            return NRecords::TNodeIdToPathKey{
                .NodeId = nodeId,
                .TransactionId = cypressTransactionId,
            };
        }),
        keys.begin());

    auto rsp = WaitFor(sequoiaTransaction->LookupRows(keys))
        .ValueOrThrow();
    YT_VERIFY(rsp.size() == keys.size());
    for (auto& record : rsp) {
        if (record.has_value()) {
            return std::move(record);
        }
    }

    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

void FillProgenitorTransactionCache(
    TProgenitorTransactionCache* cache,
    TRange<std::optional<NRecords::TPathToNodeId>> parentRecords,
    TRange<std::optional<NRecords::TPathToNodeId>> currentRecords)
{
    YT_VERIFY(cache);

    // Since it is lookup result here record count must always be the same.
    // Exception is scion since it doesn't have a parent so #parentRecords is
    // empty.
    YT_VERIFY(parentRecords.Empty() || parentRecords.Size() == currentRecords.Size());

    const TMangledSequoiaPath* currentPath = nullptr;

    // Find the progenitor tx under which such path exists.
    // NB: absence of the record for some transactions means that path wasn't
    // changed in this transaction (i.e. neither new node was created nor
    // existing node was removed or replaced).
    for (const auto& record : currentRecords) {
        if (!record.has_value()) {
            continue;
        }

        if (!currentPath) {
            cache->Path.emplace(record->Key.Path, record->Key.TransactionId);
            currentPath = &record->Key.Path;
        } else {
            // All records must have the same path.
            YT_VERIFY(record->Key.Path == *currentPath);
        }
    }

    if (!currentPath) {
        return;
    }

    for (const auto& record : currentRecords) {
        // Every non-tombstone record represents created or existing node so it
        // can be used to fill ProgenitorTxCache for "node_id_to_path" Sequoia
        // table.
        if (record.has_value() && !IsTombstone(*record)) {
            cache->Node.emplace(record->NodeId, record->Key.TransactionId);
        }
    }

    if (parentRecords.Empty()) {
        // NB: scions and snapshot nodes don't have a parent.
        return;
    }

    YT_VERIFY(parentRecords.Size() == currentRecords.Size());

    // Note that (parentId, childKey, childId, progenitorTxId) is always like a
    // (non-null parentId, childKey, non-null childId). Therefore, for each
    // non-null parent ID we need to try to find the closest non-null child ID.
    // Transaction of child must _not_ be less deeper than the parent's one.

    const int maxTransactionDepth = std::ssize(parentRecords);
    const auto childKey = std::string(TAbsoluteYPath(*currentPath).GetBaseName());

    auto parentNodeWithoutProgenitorTransaction = NullObjectId;
    for (int transactionDepth : std::views::iota(0, maxTransactionDepth)) {
        if (const auto& parentRecord = parentRecords[transactionDepth]) {
            if (IsTombstone(*parentRecord)) {
                parentNodeWithoutProgenitorTransaction = NullObjectId;
            } else {
                parentNodeWithoutProgenitorTransaction = parentRecord->NodeId;
            }
        }

        if (const auto& childRecord = currentRecords[transactionDepth]) {
            if (parentNodeWithoutProgenitorTransaction && !IsTombstone(*childRecord)) {
                cache->Child.emplace(
                    std::pair(parentNodeWithoutProgenitorTransaction, childKey),
                    childRecord->Key.TransactionId);
                parentNodeWithoutProgenitorTransaction = NullObjectId;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CSelectedSubtreeTraverserCallback =
    requires (
        T* callback,
        TRange<NRecords::TPathToNodeId> parentRecords,
        TRange<NRecords::TPathToNodeId> currentRecords)
    {
        { callback ->ProcessPath(parentRecords, currentRecords) } -> std::same_as<void>;
    };

//! For each path in subtree returns the deepest fork if it is not a tombstone.
class TPathForkResolver
{
public:
    void ProcessPath(
        TRange<NRecords::TPathToNodeId> parentRecords,
        TRange<NRecords::TPathToNodeId> currentRecords)
    {
        YT_VERIFY(!currentRecords.Empty());

        const auto& deepestFork = currentRecords.Back();
        if (!IsTombstone(deepestFork)) {
            Result_.push_back({
                .Id = deepestFork.NodeId,
                .Path = TAbsoluteYPath(deepestFork.Key.Path),
            });
        }
    }

    std::vector<TCypressNodeDescriptor> GetResult() &&
    {
        return std::move(Result_);
    }

private:
    std::vector<TCypressNodeDescriptor> Result_;
};
static_assert(CSelectedSubtreeTraverserCallback<TPathForkResolver>);

class TProgenitorTransactionCacheFiller
{
public:
    TProgenitorTransactionCacheFiller(
        TProgenitorTransactionCache* progenitorTransactionCache,
        const TCypressTransactionDepths& cypressTransactionDepths)
        : ProgenitorTransactionCache_(*progenitorTransactionCache)
        , CypressTransactionDepths_(cypressTransactionDepths)
    { }

    // This method does almost the same as FillProgenitorTransactionCache() but
    // uses the "select" result rather than "lookup" one.
    void ProcessPath(
        TRange<NRecords::TPathToNodeId> parentRecords,
        TRange<NRecords::TPathToNodeId> currentRecords)
    {
        YT_VERIFY(!currentRecords.Empty());

        auto parsedPath = TAbsoluteYPath(currentRecords.Front().Key.Path);

        // Progenitor transaction for path record is always the first record
        // since records are sorted by transaction depth.
        ProgenitorTransactionCache_.Path.emplace(
            currentRecords.Front().Key.Path,
            currentRecords.Front().Key.TransactionId);

        for (const auto& record : currentRecords) {
            // For every node ID there are no more than 2 records in
            // "path_to_node_id" Sequoia table: the record with progenitor
            // transaction ID and (possibly) the tombstone. Use the first one
            // to determine progenitor transaction id.
            // Note, that there is no any snapshot records in "path_to_node_id"
            // Sequoia table.
            if (!IsTombstone(record)) {
                ProgenitorTransactionCache_.Node.emplace(record.NodeId, record.Key.TransactionId);
            }
        }

        if (!parentRecords.empty()) {
            auto childKey = parsedPath.GetBaseName();

            int childForkIndex = 0;
            for (int parentForkIndex : std::views::iota(0, std::ssize(parentRecords))) {
                const auto& parentRecord = parentRecords[parentForkIndex];
                const auto* nextParentRecord = parentForkIndex + 1 < std::ssize(parentRecords)
                    ? &parentRecords[parentForkIndex + 1]
                    : nullptr;

                if (IsTombstone(parentRecord)) {
                    continue;
                }

                // Find first (progenitor) non-tombstone child.
                while (childForkIndex < std::ssize(currentRecords))
                {
                    const auto& childRecord = currentRecords[childForkIndex];
                    if (IsTombstone(childRecord)) {
                        ++childForkIndex;
                        continue;
                    }

                    if (GetTransactionDepth(parentRecord) > GetTransactionDepth(childRecord)) {
                        ++childForkIndex;
                        continue;
                    }

                    // Closes child found.
                    break;
                }

                if (childForkIndex == std::ssize(currentRecords)) {
                    break;
                }

                if (nextParentRecord &&
                    GetTransactionDepth(*nextParentRecord) <= GetTransactionDepth(currentRecords[childForkIndex]))
                {
                    // Found child record belongs to next (i.e. under deeper transaction) parent.
                    continue;
                }

                ProgenitorTransactionCache_.Child.emplace(
                    std::pair(parentRecord.NodeId, std::string(childKey)),
                    currentRecords[childForkIndex].Key.TransactionId);
            }
        }
    }

private:
    TProgenitorTransactionCache& ProgenitorTransactionCache_;
    const TCypressTransactionDepths& CypressTransactionDepths_;

    int GetTransactionDepth(const NRecords::TPathToNodeId& record) const
    {
        return GetOrCrash(CypressTransactionDepths_, record.Key.TransactionId);
    }
};
static_assert(CSelectedSubtreeTraverserCallback<TProgenitorTransactionCacheFiller>);

template <CSelectedSubtreeTraverserCallback... TCallback>
void TraverseSelectedSubtree(
    std::vector<NRecords::TPathToNodeId> records,
    const TCypressTransactionDepths& transactionDepths,
    TCallback*... callback)
{
    SortRecordsByTransactionDepth(&records, transactionDepths);

    std::stack<TRange<NRecords::TPathToNodeId>> currentAncestors;
    auto it = records.begin();
    while (it != records.end()) {
        // Find records related to the next path.
        auto [_, currentEnd] = std::equal_range(
            it,
            records.end(),
            *it,
            [] (const NRecords::TPathToNodeId& lhs, const NRecords::TPathToNodeId& rhs) {
                return lhs.Key.Path < rhs.Key.Path;
            });

        TRange<NRecords::TPathToNodeId> currentRecords(
            records.data() + (it - records.begin()),
            currentEnd - it);

        // Actualize current ancestors.
        const auto& currentPath = currentRecords.Front().Key.Path;
        while (
            !currentAncestors.empty() &&
            !currentPath.Underlying().StartsWith(currentAncestors.top().Front().Key.Path.Underlying()))
        {
            currentAncestors.pop();
        }

        TRange<NRecords::TPathToNodeId> parentRecords;
        if (!currentAncestors.empty()) {
            parentRecords = currentAncestors.top();
        }

        (callback->ProcessPath(parentRecords, currentRecords), ...);

        currentAncestors.push(currentRecords);
        it = currentEnd;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TSequoiaSession)

NCypressClient::TTransactionId TSequoiaSession::GetCurrentCypressTransactionId() const
{
    return CypressTransactionAncestry_.back();
}

TSequoiaSessionPtr TSequoiaSession::Start(
    IBootstrap* bootstrap,
    TTransactionId cypressTransactionId)
{
    auto sequoiaTransaction = WaitFor(StartCypressProxyTransaction(bootstrap->GetSequoiaClient()))
        .ValueOrThrow();

    std::vector<TTransactionId> cypressTransactions;

    if (cypressTransactionId) {
        auto cypressTransactionRecords = WaitFor(sequoiaTransaction->LookupRows(
            std::vector<NRecords::TTransactionKey>{{.TransactionId = cypressTransactionId}}))
            .ValueOrThrow();
        YT_VERIFY(cypressTransactionRecords.size() == 1);

        const auto& record = cypressTransactionRecords[0];

        if (!record.has_value()) {
            NTransactionServer::ThrowNoSuchTransaction(cypressTransactionId);
        }

        cypressTransactions.resize(2 + record->AncestorIds.size());
        cypressTransactions[0] = NullTransactionId;
        std::copy(
            record->AncestorIds.begin(),
            record->AncestorIds.end(),
            cypressTransactions.begin() + 1);
        cypressTransactions.back() = cypressTransactionId;
    } else {
        cypressTransactions = {NullTransactionId};
    }

    return New<TSequoiaSession>(
        bootstrap,
        std::move(sequoiaTransaction),
        std::move(cypressTransactions));
}

void TSequoiaSession::MaybeLockAndReplicateCypressTransaction()
{
    auto cypressTransactionId = GetCurrentCypressTransactionId();

    if (!cypressTransactionId) {
        return;
    }

    // To prevent concurrent finishing of this Cypress tx.
    SequoiaTransaction_->LockRow(
        NRecords::TTransactionKey{.TransactionId = cypressTransactionId},
        ELockType::SharedStrong);

    auto affectedCellTags = SequoiaTransaction_->GetAffectedMasterCellTags();
    Erase(affectedCellTags, CellTagFromId(cypressTransactionId));

    std::vector<NRecords::TTransactionReplicaKey> replicaKeys(affectedCellTags.size());
    std::transform(affectedCellTags.begin(), affectedCellTags.end(), replicaKeys.begin(), [&] (TCellTag cellTag) {
        return NRecords::TTransactionReplicaKey{
            .TransactionId = cypressTransactionId,
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
        return;
    }

    auto coordinatorCellId = Bootstrap_
        ->GetNativeConnection()
        ->GetMasterCellId(CellTagFromId(cypressTransactionId));

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

    WaitFor(ReplicateCypressTransactions(
        SequoiaTransaction_->GetClient(),
        {cypressTransactionId},
        dstCellTags,
        coordinatorCellId,
        NRpc::TDispatcher::Get()->GetHeavyInvoker(),
        Logger()))
        .ThrowOnError();
}

void TSequoiaSession::Commit(TCellId coordinatorCellId)
{
    YT_VERIFY(coordinatorCellId);

    if (coordinatorCellId && TypeFromId(coordinatorCellId) == EObjectType::MasterCell) {
        RequireLatePrepareOnNativeCellFor(coordinatorCellId);
    }

    ProcessAcquiredCypressLocksInSequoia();

    MaybeLockAndReplicateCypressTransaction();

    WaitFor(SequoiaTransaction_->Commit({
        .CoordinatorCellId = coordinatorCellId,
        .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
    }))
        .ThrowOnError();

    Finished_ = true;
}

void TSequoiaSession::RequireLatePrepare(TCellTag coordinatorCellTag)
{
    YT_VERIFY(coordinatorCellTag);

    // This is a check that late prepare mode was either not requested or the
    // same coordinator cell tag was specified.

    if (!RequiredCoordinatorCellTag_) {
        RequiredCoordinatorCellTag_ = coordinatorCellTag;
    } else {
        YT_VERIFY(RequiredCoordinatorCellTag_ == coordinatorCellTag);
    }
}

void TSequoiaSession::RequireLatePrepareOnNativeCellFor(TNodeId nodeId)
{
    RequireLatePrepare(CellTagFromId(nodeId));
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
    YT_VERIFY(GetCurrentCypressTransactionId());

    // This method can be used only to implement "lock" verb so no nodes could
    // be created in the current Sequoia tx.
    YT_VERIFY(!JustCreated(nodeId));

    RequireLatePrepareOnNativeCellFor(nodeId);

    if (lockMode == ELockMode::Snapshot) {
        auto record = LookupNodeById(SequoiaTransaction_, nodeId, CypressTransactionAncestry_);
        // This node has been already resolved.
        YT_VERIFY(record.has_value());
        YT_VERIFY(record->ForkKind != EForkKind::Tombstone);

        CreateSnapshotLockInSequoia(
            {nodeId, GetCurrentCypressTransactionId()},
            TAbsoluteYPath(record->Path),
            record->TargetPath.empty() ? std::nullopt : std::optional(TAbsoluteYPath(record->TargetPath)),
            SequoiaTransaction_);
    }

    AcquireCypressLockInSequoia(nodeId, lockMode);

    return LockNodeInMaster(
        {nodeId, GetCurrentCypressTransactionId()},
        lockMode,
        childKey,
        attributeKey,
        timestamp,
        waitable,
        SequoiaTransaction_);
}

void TSequoiaSession::UnlockNode(TNodeId nodeId, bool snapshot)
{
    YT_VERIFY(GetCurrentCypressTransactionId());

    // This method can be used only to implement "unlock" verb so no nodes could
    // be created in the current Sequoia tx.
    YT_VERIFY(!JustCreated(nodeId));

    RequireLatePrepareOnNativeCellFor(nodeId);

    if (snapshot) {
        RemoveSnapshotLockFromSequoia(
            {nodeId, GetCurrentCypressTransactionId()},
            SequoiaTransaction_);
    }

    return UnlockNodeInMaster({nodeId, GetCurrentCypressTransactionId()}, SequoiaTransaction_);
}

void TSequoiaSession::AcquireCypressLockInSequoia(
    TNodeId nodeId,
    ELockMode mode)
{
    if (JustCreated(nodeId)) {
        // We don't have to check conflicts if node hasn't been created before
        // current Sequoia tx.
        return;
    }

    AcquiredCypressLocks_.push_back({
        .NodeId = nodeId,
        .IsSnapshot = mode == ELockMode::Snapshot,
    });
}

void TSequoiaSession::ProcessAcquiredCypressLocksInSequoia()
{
    YT_VERIFY(RequiredCoordinatorCellTag_);

    for (const auto& lock : AcquiredCypressLocks_) {
        auto nodeCellTag = CellTagFromId(lock.NodeId);
        auto latePrepare = nodeCellTag == RequiredCoordinatorCellTag_;

        std::vector<NRecords::TNodeIdToPathKey> rowsToLock;
        auto lockType = ELockType::None;

        if (lock.IsSnapshot) {
            YT_VERIFY(latePrepare);

            // Using current transaction instead of trunk to avoid conflicts
            // with 2PC locks in ancestor transactions.

            lockType = ELockType::SharedStrong;
            rowsToLock.push_back({
                .NodeId = lock.NodeId,
                .TransactionId = GetCurrentCypressTransactionId(),
            });
        } else if (latePrepare) {
            lockType = ELockType::SharedStrong;
            rowsToLock.push_back({
                .NodeId = lock.NodeId,
                .TransactionId = NullTransactionId,
            });
        } else {
            lockType = ELockType::Exclusive;

            rowsToLock.reserve(CypressTransactionAncestry_.size());

            // Should conflict with both non-snapshot late prepare locks and
            // snapshot locks in ancestor transactions.
            for (auto cypressTransactionId : CypressTransactionAncestry_) {
                rowsToLock.push_back({
                    .NodeId = lock.NodeId,
                    .TransactionId = cypressTransactionId,
                });
            }
        }

        for (const auto& key : rowsToLock) {
            SequoiaTransaction_->LockRow(key, lockType);
        }
    }
}

bool TSequoiaSession::JustCreated(TObjectId id)
{
    return SequoiaTransaction_->CouldGenerateId(id);
}

void TSequoiaSession::SetNode(
    TNodeId nodeId,
    TYsonString value,
    const TSuppressableAccessTrackingOptions& options)
{
    // NB: Force flag is irrelevant when setting node's value.
    DoSetNode(nodeId, EmptyYPath, value, /*force*/ false, options);
}

void TSequoiaSession::SetNodeAttribute(
    TNodeId nodeId,
    TYPathBuf path,
    TYsonString value,
    bool force,
    const TSuppressableAccessTrackingOptions& options)
{
    YT_VERIFY(path.Underlying().StartsWith("/@"));
    DoSetNode(nodeId, path, value, force, options);
}

void TSequoiaSession::MultisetNodeAttributes(
    TNodeId nodeId,
    TYPathBuf path,
    const std::vector<TMultisetAttributesSubrequest>& subrequests,
    bool force,
    const NApi::TSuppressableAccessTrackingOptions& options)
{
    AcquireCypressLockInSequoia(nodeId, ELockMode::Shared);

    NYT::NCypressProxy::MultisetNodeAttributes(
        {nodeId, GetCurrentCypressTransactionId()},
        path,
        subrequests,
        force,
        options,
        SequoiaTransaction_);
}

void TSequoiaSession::RemoveNodeAttribute(TNodeId nodeId, TYPathBuf path, bool force)
{
    AcquireCypressLockInSequoia(nodeId, ELockMode::Shared);

    NYT::NCypressProxy::RemoveNodeAttribute(
        {nodeId, GetCurrentCypressTransactionId()},
        path,
        force,
        SequoiaTransaction_);
}

bool TSequoiaSession::IsMapNodeEmpty(TNodeId nodeId)
{
    // TODO(kvk1920): optimize. Emptiness check could be done simplier then the
    // whole subtree fetch.

    YT_VERIFY(IsSequoiaCompositeNodeType(TypeFromId(nodeId)));

    return WaitFor(FetchChildren(nodeId))
        .ValueOrThrow()
        .empty();
}

void TSequoiaSession::DetachAndRemoveSingleNode(
    TNodeId nodeId,
    TAbsoluteYPathBuf path,
    TNodeId parentId)
{
    // It's weird to create and remove node in the same Sequoia transaction.
    YT_VERIFY(!JustCreated(nodeId));
    YT_VERIFY(!JustCreated(parentId));

    // Just a sanity check.
    YT_VERIFY(parentId || TypeFromId(nodeId) == EObjectType::Scion);

    if (TypeFromId(nodeId) != EObjectType::Scion) {
        RequireLatePrepareOnNativeCellFor(parentId);
        AcquireCypressLockInSequoia(parentId, ELockMode::Shared);

        DetachChild(
            {parentId, GetCurrentCypressTransactionId()},
            path.GetBaseName(),
            /*options*/ {},
            ProgenitorTransactionCache_,
            SequoiaTransaction_);
    }

    RemoveNode(
        {nodeId, GetCurrentCypressTransactionId()},
        path,
        ProgenitorTransactionCache_,
        SequoiaTransaction_);
}

TSequoiaSession::TSubtree TSequoiaSession::FetchSubtree(TAbsoluteYPathBuf path)
{
    auto records = WaitFor(SelectSubtree(SequoiaTransaction_, path, CypressTransactionAncestry_))
        .ValueOrThrow();

    TProgenitorTransactionCacheFiller cacheFiller(
        &ProgenitorTransactionCache_,
        CypressTransactionDepths_);
    TPathForkResolver forkResolver;

    TraverseSelectedSubtree(
        std::move(records),
        CypressTransactionDepths_,
        &cacheFiller,
        &forkResolver);

    return TSubtree{.Nodes = std::move(forkResolver).GetResult()};
}

void TSequoiaSession::DetachAndRemoveSubtree(
    const TSubtree& subtree,
    TNodeId parentId,
    bool detachInLatePrepare)
{
    for (auto node : subtree.Nodes) {
        AcquireCypressLockInSequoia(node.Id, ELockMode::Exclusive);
    }

    // NB: scions don't have parents.
    if (parentId) {
        if (detachInLatePrepare) {
            RequireLatePrepareOnNativeCellFor(parentId);
        }

        AcquireCypressLockInSequoia(parentId, ELockMode::Shared);
    }

    RemoveSelectedSubtree(
        subtree.Nodes,
        SequoiaTransaction_,
        GetCurrentCypressTransactionId(),
        ProgenitorTransactionCache_,
        /*removeRoot*/ true,
        parentId);
}

TNodeId TSequoiaSession::CopySubtree(
    const TSubtree& subtree,
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
        subtree.Nodes[0].Path,
        destinationRoot,
        destinationParentId,
        GetCurrentCypressTransactionId(),
        options,
        links,
        ProgenitorTransactionCache_,
        SequoiaTransaction_);

    AttachChild(
        {destinationParentId, GetCurrentCypressTransactionId()},
        createdSubtreeRootId,
        destinationRoot.GetBaseName(),
        /*options*/ {},
        ProgenitorTransactionCache_,
        SequoiaTransaction_);

    return createdSubtreeRootId;
}

void TSequoiaSession::ClearSubtree(
    TAbsoluteYPathBuf path,
    const TSuppressableAccessTrackingOptions& options)
{
    auto records = WaitFor(SelectSubtree(SequoiaTransaction_, path, CypressTransactionAncestry_))
        .ValueOrThrow();

    auto targetNodeId = records.front().NodeId;
    RequireLatePrepareOnNativeCellFor(targetNodeId);
    AcquireCypressLockInSequoia(targetNodeId, ELockMode::Exclusive);

    TProgenitorTransactionCacheFiller cacheFiller(
        &ProgenitorTransactionCache_,
        CypressTransactionDepths_);
    TPathForkResolver forkResolver;

    TraverseSelectedSubtree(
        std::move(records),
        CypressTransactionDepths_,
        &cacheFiller,
        &forkResolver);

    auto subtreeNodes = std::move(forkResolver).GetResult();

    RemoveSelectedSubtree(
            subtreeNodes,
            SequoiaTransaction_,
            GetCurrentCypressTransactionId(),
            ProgenitorTransactionCache_,
            /*removeRoot*/ false,
            /*subtreeParentId*/ NullObjectId,
            /*options*/ options);
}

std::optional<TSequoiaSession::TResolvedNodeId> TSequoiaSession::FindNodePath(TNodeId id)
{
    std::vector<NRecords::TNodeIdToPathKey> keys(CypressTransactionAncestry_.size());
    std::transform(
        CypressTransactionAncestry_.rbegin(),
        CypressTransactionAncestry_.rend(),
        keys.begin(),
        [=] (TTransactionId cypressTransactionId) -> NRecords::TNodeIdToPathKey {
            return {.NodeId = id, .TransactionId = cypressTransactionId};
        });

    auto rsp = WaitFor(SequoiaTransaction_->LookupRows(keys))
        .ValueOrThrow();
    YT_VERIFY(rsp.size() == CypressTransactionAncestry_.size());

    // Note that we've requested records with order of transactions from nested
    // to progenitor.
    for (const auto& record : rsp) {
        if (!record.has_value()) {
            continue;
        }

        if (record->ForkKind == EForkKind::Tombstone) {
            return std::nullopt;
        }

        if (IsLinkType(TypeFromId(id))) {
            CachedLinkTargetPaths_.emplace(id, TAbsoluteYPath(record->TargetPath));
        }

        return TResolvedNodeId{
            .Path = TAbsoluteYPath(record->Path),
            .IsSnapshot = record->ForkKind == EForkKind::Snapshot,
        };
    }

    return std::nullopt;
}

THashMap<TNodeId, TAbsoluteYPath> TSequoiaSession::GetLinkTargetPaths(TRange<TNodeId> linkIds)
{
    THashMap<TNodeId, TAbsoluteYPath> result;
    result.reserve(linkIds.Size());

    std::vector<NRecords::TNodeIdToPathKey> linksToFetch;

    for (auto linkId : linkIds) {
        YT_VERIFY(IsLinkType(TypeFromId(linkId)));

        auto it = CachedLinkTargetPaths_.find(linkId);
        if (it != CachedLinkTargetPaths_.end()) {
            result.emplace(it->first, it->second);
        } else {
            linksToFetch.push_back({.NodeId = linkId});
        }
    }

    // Fast path.
    if (linksToFetch.empty()) {
        return result;
    }

    if (!linksToFetch.empty()) {
        auto fetchedLinks = WaitFor(SequoiaTransaction_->LookupRows(linksToFetch))
            .ValueOrThrow();

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

namespace {

bool ArePrefixes(TRange<TAbsoluteYPathBuf> prefixes)
{
    YT_VERIFY(prefixes.Front().Underlying() == "/");

    for (int i = 1; i < std::ssize(prefixes); ++i) {
        if (!prefixes[i].Underlying().StartsWith(prefixes[i - 1].Underlying())) {
            return false;
        }
    }

    return true;
}

} // namespace

std::vector<TNodeId> TSequoiaSession::FindNodeIds(TRange<TAbsoluteYPathBuf> paths)
{
    YT_VERIFY(ArePrefixes(paths));

    auto keys = std::vector<NRecords::TPathToNodeIdKey>(paths.size() * CypressTransactionAncestry_.size());
    for (int index = 0; index < std::ssize(paths); ++index) {
        std::transform(
            CypressTransactionAncestry_.begin(),
            CypressTransactionAncestry_.end(),
            keys.begin() + index * CypressTransactionAncestry_.size(),
            [&] (auto cypressTransactionId) -> NRecords::TPathToNodeIdKey {
                return {
                    .Path = paths[index].ToMangledSequoiaPath(),
                    .TransactionId = cypressTransactionId,
                };
            });
    }

    auto rsps = WaitFor(SequoiaTransaction_->LookupRows(keys))
        .ValueOrThrow();

    auto records = TRange(rsps);
    for (int index = 0; index < std::ssize(paths); ++index) {
        FillProgenitorTransactionCache(
            &ProgenitorTransactionCache_,
            index == 0
                ? TRange<std::optional<NRecords::TPathToNodeId>>{}
                : records.Slice(
                    (index - 1) * CypressTransactionAncestry_.size(),
                    index * CypressTransactionAncestry_.size()),
            records.Slice(
                index * CypressTransactionAncestry_.size(),
                (index + 1) * CypressTransactionAncestry_.size()));
    }

    std::vector<TNodeId> result(paths.size());

    for (int pathIndex = 0; pathIndex < std::ssize(paths); ++pathIndex) {
        auto pathRecords = TRange(rsps).Slice(
            pathIndex * CypressTransactionAncestry_.size(),
            (pathIndex + 1) * CypressTransactionAncestry_.size());
        // Find record with the deepest transaction for each path.
        for (int transactionIndex = std::ssize(CypressTransactionAncestry_) - 1; transactionIndex >= 0; --transactionIndex) {
            const auto& record = pathRecords[transactionIndex];
            if (record.has_value()) {
                result[pathIndex] = record->NodeId;
                break;
            }
        }
    }
    return result;
}

TNodeId TSequoiaSession::CreateMapNodeChain(
    TAbsoluteYPathBuf startPath,
    TNodeId startId,
    TRange<std::string> names,
    const TSuppressableAccessTrackingOptions& options)
{
    // We suppose that |startId| has existed before current Sequoia tx.
    YT_VERIFY(!JustCreated(startId));

    // Creation of subtree has to be done in late prepare on that cell which
    // owns the attachment point (node which already exists).
    RequireLatePrepareOnNativeCellFor(startId);

    // NB: this is the only node which was visible before current Sequoia
    // request so there can be concurrent (Cypress) lock request for this node.
    // To simplify conflict detection we C-lock this node in late prepare.
    if (!names.Empty()) {
        AcquireCypressLockInSequoia(startId, ELockMode::Shared);
    }

    return CreateIntermediateMapNodes(
        startPath,
        {startId, GetCurrentCypressTransactionId()},
        names,
        options,
        ProgenitorTransactionCache_,
        SequoiaTransaction_);
}

TNodeId TSequoiaSession::CreateNode(
    EObjectType type,
    TAbsoluteYPathBuf path,
    const IAttributeDictionary* explicitAttributes,
    TNodeId parentId,
    const TSuppressableAccessTrackingOptions& options)
{
    auto createdNodeId = SequoiaTransaction_->GenerateObjectId(type);

    NCypressProxy::CreateNode(
        {createdNodeId, GetCurrentCypressTransactionId()},
        parentId,
        path,
        explicitAttributes,
        ProgenitorTransactionCache_,
        SequoiaTransaction_);

    AttachChild(
        {parentId, GetCurrentCypressTransactionId()},
        createdNodeId,
        path.GetBaseName(),
        options,
        ProgenitorTransactionCache_,
        SequoiaTransaction_);

    return createdNodeId;
}

TFuture<std::vector<TCypressChildDescriptor>> TSequoiaSession::FetchChildren(TNodeId nodeId)
{
    YT_VERIFY(IsSequoiaCompositeNodeType(TypeFromId(nodeId)));

    auto transactionDepths = EnumerateCypressTransactionAncestry(CypressTransactionAncestry_);

    return SequoiaTransaction_->SelectRows<NRecords::TChildNode>({
        .WhereConjuncts = {
            Format("parent_id = %Qv", nodeId),
            BuildMultipleTransactionSelectCondition(CypressTransactionAncestry_),
        },
        .OrderBy = {"child_key"},
    }).ApplyUnique(BIND([transactionDepths = std::move(transactionDepths)] (
        std::vector<NRecords::TChildNode>&& records)
    {
        SortRecordsByTransactionDepth(&records, transactionDepths);

        std::vector<TCypressChildDescriptor> result;
        for (const auto& [index, record] : Enumerate(records)) {
            // Find the entry with the deepest transaction for each child key.
            if (index + 1 == records.size() || records[index + 1].Key.ChildKey != record.Key.ChildKey) {
                if (!record.ChildId) {
                    // Skip tombstone.
                    continue;
                }

                result.push_back({
                    .ParentId = record.Key.ParentId,
                    .ChildId = record.ChildId,
                    .ChildKey = record.Key.ChildKey,
                });
            }
        }
        return result;
    }));
}

void TSequoiaSession::DoSetNode(
    TNodeId nodeId,
    TYPathBuf path,
    TYsonString value,
    bool force,
    const TSuppressableAccessTrackingOptions& options)
{
    // "set" verb can touch not more than one previously existed node. To make
    // reads of this node atomic we have to use late prepare here.
    if (!JustCreated(nodeId)) {
        RequireLatePrepareOnNativeCellFor(nodeId);
    }

    AcquireCypressLockInSequoia(nodeId, ELockMode::Exclusive);

    NYT::NCypressProxy::SetNode(
        {nodeId, GetCurrentCypressTransactionId()},
        path,
        value,
        force,
        options,
        SequoiaTransaction_);
}

TSequoiaSession::TSequoiaSession(
    IBootstrap* bootstrap,
    ISequoiaTransactionPtr sequoiaTransaction,
    std::vector<TTransactionId> cypressTransactionIds)
    : SequoiaTransaction_(std::move(sequoiaTransaction))
    , Bootstrap_(bootstrap)
    , CypressTransactionAncestry_(std::move(cypressTransactionIds))
    , CypressTransactionDepths_(EnumerateCypressTransactionAncestry(CypressTransactionAncestry_))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
