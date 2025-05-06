#include "cypress_transaction.h"

#include "protobuf_helpers.h"

#include <yt/yt/server/lib/sequoia/proto/transaction_manager.pb.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/ytlib/cypress_transaction_client/proto/cypress_transaction_service.pb.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/record_helpers.h>
#include <yt/yt/ytlib/sequoia_client/table_descriptor.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>
#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/dependent_transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_snapshots.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_descendants.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_replicas.record.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt_proto/yt/core/ytree/proto/attributes.pb.h>

namespace NYT::NSequoiaServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NLogging;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NTableClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

// Helpers.

template <class T>
std::vector<T> MakeSortedAndUnique(std::vector<T>&& items)
{
    SortUnique(items);
    return items;
}

std::vector<NRecords::TTransactionKey> ToTransactionKeys(
    const std::vector<TTransactionId>& cypressTransactionIds)
{
    std::vector<NRecords::TTransactionKey> keys(cypressTransactionIds.size());
    std::transform(
        cypressTransactionIds.begin(),
        cypressTransactionIds.end(),
        keys.begin(),
        [] (auto transactionId) -> NRecords::TTransactionKey {
            return {.TransactionId = transactionId};
        });
    return keys;
}

void ValidateTransactionAncestors(const NRecords::TTransaction& record)
{
    auto isNested = TypeFromId(record.Key.TransactionId) == EObjectType::NestedTransaction;
    auto hasAncestors = !record.AncestorIds.empty();
    THROW_ERROR_EXCEPTION_IF(isNested != hasAncestors,
        NSequoiaClient::EErrorCode::SequoiaTableCorrupted,
        "Sequoia table %Qv is corrupted: %v",
        ITableDescriptor::Get(ESequoiaTable::Transactions)->GetTableName(),
        isNested
            ? "transaction is nested but its ancestor list is empty"
            : "transaction is progenitor but its ancestor list is not empty");
}

void ValidateTransactionAncestors(
    const std::vector<std::optional<NRecords::TTransaction>>& records)
{
    for (const auto& record : records) {
        ValidateTransactionAncestors(*record);
    }
}

void ValidateAllTransactionsExist(
    const std::vector<std::optional<NRecords::TTransaction>>& records)
{
    for (const auto& record : records) {
        if (!record) {
            // TODO(kvk1920): more verbose message (e.g. list all transactions).
            THROW_ERROR_EXCEPTION(
                NSequoiaClient::EErrorCode::SequoiaTableCorrupted,
                "Sequoia table %Qv is corrupted: some transactions are mentioned as ancestors but "
                "they are missing at Sequoia table",
                ITableDescriptor::Get(ESequoiaTable::Transactions)->GetTableName());
        }
    }
}

TSelectRowsQuery DoBuildSelectByTransactionIds(const auto& cypressTransactions, const auto& getId)
{
    YT_VERIFY(!cypressTransactions.empty());

    TStringBuilder builder;
    auto it = cypressTransactions.begin();
    builder.AppendFormat("transaction_id in (%Qv", getId(*it++));
    while (it != cypressTransactions.end()) {
        builder.AppendFormat(", %Qv", getId(*it++));
    }
    builder.AppendChar(')');
    return {.WhereConjuncts = {builder.Flush()}};
}

TSelectRowsQuery BuildSelectByTransactionIds(
    const THashMap<TTransactionId, NRecords::TTransaction>& cypressTransactions)
{
    return DoBuildSelectByTransactionIds(cypressTransactions, [] (const auto& pair) {
        return pair.first;
    });
}

TSelectRowsQuery BuildSelectByTransactionIds(
    const std::vector<TTransactionId>& cypressTransactions)
{
    return DoBuildSelectByTransactionIds(cypressTransactions, [] (TTransactionId id) {
        return id;
    });
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CForkRecordType =
    std::same_as<T, NRecords::TPathFork> ||
    std::same_as<T, NRecords::TNodeFork> ||
    std::same_as<T, NRecords::TChildFork>;

template <class T>
concept CResolveRecordType =
    std::same_as<T, NRecords::TPathToNodeId> ||
    std::same_as<T, NRecords::TNodeIdToPath> ||
    std::same_as<T, NRecords::TChildNode>;

template <class T>
using TRecordKey = std::decay_t<decltype(std::declval<T>().Key)>;

template <CForkRecordType TForkRecord>
auto MakeResolveRecordKey(const TForkRecord& forkRecord);

template <>
auto MakeResolveRecordKey<NRecords::TNodeFork>(const NRecords::TNodeFork& forkRecord)
{
    return NRecords::TNodeIdToPathKey{
        .NodeId = forkRecord.Key.NodeId,
        .TransactionId = forkRecord.Key.TransactionId,
    };
}

template <>
auto MakeResolveRecordKey<NRecords::TPathFork>(const NRecords::TPathFork& forkRecord)
{
    return NRecords::TPathToNodeIdKey{
        .Path = forkRecord.Key.Path,
        .TransactionId = forkRecord.Key.TransactionId,
    };
}

template <>
auto MakeResolveRecordKey<NRecords::TChildFork>(const NRecords::TChildFork& forkRecord)
{
    return NRecords::TChildNodeKey{
        .ParentId = forkRecord.Key.ParentId,
        .TransactionId = forkRecord.Key.TransactionId,
        .ChildKey = forkRecord.Key.ChildKey,
    };
}

NRecords::TNodeIdToPathKey MakeResolveRecordKey(const NRecords::TNodeSnapshot& forkRecord)
{
    return {.NodeId = forkRecord.Key.NodeId, .TransactionId = forkRecord.Key.TransactionId};
}

template <CForkRecordType TForkRecord>
auto MakeResolveRecord(const TForkRecord& forkRecord);

template <>
auto MakeResolveRecord<NRecords::TNodeFork>(const NRecords::TNodeFork& forkRecord)
{
    return NRecords::TNodeIdToPath{
        .Key = MakeResolveRecordKey(forkRecord),
        .Path = forkRecord.Path,
        .TargetPath = forkRecord.TargetPath,
        .ForkKind = IsTombstone(forkRecord) ? EForkKind::Tombstone : EForkKind::Regular,
    };
}

template <>
auto MakeResolveRecord<NRecords::TPathFork>(const NRecords::TPathFork& forkRecord)
{
    return NRecords::TPathToNodeId{
        .Key = MakeResolveRecordKey(forkRecord),
        .NodeId = forkRecord.NodeId,
    };
}

template <>
auto MakeResolveRecord(const NRecords::TChildFork& forkRecord)
{
    return NRecords::TChildNode{
        .Key = MakeResolveRecordKey(forkRecord),
        .ChildId = forkRecord.ChildId,
    };
}

////////////////////////////////////////////////////////////////////////////////

//! This class handles node/path forks on Cypress transaction commit.
/*!
 *  There are 2 different cases:
 *    - if committed fork is a tombstone and fork exists only under committed
 *      and parent transactions it should completely disappear from all tables
 *      to avoid accumulation of redundant tombstone forks under the progenitor
 *      transaction.
 *    - in every other case we should just propagate all fork-related records to
 *      the parent transaction. Note that it doesn't matter if this fork is
 *      replacement, creation or removal: even tombstone should be just
 *      propagated to the parent transaction.
 *
 *  Note that propagation for progenitor tx is occured only in resolve tables,
 *  because fork tables don't contain any records for trunk nodes.
 */
class TCypressTransactionChangesMerger
{
public:
    TCypressTransactionChangesMerger(
        ISequoiaTransaction* sequoiaTransaction,
        const NRecords::TTransaction& committedCypressTransaction,
        TRange<NRecords::TNodeFork> nodeForks,
        TRange<NRecords::TPathFork> pathForks,
        TRange<NRecords::TChildFork> childForks)
        : SequoiaTransaction_(sequoiaTransaction)
        , CommittedCypressTransactionId_(committedCypressTransaction.Key.TransactionId)
        , ParentCypressTransactionId_(committedCypressTransaction.AncestorIds.empty()
            ? NullTransactionId
            : committedCypressTransaction.AncestorIds.back())
        , NodeForks_(nodeForks)
        , PathForks_(pathForks)
        , ChildForks_(childForks)
    { }

    void Run()
    {
        MergeForks(NodeForks_);
        MergeForks(PathForks_);
        MergeForks(ChildForks_);
    }

private:
    ISequoiaTransaction* const SequoiaTransaction_;
    const TTransactionId CommittedCypressTransactionId_;
    const TTransactionId ParentCypressTransactionId_;
    const TRange<NRecords::TNodeFork> NodeForks_;
    const TRange<NRecords::TPathFork> PathForks_;
    const TRange<NRecords::TChildFork> ChildForks_;

    template <class TForkRecord>
    void MergeForks(TRange<TForkRecord> committedForks)
    {
        static_assert(CForkRecordType<TForkRecord>);

        for (const auto& record : committedForks) {
            // Every record in "{node,path}_forks" Sequoia table can be one of
            // these 3 kinds: creation, removal and replacement. The last kind
            // is different: when removal is committed to transaction under
            // which this node/path was created the record should be deleted
            // from "forks" and "resolve" tables.
            // NB: of course, "node_forks" cannot have "replacement" records.

            if (IsTombstone(record) && record.ProgenitorTransactionId == ParentCypressTransactionId_) {
                DeleteForkFromParent(record);
            } else {
                // Non-progenitor removal is non-distinguishable from creation: it
                // is just a propagation of record (with either created node or
                // tombstone) to parent transaction.
                WriteForkToParent(record);
            }
        }
    }

    template <class TForkRecord>
    void WriteForkToParent(const TForkRecord& forkRecord)
    {
        static_assert(CForkRecordType<TForkRecord>);

        if (ParentCypressTransactionId_) {
            // Fork tables don't contain records for trunk versions of nodes.
            SequoiaTransaction_->WriteRow(UnderParentCypressTransaction(forkRecord));
        }

        SequoiaTransaction_->WriteRow(UnderParentCypressTransaction(MakeResolveRecord(forkRecord)));
    }

    template <class TForkRecord>
    void DeleteForkFromParent(const TForkRecord& forkRecord)
    {
        static_assert(CForkRecordType<TForkRecord>);

        if (ParentCypressTransactionId_) {
            // Fork tables don't contain records for trunk versions of nodes.
            SequoiaTransaction_->DeleteRow(UnderParentCypressTransaction(forkRecord.Key));
        }

         SequoiaTransaction_->DeleteRow(UnderParentCypressTransaction(MakeResolveRecordKey(forkRecord)));
    }

    //! Used to propagate (or delete) records from committed Cypress tx to
    //! parent one. Can handle the following record types (and their keys):
    //! TNodeFork, TPathFork, TNodeIdToPath, TPathToNodeId, TChildNode.
    template <class T>
    T UnderParentCypressTransaction(T record)
    {
        if constexpr (requires { record.TransactionId; }) {
            using TRecord = T::TRecordDescriptor::TRecord;
            static_assert(CForkRecordType<TRecord> || CResolveRecordType<TRecord>);
        } else {
            static_assert(CForkRecordType<T> || CResolveRecordType<T>);
        }

        if constexpr (CForkRecordType<T>) {
            // On transaction commit all forks are propagated to parent tx
            // which may cause change of "progenitor_transaction_id".
            if (record.ProgenitorTransactionId == record.Key.TransactionId) {
                record.ProgenitorTransactionId = ParentCypressTransactionId_;
            }
        }

        // Replace transaction ID to parent's one for either key or non-key.
        if constexpr (requires (T record) { record.TransactionId; }) {
            record.TransactionId = ParentCypressTransactionId_;
        } else {
            record.Key.TransactionId = ParentCypressTransactionId_;
        }

        return record;
    }
};

//! This class is responsible for handling Cypress topology changes on
//! transaction finishing.
/*!
 *  On transaction finish the following tables are modified:
 *    - node_forks
 *    - node_snapshots
 *    - child_node
 *    - path_to_node_id
 *    - node_id_to_path
 *
 *  NB: Select(* from <resolve table> where tx_id == {T}) can be ineffective
 *  because tx_id isn't the first key column in resolve tables. Therefore, we
 *  start with fetching transaction's "delta" from "node_forks" and
 *  "node_snapshots" Sequoia table and then we know all changes under Cypress
 *  transaction which have to be cleaned up. "forks" tables are designed to be
 *  sufficient to handle all changes to parent tx on commit so no read of
 *  resolve tables is needed.
 *
 *  NB: Every Sequoia transaction which can finish some Cypress transactions is
 *  either commit or abort of the single Cypress transaction. This Sequoia
 *  transaction may abort more than one Cypress transactions (e.g. nested or
 *  dependent transactions) but there is no more than one _committed_ Cypress
 *  transactions. The committed Cypress transaction have to be handled in a
 *  different way (see TCypressTransactionChangesMerger).
 *
 *  So the final algorithm is following:
 *
 *  1. Select "delta" from "node_forks" and "node_snapshots" Sequoia tables for
 *     target tx. It allows us to avoid some of selects from resolve tables
 *     replacing them with lookup.
 *
 *  2. After the first step we know every changed row in resolve table and can
  *    just remove them.
 *
 *  3. For committed transaction we have to lookup changed rows in Sequoia table
 *     and merge them, but it's responsibility of another class.
  *    See TTransactionChangesMerger.
 */
class TCypressTransactionChangesProcessor
    : public TRefCounted
{
public:
    TCypressTransactionChangesProcessor(
        ISequoiaTransactionPtr sequoiaTransaction,
        const THashMap<TTransactionId, NRecords::TTransaction>& cypressTransactions,
        std::optional<NRecords::TTransaction> committedCypressTransaction,
        IInvokerPtr invoker)
        : SequoiaTransaction_(std::move(sequoiaTransaction))
        , FetchChangesQuery_(BuildSelectByTransactionIds(cypressTransactions))
        , CommittedCypressTransaction_(std::move(committedCypressTransaction))
        , Invoker_(std::move(invoker))
    {
        YT_VERIFY(
            !CommittedCypressTransaction_.has_value() ||
            cypressTransactions.contains(CommittedCypressTransaction_->Key.TransactionId));
    }

    TFuture<void> Run()
    {
        return AllSucceeded<void>({
            FetchChanges(&NodeForks_),
            FetchChanges(&Snapshots_),
            FetchChanges(&PathForks_),
            FetchChanges(&ChildForks_),
        })
            .Apply(BIND(&TCypressTransactionChangesProcessor::ProcessChanges, MakeStrong(this))
                .AsyncVia(Invoker_));
    }

private:
    const ISequoiaTransactionPtr SequoiaTransaction_;
    const TSelectRowsQuery FetchChangesQuery_;
    const std::optional<NRecords::TTransaction> CommittedCypressTransaction_;
    const IInvokerPtr Invoker_;

    // These fields are fetched from Sequoia tables once and then never changed.
    std::vector<NRecords::TNodeFork> NodeForks_;
    std::vector<NRecords::TPathFork> PathForks_;
    std::vector<NRecords::TChildFork> ChildForks_;
    std::vector<NRecords::TNodeSnapshot> Snapshots_;

    template <class T>
    TRange<T> FindCommittedForks(const std::vector<T>& records)
    {
        YT_VERIFY(CommittedCypressTransaction_.has_value());

        // Since |CommittedCypressTransaction_| is one of |cypressTransactions|
        // passed into constructor we've already fetched records for committed
        // Cypress transactions. We need just to find them.

        constexpr auto getTransactionId = [] (const auto& record) {
            return record.Key.TransactionId;
        };

        YT_ASSERT(IsSortedBy(records, getTransactionId));

        auto committedForksBegin = LowerBoundBy(
            records.begin(),
            records.end(),
            getTransactionId(*CommittedCypressTransaction_),
            getTransactionId);
        auto committedForksEnd = UpperBoundBy(
            records.begin(),
            records.end(),
            getTransactionId(*CommittedCypressTransaction_),
            getTransactionId);

        return TRange(
            records.data() + (committedForksBegin - records.begin()),
            committedForksEnd - committedForksBegin);
    }

    template <class T>
    TFuture<void> FetchChanges(std::vector<T>* to)
    {
        return SequoiaTransaction_->SelectRows<T>(FetchChangesQuery_)
            .ApplyUnique(BIND([to, this_ = MakeStrong(this)] (std::vector<T>&& changes) {
                *to = std::move(changes);
                SortBy(*to, [] (const T& record) {
                    return record.Key.TransactionId;
                });
            }));
    }

    void ProcessChanges()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (CommittedCypressTransaction_) {
            TCypressTransactionChangesMerger(
                SequoiaTransaction_.Get(),
                *CommittedCypressTransaction_,
                FindCommittedForks(NodeForks_),
                FindCommittedForks(PathForks_),
                FindCommittedForks(ChildForks_))
                .Run();
        }

        CleanupChanges(Snapshots_);
        CleanupChanges(NodeForks_);
        CleanupChanges(PathForks_);
        CleanupChanges(ChildForks_);
    }

    void CleanupChanges(const auto& changes)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        for (const auto& record : changes) {
            SequoiaTransaction_->DeleteRow(MakeResolveRecordKey(record));
            SequoiaTransaction_->DeleteRow(record.Key);
        }
    }
};

using TCypressTransactionChangesProcessorPtr = TIntrusivePtr<TCypressTransactionChangesProcessor>;

////////////////////////////////////////////////////////////////////////////////

//! This class is responsible for instantiation of transactions' replicas on
//! foreign cells and modification of "transaction_replicas" Sequoia table.
/*!
 *  It is not responsible for neither transaction hierarchy handling nor
 *  transaction coordinator's state modification. This class is used as:
 *    -  part of complete transaction replication;
 *    -  fast path for explicitly requested replication on transaction start.
 *  This class is designed to be used locally (e.g. it assumes that Sequoia
 *  transaction won't be destroyed during lifetime of this class).
 */
class TSimpleTransactionReplicator
{
public:
    explicit TSimpleTransactionReplicator(ISequoiaTransaction* sequoiaTransaction)
        : SequoiaTransaction_(sequoiaTransaction)
    { }

    TSimpleTransactionReplicator& AddTransaction(const NRecords::TTransaction& cypressTransaction)
    {
        auto* subrequest = Action_.add_transactions();
        ToProto(subrequest->mutable_id(), cypressTransaction.Key.TransactionId);
        ToProto(subrequest->mutable_parent_id(), cypressTransaction.AncestorIds.empty()
            ? NullTransactionId
            : cypressTransaction.AncestorIds.back());
        subrequest->set_upload(false);
        subrequest->set_enable_native_tx_externalization(true);

        auto attributes = ConvertTo<IMapNodePtr>(cypressTransaction.Attributes);

        #define MAYBE_SET_ATTRIBUTE(attribute_name) \
            if (auto attributeValue = \
                    attributes->FindChildValue<TString>(#attribute_name)) \
            { \
                subrequest->set_##attribute_name(*attributeValue); \
            }

        MAYBE_SET_ATTRIBUTE(title)
        MAYBE_SET_ATTRIBUTE(operation_type)
        MAYBE_SET_ATTRIBUTE(operation_id)
        MAYBE_SET_ATTRIBUTE(operation_title)

        #undef MAYBE_SET_ATTRIBUTE

        TransactionIds_.push_back(cypressTransaction.Key.TransactionId);
        return *this;
    }

    TSimpleTransactionReplicator& AddCell(TCellTag cellTag)
    {
        CellTags_.push_back(cellTag);
        return *this;
    }

    TSimpleTransactionReplicator& AddCells(TRange<TCellTag> cellTags)
    {
        CellTags_.insert(CellTags_.end(), cellTags.begin(), cellTags.end());
        return *this;
    }

    void Run()
    {
        auto transactionActionData = MakeTransactionActionData(Action_);
        for (auto cellTag : CellTags_) {
            SequoiaTransaction_->AddTransactionAction(cellTag, transactionActionData);

            for (auto transactionId : TransactionIds_) {
                SequoiaTransaction_->WriteRow(
                    NRecords::TTransactionReplica{
                        .Key = {.TransactionId = transactionId, .CellTag = cellTag},
                        .Dummy = 0,
                    },
                    ELockType::SharedWrite);
            }
        }
    }

private:
    ISequoiaTransaction* const SequoiaTransaction_;
    TCompactVector<TTransactionId, 1> TransactionIds_;
    TTransactionReplicationDestinationCellTagList CellTags_;
    NTransactionServer::NProto::TReqMaterializeCypressTransactionReplicas Action_;
};

//! Handles transaction replication in common case.
/*!
 *  Since different transactions may be in ancestor-descendant relationship
 *  transaction hierarchy is properly handled here in a non-trivial way:
 *  1. collect all ancestors, topologically sort them and remove duplicates;
 *  2. fetch ancestors' replicas to not replicate transaction to the same cell
 *     twice;
 *  3. materialize transaction on foreign cells via transaction actions;
 *  4. modify "transaction_replicas" Sequoia table.
 */
class TTransactionReplicator
    : public TRefCounted
{
public:
    TTransactionReplicator(
        ISequoiaTransactionPtr sequoiaTransaction,
        std::vector<std::optional<NRecords::TTransaction>> transactions,
        TTransactionReplicationDestinationCellTagList cellTags,
        IInvokerPtr invoker)
        : SequoiaTransaction_(std::move(sequoiaTransaction))
        , CellTags_(std::move(cellTags))
        , Invoker_(std::move(invoker))
    {
        CollectAndTopologicallySortAllAncestors(std::move(transactions));
    }

    template <CInvocable<void(TRange<std::optional<NRecords::TTransaction>>)> F>
    void IterateOverInnermostTransactionGroupedByCoordinator(F&& callback)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(!InnermostTransactions_.empty());

        int currentGroupStart = 0;
        for (int i = 1; i < std::ssize(InnermostTransactions_); ++i) {
            const auto& previous = CellTagFromId(InnermostTransactions_[i - 1]->Key.TransactionId);
            const auto& current = CellTagFromId(InnermostTransactions_[i]->Key.TransactionId);
            if (previous != current) {
                callback(TRange(InnermostTransactions_).Slice(currentGroupStart, i));
                currentGroupStart = i;
            }
        }

        callback(TRange(InnermostTransactions_)
            .Slice(currentGroupStart, InnermostTransactions_.size()));
    }

    TFuture<void> Run()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        return FetchAncestorsAndReplicas()
            .ApplyUnique(BIND(
                &TTransactionReplicator::ReplicateTransactions,
                MakeStrong(this))
                .AsyncVia(Invoker_));
    }

private:
    const ISequoiaTransactionPtr SequoiaTransaction_;
    const TTransactionReplicationDestinationCellTagList CellTags_;
    const IInvokerPtr Invoker_;
    std::vector<std::optional<NRecords::TTransaction>> InnermostTransactions_;
    std::vector<TTransactionId> AncestorIds_;

    struct TFetchedInfo
    {
        // |nullopt| means that certain transaction is NOT presented on certain
        // master cell. Of course, it's simplier to use vector<bool> instead but
        // we want to avoid unnecessary allocations here.
        // Order is a bit complicated:
        // (cell1, ancestor1), (cell1, ancestor2), ...
        // (cell1, transaction1), (cell1, transaction2), ...
        // (cell2, ancestor1), ...
        std::vector<std::optional<NRecords::TTransactionReplica>> Replicas;
        std::vector<std::optional<NRecords::TTransaction>> Ancestors;

        // TODO(kvk1920): add method IsReplicatedToCell() and use it instead of
        // looking into #Replicas directly.
    };

    void ReplicateTransactions(TFetchedInfo&& fetchedInfo)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto totalTransactionCount = AncestorIds_.size() + InnermostTransactions_.size();

        auto cellCount = std::ssize(CellTags_);
        // See comment in |TFetchedInfo|.
        for (int cellIndex = 0; cellIndex < cellCount; ++cellIndex) {
            auto replicaPresence = TRange(fetchedInfo.Replicas).Slice(
                totalTransactionCount * cellIndex,
                totalTransactionCount * (cellIndex + 1));
            auto ancestorReplicaPresence = replicaPresence.Slice(0, AncestorIds_.size());
            auto transactionReplicaPresence = replicaPresence.Slice(
                AncestorIds_.size(),
                replicaPresence.size());
            ReplicateToCell(
                fetchedInfo.Ancestors,
                ancestorReplicaPresence,
                transactionReplicaPresence,
                CellTags_[cellIndex]);
        }
    }

    void ReplicateToCell(
        TRange<std::optional<NRecords::TTransaction>> ancestors,
        TRange<std::optional<NRecords::TTransactionReplica>> ancestorReplicas,
        TRange<std::optional<NRecords::TTransactionReplica>> transactionReplicas,
        TCellTag cellTag)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        TSimpleTransactionReplicator replicator(SequoiaTransaction_.Get());
        replicator.AddCell(cellTag);

        auto replicateTransactions = [&] (
            TRange<std::optional<NRecords::TTransaction>> transactions,
            TRange<std::optional<NRecords::TTransactionReplica>> replicas)
        {
            YT_VERIFY(transactions.size() == replicas.size());

            for (int i = 0; i < std::ssize(replicas); ++i) {
                if (!replicas[i] && CellTagFromId(transactions[i]->Key.TransactionId) != cellTag) {
                    // There is no such replica so replication is needed.
                    replicator.AddTransaction(*transactions[i]);
                }
            }
        };

        replicateTransactions(ancestors, ancestorReplicas);
        replicateTransactions(InnermostTransactions_, transactionReplicas);

        replicator.Run();
    }

    TFuture<TFetchedInfo> FetchAncestorsAndReplicas()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto ancestors = FetchAncestors();
        auto replicas = FetchReplicas();

        // Fast path.
        if (!ancestors) {
            return replicas.ApplyUnique(BIND(
                [] (std::vector<std::optional<NRecords::TTransactionReplica>>&& replicas) {
                    return TFetchedInfo{
                        .Replicas = std::move(replicas),
                    };
                }).AsyncVia(Invoker_));
        }

        return ancestors.ApplyUnique(BIND([
            replicas = std::move(replicas),
            this,
            this_ = MakeStrong(this)
        ] (std::vector<std::optional<NRecords::TTransaction>>&& ancestors) {
            ValidateAncestors(ancestors);

            return replicas.ApplyUnique(BIND([
                ancestors = std::move(ancestors)
            ] (std::vector<std::optional<NRecords::TTransactionReplica>>&& replicas) {
                return TFetchedInfo{
                    .Replicas = std::move(replicas),
                    .Ancestors = std::move(ancestors),
                };
            }));
        })
            .AsyncVia(Invoker_));
    }

    TFuture<std::vector<std::optional<NRecords::TTransaction>>> FetchAncestors()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        // Fast path.
        if (AncestorIds_.empty()) {
            return std::nullopt;
        }

        auto keys = ToTransactionKeys(AncestorIds_);

        return SequoiaTransaction_->LookupRows(keys);
    }

    TFuture<std::vector<std::optional<NRecords::TTransactionReplica>>> FetchReplicas()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto totalTransactionCount = AncestorIds_.size() + InnermostTransactions_.size();
        std::vector<NRecords::TTransactionReplicaKey> keys(
            totalTransactionCount * CellTags_.size());
        for (int cellTagIndex = 0; cellTagIndex < std::ssize(CellTags_); ++cellTagIndex) {
            auto cellTag = CellTags_[cellTagIndex];

            auto ancestorsOffset = totalTransactionCount * cellTagIndex;
            std::transform(
                AncestorIds_.begin(),
                AncestorIds_.end(),
                keys.begin() + ancestorsOffset,
                [=] (TTransactionId id) {
                    return NRecords::TTransactionReplicaKey{
                        .TransactionId = id,
                        .CellTag = cellTag,
                    };
                });

            auto transactionsOffset = ancestorsOffset + AncestorIds_.size();
            std::transform(
                InnermostTransactions_.begin(),
                InnermostTransactions_.end(),
                keys.begin() + transactionsOffset,
                [=] (const std::optional<NRecords::TTransaction>& record) {
                    return NRecords::TTransactionReplicaKey{
                        .TransactionId = record->Key.TransactionId,
                        .CellTag = cellTag,
                    };
                });
        }

        return SequoiaTransaction_->LookupRows(keys);
    }

    void ValidateAncestors(const std::vector<std::optional<NRecords::TTransaction>>& records)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        ValidateAllTransactionsExist(records);
        ValidateTransactionAncestors(records);
    }

    void CollectAndTopologicallySortAllAncestors(
        std::vector<std::optional<NRecords::TTransaction>> transactions)
    {
        // We need to process every ancestor only once so we need to collect and
        // remove duplicates.
        THashSet<TTransactionId> allAncestors;
        for (const auto& transaction : transactions) {
            allAncestors.insert(
                transaction->AncestorIds.begin(),
                transaction->AncestorIds.end());
        }

        // We don't have to send replication requests for ancestors because
        // innermost transactions' replication already causes replication of
        // ancestors.
        transactions.erase(
            std::remove_if(
                transactions.begin(),
                transactions.end(),
                [&] (const std::optional<NRecords::TTransaction>& record) {
                    return allAncestors.contains(record->Key.TransactionId);
                }),
            transactions.end());
        SortBy(transactions, [] (const std::optional<NRecords::TTransaction>& record) {
            return CellTagFromId(record->Key.TransactionId);
        });

        // TODO(kvk1920): optimize.
        // #transactions may contain some ancestors, but we throw them away and
        // fetch again. We could avoid some lookups here. (Of course, it is
        // unlikely to be a bottleneck because lookups are done in parallel.
        // Rather, it's all about lookup latency).

        // Because transactions are instantiated in the order they are presented
        // here we have to sort them topologically: every ancestor of
        // transaction "T" must take a place somewhere before transaction "T".
        // This is the reason for this instead of just
        // |std::vector(allAncestors.begin(), allAncestors.end())|.
        AncestorIds_.reserve(allAncestors.size());
        for (const auto& record : transactions) {
            // NB: Ancestor_ids in "transactions" Sequoia table are always
            // topologically sorted.
            for (auto ancestorId : record->AncestorIds) {
                if (auto it = allAncestors.find(ancestorId); it != allAncestors.end()) {
                    allAncestors.erase(it);
                    AncestorIds_.push_back(ancestorId);
                }
            }
        }
        InnermostTransactions_ = std::move(transactions);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Modifies both master's persistent state and Sequoia tables.
template <class TResult, ESequoiaTransactionType TransactionType>
class TSequoiaMutation
    : public TRefCounted
{
public:
    TFuture<TResult> Apply()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND(&TSequoiaMutation::DoApply, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

protected:
    const ISequoiaClientPtr SequoiaClient_;
    const TCellId CoordinatorCellId_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;

    // Initialized once per class lifetime.
    ISequoiaTransactionPtr SequoiaTransaction_;

    TSequoiaMutation(
        ISequoiaClientPtr sequoiaClient,
        TCellId coordinatorCellId,
        TStringBuf description,
        IInvokerPtr invoker,
        TLogger logger)
        : SequoiaClient_(std::move(sequoiaClient))
        , CoordinatorCellId_(coordinatorCellId)
        , Invoker_(std::move(invoker))
        , Logger(std::move(logger))
        , Description_(description)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
    }

    TFuture<void> CommitSequoiaTransaction()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        // NB: |CoordinatorCellId_| may be null here but it's OK.
        return SequoiaTransaction_->Commit({
            .CoordinatorCellId = CoordinatorCellId_,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
            .StronglyOrdered = true,
        });
    }

    virtual TFuture<TResult> ApplyAndCommitSequoiaTransaction() = 0;

private:
    const TStringBuf Description_;

    TFuture<TResult> DoApply()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        return SequoiaClient_
            ->StartTransaction(TransactionType)
            .ApplyUnique(
                BIND(&TSequoiaMutation::OnSequoiaTransactionStarted, MakeStrong(this))
                    .AsyncVia(Invoker_));
    }

    TFuture<TResult> OnSequoiaTransactionStarted(ISequoiaTransactionPtr&& sequoiaTransaction)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(!SequoiaTransaction_);

        SequoiaTransaction_ = std::move(sequoiaTransaction);

        return ApplyAndCommitSequoiaTransaction()
            .Apply(BIND(&TSequoiaMutation::ProcessResult, MakeStrong(this))
                .AsyncVia(Invoker_));
    }

    TResult ProcessResult(const TErrorOr<TResult>& result)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (result.IsOK()) {
            if constexpr (std::is_void_v<TResult>) {
                return;
            } else {
                return result.Value();
            }
        }

        if (auto error = result.FindMatching(NSequoiaClient::EErrorCode::SequoiaTableCorrupted)) {
            // NB: Consider disabling Cypress tx mirroring by setting
            // //sys/@config/sequoia_manager/enable_cypress_transactions_in_sequoia to false.
            // Ensure that you actually know what are you doing.
            YT_LOG_ALERT(
                *error,
                "Failed to %v Cypress transaction on Sequoia",
                Description_);
        }

        if (IsRetriableSequoiaError(result)) {
            THROW_ERROR_EXCEPTION(
                NSequoiaClient::EErrorCode::SequoiaRetriableError,
                "Sequoia retriable error")
                << std::move(result);
        }

        THROW_ERROR result;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Starts Cypress transaction mirrored to Sequoia tables.
/*!
 *   1. Generate new transaction id;
 *   2. If there is no parent transaction then go to step 6;
 *   3. Lock parent transaction in "transactions" Sequoia table;
 *   4. Fetch parent's ancestors;
 *   5. Write (ancestor_id, transaction_id) to table "transaction_descendants" for
 *      every ancestor;
 *   6. Write (transaction_id, ancestor_ids) to table "transactions";
 *   7. Write (prerequisite_id, transaction_id) to table "dependent_transactions";
 *   8. Execute StartCypressTransaction tx action on coordinator;
 *   9. Execute StartForeignTransaction tx action on every cell which this
 *      transaction should be replicated to;
 *  10. Reply with transaction id generated in step 1.
 */
class TStartCypressTransaction
    : public TSequoiaMutation<TTransactionId, ESequoiaTransactionType::CypressTransactionMirroring>
{
public:
    TStartCypressTransaction(
        ISequoiaClientPtr sequoiaClient,
        TCellId coordinatorCellId,
        NCypressTransactionClient::NProto::TReqStartTransaction request,
        NRpc::TAuthenticationIdentity authenticationIdentity,
        IInvokerPtr invoker,
        TLogger logger)
        : TSequoiaMutation(
            std::move(sequoiaClient),
            coordinatorCellId,
            "start",
            std::move(invoker),
            std::move(logger))
        , ParentId_(FromProto<TTransactionId>(request.parent_id()))
        , ReplicateToCellTags_(BuildReplicateToCellTags(
            CellTagFromId(coordinatorCellId),
            FromProto<TCellTagList>(request.replicate_to_cell_tags())))
        , PrerequisiteTransactionIds_(MakeSortedAndUnique(
            FromProto<std::vector<TTransactionId>>(request.prerequisite_transaction_ids())))
        , Request_(BuildStartCypressTransactionRequest(
            std::move(request),
            std::move(authenticationIdentity)))
    { }

protected:
    TFuture<TTransactionId> ApplyAndCommitSequoiaTransaction() override
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(SequoiaTransaction_);

        // Should be the cell tag of Cypress tx coordinator.
        YT_VERIFY(CoordinatorCellId_);

        auto transactionId = SequoiaTransaction_->GenerateObjectId(
            ParentId_ ? EObjectType::NestedTransaction : EObjectType::Transaction,
            CellTagFromId(CoordinatorCellId_));
        ToProto(Request_.mutable_hint_id(), transactionId);

        auto returnResult = BIND([=] {
            return transactionId;
        });

        // Fast path.
        if (!ParentId_ && PrerequisiteTransactionIds_.empty()) {
            auto asyncResult = ModifyTablesAndRegisterActions();
            // Fast path is synchronous.
            YT_VERIFY(asyncResult.IsSet());
            return CommitSequoiaTransaction()
                .Apply(std::move(returnResult));
        }

        return HandlePrerequisiteTransactions()
            .Apply(BIND(
                &TStartCypressTransaction::LockParentAndCollectAncestors,
                MakeStrong(this))
                    .AsyncVia(Invoker_))
            .ApplyUnique(BIND(
                &TStartCypressTransaction::ModifyTablesAndRegisterActions,
                MakeStrong(this))
                    .AsyncVia(Invoker_))
            .Apply(BIND(
                &TStartCypressTransaction::CommitSequoiaTransaction,
                MakeStrong(this))
                    .AsyncVia(Invoker_))
            .Apply(std::move(returnResult));
    }

private:
    const TTransactionId ParentId_;
    const TCellTagList ReplicateToCellTags_;
    const std::vector<TTransactionId> PrerequisiteTransactionIds_;

    // NB: Transaction ID is set after Sequoia tx is started.
    NTransactionServer::NProto::TReqStartCypressTransaction Request_;

    static TCellTagList BuildReplicateToCellTags(TCellTag thisCellTag, TCellTagList cellTags)
    {
        cellTags.erase(std::remove(cellTags.begin(), cellTags.end(), thisCellTag), cellTags.end());
        Sort(cellTags);
        return cellTags;
    }

    TFuture<void> ModifyTablesAndRegisterActions(
        std::vector<TTransactionId>&& ancestorIds = {}) const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto transactionId = FromProto<TTransactionId>(Request_.hint_id());

        for (auto ancestorId : ancestorIds) {
            SequoiaTransaction_->WriteRow(NRecords::TTransactionDescendant{
                .Key = {.TransactionId = ancestorId, .DescendantId = transactionId},
                .Dummy = 0,
            });
        }

        auto attributes = FromProto(Request_.attributes());
        auto attributeKeys = attributes->ListKeys();
        for (const auto& attributeName : attributeKeys) {
            if (attributeName != "operation_type" &&
                attributeName != "operation_id" &&
                attributeName != "operation_title")
            {
                attributes->Remove(attributeName);
            }
        }

        if (Request_.has_title()) {
            attributes->Set("title", Request_.title());
        }

        auto createdTransaction = NRecords::TTransaction{
            .Key = {.TransactionId = transactionId},
            .AncestorIds = std::move(ancestorIds),
            .Attributes = NYson::ConvertToYsonString(attributes->ToMap()),
            .PrerequisiteTransactionIds = PrerequisiteTransactionIds_,
        };

        SequoiaTransaction_->WriteRow(createdTransaction);

        SequoiaTransaction_->AddTransactionAction(
            CellTagFromId(CoordinatorCellId_),
            MakeTransactionActionData(Request_));

        // NB: All of these transactions should be already locked.
        for (auto prerequisiteTransactionId : PrerequisiteTransactionIds_) {
            if (!IsSequoiaId(prerequisiteTransactionId)) {
                // One may use system transaction as prerequisite. Since system
                // transactions are not mirrored we shouldn't put any info about
                // them into Sequoia tables.

                // NB: Abort of such dependent transactions will be replicated
                // via Hive.
                continue;
            }

            SequoiaTransaction_->WriteRow(NRecords::TDependentTransaction{
                .Key = {
                    .TransactionId = prerequisiteTransactionId,
                    .DependentTransactionId = transactionId,
                },
                .Dummy = 0,
            });
        }

        // Fast path.
        if (ReplicateToCellTags_.empty()) {
            return VoidFuture;
        }

        // Another fast path.
        if (!ParentId_) {
            // Transaction hierarchy is trivial and coordinator is already knows
            // about replicas so we can use TSimpleTransactionReplicator here.
            TSimpleTransactionReplicator(SequoiaTransaction_.Get())
                .AddTransaction(std::move(createdTransaction))
                .AddCells(ReplicateToCellTags_)
                .Run();
            return VoidFuture;
        }

        return New<TTransactionReplicator>(
            SequoiaTransaction_,
            std::vector{std::optional(std::move(createdTransaction))},
            ReplicateToCellTags_,
            Invoker_)
            ->Run();
    }

    TFuture<std::vector<TTransactionId>> CheckParentAndGetParentAncestors(
        std::vector<std::optional<NRecords::TTransaction>>&& responses) const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(responses.size() == 1);

        if (!responses.front()) {
            ThrowNoSuchTransaction(ParentId_);
        }

        ValidateTransactionAncestors(*responses.front());

        auto ancestors = std::move(responses.front()->AncestorIds);
        ancestors.push_back(responses.front()->Key.TransactionId);

        return MakeFuture(ancestors);
    }

    TFuture<std::vector<TTransactionId>> LockParentAndCollectAncestors() const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (!ParentId_) {
            return MakeFuture<std::vector<TTransactionId>>({});
        }

        // Shared read lock prevents concurrent parent transaction commit or
        // abort but still allows to start another nested transaction
        // concurrently.
        SequoiaTransaction_->LockRow(
            NRecords::TTransactionKey{.TransactionId = ParentId_},
            ELockType::SharedStrong);

        const auto& idMapping = NRecords::TTransactionDescriptor::Get()->GetIdMapping();
        return SequoiaTransaction_->LookupRows<NRecords::TTransactionKey>(
            {{.TransactionId = ParentId_}},
            {*idMapping.TransactionId, *idMapping.AncestorIds})
            .ApplyUnique(BIND(
                &TStartCypressTransaction::CheckParentAndGetParentAncestors,
                MakeStrong(this))
                    .AsyncVia(Invoker_));
    }

    void ValidateAndLockPrerequisiteTransactions(
        std::vector<std::optional<NRecords::TTransaction>>&& records)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(PrerequisiteTransactionIds_.size() == records.size());

        for (int i = 0; i < std::ssize(records); ++i) {
            if (!records[i]) {
                ThrowPrerequisiteCheckFailedNoSuchTransaction(PrerequisiteTransactionIds_[i]);
            }
        }

        ValidateTransactionAncestors(records);

        for (const auto& record : records) {
            SequoiaTransaction_->LockRow(record->Key, ELockType::SharedStrong);
        }
    }

    TFuture<void> HandlePrerequisiteTransactions()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (PrerequisiteTransactionIds_.empty()) {
            return VoidFuture;
        }

        return SequoiaTransaction_->LookupRows(ToTransactionKeys(PrerequisiteTransactionIds_))
            .ApplyUnique(BIND(
                &TStartCypressTransaction::ValidateAndLockPrerequisiteTransactions,
                MakeStrong(this))
                    .AsyncVia(Invoker_));
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Collects all dependent transactions transitively and finds progenitor unique
//! dependent transactions.
/*!
 *  This class is used to implement transaction finishing: when transaction is
 *  committed or aborted, all its dependent (and nested) transactions are
 *  aborted too. To achieve this we have to collect all dependent transactions
 *  and find progenitor ones: it's sufficient to abort only subtree's root because
 *  it leads to abortion of all subtree.
 *
 *  "dependent_transactions" Sequoia table does not contains transitive closure
 *  of all dependent transactions (in opposite to "transaction_descendants")
 *  because there is no any sane bound for number of dependent transactions.
 *  So the collection of all dependent transactions is a bit non-trivial:
 *
 *  collectedTransactions -- set of fetched transactions;
 *  currentTransaction -- set of transaction IDs with unchecked dependent
 *      transactions and descendants;
 *
 *  collectedTransactions := {targetTransaction}
 *  currentTransaction := {targetTransaction.Id}
 *  while not currentTransaction.empty():
 *      nextTransaction :=
 *          select descendant_id
 *              from transaction_descendants
 *              where transaction_id in currentTransaction
 *          +
 *          select dependent_transactions_id
 *              from dependent_transactions
 *              where transaction_id in currentTransaction
 *
 *      currentTransaction := {}
 *      for transaction in nextTransaction:
 *          if transaction not in collectedTransactions:
 *              currentTransaction.add(transaction.Id)
 *              collectedTransactions.add(transaction)
 *  return collectedTransactions
 */
class TDependentTransactionCollector
    : public TRefCounted
{
public:
    TDependentTransactionCollector(
        ISequoiaTransactionPtr sequoiaTransaction,
        NRecords::TTransaction targetTransaction,
        IInvokerPtr invoker)
        : SequoiaTransaction_(std::move(sequoiaTransaction))
        , TargetTransaction_(std::move(targetTransaction))
        , Invoker_(std::move(invoker))
    { }

    struct TResult
    {
        // Contains progenitor dependent transactions.
        std::vector<TTransactionId> DependentTransactionSubtreeRoots;
        THashMap<TTransactionId, NRecords::TTransaction> Transactions;

        // NB: Despite we fetch records from "dependent_transactions" and
        // "transaction_descendants" we don't return them since they are not
        // required to handle transaction finish: record from "transactions"
        // table contains "prerequisite_transaction_ids" and "ancestor_ids" and
        // it is enough to clean up "dependent_transactions" and
        // "transaction_descendants".
    };

    TFuture<TResult> Run()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        CollectedTransactions_[TargetTransaction_.Key.TransactionId] = TargetTransaction_;
        CurrentTransactions_.push_back(TargetTransaction_.Key.TransactionId);

        return CollectMoreTransactions()
            .Apply(BIND(&TDependentTransactionCollector::MakeResult, MakeStrong(this))
                .AsyncVia(Invoker_));
    }

private:
    const ISequoiaTransactionPtr SequoiaTransaction_;
    const NRecords::TTransaction TargetTransaction_;
    const IInvokerPtr Invoker_;

    // This state is shared between different callback invocations.
    THashMap<TTransactionId, NRecords::TTransaction> CollectedTransactions_;
    std::vector<TTransactionId> CurrentTransactions_;

    TResult MakeResult() const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        std::vector<TTransactionId> roots;

        for (const auto& [transactionId, record] : CollectedTransactions_) {
            if (transactionId == TargetTransaction_.Key.TransactionId) {
                continue;
            }

            // NB: Checking transaction's parent is sufficient: if some ancestor
            // "A" of transaction "T" is collected then all its descendants are
            // collected too; so one of these descendants is parent of "T".
            if (record.AncestorIds.empty() ||
                !CollectedTransactions_.contains(record.AncestorIds.back()))
            {
                roots.push_back(transactionId);
            }
        }

        return {
            .DependentTransactionSubtreeRoots = std::move(roots),
            .Transactions = CollectedTransactions_,
        };
    }

    TFuture<void> CollectMoreTransactions()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (CurrentTransactions_.empty()) {
            return VoidFuture;
        }

        return FetchNextTransaction()
            .ApplyUnique(BIND(
                &TDependentTransactionCollector::ProcessNextTransaction,
                MakeStrong(this))
                .AsyncVia(Invoker_))
            .Apply(
                BIND(&TDependentTransactionCollector::CollectMoreTransactions, MakeStrong(this))
                    .AsyncVia(Invoker_));
    }

    void ProcessNextTransaction(std::vector<std::optional<NRecords::TTransaction>>&& records)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        ValidateAllTransactionsExist(records);
        ValidateTransactionAncestors(records);

        CurrentTransactions_.clear();
        CurrentTransactions_.reserve(records.size());
        for (auto& record : records) {
            auto transactionId = record->Key.TransactionId;
            if (CollectedTransactions_.emplace(transactionId, std::move(*record)).second) {
                CurrentTransactions_.push_back(transactionId);
            }
        }
    }

    TFuture<std::vector<std::optional<NRecords::TTransaction>>> FetchNextTransaction() const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto condition = BuildSelectByTransactionIds(CurrentTransactions_);
        auto descendentTransaction = SequoiaTransaction_
            ->SelectRows<NRecords::TTransactionDescendant>({condition});

        auto dependentTransaction = SequoiaTransaction_
            ->SelectRows<NRecords::TDependentTransaction>({condition});

        return AllSucceeded(
            std::vector{descendentTransaction.AsVoid(), dependentTransaction.AsVoid()})
                .Apply(BIND([
                    this,
                    this_ = MakeStrong(this),
                    descendentTransactionFuture = descendentTransaction,
                    dependentTransactionFuture = dependentTransaction
                ] {
                    YT_ASSERT_INVOKER_AFFINITY(Invoker_);
                    YT_VERIFY(descendentTransactionFuture.IsSet());
                    YT_VERIFY(dependentTransactionFuture.IsSet());

                    // NB: AllSucceeded() guarantees that all futures contain values.
                    const auto& descendentTransaction = descendentTransactionFuture.Get().Value();
                    const auto& dependentTransaction = dependentTransactionFuture.Get().Value();

                    if (descendentTransaction.empty() && dependentTransaction.empty()) {
                        return MakeFuture(std::vector<std::optional<NRecords::TTransaction>>{});
                    }

                    std::vector<NRecords::TTransactionKey> keys;
                    keys.reserve(descendentTransaction.size() + dependentTransaction.size());

                    for (const auto& record : dependentTransaction) {
                        auto id = record.Key.DependentTransactionId;
                        if (!CollectedTransactions_.contains(id)) {
                            keys.push_back(NRecords::TTransactionKey{.TransactionId = id});
                        }
                    }

                    for (const auto& record : descendentTransaction) {
                        auto id = record.Key.DescendantId;
                        if (!CollectedTransactions_.contains(id)) {
                            keys.push_back(NRecords::TTransactionKey{.TransactionId = id});
                        }
                    }

                    return SequoiaTransaction_->LookupRows(keys);
                })
                    .AsyncVia(Invoker_));
    }
};

//! This class is responsible for finishing transactions: commit and abort are
//! handled in a similar way.
/*!
 *  When transaction is finished (because of commit or abort) every descendant
 *  and dependent transaction has to be aborted. On transaction coordinator it's
 *  handled in commit/abort mutation, but we still need to clean Sequoia tables
 *  and replicate abort mutations to all participants.
 *  1. Fetch target transaction (and validate it);
 *  2. Fetch all descendant and dependent transactions (transitively);
 *  3. Find all subtrees' roots (target tx + all dependent txs);
 *  4. For each transaction to finish:
 *     4.1. Execute abort tx action on transaction coordinator;
 *     4.2. Execute abort tx action on every participant;
 *     4.3. Remove all replicas from "transaction_replicas" table;
 *     4.4. Remove (prerequisite_transaction_id, transaction_id) from
 *          "dependent_transactions" table;
 *     4.5. Remove (ancestor_id, transaction_id) for every its ancestor from
 *          "transaction_descendants" table;
 *     4.6. Remove transaction from "transactions" table;
 *     4.7. Merge/remove branches in resolve tables:
 *           - node_id_to_path
 *           - path_to_node_id
 *           - child_node
 */
class TFinishCypressTransaction
    : public TSequoiaMutation<void, ESequoiaTransactionType::CypressTransactionMirroring>
{
protected:
    const TTransactionId TransactionId_;
    const TAuthenticationIdentity AuthenticationIdentity_;

    TFinishCypressTransaction(
        ISequoiaClientPtr sequoiaClient,
        TCellId cypressTransactionCoordinatorCellId,
        TStringBuf description,
        TTransactionId transactionId,
        TAuthenticationIdentity authenticationIdentity,
        IInvokerPtr invoker,
        TLogger logger)
        : TSequoiaMutation(
            std::move(sequoiaClient),
            cypressTransactionCoordinatorCellId,
            description,
            std::move(invoker),
            std::move(logger))
        , TransactionId_(transactionId)
        , AuthenticationIdentity_(std::move(authenticationIdentity))
    { }

    TFuture<void> ApplyAndCommitSequoiaTransaction() final
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        return FetchTargetTransaction()
            .ApplyUnique(BIND(
                &TFinishCypressTransaction::ValidateAndFinishTargetTransaction,
                MakeStrong(this))
                    .AsyncVia(Invoker_))
            .Apply(BIND(
                &TFinishCypressTransaction::CommitSequoiaTransaction,
                MakeStrong(this))
                    .AsyncVia(Invoker_));
    }

    // Returns |false| if transaction shouldn't be processed (e.g. force abort
    // of non-existent transaction should not be treated as an error).
    virtual bool TransactionFinishIsNoop(const std::optional<NRecords::TTransaction>& record) = 0;

    // Register transaction actions for Sequoia transaction.
    virtual void FinishTargetTransactionOnMaster(
        TRange<NRecords::TTransactionReplica> replicas) = 0;

    void AbortTransactionOnParticipants(TRange<NRecords::TTransactionReplica> replicas)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (replicas.empty()) {
            // This transaction is not replicated to anywhere.
            return;
        }

        NTransactionServer::NProto::TReqAbortTransaction request;
        ToProto(request.mutable_transaction_id(), replicas.Front().Key.TransactionId);
        request.set_force(true);
        auto transactionAction = MakeTransactionActionData(request);
        for (const auto& replica : replicas) {
            SequoiaTransaction_->AddTransactionAction(replica.Key.CellTag, transactionAction);
        }
    }

    virtual TCypressTransactionChangesProcessorPtr CreateTransactionChangesProcessor(
        const THashMap<TTransactionId, NRecords::TTransaction>& transactions) = 0;

private:
    TFuture<void> DoFinishTransactions(TDependentTransactionCollector::TResult&& transactionInfos)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto handleResolveTablesFuture = CreateTransactionChangesProcessor(transactionInfos.Transactions)
            ->Run();

        auto handleTransactionReplicasFuture = FetchReplicas(transactionInfos.Transactions)
            .ApplyUnique(BIND(
                &TFinishCypressTransaction::OnReplicasFetched,
                MakeStrong(this),
                std::move(transactionInfos))
                    .AsyncVia(Invoker_));

        return AllSucceeded<void>({
            std::move(handleTransactionReplicasFuture),
            std::move(handleResolveTablesFuture),
        });
    }

    void OnReplicasFetched(
        TDependentTransactionCollector::TResult transactionsInfo,
        std::vector<NRecords::TTransactionReplica>&& replicas)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        // ORDER BY expression cannot help us here since IDs are stored as
        // strings and string and ID orders are different.
        SortBy(replicas, [] (const auto& record) {
            return record.Key.TransactionId;
        });

        FinishTargetTransactionOnMaster(FindReplicas(replicas, TransactionId_));

        // On transaction coordinator dependent transaction aborts are caused by
        // target transaction finishing. However, this abort has to be
        // replicated to other participants.
        for (auto transactionId : transactionsInfo.DependentTransactionSubtreeRoots) {
            AbortTransactionOnParticipants(FindReplicas(replicas, transactionId));
        }

        // Remove transactions from Sequoia tables.

        // TODO(kvk1920): remove branches.

        // "transaction_replicas"
        for (const auto& replica : replicas) {
            SequoiaTransaction_->DeleteRow(replica.Key);
        }

        for (const auto& [transactionId, transactionInfo] : transactionsInfo.Transactions) {
            // "dependent_transactions"
            for (auto prerequisiteTransactionId : transactionInfo.PrerequisiteTransactionIds) {
                SequoiaTransaction_->DeleteRow(NRecords::TDependentTransactionKey{
                    .TransactionId = prerequisiteTransactionId,
                    .DependentTransactionId = transactionId,
                });
            }
            // "transaction_descendants"
            for (auto ancestorId : transactionInfo.AncestorIds) {
                SequoiaTransaction_->DeleteRow(NRecords::TTransactionDescendantKey{
                    .TransactionId = ancestorId,
                    .DescendantId = transactionId,
                });
            }
            // "transactions"
            SequoiaTransaction_->DeleteRow(transactionInfo.Key);
        }
    }

    static TRange<NRecords::TTransactionReplica> FindReplicas(
        const std::vector<NRecords::TTransactionReplica>& replicas,
        TTransactionId transactionId)
    {
        static constexpr auto idFromReplica = [] (const NRecords::TTransactionReplica& record) {
            return record.Key.TransactionId;
        };

        auto begin = LowerBoundBy(replicas.begin(), replicas.end(), transactionId, idFromReplica);
        auto end = UpperBoundBy(replicas.begin(), replicas.end(), transactionId, idFromReplica);

        return TRange(replicas).Slice(begin - replicas.begin(), end - replicas.begin());
    }

    TFuture<std::vector<std::optional<NRecords::TTransaction>>> FetchTargetTransaction()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return SequoiaTransaction_->LookupRows<NRecords::TTransactionKey>(
            {{.TransactionId = TransactionId_}});
    }

    TFuture<void> ValidateAndFinishTargetTransaction(
        std::vector<std::optional<NRecords::TTransaction>>&& target)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(target.size() == 1);

        // Fast path.
        if (TransactionFinishIsNoop(target.front())) {
            return VoidFuture;
        }

        // Case of absence target transaction is handled in
        // TransactionFinishIsNoop().
        YT_VERIFY(target.front());

        ValidateTransactionAncestors(*target.front());

        return CollectDependentAndNestedTransactionsAndFinishThem(std::move(*target.front()));
    }

    TFuture<void> CollectDependentAndNestedTransactionsAndFinishThem(
        NRecords::TTransaction targetTransaction)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        return New<TDependentTransactionCollector>(
            SequoiaTransaction_,
            std::move(targetTransaction),
            Invoker_)
                ->Run()
                .ApplyUnique(
                    BIND(&TFinishCypressTransaction::DoFinishTransactions, MakeStrong(this))
                        .AsyncVia(Invoker_));
    }

    TFuture<std::vector<NRecords::TTransactionReplica>> FetchReplicas(
        const THashMap<TTransactionId, NRecords::TTransaction>& transactions)
    {
        return SequoiaTransaction_->SelectRows<NRecords::TTransactionReplica>(
            BuildSelectByTransactionIds(transactions));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAbortCypressTransaction
    : public TFinishCypressTransaction
{
public:
    TAbortCypressTransaction(
        ISequoiaClientPtr sequoiaClient,
        TCellId cypressTransactionCoordinatorCellId,
        TTransactionId transactionId,
        bool force,
        NRpc::TAuthenticationIdentity authenticationIdentity,
        IInvokerPtr invoker,
        TLogger logger)
        : TFinishCypressTransaction(
            std::move(sequoiaClient),
            cypressTransactionCoordinatorCellId,
            "abort",
            transactionId,
            std::move(authenticationIdentity),
            std::move(invoker),
            std::move(logger))
        , Force_(force)
    { }

    TAbortCypressTransaction(
        ISequoiaClientPtr sequoiaClient,
        TCellId cypressTransactionCoordinatorCellId,
        TTransactionId transactionId,
        IInvokerPtr invoker,
        TLogger logger)
        : TFinishCypressTransaction(
            std::move(sequoiaClient),
            cypressTransactionCoordinatorCellId,
            "abort expired",
            transactionId,
            GetRootAuthenticationIdentity(),
            std::move(invoker),
            std::move(logger))
        , Force_(false)
    { }

protected:
    TCypressTransactionChangesProcessorPtr CreateTransactionChangesProcessor(
        const THashMap<TTransactionId, NRecords::TTransaction>& transactions) override
    {
        return New<TCypressTransactionChangesProcessor>(
            SequoiaTransaction_,
            transactions,
            /*committedTransaction*/ std::nullopt,
            Invoker_);
    }

    bool TransactionFinishIsNoop(const std::optional<NRecords::TTransaction>& record) override
    {
        if (record) {
            return false;
        }

        if (Force_) {
            return true;
        }

        ThrowNoSuchTransaction(TransactionId_);
    }

    void FinishTargetTransactionOnMaster(TRange<NRecords::TTransactionReplica> replicas) override
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        SequoiaTransaction_->AddTransactionAction(
            CellTagFromId(CoordinatorCellId_),
            MakeTransactionActionData(BuildAbortCypressTransactionRequest(
                TransactionId_,
                Force_,
                AuthenticationIdentity_)));

        AbortTransactionOnParticipants(replicas);
    }

private:
    const bool Force_;
};

////////////////////////////////////////////////////////////////////////////////

class TCommitCypressTransaction
    : public TFinishCypressTransaction
{
public:
    TCommitCypressTransaction(
        ISequoiaClientPtr sequoiaClient,
        TCellId cypressTransactionCoordinatorCellId,
        TTransactionId transactionId,
        std::vector<TTransactionId> prerequisiteTransactionIds,
        TTimestamp commitTimestamp,
        NRpc::TAuthenticationIdentity authenticationIdentity,
        IInvokerPtr invoker,
        TLogger logger)
        : TFinishCypressTransaction(
            std::move(sequoiaClient),
            cypressTransactionCoordinatorCellId,
            "commit",
            transactionId,
            std::move(authenticationIdentity),
            std::move(invoker),
            std::move(logger))
        , CommitTimestamp_(commitTimestamp)
        , PrerequisiteTransactionIds_(std::move(prerequisiteTransactionIds))
    { }

protected:
    TCypressTransactionChangesProcessorPtr CreateTransactionChangesProcessor(
        const THashMap<TTransactionId, NRecords::TTransaction>& transactions) override
    {
        return New<TCypressTransactionChangesProcessor>(
            SequoiaTransaction_,
            transactions,
            transactions.at(TransactionId_),
            Invoker_);
    }

    bool TransactionFinishIsNoop(const std::optional<NRecords::TTransaction>& record) override
    {
        if (record) {
            return false;
        }

        ThrowNoSuchTransaction(TransactionId_);
    }

    void FinishTargetTransactionOnMaster(
        TRange<NRecords::TTransactionReplica> replicas) override
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        SequoiaTransaction_->AddTransactionAction(
            CellTagFromId(CoordinatorCellId_),
            MakeTransactionActionData(BuildCommitCypressTransactionRequest(
                TransactionId_,
                CommitTimestamp_,
                PrerequisiteTransactionIds_,
                AuthenticationIdentity_)));

        CommitTransactionOnParticipants(replicas);
    }

private:
    const TTimestamp CommitTimestamp_;
    const std::vector<TTransactionId> PrerequisiteTransactionIds_;

    void CommitTransactionOnParticipants(TRange<NRecords::TTransactionReplica> replicas)
    {
        if (!replicas.empty()) {
            NTransactionServer::NProto::TReqCommitTransaction req;
            ToProto(req.mutable_transaction_id(), TransactionId_);
            auto transactionActionData = MakeTransactionActionData(req);

            for (const auto& replica : replicas) {
                SequoiaTransaction_->AddTransactionAction(
                    replica.Key.CellTag,
                    transactionActionData);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Replicates given transactions from given cell to current cell. Note that
//! non-existent transaction is considered replicated to everywhere.
/*!
 *  For every request transaction which is not replicated to current cell:
 *  1. Lock row in table "transactions";
 *  2. Modify "transaction_replicas" table;
 *  3. Modify Tx coordinator's state;
 *  4. Modify current cell's state.
 */
class TReplicateCypressTransactions
    : public TSequoiaMutation<void, ESequoiaTransactionType::CypressTransactionMirroring>
{
private:
    using TThis = TReplicateCypressTransactions;

protected:
    TReplicateCypressTransactions(
        ISequoiaClientPtr sequoiaClient,
        TCellId hintCoordinatorCellId,
        TTransactionReplicationDestinationCellTagList destinationCellTags,
        std::vector<TTransactionId> transactionIds,
        IInvokerPtr invoker,
        TLogger logger)
        : TSequoiaMutation(
            std::move(sequoiaClient),
            hintCoordinatorCellId,
            "replicate Cypress",
            std::move(invoker),
            std::move(logger))
        , TransactionIds_(std::move(transactionIds))
        , DestinationCellTags_(std::move(destinationCellTags))
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
    }

    TFuture<void> ApplyAndCommitSequoiaTransaction() override
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        return SequoiaTransaction_->LookupRows(ToTransactionKeys(TransactionIds_))
            .ApplyUnique(BIND(&TThis::ReplicateTransactions, MakeStrong(this))
                .AsyncVia(Invoker_))
            .Apply(
                BIND(&TThis::CommitSequoiaTransaction, MakeStrong(this))
                    .AsyncVia(Invoker_));
    }

private:
    const std::vector<TTransactionId> TransactionIds_;
    const TTransactionReplicationDestinationCellTagList DestinationCellTags_;

    TFuture<void> ReplicateTransactions(
        std::vector<std::optional<NRecords::TTransaction>>&& transactions)
    {
        // NB: "no such transaction" shouldn't be thrown here. Instead we make
        // it look like everything is replicated and request under transaction
        // will try to find transaction and get "no such transaction" error.
        transactions.erase(
            std::remove(transactions.begin(), transactions.end(), std::nullopt),
            transactions.end());

        ValidateTransactionAncestors(transactions);

        if (transactions.empty()) {
            return VoidFuture;
        }

        // |TTransactionReplicator| handles transactions hierarchy to allow us
        // to avoid replicating the same tx twice.
        auto replicator = New<TTransactionReplicator>(
            SequoiaTransaction_,
            std::move(transactions),
            DestinationCellTags_,
            Invoker_);

        // NB: replication of transaction T with ancestors (P1, P2, ...) causes
        // replication of these ancestors too. So we don't need to send
        // replication requests for (P1, P2, ...).
        replicator->IterateOverInnermostTransactionGroupedByCoordinator(
            [&] (TRange<std::optional<NRecords::TTransaction>> group) {
                YT_VERIFY(!group.empty());

                auto coordinatorCellTag = CellTagFromId(group.Front()->Key.TransactionId);
                NTransactionServer::NProto::TReqMarkCypressTransactionsReplicatedToCells action;

                action.mutable_destination_cell_tags()->Reserve(DestinationCellTags_.size());
                for (auto cellTag : DestinationCellTags_) {
                    if (coordinatorCellTag != cellTag) {
                        action.add_destination_cell_tags(ToProto(cellTag));
                    }
                }

                action.mutable_transaction_ids()->Reserve(group.size());

                for (const auto& transaction : group) {
                    // To prevent concurrent commit/abort.
                    SequoiaTransaction_->LockRow(transaction->Key, ELockType::SharedStrong);

                    ToProto(action.add_transaction_ids(), transaction->Key.TransactionId);
                }

                SequoiaTransaction_->AddTransactionAction(
                    coordinatorCellTag,
                    MakeTransactionActionData(action));
            });

        return replicator->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<TTransactionId> StartCypressTransaction(
    ISequoiaClientPtr sequoiaClient,
    TCellId cypressTransactionCoordinatorCellId,
    NCypressTransactionClient::NProto::TReqStartTransaction* request,
    TAuthenticationIdentity authenticationIdentity,
    IInvokerPtr invoker,
    TLogger logger)
{
    return New<TStartCypressTransaction>(
        std::move(sequoiaClient),
        cypressTransactionCoordinatorCellId,
        std::move(*request),
        std::move(authenticationIdentity),
        std::move(invoker),
        std::move(logger))
        ->Apply();
}

TFuture<void> AbortCypressTransaction(
    ISequoiaClientPtr sequoiaClient,
    TCellId cypressTransactionCoordinatorCellId,
    TTransactionId transactionId,
    bool force,
    TAuthenticationIdentity authenticationIdentity,
    IInvokerPtr invoker,
    TLogger logger)
{
    return New<TAbortCypressTransaction>(
        std::move(sequoiaClient),
        cypressTransactionCoordinatorCellId,
        transactionId,
        force,
        std::move(authenticationIdentity),
        std::move(invoker),
        std::move(logger))
        ->Apply();
}

TFuture<void> AbortExpiredCypressTransaction(
    ISequoiaClientPtr sequoiaClient,
    TCellId cypressTransactionCoordinatorCellId,
    TTransactionId transactionId,
    IInvokerPtr invoker,
    TLogger logger)
{
    return New<TAbortCypressTransaction>(
        std::move(sequoiaClient),
        cypressTransactionCoordinatorCellId,
        transactionId,
        std::move(invoker),
        std::move(logger))
        ->Apply();
}

TFuture<void> CommitCypressTransaction(
    ISequoiaClientPtr sequoiaClient,
    TCellId cypressTransactionCoordinatorCellId,
    TTransactionId transactionId,
    std::vector<NTransactionClient::TTransactionId> prerequisiteTransactionIds,
    TTimestamp commitTimestamp,
    TAuthenticationIdentity authenticationIdentity,
    IInvokerPtr invoker,
    TLogger logger)
{
    return New<TCommitCypressTransaction>(
        std::move(sequoiaClient),
        cypressTransactionCoordinatorCellId,
        transactionId,
        std::move(prerequisiteTransactionIds),
        commitTimestamp,
        std::move(authenticationIdentity),
        std::move(invoker),
        std::move(logger))
        ->Apply();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> ReplicateCypressTransactions(
    ISequoiaClientPtr sequoiaClient,
    std::vector<TTransactionId> transactionIds,
    TTransactionReplicationDestinationCellTagList destinationCellTags,
    TCellId hintCoordinatorCellId,
    IInvokerPtr invoker,
    TLogger logger)
{
    // Fast path.
    if (transactionIds.empty()) {
        return VoidFuture;
    }

    return New<TReplicateCypressTransactions>(
        std::move(sequoiaClient),
        hintCoordinatorCellId,
        std::move(destinationCellTags),
        std::move(transactionIds),
        std::move(invoker),
        std::move(logger))
        ->Apply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
